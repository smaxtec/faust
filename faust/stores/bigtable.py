"""BigTable storage."""
import logging
import time
import traceback
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    Optional,
    Set,
    Tuple,
    Union,
)

from google.cloud.bigtable import column_family
from google.cloud.bigtable.client import Client
from google.cloud.bigtable.instance import Instance
from google.cloud.bigtable.row import DirectRow
from google.cloud.bigtable.row_data import DEFAULT_RETRY_READ_ROWS
from google.cloud.bigtable.row_filters import CellsColumnLimitFilter
from google.cloud.bigtable.row_set import RowSet
from google.cloud.bigtable.table import Table
from mode.utils.collections import LRUCache
from yarl import URL

from faust.stores import base
from faust.streams import current_event
from faust.types import TP, AppT, CollectionT, EventT


def get_current_partition():
    event = current_event()
    assert event is not None
    return event.message.partition


class BigtableMutationBuffer:
    rows: Dict[bytes, Tuple[DirectRow, Optional[bytes]]]
    mutation_limit: int

    def __init__(
        self, bigtable_table: Table, mutation_freq: int, mutation_limit: int
    ) -> None:

        self.mutation_freq: int = mutation_freq
        self.last_flush = int(time.time())  # set to now
        self.mutation_limit: int = mutation_limit
        self.bigtable_table: Table = bigtable_table
        self.log = logging.getLogger(self.__class__.__name__)
        self.rows = {}

    def flush(self) -> None:
        mutated_rows = []
        rows_to_flush = self.rows.copy().values()
        for row, val in rows_to_flush:
            if val is None:
                row.delete()
            else:
                row.set_cell(
                    "FaustColumnFamily",
                    "DATA",
                    val,
                )
            mutated_rows.append(row)
        response = self.bigtable_table.mutate_rows(mutated_rows)
        for (status, row) in zip(response, rows_to_flush):
            if status.code != 0:
                self.log.error(
                    "BigTableStore: BigtableMutationBuffer, "
                    f"Row {row[0].row_key} failed to write"
                )
            else:
                # Remove only rows that were successfully written
                self.rows.pop(row[0].row_key, None)

        self.last_flush = int(time.time())  # set to now

    def check_flush(self) -> bool:
        limit_reached = len(self.rows) >= self.mutation_limit
        time_exceeded = self.last_flush + self.mutation_freq < int(time.time())
        return limit_reached or time_exceeded

    def submit(self, row: DirectRow, value: Optional[bytes] = None):
        self.rows[row.row_key] = row, value


class BigtableStartupCache:
    """
    This is a dictionary which is only filled once, after that, every
    successful access to a key, will remove it.
    """

    def __init__(self, ttl: Optional[int]) -> None:
        self.log = logging.getLogger(self.__class__.__name__)
        self.data: Dict = {}
        self.ttl = ttl
        self.ttl_over = False
        self.init_ts = int(time.time())

    def __len__(self):
        return len(self.data)

    def __getitem__(self, key):
        if self.ttl is not None:
            res = self.data[key]
            self._maybe_ttl_clear()
            return res

    def __setitem__(self, key, value) -> None:
        if value is not None:
            self._maybe_ttl_clear()
            if self.ttl is not None:
                self.data[key] = value

    def __delitem__(self, key):
        self.data.pop(key, None)

    def _maybe_ttl_clear(self):
        if self.ttl is not None:
            now = int(time.time())
            if now > self.init_ts + self.ttl:
                self.data = {}
                self.ttl = None
                self.log.info(
                    "BigTableStore: Cleard startupcache because TTL is over"
                )

    def keys(self):
        return self.data.keys()


class BigTableKeyCache:
    _keys: Set[bytes] = set()

    def add(self, key: bytes):
        self._keys.add(key)

    def discard(self, key: bytes):
        self._keys.discard(key)

    def exists(self, key: bytes) -> bool:
        return key in self._keys


def _register_partition(func):
    def inner(self, bt_key: bytes, *args):
        partition = bt_key[0]
        self._fill_caches({partition})
        return func(self, bt_key, *args)

    return inner


class BigTableCacheManager:
    _partition_cache: LRUCache[bytes, int]
    _value_cache: Optional[
        Union[LRUCache[bytes, Union[bytes, None]], BigtableStartupCache]
    ]
    _mutation_buffer: Optional[BigtableMutationBuffer]
    _key_cache: Optional[BigTableKeyCache]

    def __init__(self, app, options: Dict, bt_table: Table) -> None:
        self.log = logging.getLogger(__name__)
        self._registered_partitions = set()
        self.bt_table = bt_table
        self._partition_cache = LRUCache(limit=app.conf.table_key_index_size)
        self._init_value_cache(options)
        self._init_key_cache(options)
        self._init_mutation_buffer(options, bt_table)

    def _fill_caches(self, partitions: Set[int]):

        partitions = partitions.difference(self._registered_partitions)
        if len(partitions) == 0:
            return  # Nothing todo
        start = time.time()
        row_set = RowSet()
        for p in partitions:
            row_set.add_row_range_with_prefix(chr(p))

        for row in self.bt_table.read_rows(
            row_set=row_set,
            filter_=CellsColumnLimitFilter(1),
            retry=DEFAULT_RETRY_READ_ROWS.with_deadline(
                10 * 60
            ),  # High deadline cause slow
        ):
            if isinstance(self._value_cache, BigtableStartupCache):
                row_val = BigTableStore.bigtable_exrtact_row_data(row)
                self._value_cache.data[row.row_key] = row_val
            if self._key_cache is not None:
                self._key_cache.add(row.row_key)
        self._registered_partitions.update(partitions)
        td = time.time() - start
        self.log.info(
            f"BigTableStore: filled cache for {self.bt_table.table_id}:"
            f"{partitions=} in {td}s"
        )

    @_register_partition
    def get(
        self, bt_key: bytes
    ) -> Tuple[Optional[DirectRow], Optional[bytes]]:
        row = None
        value = None
        if self._mutation_buffer is not None:
            row, value = self._mutation_buffer.rows.get(bt_key, (None, None))
            if row is not None:
                return row, value
        if self._value_cache is not None:
            if bt_key in self._value_cache.keys():
                value = self._value_cache[bt_key]
        return row, value

    def get_partition(self, user_key: bytes) -> int:
        return self._partition_cache[user_key]

    def set_partition(self, user_key: bytes, partition: int):
        self._partition_cache[user_key] = partition

    def get_mutation_buffer(self) -> Optional[BigtableMutationBuffer]:
        return self._mutation_buffer

    @_register_partition
    def contains(self, bt_key: bytes) -> Optional[bool]:
        """
        If we return None here, this means, that no assumption
        about the current key can be made.
        """
        if self._key_cache is not None:
            return self._key_cache.exists(bt_key)
        if (
            isinstance(self._value_cache, BigtableStartupCache)
            and not self._value_cache.ttl_over
        ):
            return bt_key in self._value_cache.keys()
        if self._mutation_buffer is not None:
            if bt_key in self._mutation_buffer.rows.keys():
                row, value = self._mutation_buffer.rows[bt_key]
                if row is not None and value is not None:
                    return True
                elif row is not None and value is None:
                    return False
        return None

    def contains_any(self, key_set: Set[bytes]) -> Optional[bool]:
        partitions = {k[0] for k in key_set}
        self._fill_caches(partitions)
        if self._key_cache is not None:
            return not self._key_cache._keys.isdisjoint(key_set)
        if (
            isinstance(self._value_cache, BigtableStartupCache)
            and not self._value_cache.ttl_over
        ):
            return not self._value_cache.keys().isdisjoint(key_set)

        if self._mutation_buffer is not None:
            keys_in_buffer = key_set.intersection(self._mutation_buffer.rows.keys())
            if len(keys_in_buffer) == 1:
                k = keys_in_buffer.pop()
                row, value = self._mutation_buffer.rows[k]
                if row is not None and value is not None:
                    return True
                elif row is not None and value is None:
                    return False
        # No assumption possible
        return None

    def get_key_iterable_if_exists(self) -> Optional[Iterable]:
        if (
            self._key_cache is not None
            and len(self._registered_partitions) > 0
        ):
            return self._key_cache._keys
        else:
            return None

    @_register_partition
    def set(
        self, bt_key: bytes, row: DirectRow, value: Optional[bytes]
    ) -> None:
        if self._value_cache is not None:
            self._value_cache[bt_key] = value
        if self._mutation_buffer is not None:
            self._mutation_buffer.submit(row, value)
        if self._key_cache is not None:
            self._key_cache.add(bt_key)

    def delete(self, bt_key: bytes, row: DirectRow) -> None:
        user_key = bt_key[1:]
        self._partition_cache.pop(user_key, None)
        if self._mutation_buffer is not None:
            self._mutation_buffer.submit(row, None)
        if self._value_cache is not None:
            del self._value_cache[bt_key]
        if self._key_cache is not None:
            self._key_cache.discard(bt_key)

    def _init_value_cache(
        self, options
    ) -> Optional[Union[LRUCache, BigtableStartupCache]]:
        value_cache_type = options.get(
            BigTableStore.VALUE_CACHE_TYPE_KEY, None
        )

        if value_cache_type == "startup":
            startup_cache_ttl = options.get(
                BigTableStore.STARTUPCACHE_TTL_KEY, None
            )
            self._value_cache = BigtableStartupCache(startup_cache_ttl)
        elif value_cache_type == "forever":
            value_cache_size = options.get(
                BigTableStore.VALUE_CACHE_SIZE_KEY, 1_000
            )

            self._value_cache = LRUCache(limit=value_cache_size)
        else:
            self._value_cache = None

    def _init_key_cache(self, options: Dict):
        key_cache_enabled = options.get(
            BigTableStore.KEY_CACHE_ENABLE_KEY, False
        )
        if key_cache_enabled:
            self._key_cache = BigTableKeyCache()
        else:
            self._key_cache = None

    def _init_mutation_buffer(self, options: Dict, bt_table: Table):
        mutation_buffer_enabled = options.get(
            BigTableStore.BT_ENABLE_MUTATION_BUFFER_KEY, False
        )
        if mutation_buffer_enabled:
            limit = options.get(
                BigTableStore.BT_MUTATION_BUFFER_LIMIT_KEY, 100
            )
            freq = options.get(
                BigTableStore.BT_MUTATION_BUFFER_FREQ_KEY, 30 * 60
            )
            self._mutation_buffer = BigtableMutationBuffer(
                bt_table, freq, limit
            )
        else:
            self._mutation_buffer = None


class BigTableStore(base.SerializedStore):
    """Bigtable table storage."""

    client: Client
    instance: Instance
    bt_table: Table
    _cache: BigTableCacheManager

    BT_COLUMN_NAME_KEY = "bt_column_name_key"
    BT_ENABLE_MUTATION_BUFFER_KEY = "bt_enable_mutation_buffer_key"
    BT_INSTANCE_KEY = "bt_instance_key"
    BT_MUTATION_BUFFER_FREQ_KEY = "bt_mutation_buffer_freq_key"
    BT_MUTATION_BUFFER_LIMIT_KEY = "bt_mutation_buffer_limit_key"
    BT_OFFSET_KEY_PREFIX = "bt_offset_key_prefix"
    BT_PROJECT_KEY = "bt_project_key"
    BT_READ_ROWS_BORDERS_KEY = "bt_read_rows_borders_key"
    BT_ROW_FILTERS_KEY = "bt_row_filter_key"
    BT_TABLE_NAME_GENERATOR_KEY = "bt_table_name_generator_key"
    KEY_CACHE_ENABLE_KEY = "key_cache_enable_key"
    STARTUPCACHE_TTL_KEY = "startupcache_ttl_key"
    VALUE_CACHE_SIZE_KEY = "value_cache_size_key"
    VALUE_CACHE_TYPE_KEY = "value_cache_type_key"

    def __init__(
        self,
        url: Union[str, URL],
        app: AppT,
        table: CollectionT,
        options: Dict[str, Any],
        **kwargs: Any,
    ) -> None:
        self._set_options(options)
        self._tracked_key = b'635b961bac0a961c562f61bf'
        try:
            self._bigtable_setup(table, options)
            self._cache = BigTableCacheManager(app, options, self.bt_table)
        except Exception as ex:
            logging.getLogger(__name__).error(f"Error in Bigtable init {ex}")
            raise ex
        super().__init__(url, app, table, **kwargs)

    def _set_options(self, options) -> None:
        self.table_name_generator = options.get(
            BigTableStore.BT_TABLE_NAME_GENERATOR_KEY, lambda t: t.name
        )
        self.bt_start_key, self.bt_end_key = options.get(
            BigTableStore.BT_READ_ROWS_BORDERS_KEY, [b"", b""]
        )
        self.column_name = options.get(
            BigTableStore.BT_COLUMN_NAME_KEY, "DATA"
        )
        self.row_filter = options.get(
            BigTableStore.BT_ROW_FILTERS_KEY, CellsColumnLimitFilter(1)
        )
        self.offset_key_prefix = options.get(
            BigTableStore.BT_OFFSET_KEY_PREFIX, "offset_partitiion:"
        )

    def _bigtable_setup(self, table, options: Dict[str, Any]):
        self.bt_table_name = self.table_name_generator(table)
        self.client: Client = Client(
            options.get(BigTableStore.BT_PROJECT_KEY),
            admin=True,
        )
        self.instance: Instance = self.client.instance(
            options.get(BigTableStore.BT_INSTANCE_KEY)
        )
        self.bt_table: Table = self.instance.table(self.bt_table_name)
        self.column_family_id = "FaustColumnFamily"
        if not self.bt_table.exists():
            logging.getLogger(__name__).info(
                f"BigTableStore: Making new bigtablestore with {self.bt_table_name=} "
                f"for {table.name}"
            )
            self.bt_table.create(
                column_families={
                    self.column_family_id: column_family.MaxVersionsGCRule(1)
                }
            )
        else:
            logging.getLogger(__name__).info(
                "BigTableStore: Using existing "
                f"bigtablestore with {self.bt_table_name=} for {table.name}"
            )

    @staticmethod
    def bigtable_exrtact_row_data(row_data):
        return list(row_data.to_dict().values())[0][0].value

    def _bigtable_get(self, key: bytes) -> Optional[bytes]:
        row, value = self._cache.get(key)
        if value is not None:
            return value
        elif row is not None and value is None:
            return value
        else:
            self._log_and_maybe_set_tracked_key(
                key[1:], "'not found in cache get'"
            )
            res = self.bt_table.read_row(key, filter_=self.row_filter)
            row = self.bt_table.direct_row(key)
            if res is None:
                self.log.info(f"{key=} not found in {self.table_name}")
                value = None
            else:
                value = self.bigtable_exrtact_row_data(res)
        return value

    def _bigtable_get_range(
        self, keys: Set[bytes]
    ) -> Tuple[Optional[bytes], Optional[bytes]]:
        # first search cache:
        for key in keys:
            row, value = self._cache.get(key)
            if value is not None:
                return key, value
            elif row is not None and value is None:
                return key, value

        rows = RowSet()
        for key in keys:
            rows.add_row_key(key)

        self._log_and_maybe_set_tracked_key(
            keys.pop()[1:], "'not found in cache get (no partition)'"
        )
        for row in self.bt_table.read_rows(
            row_set=rows, filter_=CellsColumnLimitFilter(1)
        ):
            # First hit will return
            return row.row_key, BigTableStore.bigtable_exrtact_row_data(row)
        # Not found
        self._log_and_maybe_set_tracked_key(
            keys.pop()[1:], "'not found in cache get (no partition)'"
        )
        return None, None

    def _bigtable_set(
        self, key: bytes, value: Optional[bytes], persist_offset=False
    ):
        if not persist_offset:
            row = self._cache.get(key)[0]
            if row is None:
                row = self.bt_table.direct_row(key)
            if self._cache.get_mutation_buffer() is None:
                row.set_cell(
                    self.column_family_id,
                    self.column_name,
                    value,
                )
            self._cache.set(key, row, value)
        else:
            row = self.bt_table.direct_row(key)
            row.set_cell(
                self.column_family_id,
                self.column_name,
                value,
            )
            row.commit()

    def _bigtable_del(self, key: bytes):
        row = self.bt_table.direct_row(key)
        self._log_and_maybe_set_tracked_key(key[1:], "'deleted'")
        if self._cache.get_mutation_buffer() is None:
            row.delete()
            row.commit()
        self._cache.delete(key, row)

    def _maybe_get_partition_from_message(self) -> Optional[int]:
        event = current_event()
        if (
            event is not None
            and not self.table.is_global
            and not self.table.use_partitioner
        ):
            return event.message.partition
        else:
            return None

    def _get_key_with_partition(self, key: bytes, partition):
        partition_prefix = partition.to_bytes(1, "little")
        key = b"".join([partition_prefix, key])
        return key

    def _partitions_for_key(self, key: bytes) -> Iterable[int]:
        try:
            return [self._cache.get_partition(key)]
        except KeyError:
            return self._active_partitions()

    def _log_and_maybe_set_tracked_key(self, key, msg):
        if self._tracked_key in key:
            self.log.info(f"Tracked {self._tracked_key=}: {msg}")

    def _get(self, key: bytes) -> Optional[bytes]:
        try:
            self._log_and_maybe_set_tracked_key(key, "'get'")
            partition = self._maybe_get_partition_from_message()
            if partition is not None:
                key_with_partition = self._get_key_with_partition(
                    key, partition=partition
                )

                value = self._bigtable_get(key_with_partition)
                if value is not None:
                    self._cache.set_partition(key, partition)
                    return value
            else:
                keys = set()
                for partition in self._partitions_for_key(key):
                    key_with_partition = self._get_key_with_partition(
                        key, partition=partition
                    )
                    keys.add(key_with_partition)

                key, value = self._bigtable_get_range(keys)
                if value is not None:
                    partition = key[0]
                    self._cache.set_partition(key[1:], partition)
                    return value
            return None
        except KeyError as ke:
            self.log.error(
                f"KeyError in get for table {self.table_name} for {key=}"
            )
            raise ke
        except Exception as ex:
            self.log.error(
                f"Error in get for table {self.table_name} exception {ex} key {key}"
            )
            raise ex

    def _set(self, key: bytes, value: Optional[bytes]) -> None:
        try:
            self._log_and_maybe_set_tracked_key(key, "'set'")
            partition = get_current_partition()
            key_with_partition = self._get_key_with_partition(
                key, partition=partition
            )
            self._bigtable_set(key_with_partition, value)
            self._cache.set_partition(key, partition)
        except Exception as ex:
            self.log.error(
                f"FaustBigtableException Error in set for "
                f"table {self.table_name} exception {ex} key {key} "
                f"Traceback: {traceback.format_exc()}"
            )
            raise ex

    def _del(self, key: bytes) -> None:
        try:
            self._log_and_maybe_set_tracked_key(key, "'del'")
            for partition in self._partitions_for_key(key):
                key_with_partition = self._get_key_with_partition(
                    key, partition=partition
                )
                self._bigtable_del(key_with_partition)
        except Exception as ex:
            self.log.error(
                f"FaustBigtableException Error in delete for "
                f"table {self.table_name} exception {ex} key {key}"
            )
            raise ex

    def _iterkeys(self) -> Iterator[bytes]:
        try:
            cache_iterator = self._cache.get_key_iterable_if_exists()
            if cache_iterator is not None:
                for key in cache_iterator:
                    yield key[1:]
            else:
                for row in self._iteritems():
                    yield row[0]
        except Exception as ex:
            self.log.error(
                f"FaustBigtableException Error in _iterkeys "
                f"for table {self.table_name} exception {ex}"
            )
            raise ex

    def _itervalues(self) -> Iterator[bytes]:
        try:
            for row in self._iteritems():
                yield row[1]
        except Exception as ex:
            self.log.error(
                f"FaustBigtableException Error "
                f"in _itervalues for table {self.table_name}"
                f" exception {ex}"
            )
            raise ex

    def _active_partitions(self) -> Iterator[int]:
        actives = self.app.assignor.assigned_actives()
        topic = self.table.changelog_topic_name
        for partition in range(self.app.conf.topic_partitions):
            tp = TP(topic=topic, partition=partition)
            # for global tables, keys from all
            # partitions are available.
            if tp in actives or self.table.is_global:
                yield partition

    def _iteritems(self) -> Iterator[Tuple[bytes, bytes]]:
        try:
            for partition in self._active_partitions():
                partition_prefix = partition.to_bytes(1, "little")
                start_key = b"".join([partition_prefix, self.bt_start_key])
                end_key = b"".join([partition_prefix, self.bt_end_key])

                for row in self.bt_table.read_rows(
                    start_key=start_key,
                    end_key=end_key,
                ):
                    mutation_buffer = self._cache.get_mutation_buffer()
                    if mutation_buffer is not None:
                        # We want to yield the mutation if any is buffered
                        mut_row, value = mutation_buffer.rows.get(
                            row.row_key, (None, None)
                        )
                        if value is not None:
                            yield (row.row_key[1:], value)
                            continue
                        elif mut_row is not None:
                            # This means that row will be deleted
                            continue
                    yield (
                        row.row_key[1:],
                        self.bigtable_exrtact_row_data(row),
                    )
        except Exception as ex:
            self.log.error(
                f"FaustBigtableException Error "
                f"in _iteritems for table {self.table_name}"
                f" exception {ex}"
            )
            raise ex

    def _size(self) -> int:
        """Always returns 0 for Bigtable."""
        return 0

    def _contains(self, key: bytes) -> bool:
        try:
            self._log_and_maybe_set_tracked_key(key, "'contains'")
            partition = self._maybe_get_partition_from_message()
            if partition is not None:
                key_with_partition = self._get_key_with_partition(
                    key, partition=partition
                )
                found = self._cache.contains(key_with_partition)
                if found is None:
                    found = self._bigtable_get(key_with_partition) is not None
                return found
            else:
                keys_to_search = set()
                for partition in self._partitions_for_key(key):
                    key_with_partition = self._get_key_with_partition(
                        key, partition=partition
                    )
                    keys_to_search.add(key_with_partition)

                found = self._cache.contains_any(keys_to_search)
                if found is None:
                    found = (
                        self._bigtable_get_range(keys_to_search)[1] is not None
                    )
                elif found is False:
                    self._log_and_maybe_set_tracked_key(
                        key, "'not found in cache'"
                    )
                else:
                    self._log_and_maybe_set_tracked_key(
                        key, "'found in cache'"
                    )
                return found
        except Exception as ex:
            self.log.error(
                f"FaustBigtableException Error in _contains for table "
                f"{self.table_name} exception "
                f"{ex} key {key}. "
                f"Traceback: {traceback.format_exc()}"
            )
            raise ex

    def _clear(self) -> None:
        """This is typically used to clear data.

        This does nothing when using the Bigtable store.

        """
        ...

    def reset_state(self) -> None:
        """Remove system state.

        This does nothing when using the Bigtable store.

        """
        ...

    def get_offset_key(self, tp: TP):
        return self.offset_key_prefix + str(tp.partition)

    def persisted_offset(self, tp: TP) -> Optional[int]:
        """Return the last persisted offset.
        See :meth:`set_persisted_offset`.
        """
        offset_key = self.get_offset_key(tp)
        row_res = self.bt_table.read_row(offset_key, filter_=self.row_filter)
        if row_res is not None:
            offset = int(self.bigtable_exrtact_row_data(row_res))
            return offset
        return None

    def set_persisted_offset(
        self, tp: TP, offset: int, recovery=False
    ) -> None:
        """Set the last persisted offset for this table.

        This will remember the last offset that we wrote to BigTableStore,
        so that on rebalance/recovery we can seek past this point
        to only read the events that occurred recently while
        we were not an active replica.
        """
        try:
            mutation_buffer = self._cache.get_mutation_buffer()
            if mutation_buffer is not None and not recovery:
                if mutation_buffer.check_flush():
                    num_mutations = len(mutation_buffer.rows)
                    self.log.info(
                        f"Will flush BigtableMutationBuffer with {num_mutations} "
                        f"mutations for table {self.table_name}..."
                    )
                    mutation_buffer.flush()
                    offset_key = self.get_offset_key(tp).encode()
                    self._bigtable_set(
                        offset_key, str(offset).encode(), persist_offset=True
                    )
            else:
                offset_key = self.get_offset_key(tp).encode()
                self._bigtable_set(
                    offset_key, str(offset).encode(), persist_offset=True
                )
        except Exception as e:
            self.log.error(
                f"Failed to commit offset for {self.table.name}"
                " -> will crash faust app! "
                f"TRACEBACK: {traceback.format_exc()}"
            )
            self.app._crash(e)

    def _persist_changelog_batch(self, row_mutations, tp_offsets):
        response = self.bt_table.mutate_rows(row_mutations)
        for i, status in enumerate(response):
            if status.code != 0:
                self.log.error("Row number {} failed to write".format(i))

        for tp, offset in tp_offsets.items():
            self.set_persisted_offset(tp, offset, recovery=True)

    def apply_changelog_batch(
        self,
        batch: Iterable[EventT],
        to_key: Callable[[Any], Any],
        to_value: Callable[[Any], Any],
    ) -> None:
        """Write batch of changelog events to local BigTableStore storage.

        Arguments:
            batch: Iterable of changelog events (:class:`faust.Event`)
            to_key: A callable you can use to deserialize the key
                of a changelog event.
            to_value: A callable you can use to deserialize the value
                of a changelog event.
        """
        tp_offsets: Dict[TP, int] = {}
        row_mutations = []
        for event in batch:
            tp, offset = event.message.tp, event.message.offset
            tp_offsets[tp] = (
                offset if tp not in tp_offsets else max(offset, tp_offsets[tp])
            )
            msg = event.message
            key: bytes = msg.key
            partition_bytes = tp.partition.to_bytes(1, "little")
            offset_key = b"".join([partition_bytes, key])
            row = self.bt_table.direct_row(offset_key)
            if msg.value is None:
                row.delete()
            else:
                row.set_cell(
                    self.column_family_id,
                    self.column_name,
                    msg.value,
                )
            row_mutations.append(row)
        self._persist_changelog_batch(
            row_mutations,
            tp_offsets,
        )

    async def backup_partition(
        self,
        tp: Union[TP, int],
        flush: bool = True,
        purge: bool = False,
        keep: int = 1,
    ) -> None:
        """Backup partition from this store.

        Not yet implemented for Bigtable.

        """
        raise NotImplementedError("Not yet implemented for Bigtable.")

    def restore_backup(
        self, tp: Union[TP, int], latest: bool = True, backup_id: int = 0
    ) -> None:
        """Restore partition backup from this store.

        Not yet implemented for Bigtable.

        """
        raise NotImplementedError("Not yet implemented for Bigtable.")
