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
from google.cloud.bigtable.row_set import RowSet
from google.cloud.bigtable.row_filters import CellsColumnLimitFilter
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
        self._filled_partitions: Set[int] = set()

    def __len__(self):
        return len(self.data)

    def __getitem__(self, key):
        if self.ttl is not None:
            res = self.data[key]
            self._maybe_ttl_clear()
            return res

    def __setitem__(self, key, value) -> None:
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
                    "BigtableStore: Cleard startupcache because TTL is over"
                )

    def keys(self):
        return self.data.keys()

    def fill(self, table: Table, partition: int) -> None:
        start_key = partition.to_bytes(1, "little")
        end_key = (partition + 1).to_bytes(1, "little")
        for row in table.read_rows(start_key, end_key):
            row_val = BigTableStore.bigtable_exrtact_row_data(row)
            self.data[row.row_key] = row_val
        self._filled_partitions.add(partition)

    def check_filled(self, partition: int) -> bool:
        return partition in self._filled_partitions


class BigTableKeyCache:
    _filled_partitions: Set[int] = set()
    _keys: Set[bytes] = set()

    def fill(self, table: Table, partition: int):
        start_key = partition.to_bytes(1, "little")
        end_key = (partition + 1).to_bytes(1, "little")
        for row in table.read_rows(start_key, end_key):
            self.add(row.row_key)
        self._filled_partitions.add(partition)

    def add(self, key: bytes):
        self._keys.add(key)

    def discard(self, key: bytes):
        self._keys.discard(key)

    def exists(self, key: bytes) -> bool:
        return key in self._keys

    def check_filled(self, partition: int) -> bool:
        return partition in self._filled_partitions


class BigTableStore(base.SerializedStore):
    """Bigtable table storage."""

    client: Client
    instance: Instance
    bt_table: Table

    _key_index: LRUCache[bytes, int]
    _cache: Optional[Union[LRUCache[bytes, bytes], BigtableStartupCache]]
    _mutation_buffer: Optional[BigtableMutationBuffer]
    _key_cache: Optional[BigTableKeyCache]
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
        self._set_options(app, options)
        self._key_index = LRUCache(limit=app.conf.table_key_index_size)
        self._cache = None
        self._key_cache = None
        self._mutation_buffer = None
        try:
            self._bigtable_setup(table, options)
            self._setup_mutation_buffer(options)
            self._setup_key_and_value_cache(options)
        except Exception as ex:
            logging.getLogger(__name__).error(f"Error in Bigtable init {ex}")
            raise ex
        super().__init__(url, app, table, **kwargs)

    def _set_options(self, app, options) -> None:
        self.key_cache_enabled = options.get(
            BigTableStore.KEY_CACHE_ENABLE_KEY, False
        )

        self.table_name_generator = options.get(
            BigTableStore.BT_TABLE_NAME_GENERATOR_KEY, lambda t: t.name
        )
        self.bt_start_key, self.bt_end_key = options.get(
            BigTableStore.BT_READ_ROWS_BORDERS_KEY, [b"", b""]
        )
        self.value_cache_type = options.get(
            BigTableStore.VALUE_CACHE_TYPE_KEY, None
        )
        self.value_cache_size = options.get(
            BigTableStore.VALUE_CACHE_SIZE_KEY, app.conf.table_key_index_size
        )
        self.mutation_buffer_enabled = options.get(
            BigTableStore.BT_ENABLE_MUTATION_BUFFER_KEY, False
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

    def _setup_mutation_buffer(self, options) -> None:
        if self.mutation_buffer_enabled:
            limit = options.get(
                BigTableStore.BT_MUTATION_BUFFER_LIMIT_KEY, 100
            )
            freq = options.get(
                BigTableStore.BT_MUTATION_BUFFER_FREQ_KEY, 30 * 60
            )
            self._mutation_buffer = BigtableMutationBuffer(
                self.bt_table, freq, limit
            )

    def _setup_key_and_value_cache(self, options) -> None:
        if self.value_cache_type == "startup":
            startup_cache_ttl = options.get(
                BigTableStore.STARTUPCACHE_TTL_KEY, None
            )
            self._cache = BigtableStartupCache(startup_cache_ttl)
        elif self.value_cache_type == "forever":
            self._cache = LRUCache(limit=self.value_cache_size)
        else:
            raise NotImplementedError(
                f"VALUE_CACHE_TYPE '{self.value_cache_type}'"
            )
        if self.key_cache_enabled and self._key_cache is None:
            self._key_cache = BigTableKeyCache()

    def _fill_caches_if_empty(self, partition: int):
        if self._key_cache is not None:
            if not self._key_cache.check_filled(partition):
                start_time = time.time()
                self._key_cache.fill(self.bt_table, partition)
                td = time.time() - start_time
                self.log.info(
                    f"KeyCache fill took {td}s for {self.table_name}:{partition}"
                )
        if isinstance(self._cache, BigtableStartupCache):
            if not self._cache.check_filled(partition):
                self._cache.fill(self.bt_table, partition)
                self.log.info(
                    f"KeyCache fill took {td}s for {self.table_name}:{partition}"
                )

    def _cache_set(self, key: bytes, row: DirectRow, value: bytes) -> None:
        partition = key[0]
        self._fill_caches_if_empty(partition)
        if self._cache is not None:
            self._cache[key] = value
        if self.mutation_buffer_enabled:
            self._mutation_buffer.submit(row, value)
        if self._key_cache:
            self._key_cache.add(key)

    def _cache_del(self, key: bytes, row: DirectRow) -> None:
        partition = key[0]
        self._fill_caches_if_empty(partition)
        if self.mutation_buffer_enabled:
            self._mutation_buffer.submit(row, None)
        if self._cache:
            del self._cache[key]
        if self._key_cache:
            self._key_cache.discard(key)

    def _cache_get(
        self, key: bytes
    ) -> Tuple[Optional[DirectRow], Optional[bytes]]:
        row = None
        value = None
        partition = key[0]
        self._fill_caches_if_empty(partition)
        if self.mutation_buffer_enabled:
            row, value = self._mutation_buffer.rows.get(key, (None, None))
            if value is not None:
                return row, value
        if self._cache is not None:
            if key in self._cache.keys():
                value = self._cache[key]
        return row, value

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
            # TODO: add columns families to options
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

    def _bigtable_get(self, key: bytes):
        row, value = self._cache_get(key)
        if value is not None:
            return value
        elif row is not None and value is None:
            return value
        else:
            res = self.bt_table.read_row(key, filter_=self.row_filter)
            row = self.bt_table.direct_row(key)
            if res is None:
                self.log.info(f"{key=} not found in {self.table_name}")
                value = None
            else:
                value: bytes = self.bigtable_exrtact_row_data(res)
        return value

    def _bigtable_get_range(
        self, keys: Set[bytes]
    ) -> Tuple[bytes, Optional[bytes]]:
        # first search cache:
        for key in keys:
            row, value = self._cache_get(key)
            if value is not None:
                return key, value
            elif row is not None and value is None:
                return key, value

        rows = RowSet()
        for key in keys:
            rows.add_row_key(key)

        for row in self.bt_table.read_rows(row_set=rows):
            # First hit will return
            return row.row_key, BigTableStore.bigtable_exrtact_row_data(row)

    def _bigtable_set(
        self, key: bytes, value: Optional[bytes], persist_offset=False
    ):
        if not persist_offset:
            row = self._cache_get(key)[0]
            if row is None:
                row = self.bt_table.direct_row(key)
            if not self.mutation_buffer_enabled:
                row.set_cell(
                    self.column_family_id,
                    self.column_name,
                    value,
                )
            self._cache_set(key, row, value)
        else:
            row = self.bt_table.direct_row(key)
            row.set_cell(
                self.column_family_id,
                self.column_name,
                value,
            )
            row.commit()

    def _bigtable_del(self, key: bytes):
        row = self._cache_get(key)[0]
        if row is None:
            row = self.bt_table.direct_row(key)
        if not self.mutation_buffer_enabled:
            row.delete()
            row.commit()
        self._cache_del(key, row)

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
            return [self._key_index[key]]
        except KeyError:
            active_partitions = set(self._active_partitions())
            if self._key_cache is not None:
                for partition in active_partitions:
                    if not self._key_cache.check_filled(partition):
                        continue
                    else:
                        self._fill_caches_if_empty(partition)
                        key_with_partition = self._get_key_with_partition(
                            key, partition
                        )
                        if self._key_cache.exists(key_with_partition):
                            return [partition]
            return active_partitions

    def _check_key_cache(self, key):
        if self._key_cache:
            return self._key_cache.exists(key)
        else:
            return False

    def _get(self, key: bytes) -> Optional[bytes]:
        try:
            partition = self._maybe_get_partition_from_message()
            if partition is not None:
                key_with_partition = self._get_key_with_partition(
                    key, partition=partition
                )

                value = self._bigtable_get(key_with_partition)
                if value is not None:
                    self._key_index[key] = partition
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
                    partition = key[1]
                    self._key_index[key[1:]] = partition
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
            partition = get_current_partition()
            key_with_partition = self._get_key_with_partition(
                key, partition=partition
            )
            self._bigtable_set(key_with_partition, value)
            self._key_index[key] = partition
        except Exception as ex:
            self.log.error(
                f"FaustBigtableException Error in set for "
                f"table {self.table_name} exception {ex} key {key} "
                f"Traceback: {traceback.format_exc()}"
            )
            raise ex

    def _del(self, key: bytes) -> None:
        try:
            for partition in self._partitions_for_key(key):
                key_with_partition = self._get_key_with_partition(
                    key, partition=partition
                )
                self._bigtable_del(key_with_partition)

            if key in self._key_index:
                del self._key_index[key]
        except Exception as ex:
            self.log.error(
                f"FaustBigtableException Error in delete for "
                f"table {self.table_name} exception {ex} key {key}"
            )
            raise ex

    def _iterkeys(self) -> Iterator[bytes]:
        try:
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
                    if self.mutation_buffer_enabled:
                        # We want to yield the mutation if any is buffered
                        mut_row, value = self._mutation_buffer.rows.get(
                            row.row_key, (None, None)
                        )
                        if value is not None:
                            yield (row.row_key[1:], value)
                            continue
                        elif mut_row is not None:
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
            partition = self._maybe_get_partition_from_message()
            if partition is not None:
                key_with_partition = self._get_key_with_partition(
                    key, partition=partition
                )
                found = False
                if self._key_cache is not None:
                    self._fill_caches_if_empty(partition)
                    found = self._key_cache.exists(key_with_partition)
                else:
                    found = self._bigtable_get(key_with_partition) is not None
                return found
            else:
                for partition in self._partitions_for_key(key):
                    key_with_partition = self._get_key_with_partition(
                        key, partition=partition
                    )
                    if self._key_cache is not None:
                        self._fill_caches_if_empty(partition)
                        if self._key_cache.exists(key_with_partition):
                            return True
                    else:
                        return (
                            self._bigtable_get(key_with_partition) is not None
                        )
            return False
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
            if self.mutation_buffer_enabled and not recovery:
                if self._mutation_buffer.check_flush():
                    num_mutations = len(self._mutation_buffer.rows)
                    self.log.info(
                        f"Will flush BigtableMutationBuffer with {num_mutations} "
                        f"mutations for table {self.table_name}..."
                    )
                    self._mutation_buffer.flush()
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
