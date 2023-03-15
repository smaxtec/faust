"""BigTable storage."""
import asyncio
import gc
import logging
import time
import traceback
from collections import deque
from typing import Any, Callable, Dict, Iterable, Iterator, Optional, Set, Tuple, Union

try:  # pragma: no cover
    from google.cloud.bigtable import column_family
    from google.cloud.bigtable.client import Client
    from google.cloud.bigtable.instance import Instance
    from google.cloud.bigtable.row import DirectRow
    from google.cloud.bigtable.row_filters import CellsColumnLimitFilter
    from google.cloud.bigtable.row_set import RowSet
    from google.cloud.bigtable.table import Table

    # Make one container for all imported functions
    # This is needed for testing and controlling the imports
    class BT:
        column_family = column_family
        Client = Client
        Instance = Instance
        DirectRow = DirectRow
        CellsColumnLimitFilter = CellsColumnLimitFilter
        RowSet = RowSet
        Table = Table

except ImportError:  # pragma: no cover
    BT = None  # noqa

from mode.utils.collections import LRUCache
from yarl import URL

from faust.stores import base
from faust.streams import current_event
from faust.types import TP, AppT, CollectionT, EventT


def get_current_partition():
    event = current_event()
    assert event is not None
    return event.message.partition


class BigTableValueCache:
    """
    This is a dictionary which is only filled once, after that, every
    successful access to a key, will remove it.
    """

    data: Union[Dict, LRUCache]

    def __init__(self, ttl=-1, size: Optional[int] = None) -> None:
        self.log = logging.getLogger(self.__class__.__name__)
        if size is not None:
            self.data = LRUCache(limit=size)
        else:
            self.data = {}
        self.ttl = ttl
        self.ttl_over = False
        self.init_ts = int(time.time())

    def __len__(self):
        return len(self.data)

    def __getitem__(self, key):
        if not self.ttl_over:
            res = self.data[key]
            self._maybe_ttl_clear()
            return res

    def __setitem__(self, key, value) -> None:
        if value is not None:
            self._maybe_ttl_clear()
            if not self.ttl_over:
                self.data[key] = value

    def __delitem__(self, key):
        self.data.pop(key, None)

    def _maybe_ttl_clear(self):
        if self.ttl != -1 and not self.ttl_over:
            now = int(time.time())
            if now > self.init_ts + self.ttl:
                self.data = {}
                self.ttl_over = True

    def keys(self):
        return self.data.keys()


class BigTableCacheManager:
    _partition_cache: LRUCache[bytes, int]
    _value_cache: Optional[BigTableValueCache]
    _mutations: Dict[bytes, Tuple[BT.DirectRow, Optional[bytes]]]

    def __init__(self, app, options: Dict, bt_table: BT.Table) -> None:
        self.log = logging.getLogger(__name__)
        self.bt_table: BT.Table = bt_table
        self.get_preload_prefix_len = options.get(
            BigTableStore.CACHE_PRELOAD_PREFIX_LEN_FUN_KEY,
            lambda _: 1,  # Default partition only
        )
        self._partition_cache = LRUCache(limit=app.conf.table_key_index_size)
        self._init_value_cache(options)
        self._init_mutation_buffer(options)
        self._finished_preloads: Set[bytes] = set()

    def _fill_if_empty(self, bt_keys):
        # This is a hack, that enables iterating over all results
        # without saving them in memory
        deque(self._fill_if_empty_and_yield(bt_keys), maxlen=0)

    def _preload_id_from_key(self, bt_key):
        prefix = bt_key[: self.get_preload_prefix_len(bt_key)]
        return prefix

    def _get_preload_rowset_and_filter(self, preload_ids):
        row_set = BT.RowSet()
        row_filter = CellsColumnLimitFilter(1)
        for preload_id in preload_ids:
            row_set.add_row_range_from_keys(
                start_key=preload_id, end_key=preload_id, end_inclusive=True
            )
        return row_set, row_filter

    def _fill_if_empty_and_yield(self, bt_keys: Set[bytes]):
        preload_ids = set()
        for k in bt_keys:
            preload_ids.add(self._preload_id_from_key(bt_key=k))
        preload_ids_todo = preload_ids.difference(self._finished_preloads)
        if len(preload_ids_todo) == 0:
            return

        start = time.time()
        if self._value_cache is not None:
            try:
                row_set, row_filter = self._get_preload_rowset_and_filter(preload_ids_todo)
                for row in self.bt_table.read_rows(row_set=row_set, filter_=row_filter):
                    if row.row_key in self._mutations.keys():
                        mutation_val = self._mutations[row.row_key][1]
                        if mutation_val is not None:
                            self._value_cache[row.row_key] = mutation_val
                    else:
                        value = BigTableStore.bigtable_exrtact_row_data(row)
                        self._value_cache[row.row_key] = value
                    yield row.row_key
            except Exception as e:
                self.log.info(f"BigTableStore fill failed for {preload_ids_todo=}, {bt_keys=}")
                raise e
        end = time.time()
        self.log.info(
            "BigTableStore: Finished fill for table"
            f"{self.bt_table.name}:{preload_ids_todo} in {end-start}s"
        )
        self._finished_preloads.update(preload_ids_todo)

    def get(self, bt_key: bytes) -> Optional[bytes]:
        if bt_key in self._mutations.keys():
            return self._mutations[bt_key][1]
        if self._value_cache is not None:
            self._fill_if_empty({bt_key})
            if bt_key in self._value_cache.keys():
                return self._value_cache[bt_key]
        return None

    def set(self, bt_key: bytes, value: Optional[bytes]) -> None:
        if self._value_cache is not None:
            self._value_cache[bt_key] = value
        self._set_mutation(bt_key, value)

    def delete(self, bt_key: bytes) -> None:
        if self._value_cache is not None:
            del self._value_cache[bt_key]
        self._set_mutation(bt_key, None)

    def get_partition(self, user_key: bytes) -> int:
        return self._partition_cache[user_key]

    def set_partition(self, user_key: bytes, partition: int):
        self._partition_cache[user_key] = partition

    def contains(self, bt_key: bytes) -> Optional[bool]:
        """
        If we return None here, this means, that no assumption
        about the current key can be made.
        """
        if bt_key in self._mutations.keys():
            return self._mutations[bt_key][1] is not None
        if self._value_cache is not None:
            self._fill_if_empty({bt_key})
            return bt_key in self._value_cache.keys()
        return None

    def contains_any(self, key_set: Set[bytes]) -> Optional[bool]:
        mutations = key_set.intersection(self._mutations.keys())
        found = any(mut[1] is not None for mut in mutations)
        if found:
            return True
        if self._value_cache is not None:
            self._fill_if_empty(key_set)
            return not self._value_cache.keys().isdisjoint(key_set)
        return None

    def flush_if_timer_over(self, tp: TP) -> bool:
        now = time.time()
        flushed = False
        last_flush = self._last_flush.get(tp.partition, now - self._mut_freq)
        max_reached = len(self._mutations) >= self._max_mutations
        if now >= last_flush + self._mut_freq or max_reached:
            mutatations_copy = self._mutations.copy()
            mutatations = [
                m[0]
                for m in mutatations_copy.values()
                if tp.partition == m[0].row_key[0]
            ]
            if len(mutatations) > 0:
                response = self.bt_table.mutate_rows(mutatations)
                for i, status in enumerate(response):
                    if status.code != 0:
                        self.log.error(f"Row number {i} failed to write")
                    else:
                        self._mutations.pop(mutatations[i].row_key)
            flushed = True
            self._last_flush[tp.partition] = now
        return flushed

    def _set_mutation(self, bt_key: bytes, value: Optional[bytes]):
        if bt_key in self._mutations.keys():
            row = self._mutations[bt_key][0]
        else:
            row = self.bt_table.direct_row(bt_key)

        if value is None:
            row.delete()
        else:
            row.set_cell(
                "FaustColumnFamily",  # TODO: Define this globally
                "DATA",
                value,
            )
        self._mutations[bt_key] = row, value

    def delete_partition(self, partition: int):
        if self._value_cache is not None:
            keys = set(self._value_cache.keys())
            for k in keys:
                if k[0] == partition:
                    del self._value_cache[k]
                    self._mutations.pop(k, None)
                    self._partition_cache.pop(k[1:], None)

    def _init_value_cache(
        self, options
    ) -> Optional[Union[LRUCache, BigTableValueCache]]:
        enable = options.get(BigTableStore.VALUE_CACHE_ENABLE_KEY, False)
        if enable:
            # TODO Maybe we need to remove invalidation time and size
            ttl = options.get(BigTableStore.VALUE_CACHE_INVALIDATION_TIME_KEY, -1)
            size = options.get(BigTableStore.VALUE_CACHE_SIZE_KEY, None)
            self._value_cache = BigTableValueCache(ttl=ttl, size=size)
        else:
            self._value_cache = None

    def _init_mutation_buffer(self, options):
        self._mut_freq = options.get(BigTableStore.BT_MUTATION_FREQ_KEY, 0)
        # To prevent that all tables write at the same time
        self._last_flush = {}  # time.time() + self._mut_freq - random_start_offset
        self._max_mutations = options.get(BigTableStore.BT_MAX_MUTATIONS, 10000)
        self._mutations = {}


class BigTableStore(base.SerializedStore):
    """Bigtable table storage."""

    client: BT.Client
    instance: BT.Instance
    bt_table: BT.Table
    _cache: BigTableCacheManager
    _db_lock: asyncio.Lock

    BT_COLUMN_NAME_KEY = "bt_column_name_key"
    BT_INSTANCE_KEY = "bt_instance_key"
    BT_OFFSET_KEY_PREFIX = "bt_offset_key_prefix"
    BT_PROJECT_KEY = "bt_project_key"
    BT_TABLE_NAME_GENERATOR_KEY = "bt_table_name_generator_key"
    BT_MUTATION_FREQ_KEY = "bt_mutation_freq_key"
    BT_MAX_MUTATIONS = "bt_max_mutations"
    BT_CUSTOM_KEY_TRANSLATOR_KEY = "bt_custom_key_translator"
    VALUE_CACHE_INVALIDATION_TIME_KEY = "value_cache_invalidation_time_key"
    VALUE_CACHE_SIZE_KEY = "value_cache_size_key"
    VALUE_CACHE_ENABLE_KEY = "value_cache_enable_key"
    CACHE_PRELOAD_PREFIX_LEN_FUN_KEY = "cache_preload_prefix_len_fun_key"

    def __init__(
        self,
        url: Union[str, URL],
        app: AppT,
        table: CollectionT,
        options: Dict[str, Any],
        **kwargs: Any,
    ) -> None:
        self._set_options(options)
        try:
            self._bigtable_setup(table, options)
            self._cache = BigTableCacheManager(app, options, self.bt_table)
        except Exception as ex:
            logging.getLogger(__name__).error(f"Error in Bigtable init {ex}")
            raise ex
        super().__init__(url, app, table, **kwargs)
        self._db_lock = asyncio.Lock()

    @staticmethod
    def default_translator(user_key):
        return user_key

    def _set_options(self, options) -> None:
        self._transform_key_to_bt, self._transform_key_from_bt = options.get(
            BigTableStore.BT_CUSTOM_KEY_TRANSLATOR_KEY,
            (self.default_translator, self.default_translator)
        )
        self._all_options = options
        self.table_name_generator = options.get(
            BigTableStore.BT_TABLE_NAME_GENERATOR_KEY, lambda t: t.name
        )
        self.column_name = options.get(BigTableStore.BT_COLUMN_NAME_KEY, "DATA")
        self.row_filter = BT.CellsColumnLimitFilter(1)
        self.offset_key_prefix = options.get(
            BigTableStore.BT_OFFSET_KEY_PREFIX, "offset_partitiion:"
        )

    def _bigtable_setup(self, table, options: Dict[str, Any]):
        self.bt_table_name = self.table_name_generator(table)
        self.client: BT.Client = BT.Client(
            options.get(BigTableStore.BT_PROJECT_KEY),
            admin=True,
        )
        self.instance: BT.Instance = self.client.instance(
            options.get(BigTableStore.BT_INSTANCE_KEY)
        )
        self.bt_table: BT.Table = self.instance.table(self.bt_table_name)
        self.column_family_id = "FaustColumnFamily"
        if not self.bt_table.exists():
            logging.getLogger(__name__).info(
                f"BigTableStore: Making new bigtablestore with {self.bt_table_name=} "
                f"for {table.name}"
            )
            self.bt_table.create(
                column_families={
                    self.column_family_id: BT.column_family.MaxVersionsGCRule(1)
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
        if self._cache.contains(key) is True:
            return self._cache.get(key)
        else:
            res = self.bt_table.read_row(key, filter_=self.row_filter)
            if res is None:
                self.log.info(f"{key=} not found in {self.table_name}")
                value = None
            else:
                value = self.bigtable_exrtact_row_data(res)
        return value

    def _bigtable_contains(self, key: bytes) -> bool:
        cache_contains = self._cache.contains(key)
        if cache_contains is True:
            return cache_contains

        row = self.bt_table.read_row(key, filter_=self.row_filter)
        if row is not None:
            return True
        return False

    def _bigtable_contains_any(self, keys: Set[bytes]) -> bool:
        cache_contains = self._cache.contains_any(keys)
        if cache_contains is True:
            return cache_contains

        rows = BT.RowSet()
        for key in keys:
            rows.add_row_key(key)

        for _row in self.bt_table.read_rows(
            row_set=rows, filter_=BT.CellsColumnLimitFilter(1)
        ):
            # First hit will return
            return True
        return False

    def _bigtable_get_range(
        self, keys: Set[bytes]
    ) -> Tuple[Optional[bytes], Optional[bytes]]:
        # first search cache:
        for key in keys:
            value = self._cache.get(key)
            if value is not None:
                return key, value

        rows = BT.RowSet()
        for key in keys:
            rows.add_row_key(key)

        for row in self.bt_table.read_rows(
            row_set=rows, filter_=BT.CellsColumnLimitFilter(1)
        ):
            # First hit will return
            val = self.bigtable_exrtact_row_data(row)
            return row.row_key, val

        # Not found
        return None, None

    def _bigtable_set(self, key: bytes, value: Optional[bytes], persist_offset=False):
        if not persist_offset:
            # All mutatations set here will be flushed to BT later
            self._cache.set(key, value)
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
        self._cache.delete(key)
        row.delete()
        row.commit()

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

    def _get_partition_prefix(self, partition: int) -> bytes:
        partition_bytes = partition.to_bytes(1, "little")
        return b"".join([partition_bytes])

    def _get_faust_key(self, key: bytes) -> bytes:
        key = key[1:]
        return self._transform_key_from_bt(key)

    def _get_bigtable_key(self, key: bytes, partition: int) -> bytes:
        key = self._transform_key_to_bt(key)
        prefix = self._get_partition_prefix(partition)
        return prefix + key

    def _partitions_for_key(self, key: bytes) -> Iterable[int]:
        try:
            return [self._cache.get_partition(key)]
        except KeyError:
            return self._active_partitions()

    def _get(self, key: bytes) -> Optional[bytes]:
        try:
            partition = self._maybe_get_partition_from_message()
            if partition is not None:
                key_with_partition = self._get_bigtable_key(
                    key, partition=partition
                )

                value = self._bigtable_get(key_with_partition)
                if value is not None:
                    self._cache.set_partition(key, partition)
                    return value
            else:
                keys = set()
                for partition in self._partitions_for_key(key):
                    key_with_partition = self._get_bigtable_key(
                        key, partition=partition
                    )
                    keys.add(key_with_partition)

                key_with_partition, value = self._bigtable_get_range(keys)
                if value is not None:
                    partition = key_with_partition[0]
                    self._cache.set_partition(key, partition)
                    return value
            return None
        except KeyError as ke:
            self.log.error(f"KeyError in get for table {self.table_name} for {key=}")
            raise ke
        except Exception as ex:
            self.log.error(
                f"Error in get for table {self.table_name} exception {ex} key {key}"
            )
            raise ex

    def _set(self, key: bytes, value: Optional[bytes]) -> None:
        try:
            partition = get_current_partition()
            key_with_partition = self._get_bigtable_key(key, partition=partition)
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
            for partition in self._partitions_for_key(key):
                key_with_partition = self._get_bigtable_key(
                    key, partition=partition
                )
                self._bigtable_del(key_with_partition)
                self._cache._partition_cache.pop(key, None)
        except Exception as ex:
            self.log.error(
                f"FaustBigtableException Error in delete for "
                f"table {self.table_name} exception {ex} key {key}"
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
            row_set = BT.RowSet()
            for partition in self._active_partitions():
                prefix_start = self._get_partition_prefix(partition)
                prefix_end = self._get_partition_prefix(partition + 1)
                row_set.add_row_range_from_keys(prefix_start, prefix_end)

            for row in self.bt_table.read_rows(
                row_set=row_set, filter_=self.row_filter
            ):
                yield (
                    self._get_faust_key(row.row_key),
                    self.bigtable_exrtact_row_data(row),
                )
        except Exception as ex:
            self.log.error(
                f"FaustBigtableException Error "
                f"in _iteritems for table {self.table_name}"
                f" exception {ex}"
            )
            raise ex

    def _iterkeys(self) -> Iterator[bytes]:
        try:
            start = time.time()
            partitions = self._active_partitions()

            self.log.info(f"Start iterkeys for {self.table_name}")
            row_set = BT.RowSet()
            for partition in partitions:
                prefix_start = self._get_partition_prefix(partition)
                prefix_end = self._get_partition_prefix(partition + 1)
                row_set.add_row_range_from_keys(prefix_start, prefix_end)

            found_mutations = set()
            for k, mut in self._cache._mutations.items():
                if mut[1] is not None:
                    yield self._get_faust_key(k)
                found_mutations.add(k)

            for row in self.bt_table.read_rows(
                row_set=row_set, filter_=self.row_filter
            ):
                if row.row_key in found_mutations:
                    continue
                if self._cache._value_cache is not None:
                    data = self.bigtable_exrtact_row_data(row)
                    # We don't want to set mutations here
                    self._cache._value_cache[row.row_key] = data
                    preload_id = self._cache._preload_id_from_key(row.row_key)
                    self._cache._finished_preloads.add(preload_id)
                    partition = row.row_key[0]
                    key = self._get_faust_key(row.row_key)
                    self._cache.set_partition(key, partition)
                    yield key

            end = time.time()
            self.log.info(f"Finished iterkeys for {self.table_name} in {end - start}s")
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

    def _size(self) -> int:
        """Always returns 0 for Bigtable."""
        return 0

    def _contains(self, key: bytes) -> bool:
        try:
            if not self.app.conf.store_check_exists:
                return True
            partition = self._maybe_get_partition_from_message()
            if partition is not None:
                key_with_partition = self._get_bigtable_key(
                    key, partition=partition
                )
                return self._bigtable_contains(key_with_partition)
            else:
                keys_to_search = set()
                for partition in self._partitions_for_key(key):
                    key_with_partition = self._get_bigtable_key(
                        key, partition=partition
                    )
                    keys_to_search.add(key_with_partition)

                return self._bigtable_contains_any(keys_to_search)
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

    def set_persisted_offset(self, tp: TP, offset: int, recovery=False) -> None:
        """Set the last persisted offset for this table.

        This will remember the last offset that we wrote to BigTableStore,
        so that on rebalance/recovery we can seek past this point
        to only read the events that occurred recently while
        we were not an active replica.
        """
        try:
            if recovery or self._cache.flush_if_timer_over(tp):
                offset_key = self.get_offset_key(tp).encode()
                self._bigtable_set(
                    offset_key, str(offset).encode(), persist_offset=True
                )

        except Exception as e:
            self.log.error(
                f"Failed to commit offset for {self.table.name}"
                " -> will cause additional changelogs if restart happens"
                f"TRACEBACK: {traceback.format_exc()}"
            )

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
            offset_key = self._get_bigtable_key(msg.key, partition=tp.partition)
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

    def revoke_partitions(self, tps: Set[TP]) -> None:
        """De-assign partitions used on this worker instance.

        Arguments:
            table: The table that we store data for.
            tps: Set of topic partitions that we should no longer
                be serving data for.
        """
        for tp in tps:
            self._cache.delete_partition(tp.partition)
        gc.collect()


    async def on_rebalance(
        self,
        assigned: Set[TP],
        revoked: Set[TP],
        newly_assigned: Set[TP],
        generation_id: int = 0,
    ) -> None:
        """Rebalance occurred.

        Arguments:
            assigned: Set of all assigned topic partitions.
            revoked: Set of newly revoked topic partitions.
            newly_assigned: Set of newly assigned topic partitions,
                for which we were not assigned the last time.
            generation_id: the metadata generation identifier for the re-balance
        """
        async with self._db_lock:
            self.logger.info(
                f"BigTableStore: Rebalancing {revoked=}, {newly_assigned=}"
            )
            self.revoke_partitions(revoked)
