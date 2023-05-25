"""BigTable storage."""
import asyncio
import gc
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


COLUMN_FAMILY_ID = "FaustColumnFamily"
COLUMN_NAME = "DATA"


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
    _mutation_values: Dict[bytes, Optional[bytes]]
    _mutation_rows: Dict[bytes, BT.DirectRow]

    def __init__(self, app, options: Dict, bt_table: BT.Table) -> None:
        self.log = logging.getLogger(__name__)
        self.bt_table: BT.Table = bt_table
        self._partition_cache = LRUCache(limit=app.conf.table_key_index_size)
        self._init_value_cache(options)
        self.filled_partitions = set()
        self._last_flush = time.time()
        self._mutation_values = {}
        self._mutation_rows = {}
        self.total_mutation_count = 0

    def _get_preload_rowset(self, partitions: Set[int]):
        row_set = BT.RowSet()
        row_filter = CellsColumnLimitFilter(1)
        for partition in partitions:
            preload_id = partition.to_bytes(1, "little")
            row_set.add_row_range_from_keys(
                start_key=preload_id, end_key=preload_id + b"\xff"
            )
        return row_set, row_filter

    def submit_mutation(self, bt_key: bytes, value: Optional[bytes]) -> None:
        row = self._mutation_rows.get(bt_key, None)
        row = row if row else self.bt_table.direct_row(bt_key)
        if value is None:
            row.delete()
        else:
            row.set_cell(
                COLUMN_FAMILY_ID,
                COLUMN_NAME,
                value,
            )
        self._mutation_values[bt_key] = value
        self._mutation_rows[bt_key] = row
        self.total_mutation_count += 1
        self.flush_mutations_if_timer_over_or_full()

    def flush(self, ):
        # TODO: Make this a setting.
        # High values reduce the writes
        if self.total_mutation_count > 200:
            mutation_list = list(self._mutation_rows.values())
            actual_row_count = len(mutation_list)
            self.bt_table.mutate_rows(mutation_list)
            self._mutation_values.clear()
            self._mutation_rows.clear()
            self.log.info(
                f"BigTableStore: flushed {self.total_mutation_count}"
                f" mutations for {self.bt_table.name} table"
                f" ({actual_row_count=})"
            )
            self.total_mutation_count = 0
            self._last_flush = time.time()

    def flush_mutations_if_timer_over_or_full(self) -> None:
        five_min = 5 * 60
        if (
            self._last_flush + five_min < time.time()
            or self.total_mutation_count > 10_000
        ):
            self.flush()

    def fill(self, partitions: Set[int]):
        start = time.time()
        partitions = partitions - self.filled_partitions
        if len(partitions) == 0:
            return

        if self._value_cache is not None:
            try:
                row_set, row_filter = self._get_preload_rowset(partitions)
                for row in self.bt_table.read_rows(
                    row_set=row_set, filter_=row_filter
                ):
                    value = BigTableStore.bigtable_exrtact_row_data(row)
                    self._value_cache[row.row_key] = value
            except Exception as e:
                self.log.info(f"BigTableStore fill failed for {partitions=}")
                raise e
            self.filled_partitions.update(partitions)
        end = time.time()
        self.log.info(
            "BigTableStore: Finished fill for table"
            f"{self.bt_table.name}:{partitions} in {end-start}s"
        )

    def iteritems(self):
        if self._value_cache is None or len(self.filled_partitions) == 0:
            return []
        return self._value_cache.data.items()

    def get(self, bt_key: bytes) -> Optional[bytes]:
        if self._mutation_rows.get(bt_key) is not None:
            return self._mutation_values[bt_key]
        if self._value_cache is not None:
            return self._value_cache[bt_key]
        raise NotImplementedError(
            f"get is not implemented for {self.__class__} with no value cache"
        )

    def set(self, bt_key: bytes, value: Optional[bytes]) -> None:
        if self._value_cache is not None:
            self._value_cache[bt_key] = value

    def get_partition(self, user_key: bytes) -> int:
        return self._partition_cache[user_key]

    def set_partition(self, user_key: bytes, partition: int):
        self._partition_cache[user_key] = partition

    def contains(self, bt_key: bytes) -> Optional[bool]:
        """
        If we return None here, this means, that no assumption
        about the current key can be made.
        """
        if self._mutation_rows.get(bt_key, None) is not None:
            return True
        if self._value_cache is not None:
            return bt_key in self._value_cache.keys()
        return False

    def contains_any(self, key_set: Set[bytes]) -> Optional[bool]:
        if not self._mutation_rows.keys().isdisjoint(key_set):
            return True
        if self._value_cache is not None:
            if not self._value_cache.keys().isdisjoint(key_set):
                return True
        return False

    def delete_partition(self, partition: int):
        if self._value_cache is not None:
            keys = set(self._value_cache.keys())
            for k in keys:
                if k[0] == partition:
                    del self._value_cache[k]
                    self._partition_cache.pop(k[1:], None)

    def _init_value_cache(
        self, options
    ) -> Optional[Union[LRUCache, BigTableValueCache]]:
        enable = options.get(BigTableStore.VALUE_CACHE_ENABLE_KEY, False)
        if enable:
            ttl = options.get(
                BigTableStore.VALUE_CACHE_INVALIDATION_TIME_KEY, -1
            )
            size = options.get(BigTableStore.VALUE_CACHE_SIZE_KEY, None)
            self._value_cache = BigTableValueCache(ttl=ttl, size=size)
        else:
            self._value_cache = None


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
    VALUE_CACHE_INVALIDATION_TIME_KEY = "value_cache_invalidation_time_key"
    VALUE_CACHE_SIZE_KEY = "value_cache_size_key"
    VALUE_CACHE_ENABLE_KEY = "value_cache_enable_key"

    def __init__(
        self,
        url: Union[str, URL],
        app: AppT,
        table: CollectionT,
        options: Dict[str, Any],
        **kwargs: Any,
    ) -> None:
        self._set_options(options)
        self._log_counter = 0
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
        self._all_options = options
        self.table_name_generator = options.get(
            BigTableStore.BT_TABLE_NAME_GENERATOR_KEY, lambda t: t.name
        )
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
        if not self.bt_table.exists():
            logging.getLogger(__name__).info(
                f"BigTableStore: Making new bigtablestore with {self.bt_table_name=} "
                f"for {table.name}"
            )
            self.bt_table.create(
                column_families={
                    COLUMN_FAMILY_ID: BT.column_family.MaxVersionsGCRule(1)
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

    def _bigtable_get(self, bt_key: bytes) -> Optional[bytes]:
        if self._cache.contains(bt_key):
            return self._cache.get(bt_key)
        else:

            res = self.bt_table.read_row(bt_key, filter_=self.row_filter)
            if res is None:
                value = None
            else:
                value = self.bigtable_exrtact_row_data(res)
            # Has no effect if value_cace is None
            self._cache.set(bt_key, value)
        return value

    def _bigtable_get_range(
        self, bt_keys: Set[bytes]
    ) -> Tuple[Optional[bytes], Optional[bytes]]:
        # first search cache:
        rows = BT.RowSet()
        for bt_key in bt_keys:
            if self._cache.contains(bt_key):
                value = self._cache.get(bt_key)
                return bt_key, value

            else:
                rows.add_row_key(bt_key)

        for row in self.bt_table.read_rows(
            row_set=rows, filter_=BT.CellsColumnLimitFilter(1)
        ):
            # First hit will return
            val = self.bigtable_exrtact_row_data(row)
            return row.row_key, val

        # Not found
        return None, None

    def _bigtable_mutate(self, bt_key: bytes, value: Optional[bytes]):
        # Update the value cache if any exists
        self._cache.set(bt_key, value)
        # Update the bigtable. Mutations are batched
        self._cache.submit_mutation(bt_key, value)

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
        faust_key = key[1:]
        return faust_key

    def _get_bigtable_key(self, key: bytes, partition: int) -> bytes:
        prefix = self._get_partition_prefix(partition)
        bt_key = prefix + key
        return bt_key

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
        except Exception as ex:
            self.log.error(
                f"Error in get for table {self.table_name} exception {ex} key {key}"
            )
            raise ex

    def _set(self, key: bytes, value: Optional[bytes]) -> None:
        try:
            partition = get_current_partition()
            key_with_partition = self._get_bigtable_key(
                key, partition=partition
            )
            self._bigtable_mutate(key_with_partition, value)
            self._cache.set_partition(key, partition)
        except Exception as ex:
            self.log.error(
                f"FaustBigtableException Error in set for "
                f"table {self.table_name} exception {ex} key {key=} "
                f"{value=} Traceback: {traceback.format_exc()}"
            )
            raise ex

    def _del(self, key: bytes) -> None:
        try:
            for partition in self._partitions_for_key(key):
                key_with_partition = self._get_bigtable_key(
                    key, partition=partition
                )
                self._bigtable_mutate(key_with_partition, None)
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

            # Write all mutations to bigtable
            start = time.time()
            self._cache.flush()
            if self._cache._value_cache is not None:
                for k, v in self._cache.iteritems():
                    faust_key = self._get_faust_key(k)
                    yield faust_key, v

            left_over_partitions = set(self._active_partitions())
            left_over_partitions.difference_update(self._cache.filled_partitions)
            row_set = BT.RowSet()

            for partition in left_over_partitions:
                prefix_start = self._get_partition_prefix(partition)
                prefix_end = self._get_partition_prefix(partition + 1)
                row_set.add_row_range_from_keys(prefix_start, prefix_end)

            for row in self.bt_table.read_rows(
                row_set=row_set, filter_=self.row_filter
            ):
                faust_key = self._get_faust_key(row.row_key)
                value = self.bigtable_exrtact_row_data(row)
                self._cache.set(row.row_key, value)
                yield faust_key, value
            self._cache.filled_partitions.update(left_over_partitions)
            end = time.time()
            self.log.info(f"Time taken for _iteritems {end - start}s")

        except Exception as ex:
            self.log.error(
                f"FaustBigtableException Error "
                f"in _iteritems for table {self.table_name}"
                f" exception {ex}"
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

    def _size(self) -> int:
        """Always returns 0 for Bigtable."""
        return 0

    def _contains(self, key: bytes) -> bool:
        try:
            if not self.app.conf.store_check_exists:
                return True
            return self._get(key) is not None

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
        offset_key = self.get_offset_key(tp).encode()
        offset = self._bigtable_get(offset_key)
        return int(offset) if offset is not None else None

    def set_persisted_offset(self, tp: TP, offset: int) -> None:
        """Set the last persisted offset for this table.

        This will remember the last offset that we wrote to BigTableStore,
        so that on rebalance/recovery we can seek past this point
        to only read the events that occurred recently while
        we were not an active replica.
        """
        try:
            offset_key = self.get_offset_key(tp).encode()
            self._bigtable_mutate(offset_key, str(offset).encode())
        except Exception as e:
            self.log.error(
                f"Failed to commit offset for {self.table.name}"
                " -> will cause additional changelogs if restart happens"
                f"TRACEBACK: {traceback.format_exc()}"
            )

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
        for event in batch:
            tp, offset = event.message.tp, event.message.offset
            tp_offsets[tp] = (
                offset if tp not in tp_offsets else max(offset, tp_offsets[tp])
            )
            msg = event.message
            bt_key = self._get_bigtable_key(msg.key, partition=tp.partition)
            self._bigtable_mutate(bt_key, msg.value)

        for tp, offset in tp_offsets.items():
            self.set_persisted_offset(tp, offset)
        self._cache.flush()

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

    def assign_partitions(self, tps: Set[TP]) -> None:
        self._cache.fill({tp.partition for tp in tps})

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
            self.revoke_partitions(revoked)
            self.assign_partitions(newly_assigned)
