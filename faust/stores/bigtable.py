"""BigTable storage."""
import gc
import logging
import time
import threading
import traceback
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)
from mode.utils.collections import LRUCache

try:  # pragma: no cover
    from google.api_core.exceptions import AlreadyExists
    from google.cloud.bigtable import column_family
    from google.cloud.bigtable.batcher import MutationsBatcher
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

except ImportError as e:  # pragma: no cover
    logger = logging.getLogger(__name__).error(e)
    BT = None  # noqa

from yarl import URL

from faust.stores import base
from faust.streams import current_event
from faust.types import TP, AppT, CollectionT, EventT


COLUMN_FAMILY_ID = "FaustColumnFamily"
COLUMN_NAME = "DATA"


class BigTableStore(base.SerializedStore):
    """Bigtable table storage."""

    client: BT.Client
    instance: BT.Instance
    bt_table: BT.Table

    BT_COLUMN_NAME_KEY = "bt_column_name_key"
    BT_INSTANCE_KEY = "bt_instance_key"
    BT_OFFSET_KEY_PREFIX = "bt_offset_key_prefix"
    BT_PROJECT_KEY = "bt_project_key"
    BT_TABLE_NAME_GENERATOR_KEY = "bt_table_name_generator_key"
    BT_STARTUP_CACHE_ENABLE_KEY = "bt_startup_cache_enable_key"
    BT_KEY_CACHE_ENABLE_KEY = "bt_key_cache_enable_key"
    BT_MUTATION_BATCHER_ENABLE_KEY = "bt_mutation_batcher_enable_key"
    BT_MUTATION_BATCHER_FLUSH_COUNT_KEY = "bt_mutation_batcher_flush_count_key"
    BT_MUTATION_BATCHER_FLUSH_INTERVAL_KEY = (
        "bt_mutation_batcher_flush_interval_key"
    )

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
            self._setup_bigtable(table, options)
            self._setup_caches(options)
            self._setup_mutation_batcher(options)
            key_index_size = app.conf.table_key_index_size
            self.key_index_size = key_index_size
            self._key_index = LRUCache(limit=self.key_index_size)
        except Exception as ex:
            logging.getLogger(__name__).error(f"Error in Bigtable init {ex}")
            raise ex
        super().__init__(url, app, table, **kwargs)

    @staticmethod
    def default_translator(user_key):
        return user_key

    def _setup_mutation_batcher(self, options):
        self._mutation_batcher_enable = options.get(
            BigTableStore.BT_MUTATION_BATCHER_ENABLE_KEY, False
        )
        if self._mutation_batcher_enable:
            flush_count = options.get(
                BigTableStore.BT_MUTATION_BATCHER_FLUSH_COUNT_KEY, 10_000
            )
            flush_interval = options.get(
                BigTableStore.BT_MUTATION_BATCHER_FLUSH_INTERVAL_KEY, 300
            )
            self._mutation_batcher = MutationsBatcher(
                self.bt_table,
                flush_count=flush_count,
                flush_interval=flush_interval,
            )

    def _setup_caches(
        self,
        options: Dict[str, Any] = None,
    ):
        self._startup_cache_enable = options.get(
            BigTableStore.BT_STARTUP_CACHE_ENABLE_KEY, False
        )
        if self._startup_cache_enable:
            self._startup_cache: Dict[bytes, bytes] = {}
            self._startup_cache_partitions = set()
            self._invalidation_timer: Optional[threading.Timer] = None
        else:
            self._startup_cache_partitions = None
            self._startup_cache = None

    def _set_options(self, options) -> None:
        self._all_options = options
        self.table_name_generator = options.get(
            BigTableStore.BT_TABLE_NAME_GENERATOR_KEY, lambda t: t.name
        )
        self.row_filter = BT.CellsColumnLimitFilter(1)
        self.offset_key_prefix = options.get(
            BigTableStore.BT_OFFSET_KEY_PREFIX, "==>offset_for_partition_"
        )

    def _setup_bigtable(self, table, options: Dict[str, Any]):
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
            try:
                self.bt_table.create(
                    column_families={
                        COLUMN_FAMILY_ID: BT.column_family.MaxVersionsGCRule(1)
                    }
                )
            except AlreadyExists:
                logging.getLogger(__name__).info(
                    "BigTableStore: Using existing "
                    f"bigtablestore with {self.bt_table_name=} for {table.name} "
                    f"with {options=} due to AlreadyExists exception"
                )
                return
            logging.getLogger(__name__).info(
                f"BigTableStore: Making new bigtablestore with {self.bt_table_name=} "
                f"for {table.name} with {options=}"
            )
        else:
            logging.getLogger(__name__).info(
                "BigTableStore: Using existing "
                f"bigtablestore with {self.bt_table_name=} for {table.name} "
                f"with {options=}"
            )

    def _add_partition_prefix_to_key(
        self, key: bytes, partition: Optional[int]
    ) -> bytes:
        if partition is None:
            return key
        separator = b"_..._"
        partition_bytes = str(partition).encode("utf-8")
        return separator.join([partition_bytes, key])

    def _remove_partition_prefix_from_bigtable_key(self, key: bytes) -> bytes:
        separator = b"_..._"
        key = key.rsplit(separator, 1)[-1]
        return key

    def _get_partition_from_bigtable_key(self, key: bytes) -> int:
        separator = b"_..._"
        partition_str, _ = key.rsplit(separator, 1)
        return int(partition_str)

    def _active_partitions(self) -> List[int]:
        actives = self.app.assignor.assigned_actives()
        topic = self.table.changelog_topic_name
        partitions = []
        for partition in range(self.app.conf.topic_partitions):
            tp = TP(topic=topic, partition=partition)
            if tp in actives or self.table.is_global:
                partitions.append(partition)
        return partitions

    def _get_current_partitions(self) -> List[int]:
        event = current_event()
        if (
            event is not None
            and event.message.topic is not None
            and not self.table.is_global
            and not self.table.use_partitioner
        ):
            partition = event.message.partition
            return [partition]
        return self._active_partitions()

    def _get_partitions_for_key(self, key: bytes) -> List[int]:
        if key in self._key_index:
            return [self._key_index[key]]
        return self._get_current_partitions()

    @staticmethod
    def bigtable_exrtact_row_data(row_data):
        return list(row_data.to_dict().values())[0][0].value

    def _del_cache(self, key: bytes):
        if self._startup_cache is not None:
            self._startup_cache[key] = None

    def _set_cache(self, key: bytes, value):
        if self._startup_cache is not None:
            self._startup_cache[key] = value

    def _get_cache(self, key: bytes):
        if self._startup_cache is not None:
            if key in self._startup_cache:
                return self._startup_cache[key], True
        return None, False

    def _invalidate_startup_cache(self):
        if self._startup_cache is not None:
            self._startup_cache.clear()
            self._startup_cache = None
            self._startup_cache_partitions = None
            gc.collect()
            self.log.info(
                f"Invalidated startup cache for table {self.table_name}"
            )
        self._invalidation_timer.cancel()
        del self._invalidation_timer
        self._invalidation_timer = None

    def _set_mutation(self, mutated_row: DirectRow):
        self._mutation_batcher.mutate(mutated_row)

    def _bigtable_get(
        self, keys: List[bytes]
    ) -> Tuple[Optional[bytes], Optional[int]]:
        rowset = RowSet()
        for key in keys:
            rowset.add_row_key(key)
        if self._mutation_batcher_enable:
            self._mutation_batcher.flush()

        rows = self.bt_table.read_rows(row_set=rowset, filter_=self.row_filter)
        for row in rows:
            if row is not None:
                partition = self._get_partition_from_bigtable_key(row.row_key)
                return self.bigtable_exrtact_row_data(row), partition
        return None, None

    def _get(self, key: bytes) -> Optional[bytes]:
        try:
            value, found = self._get_cache(key)
            if found:
                return value

            partitions = self._get_partitions_for_key(key)

            keys = [
                self._add_partition_prefix_to_key(key, p) for p in partitions
            ]
            value, partition = self._bigtable_get(keys)
            if value is not None:
                self._key_index[key] = partition
            return value
        except Exception as ex:
            self.log.error(
                f"Error in get for table {self.table_name} exception {ex} key {key}"
            )
            raise ex

    def _bigtable_set(self, key: bytes, value: bytes):
        row = self.bt_table.direct_row(key)
        row.set_cell(
            COLUMN_FAMILY_ID,
            COLUMN_NAME,
            value,
        )

        if self._mutation_batcher_enable:
            self._set_mutation(row)
        else:
            row.commit()

    def _set(self, key: bytes, value: bytes) -> None:
        try:
            self._set_cache(key, value)

            event = current_event()
            assert event is not None
            partition = event.message.partition
            key = self._add_partition_prefix_to_key(key, partition)

            self._bigtable_set(key, value)
            self._key_index[key] = partition
        except Exception as ex:
            self.log.error(
                f"FaustBigtableException Error in set for "
                f"table {self.table_name} exception {ex} key {key=} "
                f"{value=} Traceback: {traceback.format_exc()}"
            )
            raise ex

    def _bigtable_del(self, key: bytes):
        row = self.bt_table.direct_row(key)
        row.delete()
        if self._mutation_batcher_enable:
            self._set_mutation(row)
        else:
            row.commit()

    def _del(self, key: bytes) -> None:
        try:
            self._del_cache(key)
            partitions = self._get_partitions_for_key(key)
            for partition in partitions:
                key = self._add_partition_prefix_to_key(key, partition)
                self._bigtable_del(key)
        except Exception as ex:
            self.log.error(
                f"FaustBigtableException Error in del for "
                f"table {self.table_name} exception {ex} key {key=} "
                f"Traceback: {traceback.format_exc()}"
            )
            raise ex

    def _bigtable_iteritems(self, partitions):
        try:
            start = time.time()
            if partitions is None:
                partitions = self._active_partitions()
            row_set = RowSet()
            self.log.info(
                f"BigtableStore: Iterating over {len(partitions)} partitions "
                f"for table {self.table_name}"
            )

            need_all_keys = self.table.is_global or self.table.use_partitioner
            if not need_all_keys:
                for partition in partitions:
                    prefix = self._add_partition_prefix_to_key(
                        b"", partition
                    ).decode()
                    row_set.add_row_range_with_prefix(prefix)

            if self._mutation_batcher_enable:
                self._mutation_batcher.flush()

            offset_key_prefix = self.offset_key_prefix.encode()
            for row in self.bt_table.read_rows(
                row_set=row_set, filter_=self.row_filter
            ):
                # abort it key is an offset key
                if need_all_keys and offset_key_prefix in row.row_key:
                    continue

                value = self.bigtable_exrtact_row_data(row)
                key = self._remove_partition_prefix_from_bigtable_key(
                    row.row_key
                )
                yield key, value
            end = time.time()
            self.log.info(
                f"{self.table_name} _bigtable_iteritems took {end - start}s "
                f"for partitions {partitions}"
            )
        except Exception as ex:
            self.log.error(
                f"FaustBigtableException Error "
                f"in _iteritems for table {self.table_name}"
                f" exception {ex}"
            )
            raise ex

    def _iteritems(
        self, partitions: Optional[List[int]] = None
    ) -> Iterator[Tuple[bytes, bytes]]:
        if self._startup_cache is not None:
            if partitions is None:
                partitions: List[int] = self._active_partitions()
                for k, v in self._startup_cache.items():
                    if v is not None:
                        yield k, v
                partitions = set(partitions)
                partitions = partitions.difference(self._startup_cache_partitions)

        if partitions is None or len(partitions) > 0:
            for key, val in self._bigtable_iteritems(partitions):
                self._set_cache(key, val)
                yield key, val

    def _iterkeys(self) -> Iterator[bytes]:
        for row in self._iteritems():
            yield row[0]

    def _itervalues(self) -> Iterator[bytes]:
        for row in self._iteritems():
            yield row[1]

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
        if self._mutation_batcher_enable:
            self._mutation_batcher.flush()
        row = self.bt_table.read_row(offset_key, filter_=self.row_filter)
        offset = (
            self.bigtable_exrtact_row_data(row) if row is not None else None
        )
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
            self._bigtable_set(offset_key, str(offset).encode())
        except Exception:
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
            bt_key = self._add_partition_prefix_to_key(msg.key, msg.partition)

            if msg.value is None:
                self._del_cache(msg.key)
                self._bigtable_del(bt_key)
            else:
                self._set_cache(msg.key, msg.value)
                self._bigtable_set(bt_key, msg.value)


        for tp, offset in tp_offsets.items():
            self.set_persisted_offset(tp, offset)

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

    def _fill_caches(self, partitions):
        for k, v in self._bigtable_iteritems(partitions=partitions):
            self._set_cache(k, v)

        if self._startup_cache_partitions is not None:
            self._startup_cache_partitions = self._startup_cache_partitions.union(
                partitions
            )
        # Invalidate startup cache after 30 minutes
        # or reset the timer if already running
        if self._startup_cache is not None:
            if self._invalidation_timer is not None:
                self._invalidation_timer.cancel()
                del self._invalidation_timer
                self._invalidation_timer = None
            self._invalidation_timer = threading.Timer(
                30 * 60, self._invalidate_startup_cache
            )
            self._invalidation_timer.start()

    def _get_active_changelogtopic_partitions(
        self, table: CollectionT, tps: Set[TP]
    ) -> Set[int]:
        if self._startup_cache is None:
            return set()

        partitions = set()
        standby_tps = self.app.assignor.assigned_standbys()
        my_topics = table.changelog_topic.topics
        for tp in tps:
            if tp.topic in my_topics and tp not in standby_tps:
                partitions.add(tp.partition)
        return partitions

    async def assign_partitions(
        self, table: CollectionT, tps: Set[TP], generation_id: int = 0
    ) -> None:
        # Fill cache with all keys for the partitions we are assigned
        partitions = self._get_active_changelogtopic_partitions(table, tps)
        if len(partitions) == 0:
            return
        self.log.info(f"Assigning partitions {partitions} for {table.name}")
        self._fill_caches(partitions)

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
        await self.assign_partitions(self.table, newly_assigned, generation_id)

    async def stop(self) -> None:
        if self._mutation_batcher_enable:
            self.log.info("Flushing to bigtable on stop")
            self._mutation_batcher.flush()
