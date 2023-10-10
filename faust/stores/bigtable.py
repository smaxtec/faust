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
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

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

except ImportError:  # pragma: no cover
    BT = None  # noqa

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
        except Exception as ex:
            logging.getLogger(__name__).error(f"Error in Bigtable init {ex}")
            raise ex
        super().__init__(url, app, table, **kwargs)

    @staticmethod
    def default_translator(user_key):
        return user_key

    def _on_mutation_batcher_flushed(self, status):
        self.log.info(
            f"Flushed {len(status)} mutations for {self.table_name}"
        )
        self._mutation_cache.clear()

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
                batch_completed_callback=lambda x: self._on_mutation_batcher_flushed(x),
            )
            self._mutation_cache: Dict[bytes, Union[bytes, None]] = {}

    def _setup_caches(
        self,
        options: Dict[str, Any] = None,
    ):
        self._startup_cache_enable = options.get(
            BigTableStore.BT_STARTUP_CACHE_ENABLE_KEY, False
        )
        if self._startup_cache_enable:
            self._startup_cache: Dict[bytes, bytes] = {}
        else:
            self._startup_cache = None

        self._key_cache_enable = options.get(
            BigTableStore.BT_KEY_CACHE_ENABLE_KEY, False
        )
        if self._key_cache_enable:
            self._key_cache: Set[bytes] = set()
        else:
            self._key_cache = None

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
        partition_bytes, _ = key.rsplit(separator, 1)
        return int(partition_bytes)

    def _active_partitions(self) -> Iterator[int]:
        actives = self.app.assignor.assigned_actives()
        topic = self.table.changelog_topic_name
        for partition in range(self.app.conf.topic_partitions):
            tp = TP(topic=topic, partition=partition)
            # for global tables, keys from all
            # partitions are available.
            if tp in actives or self.table.is_global:
                yield partition

    def _get_all_possible_partitions(self) -> Iterable[Optional[int]]:
        if self.table.is_global or self.table.use_partitioner:
            return [None]
        return list(self._active_partitions())

    def _get_current_partitions(self) -> Iterable[Optional[int]]:
        if self.table.is_global or self.table.use_partitioner:
            return [None]
        event = current_event()
        if event is not None:
            partition = event.message.partition
            return [partition]
        return list(self._active_partitions())

    def _get_possible_bt_keys(self, key: bytes) -> Iterable[bytes]:
        partitions = self._get_current_partitions()
        for partition in partitions:
            yield self._add_partition_prefix_to_key(key, partition)

    @staticmethod
    def bigtable_exrtact_row_data(row_data):
        return list(row_data.to_dict().values())[0][0].value

    def _bigtable_get(
        self, key: bytes, no_key_translation=False
    ) -> Optional[bytes]:
        keys = [key] if no_key_translation else self._get_possible_bt_keys(key)
        if self._mutation_batcher_enable:
            for bt_key in keys:
                value = self._get_mutation(bt_key)
                if value is not None:
                    return value
            self._mutation_batcher.flush()
        for bt_key in keys:
            res = self.bt_table.read_row(bt_key, filter_=self.row_filter)
            if res is not None:
                return self.bigtable_exrtact_row_data(res)
        return None

    def _set_mutation(
        self, key: bytes, row: DirectRow, value: Optional[bytes]
    ):
        self._mutation_batcher.mutate(row)
        self._mutation_cache[key] = value

    def _get_mutation(self, key: bytes) -> Optional[bytes]:
        return self._mutation_cache.get(key, None)

    def _bigtable_del(self, key: bytes, no_key_translation=False):
        if no_key_translation:
            keys = [key]
        else:
            partitions = self._get_all_possible_partitions()
            keys = [
                self._add_partition_prefix_to_key(key, p) for p in partitions
            ]

        for key in keys:
            row = self.bt_table.direct_row(key)
            row.delete()
            if self._mutation_batcher_enable:
                self._set_mutation(key, row, None)
            else:
                row.commit()

    def _bigtable_set(
        self, key: bytes, value: bytes, no_key_translation=False
    ):
        keys = (
            [key]
            if no_key_translation
            else list(self._get_possible_bt_keys(key))
        )
        assert len(keys) == 1
        key = keys[0]
        row = self.bt_table.direct_row(key)

        row.set_cell(
            COLUMN_FAMILY_ID,
            COLUMN_NAME,
            value,
        )

        if self._mutation_batcher_enable:
            self._set_mutation(key, row, value)
        else:
            row.commit()

    def _get(self, key: bytes, invalidate_cache=True) -> Optional[bytes]:
        try:
            if self._startup_cache is not None:
                if key in self._startup_cache:
                    if invalidate_cache:
                        return self._startup_cache.pop(key)
                    else:
                        return self._startup_cache[key]
            return self._bigtable_get(key)
        except Exception as ex:
            self.log.error(
                f"Error in get for table {self.table_name} exception {ex} key {key}"
            )
            raise ex

    def _set(self, key: bytes, value: Optional[bytes]) -> None:
        try:
            if self._key_cache is not None:
                self._key_cache.add(key)
            self._bigtable_set(key, value)
        except Exception as ex:
            self.log.error(
                f"FaustBigtableException Error in set for "
                f"table {self.table_name} exception {ex} key {key=} "
                f"{value=} Traceback: {traceback.format_exc()}"
            )
            raise ex

    def _del(self, key: bytes) -> None:
        try:
            if self._startup_cache is not None:
                self._startup_cache.pop(key, None)
            if self._key_cache is not None:
                self._key_cache.discard(key)
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
                partitions = list(self._active_partitions())
            row_set = RowSet()
            self.log.info(
                f"BigtableStore: Iterating over {len(partitions)} partitions "
                f"for table {self.table_name}"
            )

            need_all_keys = self.table.is_global or self.table.use_partitioner
            if not need_all_keys:
                for partition in partitions:
                    prefix = self._add_partition_prefix_to_key(b"", partition)
                    start_key = prefix + b"\x00"
                    end_key = prefix + b"\xff"

                    row_set.add_row_range_from_keys(
                        start_key=start_key, end_key=end_key
                    )

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
        yield from self._bigtable_iteritems(partitions)

    def _iterkeys(self) -> Iterator[bytes]:
        if self._key_cache is not None:
            for key in self._key_cache:
                yield key
        else:
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
            if self._key_cache is not None and key in self._key_cache:
                return True
            if self._startup_cache is not None and key in self._startup_cache:
                return True
            # We don't want to invalidate the cache here
            # because it is very likely that we will need the value soon
            return self._get(key, invalidate_cache=False) is not None

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
        offset = self._bigtable_get(offset_key, no_key_translation=True)
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
            self._bigtable_set(
                offset_key, str(offset).encode(), no_key_translation=True
            )
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

            if not (self.table.is_global or self.table.use_partitioner):
                key = self._add_partition_prefix_to_key(msg.key, tp.partition)
            else:
                key = msg.key

            if msg.value is None:
                self._bigtable_del(key, no_key_translation=True)
                if self._startup_cache is not None:
                    self._startup_cache.pop(msg.key, None)
                if self._key_cache is not None:
                    self._key_cache.discard(msg.key)
            else:
                self._bigtable_set(key, msg.value, no_key_translation=True)
                if self._startup_cache is not None:
                    self._startup_cache[msg.key] = msg.value
                if self._key_cache is not None:
                    self._key_cache.add(msg.key)

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
            if self._startup_cache is not None:
                self._startup_cache[k] = v
            if self._key_cache is not None:
                self._key_cache.add(k)

    def _get_active_changelogtopic_partitions(
        self, table: CollectionT, tps: Set[TP]
    ) -> Set[int]:
        if self._startup_cache is None and self._key_cache is None:
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
