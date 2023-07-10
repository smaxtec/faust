"""BigTable storage."""
import asyncio
import gc
import logging
import time
import traceback
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


COLUMN_FAMILY_ID = "FaustColumnFamily"
COLUMN_NAME = "DATA"


class BigTableStore(base.SerializedStore):
    """Bigtable table storage."""

    client: BT.Client
    instance: BT.Instance
    bt_table: BT.Table
    _cache: Optional[LRUCache]

    BT_COLUMN_NAME_KEY = "bt_column_name_key"
    BT_INSTANCE_KEY = "bt_instance_key"
    BT_OFFSET_KEY_PREFIX = "bt_offset_key_prefix"
    BT_PROJECT_KEY = "bt_project_key"
    BT_TABLE_NAME_GENERATOR_KEY = "bt_table_name_generator_key"
    BT_VALUE_CACHE_ENABLE_KEY = "bt_value_cache_enable_key"
    BT_MAX_MUTATIONS_PER_FLUSH_KEY = "bt_max_mutations_per_flush_key"

    def __init__(
        self,
        url: Union[str, URL],
        app: AppT,
        table: CollectionT,
        options: Dict[str, Any],
        **kwargs: Any,
    ) -> None:
        self._set_options(options)
        if table.use_partitioner is False:
            raise ValueError(
                "BigTableStore requires a partitioner to be set on the table"
            )
        try:
            self._bigtable_setup(table, options)
        except Exception as ex:
            logging.getLogger(__name__).error(f"Error in Bigtable init {ex}")
            raise ex
        super().__init__(url, app, table, **kwargs)

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
            BigTableStore.BT_OFFSET_KEY_PREFIX, "==>offset_for_partition_"
        )

        if options.get(BigTableStore.BT_VALUE_CACHE_ENABLE_KEY, True):
            # TODO - make this configurable
            self._cache = LRUCache(limit=10_000)
        else:
            self._cache = None

        self._mutation_buffer_size = options.get(
            BigTableStore.BT_MAX_MUTATIONS_PER_FLUSH_KEY, 0
        )

        if self._mutation_buffer_size <= 0:
            self._mutation_buffer = None
        else:
            self._mutation_buffer = {}
            self._num_mutations = 0


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
                f"for {table.name} with {options=}"
            )
            self.bt_table.create(
                column_families={
                    COLUMN_FAMILY_ID: BT.column_family.MaxVersionsGCRule(1)
                }
            )
        else:
            logging.getLogger(__name__).info(
                "BigTableStore: Using existing "
                f"bigtablestore with {self.bt_table_name=} for {table.name} "
                f"with {options=}"
            )

    @staticmethod
    def bigtable_exrtact_row_data(row_data):
        return list(row_data.to_dict().values())[0][0].value

    def _bigtable_get(self, key: bytes) -> Optional[bytes]:
        if self._mutation_buffer is not None:
            if key in self._mutation_buffer:
                return self._mutation_buffer[key][1]

        res = self.bt_table.read_row(key, filter_=self.row_filter)
        if res is None:
            return None
        return self.bigtable_exrtact_row_data(res)

    def _bigtable_mutate(
        self, key: bytes, value: Optional[bytes]
    ):
        row = self.bt_table.direct_row(key)

        if value is None:
            row.delete()
        else:
            row.set_cell(
                COLUMN_FAMILY_ID,
                COLUMN_NAME,
                value,
            )

        if self._mutation_buffer is not None:
            self._mutation_buffer[key] = (row, value)
            self._num_mutations += 1
        else:
            row.commit()

    def _get(self, key: bytes) -> Optional[bytes]:
        try:
            if self._cache is not None:
                if key in self._cache:
                    return self._cache.get(key)

            value = self._bigtable_get(key)

            if self._cache is not None:
                self._cache[key] = value

            if value is not None:
                self.log.info(f"Found value for key in table {key=} {value=}")
                return value
            return None
        except Exception as ex:
            self.log.error(
                f"Error in get for table {self.table_name} exception {ex} key {key}"
            )
            raise ex

    def _set(self, key: bytes, value: Optional[bytes]) -> None:
        try:
            if self._cache is not None:
                self._cache[key] = value

            self._bigtable_mutate(key, value)
        except Exception as ex:
            self.log.error(
                f"FaustBigtableException Error in set for "
                f"table {self.table_name} exception {ex} key {key=} "
                f"{value=} Traceback: {traceback.format_exc()}"
            )
            raise ex

    def _del(self, key: bytes) -> None:
        self._set(key, None)

    def _iteritems(self) -> Iterator[Tuple[bytes, bytes]]:
        try:
            start = time.time()
            offset_key = self.offset_key_prefix.encode()
            for row in self.bt_table.read_rows(filter_=self.row_filter):
                if offset_key in row.row_key:
                    continue

                if self._mutation_buffer is not None:
                    mutation_row, mutation_val = self._mutation_buffer.get(
                        row.row_key, (None, None)
                    )
                    if mutation_val is not None:
                        yield row.row_key, mutation_val

                    if mutation_row is not None:
                        continue

                value = self.bigtable_exrtact_row_data(row)
                yield row.row_key, value
            end = time.time()
            self.log.info(f"{self.table_name} _iteritems took {end - start}s")
        except Exception as ex:
            self.log.error(
                f"FaustBigtableException Error "
                f"in _iteritems for table {self.table_name}"
                f" exception {ex}"
            )
            raise ex

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

            # Check cache
            if self._cache is not None and key in self._cache:
                return self._cache[key] is not None

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
        offset = self._get(offset_key)
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
            self._set(offset_key, str(offset).encode())

            if (
                self._mutation_buffer is not None
                and self._num_mutations > self._mutation_buffer_size
            ):
                mutations = [r[0] for r in self._mutation_buffer.values()]
                response = self.bt_table.mutate_rows(mutations)

                for i, status in enumerate(response):
                    if status.code != 0:
                        raise Exception(f"Failed to commit mutation number {i}")
                    else:
                        self._mutation_buffer.pop(mutations[i].row_key, None)
                        self._num_mutations -= 1

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
            if msg.value is None:
                self._del(msg.key)
            else:
                self._set(msg.key, msg.value)

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
