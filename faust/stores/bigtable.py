"""BigTable storage."""
import logging
from os import wait
import typing
from typing import Any, Callable, Dict, Iterable, Iterator, Optional, Tuple, Union

from mode.utils.collections import LRUCache

from faust.streams import current_event

from google.cloud.bigtable import column_family
from google.cloud.bigtable.client import Client
from google.cloud.bigtable.instance import Instance
from google.cloud.bigtable.row_filters import CellsColumnLimitFilter
from google.cloud.bigtable.table import Table
from yarl import URL

from faust.stores import base
from faust.types import TP, AppT, CollectionT, EventT


def get_current_partition():
    event = current_event()
    assert event is not None
    return event.message.partition


class BigTableStore(base.SerializedStore):
    """Bigtable table storage."""

    client: Client
    instance: Instance
    bt_table: Table

    _key_index: LRUCache[bytes, int]
    PROJECT_KEY = "project_key"
    INSTANCE_KEY = "instance_key"
    BT_TABLE_NAME_GENERATOR_KEY = "bt_table_name_generator_key"
    READ_ROWS_BORDERS_KEY = "bt_read_rows_borders_key"

    def __init__(
        self,
        url: Union[str, URL],
        app: AppT,
        table: CollectionT,
        options: typing.Dict[str, Any],
        **kwargs: Any,
    ) -> None:
        self._key_index = LRUCache(limit=app.conf.table_key_index_size)
        table_name_generator = options.get(
            BigTableStore.BT_TABLE_NAME_GENERATOR_KEY, lambda t: t.name
        )
        self.bt_table_name = table_name_generator(table)

        self.bt_start_key, self.bt_end_key = options.get(
            BigTableStore.READ_ROWS_BORDERS_KEY, [b"", b""]
        )
        try:
            self.client: Client = Client(
                options.get(BigTableStore.PROJECT_KEY),
                admin=True,
            )
            self.instance: Instance = self.client.instance(
                options.get(BigTableStore.INSTANCE_KEY)
            )
            self.row_filter = CellsColumnLimitFilter(1)
            self.column_name = "DATA"
            self._bigtable_setup_table()

            table.use_partitioner = True
        except Exception as ex:
            logging.getLogger(__name__).error(f"Error in Bigtable init {ex}")
            raise ex
        self.offset_key_prefix = "offset_partitiion:"
        super().__init__(url, app, table, **kwargs)

    def _bigtable_setup_table(self):
        self.bt_table: Table = self.instance.table(self.bt_table_name)
        self.column_family_id = "FaustColumnFamily"
        if not self.bt_table.exists():
            logging.getLogger(__name__).info(
                f"BigTableStore: Making new bigtablestore with {self.bt_table_name=}"
            )
            self.bt_table.create(
                column_families={
                    self.column_family_id: column_family.MaxVersionsGCRule(1)
                }
            )
        else:
            logging.getLogger(__name__).info(
                "BigTableStore: Using existing"
                f"bigtablestore with {self.bt_table_name=}"
            )

    def _bigtable_exrtact_row_data(self, row_data):
        return list(row_data.to_dict().values())[0][0].value

    def _get_key_with_partition(self, key: bytes, partition: Optional[int] = None):
        if partition is not None:
            partition_prefix = partition.to_bytes(1, "little")
        else:
            partition_prefix = get_current_partition().to_bytes(1, "little")
        key = b"".join([partition_prefix, key])
        return key

    def _bigtbale_get(self, key: bytes):
        res = self.bt_table.read_row(key, filter_=self.row_filter)
        if res is None:
            return None
        return self._bigtable_exrtact_row_data(res)

    def _bigtbale_set(self, key: bytes, value: Optional[bytes]):
        row = self.bt_table.direct_row(key)
        row.set_cell(
            self.column_family_id,
            self.column_name,
            value,
        )
        row.commit()

    def _partitions_for_key(self, key: bytes) -> Iterable[int]:
        # Returns cached db if key is in index, otherwise all dbs
        # for linear search.
        try:
            return [self._key_index[key]]
        except KeyError:
            return range(self.app.conf.topic_partitions)

    def _bigtbale_del(self, key: bytes):
        row = self.bt_table.direct_row(key)
        row.delete()
        row.commit()

    def _get(self, key: bytes) -> Optional[bytes]:
        try:
            for partition in self._partitions_for_key(key):
                key = self._get_key_with_partition(key, partition=partition)
                key_with_partition = self._get_key_with_partition(key)
                value = self._bigtbale_get(key_with_partition)
                if value is not None:
                    self._key_index[key] = partition
                    return value
            raise KeyError
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
            self._key_index[key] = partition
            key = self._get_key_with_partition(key, partition=partition)

            self._bigtbale_set(key, value)
        except Exception as ex:
            self.log.error(
                f"FaustBigtableException Error in set for "
                f"table {self.table_name} exception {ex} key {key}"
            )
            raise ex

    def _del(self, key: bytes) -> None:
        try:
            for partition in self._partitions_for_key(key):
                key = self._get_key_with_partition(key, partition=partition)
                self._bigtbale_del(key)
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
                    yield (
                        row.row_key.replace(partition_prefix, b""),
                        self._bigtable_exrtact_row_data(row),
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
            event = current_event()
            partition_from_message = (
                event is not None
                and not self.table.is_global
            )
            if partition_from_message:
                key = self._get_key_with_partition(key, partition=event.message.partition)
                res = self.bt_table.read_row(key, filter_=self.row_filter)
                if res is not None:
                    return True
            else:
                for partition in self._partitions_for_key(key):
                    key = self._get_key_with_partition(key, partition=partition)
                    res = self.bt_table.read_row(key, filter_=self.row_filter)
                    if res is not None:
                        return True
            return False
        except Exception as ex:
            self.log.error(
                f"FaustBigtableException Error in _contains for table "
                f"{self.table_name} exception "
                f"{ex} key {key}"
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
            offset = int(self._bigtable_exrtact_row_data(row_res))
            return offset
        return None

    def set_persisted_offset(self, tp: TP, offset: int) -> None:
        """Set the last persisted offset for this table.

        This will remember the last offset that we wrote to BigTableStore,
        so that on rebalance/recovery we can seek past this point
        to only read the events that occurred recently while
        we were not an active replica.
        """
        try:
            offset_key = self.get_offset_key(tp).encode()
            self._bigtbale_set(offset_key, str(offset).encode())
        except Exception as e:
            self.log.error(
                f"Failed to commit offset for {self.table.name}"
                " -> will crash faust app!"
            )
            self.app._crash(e)

    def _persist_changelog_batch(self, row_mutations, tp_offsets):
        response = self.bt_table.mutate_rows(row_mutations)
        for i, status in enumerate(response):
            if status.code != 0:
                self.log.error("Row number {} failed to write".format(i))

        for tp, offset in tp_offsets.items():
            self.set_persisted_offset(tp, offset)

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
