"""BigTable storage."""
import logging
import typing
from typing import Any, Iterator, Optional, Tuple, Union

from google.cloud.bigtable import column_family
from google.cloud.bigtable.client import Client
from google.cloud.bigtable.instance import Instance
from google.cloud.bigtable.row_filters import CellsColumnLimitFilter
from google.cloud.bigtable.table import Table
from yarl import URL

from faust.stores import base
from faust.types import TP, AppT, CollectionT


class BigTableStore(base.SerializedStore):
    """Bigtable table storage."""

    client: Client
    instance: Instance
    bt_table: Table
    PROJECT_KEY = "project_key"
    INSTANCE_KEY = "instance_key"
    TABLE_NAME_KEY = "table_name_key"

    def __init__(
        self,
        url: Union[str, URL],
        app: AppT,
        table: CollectionT,
        options: typing.Dict[str, Any],
        **kwargs: Any,
    ) -> None:
        self.table_name = table.name
        try:
            logging.getLogger(__name__).error(
                f"BigTableStore: Making bigtablestore with {self.table_name=}"
            )
            self.client = Client(
                options.get(BigTableStore.PROJECT_KEY),
            )
            self.instance = self.client.instance(
                options.get(BigTableStore.INSTANCE_KEY)
            )

            self.bt_table_name = options.get(BigTableStore.TABLE_NAME_KEY)
            self.bt_table = self.instance.table(self.bt_table_name)
            column_family_id = "FaustColumnFamily"
            self.column_family = self.bt_table.column_family(
                column_family_id,
                gc_rule=column_family.MaxVersionsGCRule(1),
            )
            self.column_name = "DATA"

            table.use_partitioner = True
        except Exception as ex:
            self.log.error(f"Error configuring bigtable client {ex}")
            raise ex
        super().__init__(url, app, table, **kwargs)

    def bigtable_extract_row_data(self, row_data):
        return list(row_data.to_dict().values())[0][0].value

    def get_bigtable_key(self, key: bytes) -> bytes:
        decoded_key = key.decode("utf-8")
        return f"{self.table_name}_{decoded_key}".encode("utf-8")

    def get_access_key(self, bt_key: bytes) -> bytes:
        return bt_key.decode("utf-8").removeprefix(f"{self.table_name}_").encode("utf-8")

    def _get(self, key: bytes) -> Optional[bytes]:
        filter = CellsColumnLimitFilter(1)
        try:
            bt_key = self.get_bigtable_key(key)
            res = self.bt_table.read_row(
                bt_key,
                filter_=filter,
            )
            self.log.info(
                f"[Bigtable]: _get with {key=} (={bt_key.decode('utf-8')}) -> {bt_key=} (={bt_key.decode('utf-8')})"
            )
            if res is None:
                self.log.warning(
                    f"[Bigtable] KeyError in _get with {key=} (={bt_key.decode('utf-8')}) -> {bt_key=} (={bt_key.decode('utf-8')})"
                )
                raise KeyError(f"row {key} not found in bigtable {self.table=}")
            return self.bigtable_extract_row_data(res)
        except ValueError as ex:
            self.log.debug(f"key not found {key} exception {ex}")
            raise KeyError(f"key not found {key}")
        except Exception as ex:
            self.log.error(
                f"Error in get for table {self.table_name} exception {ex} key {key}"
            )
            raise ex

    def _set(self, key: bytes, value: Optional[bytes]) -> None:
        try:
            bt_key = self.get_bigtable_key(key)
            row = self.bt_table.direct_row(bt_key)
            row.set_cell(self.column_family.column_family_id, self.column_name, value)
            row.commit()
            self.log.info(
                f"[Bigtable]: _set with {key=} (={bt_key.decode('utf-8')}) -> {bt_key=} (={bt_key.decode('utf-8')})"
            )
        except Exception as ex:
            self.log.error(
                f"FaustBigtableException Error in set for "
                f"table {self.table_name} exception {ex} key {key}"
            )
            raise ex

    def _del(self, key: bytes) -> None:
        try:
            bt_key = self.get_bigtable_key(key)
            row = self.bt_table.direct_row(bt_key)
            row.delete()
            row.commit()
            self.log.info(
                f"[Bigtable]: _del with {key=} (={bt_key.decode('utf-8')}) -> {bt_key=} (={bt_key.decode('utf-8')})"
            )
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

    def _iteritems(self) -> Iterator[Tuple[bytes, bytes]]:
        try:
            end_key_str = self.table_name[:-1] + chr(ord(self.table_name[-1]) + 1)
            end_key = end_key_str.encode("utf-8")
            start_key = self.table_name.encode("utf-8")
            for row in self.bt_table.read_rows(
                start_key=start_key, end_key=end_key
            ):
                yield (
                    self.get_access_key(row.row_key),
                    self.bigtable_extract_row_data(row),
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
            for k in self._iterkeys():
                if k == key:
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

    def persisted_offset(self, tp: TP) -> Optional[int]:
        """Return the persisted offset.

        This always returns :const:`None` when using the bigtable store.
        """
        return None

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


class BigTableStoreTest(BigTableStore):
    def __init__(
        self,
        options: Optional[typing.Dict[str, Any]] = None,
    ) -> None:
        try:
            self.table_name = "TESTMEPLS"
            self.client = Client(options.get(BigTableStore.PROJECT_KEY))
            self.instance = self.client.instance(
                options.get(BigTableStore.INSTANCE_KEY)
            )

            self.bt_table_name = options.get(BigTableStore.TABLE_NAME_KEY)
            self.bt_table = self.instance.table(self.bt_table_name)
            column_family_id = "FaustColumnFamily"
            self.column_family = self.bt_table.column_family(
                column_family_id,
                gc_rule=column_family.MaxVersionsGCRule(1),
            )
            self.column_name = "DATA"

        except Exception as ex:
            logging.getLogger(__name__).error(f"Error configuring bigtable client {ex}")
            raise ex

    def bigtable_extract_row_data(self, row_data):
        return list(row_data.to_dict().values())[0][0].value

    def get_bigtable_key(self, key: bytes) -> bytes:
        decoded_key = key.decode("utf-8")
        return bytes(f"{self.table_name}_{decoded_key}", encoding="utf-8")

    def get_access_key(self, bt_key: bytes) -> bytes:
        return bytes(
            bt_key.decode("utf-8").removeprefix(f"{self.table_name}_"), encoding="utf-8"
        )

    def _get(self, key: bytes) -> Optional[bytes]:
        filter = CellsColumnLimitFilter(1)
        try:
            bt_key = self.get_bigtable_key(key)
            res = self.bt_table.read_row(
                bt_key,
                filter_=filter,
            )
            print(
                f"[Bigtable]: _get with {key=} (={bt_key.decode('utf-8')}) -> {bt_key=} (={bt_key.decode('utf-8')})"
            )
            if res is None:
                print(
                    f"[Bigtable] KeyError in _get with {key=} (={bt_key.decode('utf-8')}) -> {bt_key=} (={bt_key.decode('utf-8')})"
                )
                raise KeyError(f"row {key} not found in bigtable {self.table=}")
            return self.bigtable_extract_row_data(res)
        except ValueError as ex:
            print(f"key not found {key} exception {ex}")
            raise KeyError(f"key not found {key}")
        except Exception as ex:
            print(
                f"Error in get for table {self.table_name} exception {ex} key {key}"
            )
            raise ex

    def _set(self, key: bytes, value: Optional[bytes]) -> None:
        try:
            bt_key = self.get_bigtable_key(key)
            row = self.bt_table.direct_row(bt_key)
            row.set_cell(self.column_family.column_family_id, self.column_name, value)
            row.commit()
            print(
                f"[Bigtable]: _set with {key=} (={bt_key.decode('utf-8')}) -> {bt_key=} (={bt_key.decode('utf-8')})"
            )
        except Exception as ex:
            self.log.error(
                f"FaustBigtableException Error in set for "
                f"table {self.table_name} exception {ex} key {key}"
            )
            raise ex

    def _del(self, key: bytes) -> None:
        try:
            bt_key = self.get_bigtable_key(key)
            row = self.bt_table.direct_row(bt_key)
            print(
                f"[Bigtable]: _del with {key=} (={bt_key.decode('utf-8')}) -> {bt_key=} (={bt_key.decode('utf-8')})"
            )
            row.delete()
            row.commit()
        except Exception as ex:
            print(
                f"FaustBigtableException Error in delete for "
                f"table {self.table_name} exception {ex} key {key}"
            )
            raise ex

    def _iterkeys(self) -> Iterator[bytes]:
        try:
            for row in self._iteritems():
                yield row[0]
        except Exception as ex:
            print(
                f"FaustBigtableException Error in _iterkeys "
                f"for table {self.table_name} exception {ex}"
            )
            raise ex

    def _itervalues(self) -> Iterator[bytes]:
        try:
            for row in self._iteritems():
                yield row[1]
        except Exception as ex:
            print(
                f"FaustBigtableException Error "
                f"in _itervalues for table {self.table_name}"
                f" exception {ex}"
            )
            raise ex

    def _iteritems(self) -> Iterator[Tuple[bytes, bytes]]:
        try:
            end_key_str = self.table_name[:-1] + chr(ord(self.table_name[-1]) + 1)
            end_key = end_key_str.encode("utf-8")
            start_key = self.table_name.encode("utf-8")
            for row in self.bt_table.read_rows(
                start_key=start_key, end_key=end_key
            ):
                yield (
                    self.get_access_key(row.row_key),
                    self.bigtable_extract_row_data(row),
                )
        except Exception as ex:
            print(
                f"FaustBigtableException Error "
                f"in _iteritems for table {self.table_name}"
                f" exception {ex}"
            )
            raise ex


if __name__ == "__main__":
    options = {
        BigTableStoreTest.PROJECT_KEY: "smaxtec-system",
        BigTableStoreTest.INSTANCE_KEY: "faust-cache-test",
        BigTableStoreTest.TABLE_NAME_KEY: "sxfaust_cache",
    }
    key = "aaaa_123_bbbb".encode("utf-8")
    store = BigTableStoreTest(options)
    pass
