import sys
from unittest.mock import MagicMock, patch

import pytest

import faust
from faust.stores.bigtable import (
    BigTableCacheManager,
    BigTableStore,
    BigTableValueCache,
)


class TestBigTableStore:
    TEST_KEY1 = b"TEST_KEY1"
    TEST_KEY2 = b"TEST_KEY2"
    TEST_KEY3 = b"TEST_KEY3"

    @pytest.fixture()
    def bt_imports(self):
        # We will mock rowsets in a way that it is just a
        # list with all requested keys, so that we then just call
        # read_row of the mocked bigtable multiple times
        class RowSetMock():
            def __init__(self) -> None:
                self.keys = set()
                self.add_row_key = MagicMock(wraps=self._add_row_key)

            def _add_row_key(self, key):
                self.keys.add(key)

        with patch("faust.stores.bigtable.BT") as bt:
            bt.CellsColumnLimitFilter = MagicMock(return_value="a_filter")
            bt.column_family.MaxVersionsGCRule = MagicMock(
                return_value="a_rule"
            )
            bt.RowSet = MagicMock(return_value=RowSetMock())
            yield bt

    @pytest.mark.asyncio
    async def test_bigtable_set_options_default(self, bt_imports):
        self_mock = MagicMock()
        bt_imports.CellsColumnLimitFilter = MagicMock(return_value="a_filter")

        BigTableStore._set_options(self_mock, options={})
        assert self_mock.column_name == "DATA"
        assert self_mock.offset_key_prefix == "offset_partitiion:"
        assert self_mock.row_filter == "a_filter"

    @pytest.mark.asyncio
    async def test_bigtable_set_options(self, bt_imports):
        self_mock = MagicMock()
        bt_imports.CellsColumnLimitFilter = MagicMock(return_value="a_filter")
        bt_imports.column_family = MagicMock(return_value=MagicMock())
        name_lambda = lambda x: print(x)  # noqa
        options = {
            BigTableStore.BT_TABLE_NAME_GENERATOR_KEY: name_lambda,
            BigTableStore.BT_OFFSET_KEY_PREFIX: "offset_test",
            BigTableStore.BT_COLUMN_NAME_KEY: "name_test",
        }
        BigTableStore._set_options(self_mock, options)
        assert self_mock.column_name == "name_test"
        assert self_mock.offset_key_prefix == "offset_test"
        assert self_mock.row_filter == "a_filter"
        assert self_mock.table_name_generator == name_lambda

    @pytest.mark.asyncio
    async def test_bigtable_setup(self, bt_imports):
        self_mock = MagicMock()

        faust_table_mock = MagicMock()
        faust_table_mock.name = MagicMock(return_value="ABC")

        def table_name_gen(table):
            return table.name[::-1]

        self_mock.table_name_generator = table_name_gen
        self_mock.bt_table_name = self_mock.table_name_generator(
            faust_table_mock
        )

        client_mock = MagicMock()
        instance_mock = MagicMock()
        table_mock = MagicMock()

        client_mock.instance = MagicMock(return_value=instance_mock)
        instance_mock.table = MagicMock(return_value=table_mock)
        table_mock.exists = MagicMock(return_value=True)
        table_mock.create = MagicMock()

        bt_imports.Client = MagicMock(return_value=client_mock)
        options = {}
        options[BigTableStore.BT_INSTANCE_KEY] = "bt_instance"
        options[BigTableStore.BT_PROJECT_KEY] = "bt_project"

        return_value = BigTableStore._bigtable_setup(
            self_mock, faust_table_mock, options
        )
        bt_imports.Client.assert_called_once_with(
            options[BigTableStore.BT_PROJECT_KEY], admin=True
        )
        client_mock.instance.assert_called_once_with(
            options[BigTableStore.BT_INSTANCE_KEY]
        )

        instance_mock.table.assert_called_once_with(self_mock.bt_table_name)
        table_mock.create.assert_not_called()
        assert self_mock.column_family_id == "FaustColumnFamily"
        assert return_value is None

        # Test with no existing table
        self_mock.reset_mock()
        self_mock.table_name_generator = table_name_gen
        self_mock.bt_table_name = self_mock.table_name_generator(
            faust_table_mock
        )
        table_mock.exists = MagicMock(return_value=False)
        return_value = BigTableStore._bigtable_setup(
            self_mock, faust_table_mock, options
        )
        instance_mock.table.assert_called_once_with(self_mock.bt_table_name)
        table_mock.create.assert_called_once_with(
            column_families={self_mock.column_family_id: "a_rule"}
        )
        assert self_mock.column_family_id == "FaustColumnFamily"
        assert return_value is None

    @pytest.fixture()
    def store(self, bt_imports):
        class BigTableMock:
            def __init__(self) -> None:
                self.data = {}
                self.read_row = MagicMock(wraps=self._read_row)
                self.read_rows = MagicMock(wraps=self._read_rows)

            def _read_row(self, key: bytes, **kwargs):
                res = self.data.get(key, None)
                cell_wrapper = MagicMock()
                cell_wrapper.value = res
                row_wrapper = [cell_wrapper]
                if res is None:
                    return res
                row = MagicMock()
                row.row_key = key
                row.to_dict = MagicMock(return_value={"x": row_wrapper})
                return row

            def _read_rows(self, row_set, **kwargs):
                for k in row_set.keys:
                    res = self._read_row(k)
                    if res is None:
                        continue
                    else:
                        yield res

            def add_test_data(self, keys):
                for k in keys:
                    self.data[k] = k

        with patch("faust.stores.bigtable.BT", bt_imports):
            options = {}
            options[BigTableStore.BT_INSTANCE_KEY] = "bt_instance"
            options[BigTableStore.BT_PROJECT_KEY] = "bt_project"
            store = BigTableStore(
                "bigtable://", MagicMock(), MagicMock(), options=options
            )
            store.bt_table = BigTableMock()
            return store

    def test_bigtable_bigtable_get_on_empty(self, store):
        store._cache.get = MagicMock(return_value=None)
        return_value = store._bigtable_get(self.TEST_KEY1)
        store.bt_table.read_row.assert_called_once_with(
            self.TEST_KEY1, filter_="a_filter"
        )
        store._cache.get.assert_called_once_with(self.TEST_KEY1)
        assert return_value is None

    def test_bigtable_bigtable_get_cache_miss(self, store):
        store._cache.get = MagicMock(return_value=None)
        store.bt_table.add_test_data([self.TEST_KEY1])
        return_value = store._bigtable_get(self.TEST_KEY1)
        store._cache.get.assert_called_once_with(self.TEST_KEY1)
        store.bt_table.read_row.assert_called_once_with(
            self.TEST_KEY1, filter_="a_filter"
        )
        assert return_value == self.TEST_KEY1

    def test_bigtable_bigtable_get_cache_hit(self, store):
        store.bt_table.add_test_data([self.TEST_KEY1])
        store._cache.get = MagicMock(return_value=b"cache_res")
        return_value = store._bigtable_get(self.TEST_KEY1)
        store._cache.get.assert_called_once_with(self.TEST_KEY1)
        store.bt_table.read_row.assert_not_called()
        assert return_value == b"cache_res"

    def test_bigtable_get_range_cache_miss(self, store):
        store._cache.get = MagicMock(return_value=None)

        test_keys_in = [self.TEST_KEY1, self.TEST_KEY3] # order is important
        test_keys_not_in = {self.TEST_KEY2, }

        return_value = store._bigtable_get_range(test_keys_not_in)
        store.bt_table.read_rows.assert_called()
        store.bt_table.read_rows.reset_mock()
        assert return_value == (None, None)

        store.bt_table.add_test_data(test_keys_in)
        return_value = store._bigtable_get_range(test_keys_in)
        store.bt_table.read_rows.assert_called()
        store.bt_table.read_rows.reset_mock()
        assert return_value == (self.TEST_KEY1, self.TEST_KEY1)

    def test_bigtable_get_range_cache_hit(self, store):
        store._cache.get = MagicMock(return_value="cache_res")
        result_value = store._bigtable_get_range([self.TEST_KEY1, self.TEST_KEY3])
        store.bt_table.read_rows.assert_not_called
        assert result_value == (self.TEST_KEY1, "cache_res")

    def test_bigtable_contains(self, store):
        store._cache.contains = MagicMock(return_value=None)
        store._cache.delete = MagicMock(return_value=None)

        store.bt_table.add_test_data([self.TEST_KEY1])
        return_value = store._bigtable_contains(self.TEST_KEY1)
        store.bt_table.read_row.assert_called_with(self.TEST_KEY1, filter_="a_filter")
        store._cache.delete.assert_not_called()
        assert return_value is True

        return_value = store._bigtable_contains(self.TEST_KEY2)
        store.bt_table.read_row.assert_called_with(self.TEST_KEY2, filter_="a_filter")
        store._cache.delete.assert_called_with(self.TEST_KEY2)

        store._cache.delete.reset_mock()
        store.bt_table.read_row.reset_mock()

        store._cache.contains = MagicMock(return_value=True)
        return_value = store._bigtable_contains(self.TEST_KEY1)
        store.bt_table.read_row.assert_not_called()
        store._cache.delete.assert_not_called()
        assert return_value is True

        store._cache.contains = MagicMock(return_value=False)
        return_value = store._bigtable_contains(self.TEST_KEY1)
        store.bt_table.read_row.assert_not_called()
        store._cache.delete.assert_not_called()
        assert return_value is False

    def test_bigtable_contains_any(self, store):
        store.bt_table.add_test_data([self.TEST_KEY1])
        store._cache.contains_any = MagicMock(return_value=None)

        test_keys_in = {self.TEST_KEY1, self.TEST_KEY3}
        test_keys_not_in = {self.TEST_KEY2, }

        return_value = store._bigtable_contains_any(test_keys_not_in)
        store.bt_table.read_rows.assert_called()
        store.bt_table.read_rows.reset_mock()
        assert return_value is False

        return_value = store._bigtable_contains_any(test_keys_in)
        store.bt_table.read_rows.assert_called()
        store.bt_table.read_rows.reset_mock()
        assert return_value is True

        store._cache.contains_any = MagicMock(return_value=True)
        return_value = store._bigtable_contains_any(test_keys_not_in)
        store.bt_table.read_rows.assert_not_called()
        assert return_value == store._cache.contains_any()

    def test_bigtable_delete(self, store):
        row_mock = MagicMock()
        row_mock.commit = MagicMock()
        row_mock.delete = MagicMock()
        store.bt_table.direct_row = MagicMock(return_value=row_mock)
        store._cache.delete = MagicMock(return_value=None)

        store._bigtable_del(self.TEST_KEY1)

        store.bt_table.direct_row.assert_called_once_with(self.TEST_KEY1)
        store._cache.delete.assert_called_once_with(self.TEST_KEY1)
        row_mock.delete.assert_called_once()
        row_mock.commit.assert_called_once()

    def test_bigtable_set(self, store):
        row_mock = MagicMock()
        row_mock.set_cell = MagicMock()
        row_mock.commit = MagicMock()
        store.bt_table.direct_row = MagicMock(return_value=row_mock)
        store._cache.set = MagicMock(return_value=None)

        store._bigtable_set(self.TEST_KEY1, self.TEST_KEY1)
        store._bigtable_set(self.TEST_KEY1, self.TEST_KEY1, persist_offset=True)

        store.bt_table.direct_row.assert_called_once_with(self.TEST_KEY1)
        store._cache.set.assert_called_once_with(self.TEST_KEY1, self.TEST_KEY1)
        row_mock.set_cell.assert_called_once_with(
            store.column_family_id,
            store.column_name,
            self.TEST_KEY1,
        )
        row_mock.commit.assert_called_once()

    def test_maybe_get_partition_from_message(self, store):
        event_mock = MagicMock()
        event_mock.message = MagicMock()
        event_mock.message.partition = 69
        current_event_mock = MagicMock(return_value=event_mock)

        store.table.is_global = False
        store.table.use_partitioner = False
        with patch("faust.stores.bigtable.current_event", current_event_mock):
            return_value = store._maybe_get_partition_from_message()
            assert return_value == 69

        store.table.is_global = True
        with patch("faust.stores.bigtable.current_event", current_event_mock):
            return_value = store._maybe_get_partition_from_message()
            assert return_value is None

        store.table.is_global = False
        current_event_mock = MagicMock(return_value=None)
        with patch("faust.stores.bigtable.current_event", current_event_mock):
            return_value = store._maybe_get_partition_from_message()
            assert return_value is None
