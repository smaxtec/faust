import time
from unittest.mock import MagicMock, call, patch
from mode.utils.collections import LRUCache

import pytest

import faust
from faust.stores.bigtable import (
    BigTableCacheManager,
    BigTableStore,
    BigTableValueCache,
)
from faust.types.tuples import TP


class TestResponse:
    def __init__(self, code) -> None:
        self.code = code


class RowSetMock:
    # We will mock rowsets in a way that it is just a
    # list with all requested keys, so that we then just call
    # read_row of the mocked bigtable multiple times
    def __init__(self) -> None:
        self.keys = set()
        self.add_row_key = MagicMock(wraps=self._add_row_key)
        self.add_row_range_from_keys = MagicMock(
            wraps=self._add_row_range_from_keys
        )

    def _add_row_key(self, key):
        self.keys.add(key)

    def _add_row_range_from_keys(self, start_key: bytes, end_key: bytes):
        if isinstance(start_key, str):
            start_key = start_key.encode()
        if isinstance(end_key, str):
            end_key = end_key.encode()
        self.keys.add(b"".join([start_key, b"_*_", end_key]))


class BigTableMock:
    def __init__(self) -> None:
        self.data = {}
        self.read_row = MagicMock(wraps=self._read_row)
        self.read_rows = MagicMock(wraps=self._read_rows)
        self.name = "test_bigtable"

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
            res = None
            if b"_*_" in k:
                for key in self.data.keys():
                    start, end = k.split(b"_*_")
                    if start <= key < end:
                        yield self._read_row(key)
                continue
            else:
                res = self._read_row(k)
                if res is None:
                    continue
                else:
                    yield res

    def add_test_data(self, keys):
        for k in keys:
            self.data[k] = k


class TestBigTableValueCache:
    def test_init(self):
        # Test defaults
        cache = BigTableValueCache()
        assert cache.data == {}
        assert cache.ttl == -1
        assert cache.ttl_over is False

        # Test with custom size
        cache = BigTableValueCache(size=123)
        assert isinstance(cache.data, LRUCache)
        assert cache.data.limit == 123

    def test__set_del_len_and_getitem(self):
        cache = BigTableValueCache()
        # Scenario ttl not over and no clear
        cache._maybe_ttl_clear = MagicMock()
        assert len(cache) == 0
        cache["123"] = 123
        assert cache._maybe_ttl_clear.call_count == 1
        assert len(cache) == 1
        assert cache["123"] == 123
        assert cache._maybe_ttl_clear.call_count == 2
        del cache["123"]
        assert cache._maybe_ttl_clear.call_count == 2
        assert len(cache) == 0

    def test__set_del_len_and_getitem_after_tttl(self):
        cache = BigTableValueCache()
        # Scenario ttl over and clear
        cache._maybe_ttl_clear = MagicMock()
        cache.ttl_over = True
        assert len(cache) == 0
        cache["123"] = 123
        assert cache._maybe_ttl_clear.call_count == 1
        assert len(cache) == 0
        assert "123" not in cache.keys()
        assert cache._maybe_ttl_clear.call_count == 1
        del cache["123"]
        assert cache._maybe_ttl_clear.call_count == 1
        assert len(cache) == 0

    def test_maybe_ttl_clear(self):
        time.time = MagicMock(return_value=0)
        cache = BigTableValueCache(ttl=5)
        assert cache.init_ts == 0

        cache._maybe_ttl_clear()
        assert cache.ttl_over is False  # Nothing cleared

        time.time.return_value = 5
        cache._maybe_ttl_clear()
        assert cache.ttl_over is False  # Nothing cleared, edge case

        time.time.return_value = 6
        cache._maybe_ttl_clear()
        assert cache.ttl_over is True  # Nothing cleared, edge case


class TestBigTableCacheManager:
    def test_default__init__(self):
        bigtable_mock = BigTableMock()
        app_mock = MagicMock()
        app_mock.conf = MagicMock()
        app_mock.conf.table_key_index_size = 123
        time.time = MagicMock(return_value=0)

        test_manager = BigTableCacheManager(MagicMock(), {}, bigtable_mock)
        assert test_manager.bt_table == bigtable_mock
        assert test_manager.is_complete is False
        assert test_manager._value_cache is None
        assert test_manager._mut_freq == 0
        assert test_manager._last_flush == {}
        assert test_manager._mutations == {}
        assert test_manager._filled_partitions == set()

    def test_iscomplete__init__(self):
        bigtable_mock = BigTableMock()
        app_mock = MagicMock()
        app_mock.conf = MagicMock()
        app_mock.conf.table_key_index_size = 2
        time.time = MagicMock(return_value=0)
        options = {
            BigTableStore.VALUE_CACHE_ENABLE_KEY: True,
        }

        test_manager = BigTableCacheManager(
            MagicMock(), options, bigtable_mock
        )
        assert test_manager.bt_table == bigtable_mock
        assert test_manager.is_complete is True
        assert isinstance(test_manager._value_cache, BigTableValueCache)
        assert test_manager._mut_freq == 0
        assert test_manager._last_flush == {}
        assert test_manager._mutations == {}
        assert test_manager._filled_partitions == set()

    @pytest.fixture()
    def bt_imports(self):
        with patch("faust.stores.bigtable.BT") as bt:
            bt.CellsColumnLimitFilter = MagicMock(return_value="a_filter")
            bt.column_family.MaxVersionsGCRule = MagicMock(
                return_value="a_rule"
            )
            bt.RowSet = MagicMock(return_value=RowSetMock())
            yield bt

    @pytest.fixture()
    def manager(self, bt_imports):
        with patch("faust.stores.bigtable.BT", bt_imports):
            with patch(
                "faust.stores.bigtable.time.time", MagicMock(return_value=0)
            ):
                bigtable_mock = BigTableMock()
                app_mock = MagicMock()
                app_mock.conf = MagicMock()
                app_mock.conf.table_key_index_size = 123

                options = {
                    BigTableStore.VALUE_CACHE_ENABLE_KEY: True,
                    BigTableStore.BT_MUTATION_FREQ_KEY: 600,
                }
                manager = BigTableCacheManager(
                    MagicMock(), options, bigtable_mock
                )
                manager._partition_cache = {}
                return manager

    def test_fill_if_empty(self, manager):
        key = b"\x13AAA"
        manager.bt_table.add_test_data({key})
        # Scenario 1: Everything empty
        manager._fill_if_empty({key})
        assert manager.bt_table.read_rows.call_count == 1
        assert manager._filled_partitions == {19}

        manager._fill_if_empty({key})
        assert manager.bt_table.read_rows.call_count == 1
        assert manager._filled_partitions == {19}

        manager._fill_if_empty({b"\x10XXX"})
        assert manager.bt_table.read_rows.call_count == 2
        assert manager._filled_partitions == {19, 16}
        assert manager.contains(key)

    def test_fill_if_empty_with_mutation(self, manager):
        key = b"\x13AAA"
        manager.bt_table.add_test_data({key})
        manager._mutations = {key: (MagicMock(), "some_row_mutation")}
        manager._fill_if_empty({key})
        assert manager.contains(key)
        assert manager.get(key) == "some_row_mutation"

    def test_get(self, manager):
        # Adding the key here is sufficient, because the cache gets filled
        key_in = b"\x13AAA"
        key_not_in = b"\x13BBB"
        manager.bt_table.add_test_data({key_in})

        manager._fill_if_empty = MagicMock(wraps=manager._fill_if_empty)

        res = manager.get(key_in)
        manager._fill_if_empty.assert_called_once_with({key_in})
        assert res == key_in

        res = manager.get(key_not_in)
        manager._fill_if_empty.assert_called_with({key_not_in})
        assert res is None

        manager._value_cache = None
        res = manager.get(key_in)
        manager._fill_if_empty.assert_called_with({key_in})
        assert res is None

    def test_set(self, manager):
        manager._set_mutation = MagicMock()
        key_1 = b"\x13AAA"
        key_2 = b"\x13ABB"
        manager.set(key_1, key_1)
        manager._set_mutation.assert_called_once_with(key_1, key_1)
        assert manager.contains(key_1)
        assert manager.contains(key_2) is False

        manager.set(key_2, key_2)
        manager._set_mutation.assert_called_with(key_2, key_2)
        assert manager.contains(key_1)
        assert manager.contains(key_2)
        assert manager.get(key_1) == key_1
        assert manager.get(key_2) == key_2

    def test_delete(self, manager):
        manager._set_mutation = MagicMock()
        key_1 = b"\x13AAA"
        key_2 = b"\x13ABB"
        manager.set(key_1, key_1)
        assert manager.contains(key_1)
        manager.delete(key_1)
        manager._set_mutation.assert_called_with(key_1, None)
        assert not manager.contains(key_1)
        manager.delete(key_2)
        manager._set_mutation.assert_called_with(key_2, None)

    def test_partition_cache(self, manager):
        key = b"aaa"
        with pytest.raises(KeyError):
            manager.get_partition(key)
        manager.set_partition(key, 13)
        assert manager.get_partition(key) == 13
        manager.set_partition(key, 15)
        assert manager.get_partition(key) == 15

    def test_contains(self, manager):
        # Adding the key here is sufficient, because the cache gets filled
        key_in = b"\x13AAA"
        key_not_in = b"\x13BBB"
        manager.bt_table.add_test_data({key_in})
        manager._fill_if_empty = MagicMock(wraps=manager._fill_if_empty)

        manager.is_complete = True
        assert manager.contains(key_in) is True
        manager._fill_if_empty.assert_called_with({key_in})
        assert manager.contains(key_not_in) is False
        manager._fill_if_empty.assert_called_with({key_not_in})

        manager.is_complete = False
        assert manager.contains(key_in) is True
        manager._fill_if_empty.assert_called_with({key_in})
        assert manager.contains(key_not_in) is None
        manager._fill_if_empty.assert_called_with({key_not_in})

        manager._value_cache = None
        assert manager.contains(key_in) is None
        manager._fill_if_empty.assert_called_with({key_in})
        assert manager.contains(key_not_in) is None
        manager._fill_if_empty.assert_called_with({key_not_in})

    def test_contains_any(self, manager):
        # Adding the key here is sufficient, because the cache gets filled
        key_in = b"\x13AAA"
        key_not_in = b"\x13BBB"
        manager.bt_table.add_test_data({key_in})
        manager._fill_if_empty = MagicMock(wraps=manager._fill_if_empty)

        manager.is_complete = True
        assert manager.contains_any({key_in, key_not_in}) is True
        manager._fill_if_empty.assert_called_with({key_in, key_not_in})
        assert manager.contains_any({key_not_in}) is False
        manager._fill_if_empty.assert_called_with({key_not_in})

        manager.is_complete = False
        assert manager.contains_any({key_in, key_not_in}) is True
        manager._fill_if_empty.assert_called_with({key_in, key_not_in})
        assert manager.contains_any({key_not_in}) is None
        manager._fill_if_empty.assert_called_with({key_not_in})

        manager._value_cache = None
        assert manager.contains_any({key_in, key_not_in}) is None
        manager._fill_if_empty.assert_called_with({key_in, key_not_in})
        assert manager.contains_any({key_not_in}) is None
        manager._fill_if_empty.assert_called_with({key_not_in})

    def test_flush_if_timer_over(self, manager):
        tp = TP("a_topic", partition=19)
        tp2 = TP("a_topic", partition=0)
        time.time = MagicMock(return_value=0)
        manager.bt_table.mutate_rows = MagicMock(
            return_value=[TestResponse(404)]
        )

        row_mock = MagicMock()
        row_mock.row_key = b"\x13AAA"
        manager._mutations = {
            row_mock.row_key: (row_mock, "some_row_mutation")
        }

        with patch(
            "faust.stores.bigtable.time.time", MagicMock(return_value=0)
        ):
            assert manager.flush_if_timer_over(tp) is True
            assert manager._last_flush == {tp.partition: 0}
            assert manager.flush_if_timer_over(tp) is False


        with patch(
            "faust.stores.bigtable.time.time",
            MagicMock(return_value=manager._mut_freq),
        ):
            assert manager.flush_if_timer_over(tp2) is True
            assert manager._last_flush == {
                tp2.partition: manager._mut_freq,
                tp.partition: 0,
            }

            assert manager.flush_if_timer_over(tp) is True
            assert len(manager._mutations) == 1  # Not dropped, due to ERR. 404
            assert manager._last_flush == {
                tp2.partition: manager._mut_freq,
                tp.partition: manager._mut_freq,
            }
            assert manager.flush_if_timer_over(tp) is False

        manager._last_flush = {}
        manager.bt_table.mutate_rows = MagicMock(
            return_value=[TestResponse(0)]
        )

        with patch(
            "faust.stores.bigtable.time.time",
            MagicMock(return_value=manager._mut_freq),
        ):
            assert manager.flush_if_timer_over(tp) is True
            assert manager._last_flush == {tp.partition: manager._mut_freq}
            assert len(manager._mutations) == 0

    def test_flush_if_timer_over_on_max_count(self, manager):
        tp = TP("a_topic", partition=19)
        row_mock = MagicMock()
        row_mock.row_key = b"\x13AAA"
        manager._mutations = {
            row_mock.row_key: (row_mock, "some_row_mutation")
        }
        manager._max_mutations = 1
        manager._last_flush = {tp.partition: 999999999999}
        manager.bt_table.mutate_rows = MagicMock(
            return_value=[TestResponse(0)]
        )
        with patch(
            "faust.stores.bigtable.time.time", MagicMock(return_value=0)
        ):
            assert manager.flush_if_timer_over(tp) is True

    def test_set_mutation(self, manager):
        row_mock = MagicMock()
        row_mock.delete = MagicMock()
        row_mock.set_cell = MagicMock()
        row_mock.row_key = b"\x13AAA"

        manager.bt_table.direct_row = MagicMock(return_value=row_mock)

        assert len(manager._mutations) == 0
        manager._set_mutation(row_mock.row_key, "new_value")
        manager.bt_table.direct_row.assert_called_once_with(row_mock.row_key)
        row_mock.set_cell.assert_called_once_with(
            "FaustColumnFamily", "DATA", "new_value"
        )
        assert manager._mutations[row_mock.row_key][1] == "new_value"
        assert len(manager._mutations) == 1

        manager._set_mutation(row_mock.row_key, None)
        row_mock.delete.assert_called_once()
        assert manager._mutations[row_mock.row_key][1] is None
        assert len(manager._mutations) == 1


class TestBigTableStore:
    TEST_KEY1 = b"TEST_KEY1"
    TEST_KEY2 = b"TEST_KEY2"
    TEST_KEY3 = b"TEST_KEY3"

    @pytest.fixture()
    def bt_imports(self):
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
        with patch("faust.stores.bigtable.BT", bt_imports):
            options = {}
            options[BigTableStore.BT_INSTANCE_KEY] = "bt_instance"
            options[BigTableStore.BT_PROJECT_KEY] = "bt_project"
            options[BigTableStore.VALUE_CACHE_ENABLE_KEY] = True
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

        test_keys_in = [self.TEST_KEY1, self.TEST_KEY3]  # order is important
        test_keys_not_in = {
            self.TEST_KEY2,
        }

        return_value = store._bigtable_get_range(test_keys_not_in)
        store.bt_table.read_rows.assert_called()
        store.bt_table.read_rows.reset_mock()
        assert return_value == (None, None)

        store.bt_table.add_test_data(test_keys_in)
        return_value = store._bigtable_get_range(test_keys_in)
        store.bt_table.read_rows.assert_called()
        store.bt_table.read_rows.reset_mock()
        assert return_value == (
            self.TEST_KEY1,
            self.TEST_KEY1,
        ) or return_value == ((self.TEST_KEY3, self.TEST_KEY3))

    def test_bigtable_get_range_cache_hit(self, store):
        store._cache.get = MagicMock(return_value="cache_res")
        result_value = store._bigtable_get_range(
            [self.TEST_KEY1, self.TEST_KEY3]
        )
        store.bt_table.read_rows.assert_not_called
        assert result_value == (self.TEST_KEY1, "cache_res")

    def test_bigtable_contains(self, store):
        store._cache.contains = MagicMock(return_value=None)
        store._cache.delete = MagicMock(return_value=None)

        store.bt_table.add_test_data([self.TEST_KEY1])
        return_value = store._bigtable_contains(self.TEST_KEY1)
        store.bt_table.read_row.assert_called_with(
            self.TEST_KEY1, filter_="a_filter"
        )
        store._cache.delete.assert_not_called()
        assert return_value is True

        return_value = store._bigtable_contains(self.TEST_KEY2)
        store.bt_table.read_row.assert_called_with(
            self.TEST_KEY2, filter_="a_filter"
        )
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
        test_keys_not_in = {
            self.TEST_KEY2,
        }

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
        store._bigtable_set(
            self.TEST_KEY1, self.TEST_KEY1, persist_offset=True
        )

        store.bt_table.direct_row.assert_called_once_with(self.TEST_KEY1)
        store._cache.set.assert_called_once_with(
            self.TEST_KEY1, self.TEST_KEY1
        )
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

    def test_get_partition_prefix(self, store):
        partition = 0
        res = store._get_partition_prefix(partition)
        assert res[0] == partition
        assert res[1:] == store.partition_prefix

        partition = 19
        res = store._get_partition_prefix(partition)
        assert res[0] == partition
        assert res[1:] == store.partition_prefix

    def test_remove_partition_prefix(self, store):
        store.partition_prefix = b"abc"
        key_with_partition = b"abcTHEACTUALKEY"
        res = store._remove_partition_prefix(key_with_partition)
        assert res == b"THEACTUALKEY"

    def test_get_key_with_partition(self, store):
        partition = 19
        res = store._get_key_with_partition(self.TEST_KEY1, partition)
        assert res[0] == partition
        assert store._remove_partition_prefix(res) == self.TEST_KEY1

    def test_partitions_for_key(self, store):
        store._cache.get_partition = MagicMock(return_value=19)
        res = store._partitions_for_key(self.TEST_KEY1)
        store._cache.get_partition.assert_called_once_with(self.TEST_KEY1)
        assert res == [19]

        store._cache.get_partition = MagicMock(side_effect=KeyError)
        store._active_partitions = MagicMock(return_value=[1, 2, 3])
        res = store._partitions_for_key(self.TEST_KEY2)
        store._cache.get_partition.assert_called_once_with(self.TEST_KEY2)
        assert res == [1, 2, 3]

    def test_get_with_known_partition(self, store):
        partition = 19
        store._maybe_get_partition_from_message = MagicMock(
            return_value=partition
        )
        store._cache.set_partition = MagicMock()
        # Scenario: Found
        store._bigtable_get = MagicMock(return_value=b"a_value")
        res = store._get(self.TEST_KEY1)
        key_with_partition = store._get_key_with_partition(
            self.TEST_KEY1, partition
        )
        store._bigtable_get.assert_called_once_with(key_with_partition)
        store._cache.set_partition.assert_called_once_with(
            self.TEST_KEY1, partition
        )
        assert res == b"a_value"

        store._cache.set_partition.reset_mock()
        # Scenario: Not Found
        store._bigtable_get = MagicMock(return_value=None)
        res = store._get(self.TEST_KEY1)
        key_with_partition = store._get_key_with_partition(
            self.TEST_KEY1, partition
        )
        store._bigtable_get.assert_called_once_with(key_with_partition)
        store._cache.set_partition.assert_not_called()
        assert res is None

    def test_get_with_unknown_partition(self, store):
        store._maybe_get_partition_from_message = MagicMock(return_value=None)
        store._partitions_for_key = MagicMock(return_value=[1, 3, 19])
        store._cache.set_partition = MagicMock()
        keys_searched = set()
        keys_searched.add(store._get_key_with_partition(self.TEST_KEY1, 1))
        keys_searched.add(store._get_key_with_partition(self.TEST_KEY1, 3))
        keys_searched.add(store._get_key_with_partition(self.TEST_KEY1, 19))

        # Scenario: Found
        key_of_value = store._get_key_with_partition(self.TEST_KEY1, 19)
        store._bigtable_get_range = MagicMock(
            return_value=(key_of_value, b"a_value")
        )
        res = store._get(self.TEST_KEY1)
        store._partitions_for_key.assert_called_once_with(self.TEST_KEY1)
        store._bigtable_get_range.assert_called_once_with(keys_searched)
        store._cache.set_partition.assert_called_once_with(self.TEST_KEY1, 19)
        assert res == b"a_value"

        store._cache.set_partition.reset_mock()
        # Scenario: Not Found
        store._bigtable_get_range = MagicMock(return_value=(None, None))
        res = store._get(self.TEST_KEY1)
        store._bigtable_get_range.assert_called_once_with(keys_searched)
        store._cache.set_partition.assert_not_called()
        assert res is None

    def test_set(self, store):
        partition = 19
        faust.stores.bigtable.get_current_partition = MagicMock(
            return_value=partition
        )
        store._bigtable_set = MagicMock()
        store._cache.set_partition = MagicMock()
        store._set(self.TEST_KEY1, b"a_value")
        key_with_partition = store._get_key_with_partition(
            self.TEST_KEY1, partition
        )
        store._bigtable_set.assert_called_once_with(
            key_with_partition, b"a_value"
        )
        store._cache.set_partition.assert_called_once_with(
            self.TEST_KEY1, partition
        )

    def test_del(self, store):
        store._cache._partition_cache = {self.TEST_KEY1: 19}
        store._partitions_for_key = MagicMock(return_value=[1, 3, 19])
        store._bigtable_del = MagicMock()
        store._del(self.TEST_KEY1)
        calls = [
            call(store._get_key_with_partition(self.TEST_KEY1, 1)),
            call(store._get_key_with_partition(self.TEST_KEY1, 3)),
            call(store._get_key_with_partition(self.TEST_KEY1, 19)),
        ]
        store._bigtable_del.assert_has_calls(calls)
        assert store._cache._partition_cache == {}

    def test_active_partitions(self, store):
        active_topics = [
            TP("a_changelogtopic", 19),
            TP("a_different_chaneglogtopic", 19),
        ]
        store.app.assignor.assigned_actives = MagicMock(
            return_value=active_topics
        )
        store.app.conf.topic_partitions = 20
        store.table.changelog_topic_name = "a_changelogtopic"
        store.table.is_global = False

        # Scenario: No global table
        res = store._active_partitions()
        all_res = list(res)
        assert all_res == [19]

        # Scenario: Global table
        store.table.is_global = True
        res = store._active_partitions()
        all_res = list(res)
        assert list(range(store.app.conf.topic_partitions)) == all_res

    def test_iteritems(self, store):
        keys_in_store = []
        keys_in_store.append(store._get_key_with_partition(self.TEST_KEY1, 1))
        keys_in_store.append(store._get_key_with_partition(self.TEST_KEY2, 2))
        keys_in_store.append(store._get_key_with_partition(self.TEST_KEY3, 3))

        store.bt_table.add_test_data(keys_in_store)
        store._active_partitions = MagicMock(return_value=[1, 3])
        all_res = sorted(store._iteritems())
        assert all_res == [
            (self.TEST_KEY1, keys_in_store[0]),
            (self.TEST_KEY3, keys_in_store[2]),
        ]

    def test_iterkeys_with_complete_cache(self, store):
        store._cache.is_complete = True
        store._active_partitions = MagicMock(return_value=[1, 3])
        store._cache._fill_if_empty = MagicMock()

        keys_in_cache = []
        keys_in_cache.append(store._get_key_with_partition(self.TEST_KEY1, 1))
        keys_in_cache.append(store._get_key_with_partition(self.TEST_KEY2, 2))
        keys_in_cache.append(store._get_key_with_partition(self.TEST_KEY3, 3))

        store._cache._value_cache = MagicMock()
        store._cache._value_cache.keys = MagicMock(return_value=keys_in_cache)
        all_res = sorted(store._iterkeys())
        store._cache._fill_if_empty.assert_called_once_with(
            {
                store._get_partition_prefix(1),
                store._get_partition_prefix(3),
            }
        )
        assert all_res == [
            self.TEST_KEY1,
            self.TEST_KEY2,
            self.TEST_KEY3,
        ]

    def test_iterkeys_with_no_complete_cache(self, store):
        store._cache.is_complete = False
        store._active_partitions = MagicMock(return_value=[1, 3])
        store._cache._fill_if_empty = MagicMock()
        keys_in_store = []
        keys_in_store.append(store._get_key_with_partition(self.TEST_KEY1, 1))
        keys_in_store.append(store._get_key_with_partition(self.TEST_KEY2, 2))
        keys_in_store.append(store._get_key_with_partition(self.TEST_KEY3, 3))
        store.bt_table.add_test_data(keys_in_store)

        all_res = sorted(store._iterkeys())
        store._cache._fill_if_empty.assert_not_called()
        assert all_res == [
            self.TEST_KEY1,
            self.TEST_KEY3,
        ]

    def test_iteritems(self, store):
        keys_in_store = []
        keys_in_store.append(store._get_key_with_partition(self.TEST_KEY1, 1))
        keys_in_store.append(store._get_key_with_partition(self.TEST_KEY2, 2))
        keys_in_store.append(store._get_key_with_partition(self.TEST_KEY3, 3))

        store.bt_table.add_test_data(keys_in_store)
        store._active_partitions = MagicMock(return_value=[1, 3])
        all_res = sorted(store._itervalues())
        assert all_res == [keys_in_store[0], keys_in_store[2]]

    def test_size(self, store):
        assert 0 == store._size()

    def test_contains_without_store_check_exists(self, store):
        store._bigtable_contains = MagicMock()
        store._bigtable_contains_any = MagicMock()
        store.app.conf.store_check_exists = False

        res = store._contains(self.TEST_KEY1)

        assert res is True
        store._bigtable_contains_any.assert_not_called()
        store._bigtable_contains.assert_not_called()

    def test_contains_with_known_partition(self, store):
        store.app.conf.store_check_exists = True
        store._bigtable_contains_any = MagicMock()
        store._maybe_get_partition_from_message = MagicMock(return_value=19)

        # Scenario1: Found
        store._bigtable_contains = MagicMock(return_value="TRUE_OR_FALSE")
        key_w_partition = store._get_key_with_partition(self.TEST_KEY1, 19)
        res = store._contains(self.TEST_KEY1)
        store._bigtable_contains.assert_called_once_with(key_w_partition)
        assert res == "TRUE_OR_FALSE"

    def test_contains_with_unknown_partition(self, store):
        store.app.conf.store_check_exists = True
        store._bigtable_contains_any = MagicMock()
        store._maybe_get_partition_from_message = MagicMock(return_value=None)
        store._partitions_for_key = MagicMock(return_value=[1, 3, 19])

        store._bigtable_contains_any = MagicMock(return_value="TRUE_OR_FALSE")
        keys_to_search = set()
        keys_to_search.add(store._get_key_with_partition(self.TEST_KEY1, 1))
        keys_to_search.add(store._get_key_with_partition(self.TEST_KEY1, 3))
        keys_to_search.add(store._get_key_with_partition(self.TEST_KEY1, 19))

        res = store._contains(self.TEST_KEY1)

        store._bigtable_contains_any.assert_called_once_with(keys_to_search)
        assert res == "TRUE_OR_FALSE"

    def test_get_offset_key(self, store):
        tp = TP("AAAA", 19)
        assert store.get_offset_key(tp)[-2:] == "19"

    def test_persisted_offset(self, store):
        tp = TP("AAAA", 19)
        store.get_offset_key = MagicMock(return_value=123)
        store.bt_table.add_test_data([123])
        assert store.persisted_offset(tp) == 123

    def test_set_persisted_offset(self, store):
        tp = TP("a_topic", 19)

        store._bigtable_set = MagicMock()

        # Scenario 0: No recovery && no flush
        recovery = False
        store._cache.flush_if_timer_over = MagicMock(return_value=False)
        expected_offset_key = store.get_offset_key(tp).encode()
        store.set_persisted_offset(tp, 123, recovery=recovery)
        store._bigtable_set.assert_not_called()

        # Scenario 1: Recovery
        recovery = True
        store._cache.flush_if_timer_over = MagicMock(return_value=False)
        expected_offset_key = store.get_offset_key(tp).encode()
        store.set_persisted_offset(tp, 123, recovery=recovery)
        store._bigtable_set.assert_called_once_with(
            expected_offset_key, str(123).encode(), persist_offset=True
        )

        # Scenario 2: Mutattion buffer flush
        recovery = False
        store._cache.flush_if_timer_over = MagicMock(return_value=True)
        expected_offset_key = store.get_offset_key(tp).encode()
        store.set_persisted_offset(tp, 123, recovery=recovery)
        store._bigtable_set.assert_called_with(
            expected_offset_key, str(123).encode(), persist_offset=True
        )

    def test_persist_changelog_batch(self, store):
        # Scenario 1: no failure
        store.bt_table.mutate_rows = MagicMock(
            return_value=[TestResponse(0)] * 10
        )
        store.log = MagicMock()
        store.log.error = MagicMock()
        store.set_persisted_offset = MagicMock()
        tp1 = TP("offset1", 10)
        tp2 = TP("offset2", 10)
        tp3 = TP("offset3", 10)
        offset_batch = {
            tp1: 111,
            tp2: 222,
            tp3: 333,
        }
        store._persist_changelog_batch(
            ["row1", "row2", "etc..."], offset_batch
        )
        store.bt_table.mutate_rows.assert_called_with(
            ["row1", "row2", "etc..."]
        )

        assert store.set_persisted_offset.call_count == len(offset_batch)
        store.set_persisted_offset.assert_called_with(tp3, 333, recovery=True)
        store.log.error.assert_not_called()

        # Scenario 2: all failure
        store.set_persisted_offset.reset_mock()
        store.bt_table.mutate_rows.reset_mock()
        store.bt_table.mutate_rows = MagicMock(
            return_value=[TestResponse(404)]
        )
        store._persist_changelog_batch(
            ["row1", "row2", "etc..."], offset_batch
        )
        # FIXME: I'm not sure if we want that behaviour.
        # Question: What should happen on a failed mutated row in recovery.
        store.set_persisted_offset.assert_called()
        store.log.error.assert_called()

    def test_apply_changelog_batch(self, store):
        row_mock = MagicMock()
        row_mock.delete = MagicMock()
        row_mock.set_cell = MagicMock()
        store.bt_table.direct_row = MagicMock(return_value=row_mock)
        store.bt_table.mutate_rows = MagicMock()
        store._persist_changelog_batch = MagicMock()

        class TestMessage:
            def __init__(self, value, key, tp, offset):
                self.value = value
                self.key = key
                self.tp = tp
                self.offset = offset

        class TestEvent:
            def __init__(self, message):
                self.message = message

        tp = TP("a", 19)
        tp2 = TP("b", 19)
        messages = [
            TestEvent(TestMessage("a", self.TEST_KEY1, tp, 0)),
            TestEvent(TestMessage(None, self.TEST_KEY1, tp, 1)),  # Delete
            TestEvent(TestMessage("a", self.TEST_KEY1, tp, 3)),  # Out of order
            TestEvent(TestMessage("b", self.TEST_KEY2, tp2, 4)),
            TestEvent(TestMessage("a", self.TEST_KEY1, tp, 2)),
        ]
        store.apply_changelog_batch(messages, lambda x: x, lambda x: x)
        assert store.bt_table.direct_row.call_count == 5
        row_mock.delete.assert_called_once()
        assert row_mock.set_cell.call_count == 4
        store._persist_changelog_batch.assert_called_once()
        tp_offsets = store._persist_changelog_batch.call_args_list[0].args[1]
        assert tp_offsets == {tp: 3, tp2: 4}
