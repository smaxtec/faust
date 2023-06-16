import time
from unittest.mock import MagicMock, call, patch

import pytest
from mode.utils.collections import LRUCache

import faust
from faust.stores.bigtable import (
    BigTableCache,
    BigTableStore,
)
from faust.types.tuples import TP


def to_bt_key(key):
    len_total = len(key)
    len_prefix = 5
    len_first_id = key[len_prefix] // 2
    if len_prefix + 1 + len_first_id + 1 >= len_total:
        # This happens if there is e.g. no organisation id
        return key
    len_second_id = key[len_prefix + 1 + len_first_id + 1] // 2
    key_prefix = key[len_total - len_second_id :]
    return key_prefix + key


def from_bt_key(key):
    magic_byte_pos = key.find(bytes(4))
    if magic_byte_pos == 0:
        return key
    return key[magic_byte_pos:]


def get_preload_prefix_len(key) -> int:
    preload_len = key.find(bytes(4))
    if preload_len == 0:
        return len(key)
    return preload_len


class MyTestResponse:
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

    def _add_row_range_from_keys(
        self, start_key: bytes, end_key: bytes, end_inclusive=False
    ):
        if isinstance(start_key, str):
            start_key = start_key.encode()
        if isinstance(end_key, str):
            end_key = end_key.encode()
        if end_inclusive:
            self.keys.add(b"".join([start_key, b"_*ei_", end_key]))
        else:
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
                    if start <= key[: len(end)] < end:
                        yield self._read_row(key)
                continue
            elif b"_*ei_" in k:
                for key in self.data.keys():
                    start, end = k.split(b"_*ei_")
                    if start <= key[: len(end)] <= end:
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


class TestBigTableCache:
    def test_default__init__(self):
        bigtable_mock = BigTableMock()
        app_mock = MagicMock()
        app_mock.conf = MagicMock()
        app_mock.conf.table_key_index_size = 123
        time.time = MagicMock(return_value=0)

        test_manager = BigTableCache(MagicMock(), {}, bigtable_mock)
        assert test_manager.bt_table == bigtable_mock
        assert test_manager._value_cache is None

    def test_iscomplete__init__(self):
        bigtable_mock = BigTableMock()
        app_mock = MagicMock()
        app_mock.conf = MagicMock()
        app_mock.conf.table_key_index_size = 2
        time.time = MagicMock(return_value=0)
        options = {
            BigTableStore.BT_VALUE_CACHE_ENABLE_KEY: True,
        }

        test_manager = BigTableCache(
            MagicMock(), options, bigtable_mock
        )
        assert test_manager.bt_table == bigtable_mock
        assert isinstance(test_manager._value_cache, dict)

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
                    BigTableStore.BT_VALUE_CACHE_ENABLE_KEY: True,
                }
                manager = BigTableCache(
                    MagicMock(), options, bigtable_mock
                )
                manager._partition_cache = {}
                return manager

    def test_fill(self, manager):
        key = b"\x13AAA"
        manager.bt_table.add_test_data({key})
        # Scenario 1: Everything empty
        manager.fill({19})
        assert manager.bt_table.read_rows.call_count == 1
        assert manager.filled_partitions == {19}

        manager.fill({19})
        assert manager.bt_table.read_rows.call_count == 1
        assert manager.filled_partitions == {19}

        manager.fill({16})
        assert manager.bt_table.read_rows.call_count == 2
        assert manager.filled_partitions == {19, 16}
        assert manager.contains(key)

    def test_get(self, manager):
        key_in = b"\x13AAA"
        key_not_in = b"\x13BBB"

        manager.bt_table.add_test_data({key_in})
        with pytest.raises(KeyError):
            manager.get(key_in)

        manager.fill({19})
        res = manager.get(key_in)
        assert res == key_in

        with pytest.raises(KeyError):
            manager.get(key_not_in)

        manager._value_cache = None
        assert manager.get(key_in) is None

    def test_set(self, manager):
        key_1 = b"\x13AAA"
        key_2 = b"\x13ABB"
        manager.set(key_1, key_1)
        assert manager.contains(key_1)
        assert manager.contains(key_2) is False

        manager.set(key_2, key_2)
        assert manager.contains(key_1)
        assert manager.contains(key_2)
        assert manager.get(key_1) == key_1
        assert manager.get(key_2) == key_2

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
        manager.fill({19})

        assert manager.contains(key_in) is True
        assert manager.contains(key_not_in) is False

        assert manager.contains(key_in) is True
        assert manager.contains(key_not_in) is False

        manager._value_cache = None
        assert manager.contains(key_in) is False
        assert manager.contains(key_not_in) is False

    def test_delete_partition(self, manager):
        partition = 19
        row_mock = MagicMock()
        row_mock.delete = MagicMock()
        row_mock.set_cell = MagicMock()
        row_mock.row_key = b"\x13AAA"
        manager.bt_table.direct_row = MagicMock(return_value=row_mock)
        manager.bt_table.add_test_data({b"\x13AAA"})
        manager.set(row_mock.row_key, row_mock)
        manager.set_partition(row_mock.row_key[1:], partition)
        manager.delete_partition(3)
        assert len(manager._value_cache) == 1
        assert len(manager._partition_cache) == 1
        manager.delete_partition(partition)
        assert len(manager._value_cache) == 0
        assert len(manager._partition_cache) == 0
        # Delete something that does not exist yet should not do anything
        manager.delete_partition(999999)


class TestBigTableStore:
    TEST_KEY1 = b"TEST_KEY1"
    TEST_KEY2 = b"TEST_KEY2"
    TEST_KEY3 = b"TEST_KEY3"
    TEST_KEY4 = b"\x00\x00\x00\x00\x01\x0eNoGroup\x00063d76e3ebd7e634de234c67d"
    TEST_KEY5 = b"\x00\x00\x00\x00\x02062a99788df917508d1891ed2\x00062a99788df917508d1891ed2"
    TEST_KEY6 = b"\x00\x00\x00\x00\x02062a99788df917508d1891ed2\x02"

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
        assert self_mock.offset_key_prefix == "==>offset_for_partition_"
        assert self_mock.row_filter == "a_filter"

    @pytest.mark.asyncio
    async def test_bigtable_set_options(self, bt_imports):
        self_mock = MagicMock()
        bt_imports.CellsColumnLimitFilter = MagicMock(return_value="a_filter")
        bt_imports.column_family = MagicMock(return_value=MagicMock())
        name_lambda = lambda x: print(x)  # noqa

        def to_bt_key(key):
            len_total = len(key)
            len_prefix = 4
            len_num_bytes_len = key[len_prefix] // 2
            len_first_id = key[len_prefix + len_num_bytes_len] // 2
            len_second_id = (
                key[len_prefix + 1 + len_num_bytes_len + len_first_id + 1] // 2
            )
            key_prefix = key[len_total - len_second_id :]
            return key_prefix + key

        def from_bt_key(key):
            return key[key.find(b"\x00\x00\x00") :]

        options = {
            BigTableStore.BT_TABLE_NAME_GENERATOR_KEY: name_lambda,
            BigTableStore.BT_OFFSET_KEY_PREFIX: "offset_test",
        }
        BigTableStore._set_options(self_mock, options)
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
            column_families={"FaustColumnFamily": "a_rule"}
        )
        assert return_value is None

    @pytest.fixture()
    def store(self, bt_imports):
        with patch("faust.stores.bigtable.BT", bt_imports):
            options = {}
            options[BigTableStore.BT_INSTANCE_KEY] = "bt_instance"
            options[BigTableStore.BT_PROJECT_KEY] = "bt_project"
            options[BigTableStore.BT_VALUE_CACHE_ENABLE_KEY] = True
            store = BigTableStore(
                "bigtable://", MagicMock(), MagicMock(), options=options
            )
            store.bt_table = BigTableMock()
            return store

    def test_bigtable_bigtable_get_on_empty(self, store):
        return_value = store._bigtable_get(self.TEST_KEY1)
        store.bt_table.read_row.assert_called_once_with(
            self.TEST_KEY1, filter_="a_filter"
        )
        assert return_value is None

    def test_bigtable_delete(self, store):
        row_mock = MagicMock()
        row_mock.commit = MagicMock()
        row_mock.delete = MagicMock()
        store.bt_table.direct_row = MagicMock(return_value=row_mock)
        store._cache.set = MagicMock()

        store._bigtable_mutate(self.TEST_KEY1, None)
        store._cache.set.assert_called_once_with(self.TEST_KEY1, None)

    def test_bigtable_set(self, store):
        row_mock = MagicMock()
        row_mock.set_cell = MagicMock()

        store.bt_table.direct_row = MagicMock(return_value=row_mock)
        store._cache.set = MagicMock(return_value=None)
        store._cache.submit_mutation = MagicMock(return_value=None)
        store._bigtable_mutate(self.TEST_KEY1, self.TEST_KEY1)
        store._bigtable_mutate(self.TEST_KEY1, self.TEST_KEY1)

        store._cache.set.assert_called_with(self.TEST_KEY1, self.TEST_KEY1)
        store._cache.submit_mutation.assert_called_with(self.TEST_KEY1, self.TEST_KEY1)


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

        partition = 19
        res = store._get_partition_prefix(partition)
        assert res[0] == partition

    def test_get_faust_key(self, store):
        key_with_partition = b"\x13THEACTUALKEY"
        res = store._get_faust_key(key_with_partition)
        assert res == b"THEACTUALKEY"

    def test_get_key_with_partition(self, store):
        partition = 19
        res = store._get_bigtable_key(self.TEST_KEY1, partition)
        assert res[0] == partition
        assert store._get_faust_key(res) == self.TEST_KEY1

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

    def test_get_keyerror(self, store):
        partition = 19
        store._maybe_get_partition_from_message = MagicMock(
            return_value=partition
        )
        store._bigtable_get = MagicMock(return_value=None)
        with pytest.raises(KeyError):
            store[self.TEST_KEY1.decode()]

    def test_get_with_known_partition(self, store):
        partition = 19
        store._maybe_get_partition_from_message = MagicMock(
            return_value=partition
        )
        store._cache.set_partition = MagicMock()
        # Scenario: Found
        store._bigtable_get = MagicMock(return_value=b"a_value")
        res = store._get(self.TEST_KEY1)
        key_with_partition = store._get_bigtable_key(self.TEST_KEY1, partition)
        store._bigtable_get.assert_called_once_with(key_with_partition)
        store._cache.set_partition.assert_called_once_with(
            self.TEST_KEY1, partition
        )
        assert res == b"a_value"

        store._cache.set_partition.reset_mock()
        # Scenario: Not Found
        store._bigtable_get = MagicMock(return_value=None)
        res = store._get(self.TEST_KEY1)
        key_with_partition = store._get_bigtable_key(self.TEST_KEY1, partition)
        store._bigtable_get.assert_called_once_with(key_with_partition)
        store._cache.set_partition.assert_not_called()
        assert res is None

    def test_get_with_unknown_partition(self, store):
        store._maybe_get_partition_from_message = MagicMock(return_value=None)
        store._partitions_for_key = MagicMock(return_value=[3, 19])
        store._cache.set_partition = MagicMock()
        keys_searched = set()
        keys_searched.add(store._get_bigtable_key(self.TEST_KEY1, 1))
        keys_searched.add(store._get_bigtable_key(self.TEST_KEY1, 3))
        keys_searched.add(store._get_bigtable_key(self.TEST_KEY1, 19))

        # Scenario: Found
        store._bigtable_get = MagicMock(
            return_value=b"a_value"
        )
        res = store._get(self.TEST_KEY1)
        store._partitions_for_key.assert_called_once_with(self.TEST_KEY1)
        store._cache.set_partition.assert_called_once_with(self.TEST_KEY1, 19)
        assert res == b"a_value"

        store._cache.set_partition.reset_mock()
        # Scenario: Not Found
        store._bigtable_get = MagicMock(return_value=None)
        res = store._get(self.TEST_KEY1)
        assert store._bigtable_get.call_count == 2
        store._cache.set_partition.assert_not_called()
        assert res is None

    def test_set(self, store):
        partition = 19
        faust.stores.bigtable.get_current_partition = MagicMock(
            return_value=partition
        )
        store._bigtable_mutate = MagicMock()
        store._cache.set_partition = MagicMock()
        store._set(self.TEST_KEY1, b"a_value")
        key_with_partition = store._get_bigtable_key(self.TEST_KEY1, partition)
        store._bigtable_mutate.assert_called_once_with(
            key_with_partition, b"a_value"
        )
        store._cache.set_partition.assert_called_once_with(
            self.TEST_KEY1, partition
        )

    def test_del(self, store):
        store._cache._partition_cache = {self.TEST_KEY1: 19}
        store._partitions_for_key = MagicMock(return_value=[1, 3, 19])
        store._bigtable_mutate = MagicMock()
        store._del(self.TEST_KEY1)
        calls = [
            call(store._get_bigtable_key(self.TEST_KEY1, 1), None),
            call(store._get_bigtable_key(self.TEST_KEY1, 3), None),
            call(store._get_bigtable_key(self.TEST_KEY1, 19), None),
        ]
        store._bigtable_mutate.assert_has_calls(calls)
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

    def test_iteritems_with_cache(self, store):
        store._active_partitions = MagicMock(return_value=[1, 3])
        store._cache.flush = MagicMock(wraps=store._cache.flush)
        store._cache.fill = MagicMock()
        store.bt_table.read_rows = MagicMock()

        _ = sorted(store._iteritems())
        store._cache.flush.assert_called_once()
        store._cache.fill.assert_called_once()
        _ = sorted(store._iteritems())
        store.bt_table.read_rows.assert_not_called()

    def test_iteritems(self, store):
        store._active_partitions = MagicMock(return_value=[1, 3])
        store.bt_table.read_rows = MagicMock()
        store._cache._value_cache = None
        store._cache.flush = MagicMock(wraps=store._cache.flush)

        _ = sorted(store._iteritems())
        store._cache.flush.assert_called_once()
        _ = sorted(store._iteritems())
        store.bt_table.read_rows.assert_called()

    def test_iterkeys(self, store):
        values = [("K1", "V1"), ("K2", "V2")]
        store._iteritems = MagicMock(return_value=values)
        all_res = sorted(store._iterkeys())
        assert all_res == ["K1", "K2"]

    def test_itervalues(self, store):
        values = [("K1", "V1"), ("K2", "V2")]
        store._iteritems = MagicMock(return_value=values)
        all_res = sorted(store._itervalues())
        assert all_res == ["V1", "V2"]

    def test_size(self, store):
        assert 0 == store._size()

    def test_get_offset_key(self, store):
        tp = TP("AAAA", 19)
        assert store.get_offset_key(tp)[-2:] == "19"

    def test_set_persisted_offset(self, store):
        tp = TP("a_topic", 19)
        store._cache.submit_mutation = MagicMock(wraps=store._cache.submit_mutation)
        store._cache.flush_if_timer_over = MagicMock(return_value=False)
        expected_offset_key = store.get_offset_key(tp).encode()
        store.set_persisted_offset(tp, 123)
        store._cache.submit_mutation.assert_called_with(
            expected_offset_key, str(123).encode()
        )

    def test_apply_changelog_batch(self, store):
        row_mock = MagicMock()
        row_mock.delete = MagicMock()
        row_mock.set_cell = MagicMock()
        store.bt_table.direct_row = MagicMock(return_value=row_mock)
        store.bt_table.mutate_rows = MagicMock()
        store._bigtable_mutate = MagicMock()
        store.set_persisted_offset = MagicMock()
        store._cache.submit_mutation = MagicMock()
        store._cache.set = MagicMock()

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
        assert store._bigtable_mutate.call_count == 5
        assert store.set_persisted_offset.call_count == 2

    def test_revoke_partitions(self, store):
        store._cache.delete_partition = MagicMock()
        TP1 = MagicMock()
        TP1.partition = 1
        TP2 = MagicMock()
        TP2.partition = 2

        store.revoke_partitions({TP1, TP2})
        store._cache.delete_partition.assert_any_call(1)
        store._cache.delete_partition.assert_any_call(2)

    def test_mutation_flush(self, store):
        # Mocks
        TEST_TP = TP("a", 0)
        TEST_OFFSET = 0
        OFFSET_KEY = store.get_offset_key(TEST_TP).encode()

        def real_set_scenario(key, value, offset):
            store._set(key, value)
            store._bigtable_mutate.reset_mock()
            store.set_persisted_offset(TEST_TP, offset)
            return offset + 1

        def real_del_scenario(key, offset):
            store._del(key)
            store._cache.submit_mutation.reset_mock()
            store.set_persisted_offset(TEST_TP, offset)
            return offset + 1

        def assert_offset_persisted(offset):
            store._cache.submit_mutation.assert_called_with(
                OFFSET_KEY, str(offset).encode()
            )

        row_mock = MagicMock()
        row_mock.row_key = b"\x00TEST_KEY1"

        store.bt_table.direct_row = MagicMock(return_value=row_mock)
        store._bigtable_mutate = MagicMock(wraps=store._bigtable_mutate)
        store._cache.submit_mutation = MagicMock(wraps=store._cache.submit_mutation)

        partition = 0
        faust.stores.bigtable.get_current_partition = MagicMock(
            return_value=partition
        )
        store._cache.set_partition = MagicMock()
        res = store._contains(self.TEST_KEY1)
        store._bigtable_mutate.assert_not_called()
        assert res is False

        TEST_OFFSET = real_set_scenario(
            self.TEST_KEY1, self.TEST_KEY1, TEST_OFFSET
        )
        res = store._contains(self.TEST_KEY1)
        assert_offset_persisted(TEST_OFFSET - 1)
        assert res is True

        TEST_OFFSET = real_set_scenario(
            self.TEST_KEY1, self.TEST_KEY1, TEST_OFFSET
        )
        res = store._contains(self.TEST_KEY1)
        assert res is True
        assert_offset_persisted(TEST_OFFSET - 1)

        TEST_OFFSET = real_del_scenario(self.TEST_KEY1, TEST_OFFSET)
        res = store._contains(self.TEST_KEY1)
        assert res is False
        assert_offset_persisted(TEST_OFFSET - 1)

        TEST_OFFSET = real_set_scenario(
            self.TEST_KEY1, self.TEST_KEY1, TEST_OFFSET
        )
        res = store._contains(self.TEST_KEY1)
        assert_offset_persisted(TEST_OFFSET - 1)
        assert res is True
