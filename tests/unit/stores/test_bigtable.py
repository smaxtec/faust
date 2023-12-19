from unittest.mock import MagicMock, call, patch

import pytest

from faust.stores.bigtable import (
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
        self.add_row_range_from_keys = MagicMock(wraps=self._add_row_range_from_keys)
        self.add_row_range_with_prefix = MagicMock(
            wraps=self._add_row_range_with_prefix
        )

    def _add_row_key(self, key):
        self.keys.add(key)

    def _add_row_range_with_prefix(self, prefix):
        if isinstance(prefix, str):
            prefix = prefix.encode()
        self._add_row_range_from_keys(prefix, prefix, end_inclusive=True)

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
        iterator = row_set.keys
        if len(iterator) == 0:
            iterator = self.data.keys()
        for k in iterator:
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


class TestBigTableStore:
    TEST_KEY1 = b"TEST_KEY1"
    TEST_KEY2 = b"TEST_KEY2"
    TEST_KEY3 = b"TEST_KEY3"
    TEST_KEY4 = b"\x00\x00\x00\x00\x01\x0eNoGroup\x00063d76e3ebd7e634de234c67d"
    TEST_KEY5 = (
        b"\x00\x00\x00\x00\x02062a99788df917508d1891ed2\x00062a99788df917508d1891ed2"
    )
    TEST_KEY6 = b"\x00\x00\x00\x00\x02062a99788df917508d1891ed2\x02"

    @pytest.fixture()
    def bt_imports(self):
        with patch("faust.stores.bigtable.BT") as bt:
            bt.CellsColumnLimitFilter = MagicMock(return_value="a_filter")
            bt.column_family.MaxVersionsGCRule = MagicMock(return_value="a_rule")
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
        self_mock.bt_table_name = self_mock.table_name_generator(faust_table_mock)

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

        return_value = BigTableStore._setup_bigtable(
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
        self_mock.bt_table_name = self_mock.table_name_generator(faust_table_mock)
        table_mock.exists = MagicMock(return_value=False)
        return_value = BigTableStore._setup_bigtable(
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
            store = BigTableStore(
                "bigtable://", MagicMock(), MagicMock(), options=options
            )
            store.bt_table = BigTableMock()
            return store

    def test_bigtable_get(self, store, bt_imports):
        keys = [self.TEST_KEY1, self.TEST_KEY2]
        for idx, k in enumerate(keys):
            keys[idx] = store._add_partition_prefix_to_key(k, 2)
        store.bt_table.add_test_data(keys)

        # Test get from bigtable
        value, partition = store._bigtable_get([keys[1]])
        store.bt_table.read_rows.assert_called_once()
        assert partition == 2
        assert value == keys[1]

        # Test get from mutation buffer
        store._mutation_batcher_enable = True
        store._mutation_batcher_cache = {keys[1]: b"123"}
        value, partition = store._bigtable_get([keys[1]])
        store.bt_table.read_rows.assert_called_once()
        assert value == b"123"
        assert partition == 2

    def test_bigtable_get_on_empty(self, store, bt_imports):
        return_value = store._bigtable_get([self.TEST_KEY1, self.TEST_KEY2])
        store.bt_table.read_rows.assert_called_once()
        assert return_value == (None, None)

    def test_bigtable_delete(self, store):
        row_mock = MagicMock()
        row_mock.commit = MagicMock()
        row_mock.delete = MagicMock()
        store.bt_table.direct_row = MagicMock(return_value=row_mock)
        store._set_mutation = MagicMock()

        store._bigtable_del(self.TEST_KEY1)
        store._set_mutation.assert_not_called()
        row_mock.delete.assert_called_once()
        row_mock.commit.assert_called_once()

        # Test with mutation buffer
        store._mutation_batcher_enable = True
        store._bigtable_del(self.TEST_KEY1)
        store._set_mutation.assert_called_once_with(self.TEST_KEY1, None, row_mock)
        assert row_mock.delete.call_count == 2
        assert row_mock.commit.call_count == 1

    def test_bigtable_set(self, store):
        row_mock = MagicMock()
        row_mock.set_cell = MagicMock()
        row_mock.commit = MagicMock()
        store.bt_table.direct_row = MagicMock(return_value=row_mock)
        store._set_mutation = MagicMock()

        store._bigtable_set(self.TEST_KEY1, b"a_value")
        store._set_mutation.assert_not_called()
        row_mock.set_cell.assert_called_once()
        row_mock.commit.assert_called_once()

        # Test with mutation buffer
        store._mutation_batcher_enable = True
        store._bigtable_set(self.TEST_KEY1, "a_value")
        store._set_mutation.assert_called_once_with(self.TEST_KEY1, "a_value", row_mock)
        assert row_mock.set_cell.call_count == 2
        assert row_mock.commit.call_count == 1

    def test_get_partition_from_message(self, store):
        event_mock = MagicMock()
        event_mock.message = MagicMock()
        event_mock.message.partition = 69
        current_event_mock = MagicMock(return_value=event_mock)

        store.table.is_global = False
        store.table.use_partitioner = False
        topic = store.table.changelog_topic_name
        store.app.assignor.assigned_actives = MagicMock(
            return_value={TP(topic, 123), TP(topic, 69)}
        )
        store.app.conf.topic_partitions = 123
        with patch("faust.stores.bigtable.current_event", current_event_mock):
            return_value = store._get_current_partitions()
            assert return_value == [69]

        store.table.is_global = True
        with patch("faust.stores.bigtable.current_event", current_event_mock):
            return_value = store._get_current_partitions()
            assert return_value == list(range(123))

        store.table.is_global = False
        current_event_mock = MagicMock(return_value=None)

        with patch("faust.stores.bigtable.current_event", current_event_mock):
            return_value = store._get_current_partitions()
            assert return_value == [69]

    def test_get_faust_key(self, store):
        key_with_partition = b"\x13_..._THEACTUALKEY"
        res = store._remove_partition_prefix_from_bigtable_key(key_with_partition)
        assert res == b"THEACTUALKEY"

    def test_get_key_with_partition(self, store):
        partition = 19
        res = store._add_partition_prefix_to_key(self.TEST_KEY1, partition)
        extracted_partition = store._get_partition_from_bigtable_key(res)
        assert extracted_partition == partition
        assert store._remove_partition_prefix_from_bigtable_key(res) == self.TEST_KEY1

    def test_partitions_for_key(self, store):
        store._get_current_partitions = MagicMock(return_value=[19])
        res = list(store._get_partitions_for_key(self.TEST_KEY1))
        assert res == [19]

    def test_get_keyerror(self, store):
        partition = None
        store._get_current_partitions = MagicMock(return_value=[partition])
        store._bigtable_get = MagicMock(return_value=(None, None))
        with pytest.raises(KeyError):
            key = "123"
            store[key]

    def test_get_with_known_partition(self, store):
        partitions = [19, 20]
        store._get_cache = MagicMock(return_value=(b"this is ignored", False))
        store._key_index = {}
        store._get_current_partitions = MagicMock(return_value=partitions)
        # Scenario: Found
        store._bigtable_get = MagicMock(return_value=(b"a_value", 19))

        res = store._get(self.TEST_KEY1)
        get_keys = [
            store._add_partition_prefix_to_key(self.TEST_KEY1, p) for p in partitions
        ]
        store._bigtable_get.assert_called_once_with(get_keys)
        assert res == b"a_value"

        # Scenario: Not Found
        store._bigtable_get = MagicMock(return_value=(None, None))
        res = store._get(self.TEST_KEY1)
        store._bigtable_get.assert_called_with(
            [get_keys[0]]
        )  # because the partition is in key_index
        assert res is None

        # Scenario: Cache hit on value
        store._get_cache = MagicMock(return_value=(b"a_value_from_cache", True))
        store._bigtable_get = MagicMock(return_value=(None, None))
        res = store._get(self.TEST_KEY1)
        store._bigtable_get.assert_not_called()
        assert res == b"a_value_from_cache"

        # Scenario: Cache hit on None value
        store._get_cache = MagicMock(return_value=(None, True))
        store._bigtable_get = MagicMock(return_value=(None, None))
        res = store._get(self.TEST_KEY2)
        store._bigtable_get.assert_not_called()
        assert res is None

        # Scenario: Cache miss, but partition should be in startup cache
        store._get_cache = MagicMock(return_value=(None, False))
        store._bigtable_get = MagicMock(return_value=(None, None))
        res = store._get(self.TEST_KEY2)
        store._bigtable_get.assert_called_once()
        assert res is None

    def test_set(self, store):
        # Scenario: No cache
        event_mock = MagicMock()
        event_mock.message = MagicMock()
        event_mock.message.partition = 69
        current_event_mock = MagicMock(return_value=event_mock)
        no_event_mock = MagicMock(return_value=None)

        # Test assertion withour current event
        with patch("faust.stores.bigtable.current_event", no_event_mock):
            with pytest.raises(AssertionError):
                store["123"] = "000"

        with patch("faust.stores.bigtable.current_event", current_event_mock):
            store._key_index = {}
            store._set_cache = MagicMock()
            store._bigtable_set = MagicMock()
            store._set(self.TEST_KEY1, b"a_value")

            key = store._add_partition_prefix_to_key(self.TEST_KEY1, 69)
            store._set_cache.assert_called_with(69, self.TEST_KEY1, b"a_value")
            store._bigtable_set.assert_called_once_with(key, b"a_value")

    def test_del(self, store):
        # Scenario: No cache
        store._bigtable_del = MagicMock()
        store._del_cache = MagicMock(return_value=None)
        store._get_partitions_for_key = MagicMock(return_value=[1, 2, 3])
        store._del(self.TEST_KEY1)
        # Check one call for each partition
        keys = [
            store._add_partition_prefix_to_key(self.TEST_KEY1, p) for p in [1, 2, 3]
        ]
        store._del_cache.assert_called_once_with(self.TEST_KEY1)
        assert store._bigtable_del.call_count == 3
        expected_calls = [call(key) for key in keys]
        for call_args in store._bigtable_del.call_args_list:
            assert call_args in expected_calls

    def test_active_partitions(self, store):
        active_topics = [
            TP("a_changelogtopic", 19),
            TP("a_different_chaneglogtopic", 19),
        ]
        store.app.assignor.assigned_actives = MagicMock(return_value=active_topics)
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
        store._active_partitions = MagicMock(return_value=[1, 3])
        store._bigtable_iteritems = MagicMock(wraps=store._bigtable_iteritems)
        store.bt_table.read_rows = MagicMock()
        _ = sorted(store._iteritems())
        store.bt_table.read_rows.assert_called_once()
        # Calling with None means get all rows
        store._bigtable_iteritems.assert_called_once_with(None)

    def test_iteritems_with_startup_cache(self, store, bt_imports):
        store._active_partitions = MagicMock(return_value=[1, 3])
        store._startup_cache = {}
        store._startup_cache[1] = {
            self.TEST_KEY1: b"this is a value",
            self.TEST_KEY2: b"this is another value",
            b"Dont return this, because this is a offset key": None,
        }

        store._bigtable_iteritems = MagicMock(wraps=store._bigtable_iteritems)
        store.bt_table.read_rows = MagicMock(
            return_value=[
                MagicMock(
                    row_key=store._add_partition_prefix_to_key(self.TEST_KEY3, 3),
                    to_dict=MagicMock(return_value={"x": [MagicMock(value=b"1")]}),
                    commit=MagicMock(),
                ),
                MagicMock(
                    row_key=store._add_partition_prefix_to_key(self.TEST_KEY4, 3),
                    to_dict=MagicMock(return_value={"x": [MagicMock(value=b"2")]}),
                    commit=MagicMock(),
                ),
            ]
        )
        res = sorted(store._iteritems())
        store._bigtable_iteritems.assert_called_once_with([3])
        all_entries = {
            self.TEST_KEY1: b"this is a value",
            self.TEST_KEY2: b"this is another value",
            self.TEST_KEY3: b"1",
            self.TEST_KEY4: b"2",
        }
        assert res == sorted(list(all_entries.items()))
        keys = list(sorted(store._iterkeys()))
        values = list(sorted(store._itervalues()))

        assert keys == sorted(list(all_entries.keys()))
        assert values == sorted(list(all_entries.values()))

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
        expected_offset_key = store.get_offset_key(tp).encode()
        store._bigtable_set = MagicMock()

        store.set_persisted_offset(tp, 123)
        store._bigtable_set.called_once_with(expected_offset_key, b"123")

    def test_apply_changelog_batch(self, store):
        row_mock = MagicMock()
        row_mock.delete = MagicMock()
        row_mock.set_cell = MagicMock()
        store.bt_table.direct_row = MagicMock(return_value=row_mock)
        store._bigtable_del = MagicMock()
        store._bigtable_set = MagicMock()
        store.set_persisted_offset = MagicMock()
        store._set_cache = MagicMock()
        store._del_cache = MagicMock()

        class TestMessage:
            def __init__(self, value, key, tp, offset, partition):
                self.value = value
                self.key = key
                self.tp = tp
                self.offset = offset
                self.partition = partition

        class TestEvent:
            def __init__(self, message):
                self.message = message

        tp = TP("a", 19)
        tp2 = TP("b", 19)
        messages = [
            TestEvent(TestMessage("a", self.TEST_KEY1, tp, 0, 1)),
            TestEvent(TestMessage(None, self.TEST_KEY1, tp, 1, 1)),  # Delete
            TestEvent(TestMessage("a", self.TEST_KEY1, tp, 3, 1)),  # Out of order
            TestEvent(TestMessage("b", self.TEST_KEY2, tp2, 4, 2)),
            TestEvent(TestMessage("a", self.TEST_KEY1, tp, 2, 1)),
        ]
        store.apply_changelog_batch(messages, lambda x: x, lambda x: x)
        assert store._bigtable_set.call_count == 4
        assert store._bigtable_del.call_count == 1
        assert store.set_persisted_offset.call_count == 2

    @pytest.mark.asyncio
    async def test_fill_caches(self, store, bt_imports):
        store._bigtable_iteritems = MagicMock(
            side_effect=[[(b"key1", b"value1")], [(b"key2", b"value2")]]
        )
        store._set_cache = MagicMock()
        store._startup_cache_ttl = 1800
        store._invalidation_timer = None
        store._startup_cache = {}

        partitions = {0, 1}
        partitions2 = {0, 2}

        store._fill_caches(partitions)
        calls = [call(partitions={p}) for p in partitions]
        store._bigtable_iteritems.assert_has_calls(calls)

        store._set_cache.assert_has_calls(
            [
                call(0, b"key1", b"value1"),
                call(1, b"key2", b"value2"),
            ]
        )
        assert store._invalidation_timer is not None
        assert store._invalidation_timer.is_alive()

        # Test with different partitions
        # This should reset the _invalidation_timer
        old_invalid_timer = store._invalidation_timer.__hash__()

        store._bigtable_iteritems = MagicMock(
            side_effect=[[(b"key3", b"value3")], [(b"key4", b"value4")]]
        )
        store._set_cache = MagicMock()
        store._fill_caches(partitions2)
        new_invalid_timer = store._invalidation_timer.__hash__()
        # Check if old invalidation timer is different from new one
        assert old_invalid_timer != new_invalid_timer
        assert store._invalidation_timer is not None
        assert store._invalidation_timer.is_alive()

        store._bigtable_iteritems.assert_called_with(partitions={2})
        assert store._bigtable_iteritems.call_count == 1
        store._set_cache.assert_has_calls(
            [
                # Key 4 is ignored because it should already be loaded.
                # Because in our scenario the second key is never returned
                # call(0, b"key4", b"value4"),
                call(2, b"key3", b"value3"),
            ]
        )
        assert store._invalidation_timer is not None
        assert store._invalidation_timer.is_alive()

        # Wait for the invalidation timer to expire
        store._invalidation_timer.cancel()
        store._invalidate_startup_cache()

        assert store._startup_cache == {}
        assert store._invalidation_timer is None

    @pytest.mark.asyncio
    async def test_fill_caches_no_ttl(self, store, bt_imports):
        store._bigtable_iteritems = MagicMock(
            side_effect=[[(b"key1", b"value1")], [(b"key2", b"value2")]]
        )
        store._set_cache = MagicMock()
        store._startup_cache_ttl = 0
        store._invalidation_timer = None
        store._startup_cache = {}

        partitions = {0, 1}

        store._fill_caches(partitions)

        store._set_cache.assert_has_calls(
            [
                call(0, b"key1", b"value1"),
                call(1, b"key2", b"value2"),
            ]
        )
        assert store._set_cache.call_args_list == [
            call(0, b"key1", b"value1"),
            call(1, b"key2", b"value2"),
        ]
        assert store._invalidation_timer is None

    @pytest.mark.asyncio
    async def test__get_active_changelogtopic_partitions(self, store):
        tps_table = {
            "changelog_topic",
            "other_topic",
            "other_topic2",
        }
        store.table = MagicMock(changelog_topic=MagicMock(topics=tps_table))

        tps = {TP("changelog_topic", 0), TP("other_topic", 1)}
        active_partitions = store._get_active_changelogtopic_partitions(
            store.table, tps
        )
        assert active_partitions == {0, 1}

    @pytest.mark.asyncio
    async def test_bigtable_on_rebalance(self, store, bt_imports):
        store.assign_partitions = MagicMock(wraps=store.assign_partitions)
        store.revoke_partitions = MagicMock(wraps=store.revoke_partitions)

        tps_table = {
            "topic1",
            "topic2",
            "topic3",
            "topic4",
            "topic5",
        }
        store.table = MagicMock(changelog_topic=MagicMock(topics=tps_table))

        store._fill_caches = MagicMock()
        assigned = {TP("topic1", 0), TP("topic2", 1)}
        revoked = {TP("topic3", 2)}
        newly_assigned = {TP("topic4", 3), TP("topic5", 4)}
        store._startup_cache_enable = False
        await store.on_rebalance(assigned, revoked, newly_assigned, generation_id=1)
        store.assign_partitions.assert_called_once_with(store.table, newly_assigned, 1)
        store.revoke_partitions.assert_called_once_with(store.table, revoked)
        store._fill_caches.assert_not_called()
        newly_assigned = set()

        # Test with empty newly_assigned
        store._startup_cache_enable = True
        await store.on_rebalance(assigned, revoked, newly_assigned, generation_id=2)
        store.assign_partitions.assert_called_with(store.table, newly_assigned, 2)
        store._fill_caches.assert_not_called()

        store._startup_cache_enable = True
        newly_assigned = {TP("topic4", 3), TP("topic5", 4)}
        await store.on_rebalance(assigned, revoked, newly_assigned, generation_id=3)
        store.assign_partitions.assert_called_with(store.table, newly_assigned, 3)
        store._fill_caches.assert_called_once_with({3, 4})

    def test_revoke_partitions(self, store):
        store._startup_cache = {b"key1": b"value1", b"key2": b"value2"}
        revoked = {TP("topic", 1), TP("topic", 2)}
        store.table = MagicMock(changelog_topic=MagicMock(topics={"topic"}))
        store.revoke_partitions(store.table, revoked)

    def test_contains(self, store, bt_imports):
        store._get = MagicMock(return_value=b"test_value")

        # Test that _contains returns True when store_check_exists is False
        store.app.conf.store_check_exists = False
        assert store._contains(b"test_key") is True

        # Test that _contains returns True when _get returns a value
        store.app.conf.store_check_exists = True
        assert store._contains(b"test_key") is True

        # Test that _contains returns False when _get returns None
        store._get = MagicMock(return_value=None)
        assert store._contains(b"test_key") is False

    def test_setup_caches_startup_cache_enable(self, store):
        options = {
            BigTableStore.BT_STARTUP_CACHE_ENABLE_KEY: True,
            BigTableStore.BT_STARTUP_CACHE_TTL_KEY: 60,
        }
        store._setup_caches(options=options)
        assert store._startup_cache_enable is True
        assert store._startup_cache_ttl == 60
        assert isinstance(store._startup_cache, dict)
        assert store._invalidation_timer is None

    def test_setup_caches_startup_cache_disable(self, store):
        options = {
            BigTableStore.BT_STARTUP_CACHE_ENABLE_KEY: False,
        }
        store._setup_caches(options=options)
        assert store._startup_cache_enable is False
        assert store._startup_cache_ttl == -1  # Default value
        assert store._startup_cache is None
        assert store._startup_cache_enable is False

    def test_set_del_get_cache(self, store):
        store._startup_cache_enable = False
        store._startup_cache = None
        partition = 1

        key = self.TEST_KEY1

        store._set_cache(partition, key, b"123")
        res = store._get_cache(partition, key)
        assert store._startup_cache is None
        assert res == (None, False)

        store._del_cache(key)
        res = store._get_cache(partition, key)
        assert res == (None, False)
        assert store._startup_cache is None

        # Now with enabled startup cache
        store._startup_cache_enable = True
        store._startup_cache = {}
        store._startup_cache[partition] = {}

        store._set_cache(partition, key, b"123")
        res = store._get_cache(partition, key)
        assert partition in store._startup_cache
        assert store._startup_cache[partition] == {key: b"123"}
        assert res == (b"123", True)
        store._del_cache(key)
        res = store._get_cache(partition, key)
        assert store._startup_cache[partition] == {key: None}
        assert res == (None, True)

    def test_persisted_offset(self, store):
        tp = TP("topic", 0)
        offset_key = store.get_offset_key(tp).encode()
        store.bt_table.data = {offset_key: b"1"}
        store._mutation_batcher = MagicMock(flush=MagicMock())

        assert store.persisted_offset(tp) == 1
        store._mutation_batcher.flush.assert_not_called()

        store._mutation_batcher_enable = True
        assert store.persisted_offset(tp) == 1
        store._mutation_batcher.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop(self, store):
        store._mutation_batcher = MagicMock(flush=MagicMock())
        store._mutation_batcher_enable = False
        await store.stop()
        store._mutation_batcher.flush.assert_not_called()

        store._mutation_batcher_enable = True
        await store.stop()
        store._mutation_batcher.flush.assert_called_once()

    def test_set_mutation(self, store):
        store._mutation_batcher = MagicMock(flush=MagicMock())
        store._set_mutation(self.TEST_KEY1, b"123", MagicMock())
        store._mutation_batcher.flush.assert_not_called()
        assert store._mutation_batcher_cache[self.TEST_KEY1] == b"123"

    def test_bigtable_iteritems_with_global_table(self, store, bt_imports):
        store.table.is_global = True
        store._active_partitions = MagicMock(return_value=[1, 3])
        # Add table to data fro partition 1 to 5 with corresponding offset keys
        store.bt_table.data = {}
        for i in range(1, 5):
            key = store.get_offset_key(TP("topic", i)).encode()
            store.bt_table.data[key] = str(i).encode()
            tp_key = store._add_partition_prefix_to_key(f"key{i}".encode(), i)
            store.bt_table.data[tp_key] = str(i).encode()

        res = sorted(store._iteritems())
        assert res == [(f"key{i}".encode(), str(i).encode()) for i in range(1, 5)]
        store.bt_table.read_rows.assert_called_once()

    def test_bigtable_iteritems_with_global_table2(self, store, bt_imports):
        store.table.is_global = False
        store.table.use_partitioner = False
        store._mutation_batcher_enable = True
        store._mutation_batcher = MagicMock(flush=MagicMock())
        store._active_partitions = MagicMock(return_value={1, 3})
        # Add table to data fro partition 1 to 5 with corresponding offset keys
        store.bt_table.data = {}
        for i in range(1, 5):
            key = store.get_offset_key(TP("topic", i)).encode()
            store.bt_table.data[key] = str(i).encode()
            tp_key = store._add_partition_prefix_to_key(f"key{i}".encode(), i)
            store.bt_table.data[tp_key] = str(i).encode()

        res = sorted(store._iteritems())
        assert res == [(f"key{i}".encode(), str(i).encode()) for i in [1, 3]]
        store.bt_table.read_rows.assert_called_once()
        store._mutation_batcher.flush.assert_called_once()

    def test_get_after_delete(self, store, bt_imports):
        partitions = [19, 20]
        store._get_cache = MagicMock(return_value=(b"this is ignored", False))
        store._key_index = {}
        store._get_current_partitions = MagicMock(return_value=partitions)
        row_mock = MagicMock()
        row_mock.commit = MagicMock()
        row_mock.delete = MagicMock()
        store.bt_table.direct_row = MagicMock(return_value=row_mock)
        store.bt_table.direct_row = MagicMock(return_value=row_mock)
        store._mutation_batcher_enable = True

        key_right = b"20_..._" + self.TEST_KEY1
        key_wrong = b"19_..._" + self.TEST_KEY1

        # This is the case if a delete happened before
        store._mutation_batcher_cache = {key_right: b"123", key_wrong: None}
        store.bt_table.add_test_data(key_right)

        res = store._get(self.TEST_KEY1)
        assert res is not None
