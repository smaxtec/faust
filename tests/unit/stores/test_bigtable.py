import time
from unittest.mock import MagicMock, call, patch

import pytest

import faust
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
        store._set_mutation = MagicMock()

        store._bigtable_del(self.TEST_KEY1, no_key_translation=True)
        store._set_mutation.assert_called_once_with(
            self.TEST_KEY1, row_mock, None
        )

    def test_bigtable_set(self, store):
        row_mock = MagicMock()
        row_mock.set_cell = MagicMock()

        store.bt_table.direct_row = MagicMock(return_value=row_mock)
        store._set_mutation = MagicMock(return_value=None)
        store._bigtable_set(
            self.TEST_KEY1, self.TEST_KEY1, no_key_translation=True
        )
        store._bigtable_set(
            self.TEST_KEY1, self.TEST_KEY1, no_key_translation=True
        )

        store._set_mutation.assert_called_with(
            self.TEST_KEY1, row_mock, self.TEST_KEY1
        )

    def test_get_partition_from_message(self, store):
        event_mock = MagicMock()
        event_mock.message = MagicMock()
        event_mock.message.partition = 69
        current_event_mock = MagicMock(return_value=event_mock)

        store.table.is_global = False
        store.table.use_partitioner = False
        with patch("faust.stores.bigtable.current_event", current_event_mock):
            return_value = store._get_current_partitions()
            assert return_value == [69]

        store.table.is_global = True
        with patch("faust.stores.bigtable.current_event", current_event_mock):
            return_value = store._get_current_partitions()
            assert return_value == [None]

        store.table.is_global = False
        current_event_mock = MagicMock(return_value=None)

        topic = store.table.changelog_topic_name
        store.app.assignor.assigned_actives = MagicMock(
            return_value={TP(topic, 420)}
        )
        store.app.conf.topic_partitions = 421
        with patch("faust.stores.bigtable.current_event", current_event_mock):
            return_value = store._get_current_partitions()
            assert return_value == [420]

    def test_get_faust_key(self, store):
        key_with_partition = b"\x13_..._THEACTUALKEY"
        res = store._remove_partition_prefix_from_bigtable_key(
            key_with_partition
        )
        assert res == b"THEACTUALKEY"

    def test_get_key_with_partition(self, store):
        partition = 19
        res = store._add_partition_prefix_to_key(self.TEST_KEY1, partition)
        extracted_partition = store._get_partition_from_bigtable_key(res)
        assert extracted_partition == partition
        assert (
            store._remove_partition_prefix_from_bigtable_key(res)
            == self.TEST_KEY1
        )

    def test_partitions_for_key(self, store):
        store._get_current_partitions = MagicMock(return_value=[19])
        res = list(store._get_possible_bt_keys(self.TEST_KEY1))
        assert res == [store._add_partition_prefix_to_key(self.TEST_KEY1, 19)]

    def test_get_keyerror(self, store):
        partition = 19
        store._get_current_partitions = MagicMock(return_value=[partition])
        store._bigtable_get = MagicMock(return_value=None)
        with pytest.raises(KeyError):
            store[self.TEST_KEY1.decode()]

    def test_get_with_known_partition(self, store):
        partition = 19
        store._cache = None
        store._get_current_partitions = MagicMock(return_value=[partition])
        # Scenario: Found
        store._bigtable_get = MagicMock(return_value=b"a_value")
        res = store._get(self.TEST_KEY1)
        key_with_partition = store._add_partition_prefix_to_key(
            self.TEST_KEY1, partition
        )
        store._bigtable_get.assert_called_once_with(self.TEST_KEY1)
        assert res == b"a_value"

        # Scenario: Not Found
        store._bigtable_get = MagicMock(return_value=None)
        res = store._get(self.TEST_KEY1)
        store._bigtable_get.assert_called_once_with(self.TEST_KEY1)
        assert res is None

        # Scenario: Cache hit on value
        store._bigtable_get = MagicMock(return_value=None)
        store._cache = {self.TEST_KEY1: b"a_value_from_cache"}
        res = store._get(self.TEST_KEY1)
        store._bigtable_get.assert_not_called()
        res2 = store._get(self.TEST_KEY2)
        assert res == b"a_value_from_cache"
        store._bigtable_get.assert_called_once_with(self.TEST_KEY2)
        assert store._cache[self.TEST_KEY2] is None
        assert res2 is None

        # Scenario: Cache hit on None value
        store._bigtable_get = MagicMock(return_value=None)
        res = store._get(self.TEST_KEY2)
        store._bigtable_get.assert_not_called()
        assert res is None

    def test_set(self, store):
        # Scenario: No cache
        store._cache = None
        store._bigtable_set = MagicMock()
        store._set(self.TEST_KEY1, b"a_value")
        store._bigtable_set.assert_called_once_with(self.TEST_KEY1, b"a_value")

        # Scenario: Cache active
        store._cache = {}
        store._set(self.TEST_KEY1, b"b_value")
        assert store._cache[self.TEST_KEY1] == b"b_value"
        store._bigtable_set.assert_called_with(self.TEST_KEY1, b"b_value")

    def test_del(self, store):
        # Scenario: No cache
        store._cache = None
        store._bigtable_del = MagicMock()
        store._del(self.TEST_KEY1)
        store._bigtable_del.assert_called_once_with(self.TEST_KEY1)

        # Scenario: Cache active
        store._cache = {}
        store._del(self.TEST_KEY1)
        assert store._cache[self.TEST_KEY1] is None
        store._bigtable_del.assert_called_with(self.TEST_KEY1)

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
        store._active_partitions = MagicMock(return_value=[1, 3])
        store.bt_table.read_rows = MagicMock()
        store._mutation_buffer = None
        store._cache = {}

        _ = sorted(store._iteritems())
        store.bt_table.read_rows.assert_called_once()

    def test_iteritems_with_mutations(self, store):
        store._active_partitions = MagicMock(return_value=[1, 3])
        store._mutation_buffer = {
                self.TEST_KEY1: ("doesn't matter", b"a_value"),
                self.TEST_KEY2: ("doesn't matter", None),
        }
        store.bt_table.read_rows = MagicMock(
            return_value=[
                MagicMock(
                    row_key=self.TEST_KEY1,
                    to_dict=MagicMock(
                        return_value={"x": [MagicMock(value=b"1")]}
                    ),
                    commit=MagicMock(),
                ),
                MagicMock(
                    row_key=self.TEST_KEY2,
                    to_dict=MagicMock(
                        return_value={"x": [MagicMock(value=b"this is overwritten")]}
                    ),
                    commit=MagicMock(),
                ),
            ]
        )
        res = sorted(store._iteritems())
        store.bt_table.read_rows.assert_called_once()
        assert res == [(self.TEST_KEY1, b"a_value")]
        assert store._cache.get(self.TEST_KEY1) == b"a_value"
        assert store._cache.get(self.TEST_KEY2) is None

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
        store.bt_table.mutate_rows = MagicMock()
        store._mutation_buffer = None

        store.set_persisted_offset(tp, 123)
        store._bigtable_set.called_once_with(expected_offset_key, b"123", no_key_translation=True)
        store.bt_table.mutate_rows.assert_not_called()

        store._bigtable_set = MagicMock()
        store._mutation_buffer = {}
        store._mutation_size = 0
        store.set_persisted_offset(tp, 123)
        store._bigtable_set.assert_not_called()
        store.bt_table.mutate_rows.assert_not_called()

        store._bigtable_set = MagicMock()
        store._mutation_buffer = {

            self.TEST_KEY1: ("doesn't matter", b"a_value"),
            self.TEST_KEY2: ("doesn't matter", None),
            self.TEST_KEY3: ("doesn't matter", b"c_value"),
        }
        mutations = [
            r[0] for r in store._mutation_buffer.copy().values()
        ]
        store._num_mutations = 999999999999999999999999999999999999999999
        store.set_persisted_offset(tp, 123)
        store._bigtable_set.called_once_with(
            expected_offset_key,
            b"123",
            no_key_translation=True
        )
        store.bt_table.mutate_rows.assert_called_once_with(mutations)

    def test_apply_changelog_batch(self, store):
        row_mock = MagicMock()
        row_mock.delete = MagicMock()
        row_mock.set_cell = MagicMock()
        store.bt_table.direct_row = MagicMock(return_value=row_mock)
        store._bigtable_del = MagicMock()
        store._bigtable_set = MagicMock()
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
        assert store._bigtable_set.call_count == 4
        assert store._bigtable_del.call_count == 1
        assert store.set_persisted_offset.call_count == 2
