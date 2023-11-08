from unittest.mock import MagicMock, call, patch
import time

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
