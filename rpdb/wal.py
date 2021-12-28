import json
import time
from struct import pack, unpack
from typing import Iterator, Tuple

from rpdb.operations import Operation


class WAL:
    def __init__(self, wal_file_location: str) -> None:
        self.writer = open(wal_file_location, "+ab")

    def __del__(self):
        self.writer.close()

    def append(self, op: Operation):
        key_bytes = self.__create_key()
        value_bytes = json.dumps(op).encode("utf-8")

        self.writer.write(pack("I", len(key_bytes)))
        self.writer.write(key_bytes)
        self.writer.write(pack("I", len(value_bytes)))
        self.writer.write(value_bytes)
        self.writer.flush()

    def restore_all(self):
        self.writer.seek(0)
        yield from self.read_pairs()

    def clear(self):
        self.writer.truncate(0)

    def read_pairs(self) -> Iterator[Tuple[str, str]]:
        while True:
            key_len_bytes = self.writer.read(len(self.__create_key()))
            if not key_len_bytes:
                break
            (key_len,) = unpack("I", key_len_bytes)
            key_bytes = self.writer.read(key_len)
            (value_len,) = unpack("I", self.writer.read(4))
            value_bytes = self.writer.read(value_len)
            yield key_bytes.decode("utf-8"), value_bytes.decode("utf-8")

    @staticmethod
    def __create_key():
        return str(time.time_ns()).encode("utf-8")
