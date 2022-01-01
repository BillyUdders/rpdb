from collections.abc import MutableMapping
from typing import Any, Iterator

from sortedcontainers import SortedDict


class Storage(MutableMapping):
    def __init__(self) -> None:
        super().__init__()
        self._memtable: SortedDict = SortedDict()
        self._sstables: SortedDict = SortedDict(key=lambda x: str(x.path))

    def __getitem__(self, k):
        return self._memtable[k]

    def __setitem__(self, k, v) -> None:
        self._memtable[k] = v

    def __delitem__(self, v) -> None:
        del self._memtable[v]

    def __len__(self) -> int:
        return len(self._memtable)

    def __iter__(self) -> Iterator[Any]:
        return iter(self._memtable)
