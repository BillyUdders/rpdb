from collections.abc import Mapping
from pathlib import Path
from typing import Iterator

from sortedcontainers import SortedDict


class SSTable(Mapping):
    def __init__(self, path=None):
        self.path: Path = Path(path)
        self.index_path: Path = self.path.with_name(self.path.name + ".index")
        self.search_index: SortedDict = SortedDict()

    def __getitem__(self, k):
        pass

    def __len__(self) -> int:
        pass

    def __iter__(self) -> Iterator:
        pass
