from pathlib import Path

from .source import SourceError


class DiskBudget:
    def __init__(self, root, maximum_bytes):
        self.root = Path(root).resolve()
        self.maximum_bytes = maximum_bytes
        self.high_water_bytes = 0
        self.sizes = {}
        self.total_bytes = 0

    def check(self, path=None, size=None, force=False):
        if force:
            self.sizes = {path: path.stat().st_size for path in self.root.rglob("*") if path.is_file()}
            self.total_bytes = sum(self.sizes.values())
        elif path is not None:
            path = Path(path).resolve()
            path.relative_to(self.root)
            size = path.stat().st_size if size is None else size
            self.total_bytes += size - self.sizes.get(path, 0)
            self.sizes[path] = size
        self.high_water_bytes = max(self.high_water_bytes, self.total_bytes)
        if self.total_bytes > self.maximum_bytes:
            raise SourceError(f"Local output/scratch budget exceeded: {self.total_bytes} > {self.maximum_bytes} bytes. "
                              "No new active snapshot was published; review storage sizing before retrying.")
