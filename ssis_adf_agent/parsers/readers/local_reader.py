"""
Local filesystem reader — discovers and reads .dtsx files from a local directory.
"""
from __future__ import annotations

from pathlib import Path

from ..ssis_parser import SSISParser
from ..models import SSISPackage


class LocalReader:
    """
    Reads SSIS packages (.dtsx) from the local filesystem.

    Usage::

        reader = LocalReader()
        packages = reader.scan("/path/to/ssis/project")
        package = reader.read("/path/to/package.dtsx")
    """

    def __init__(self) -> None:
        self._parser = SSISParser()

    def scan(self, directory: str | Path, recursive: bool = True) -> list[Path]:
        """Return all .dtsx file paths found under *directory*."""
        root = Path(directory)
        if not root.exists():
            raise FileNotFoundError(f"Directory not found: {root}")
        pattern = "**/*.dtsx" if recursive else "*.dtsx"
        return sorted(root.glob(pattern))

    def read(self, path: str | Path) -> SSISPackage:
        """Parse and return a single .dtsx file."""
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Package file not found: {p}")
        return self._parser.parse(p)

    def read_all(self, directory: str | Path, recursive: bool = True) -> list[SSISPackage]:
        """Scan *directory* and parse every .dtsx file found."""
        paths = self.scan(directory, recursive)
        packages: list[SSISPackage] = []
        errors: list[str] = []
        for p in paths:
            try:
                packages.append(self.read(p))
            except Exception as exc:
                errors.append(f"{p}: {exc}")
        if errors:
            import warnings
            for err in errors:
                warnings.warn(f"Skipped package due to parse error: {err}", stacklevel=2)
        return packages
