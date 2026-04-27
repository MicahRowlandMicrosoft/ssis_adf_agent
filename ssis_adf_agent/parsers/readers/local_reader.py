"""
Local filesystem reader — discovers and reads .dtsx files from a local directory.
"""
from __future__ import annotations

from pathlib import Path

from ...warnings_collector import warn
from ..models import SSISPackage
from ..ssis_parser import SSISParser, parse_project_params


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
        """Parse and return a single .dtsx file.

        If a sibling ``Project.params`` file exists in the same directory,
        its parameters are loaded onto ``package.project_parameters``.
        """
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Package file not found: {p}")
        pkg = self._parser.parse(p)
        # Auto-load Project.params from the same directory (SSIS project layout)
        params_file = p.parent / "Project.params"
        if params_file.exists():
            try:
                pkg.project_parameters = parse_project_params(params_file)
            except Exception as exc:  # pragma: no cover - defensive
                warn(
                    phase="parse", severity="warning", source="local_reader",
                    message=f"Failed to load Project.params for {p.name}: {exc}",
                )
        return pkg

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
        for err in errors:
            warn(
                phase="parse", severity="error", source="local_reader",
                message=f"Skipped package due to parse error: {err}",
            )
        return packages
