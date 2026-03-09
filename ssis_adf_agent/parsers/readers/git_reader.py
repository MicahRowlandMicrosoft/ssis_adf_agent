"""
Git repository reader — clones or opens a Git repository and reads .dtsx files from it.

Supports:
  - Local bare/working repositories (no clone required)
  - Remote repositories (HTTPS / SSH) — cloned to a temp directory
"""
from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Iterator

from ..ssis_parser import SSISParser
from ..models import SSISPackage

try:
    import git  # gitpython
    _GIT_AVAILABLE = True
except ImportError:
    _GIT_AVAILABLE = False


class GitReader:
    """
    Reads SSIS packages (.dtsx) from a Git repository.

    Usage::

        reader = GitReader()
        # Remote repo — cloned to temp dir automatically:
        packages = reader.read_all("https://dev.azure.com/org/project/_git/repo")

        # Local repo:
        packages = reader.read_all("/path/to/local/repo")
    """

    def __init__(self, branch: str = "main") -> None:
        if not _GIT_AVAILABLE:
            raise ImportError(
                "gitpython is required for GitReader. Install it with: pip install gitpython"
            )
        self._parser = SSISParser()
        self.branch = branch

    def read_all(
        self,
        repo_url_or_path: str,
        sub_path: str = "",
        recursive: bool = True,
    ) -> list[SSISPackage]:
        """
        Clone (if remote) or open (if local) the repo and parse all .dtsx files.

        Args:
            repo_url_or_path: URL of remote repo or path to local repo.
            sub_path: Optional sub-directory within the repo to search.
            recursive: Whether to search recursively.

        Returns:
            List of parsed SSISPackage objects.
        """
        p = Path(repo_url_or_path)
        if p.exists() and (p / ".git").exists() or (p.is_dir() and p.suffix == ".git"):
            return self._read_local(p, sub_path, recursive)
        else:
            return self._read_remote(repo_url_or_path, sub_path, recursive)

    def _read_local(self, repo_path: Path, sub_path: str, recursive: bool) -> list[SSISPackage]:
        search_root = repo_path / sub_path if sub_path else repo_path
        return self._parse_dtsx_files(search_root, recursive)

    def _read_remote(self, url: str, sub_path: str, recursive: bool) -> list[SSISPackage]:
        with tempfile.TemporaryDirectory(prefix="ssis_adf_git_") as tmp:
            tmp_path = Path(tmp)
            git.Repo.clone_from(url, tmp_path, branch=self.branch, depth=1)  # shallow clone
            search_root = tmp_path / sub_path if sub_path else tmp_path
            return self._parse_dtsx_files(search_root, recursive)

    def _parse_dtsx_files(self, search_root: Path, recursive: bool) -> list[SSISPackage]:
        pattern = "**/*.dtsx" if recursive else "*.dtsx"
        packages: list[SSISPackage] = []
        errors: list[str] = []
        for dtsx in sorted(search_root.glob(pattern)):
            try:
                packages.append(self._parser.parse(dtsx))
            except Exception as exc:
                errors.append(f"{dtsx}: {exc}")
        if errors:
            import warnings
            for err in errors:
                warnings.warn(f"Skipped package due to parse error: {err}", stacklevel=2)
        return packages

    def iter_dtsx_blobs(self, repo_url_or_path: str, branch: str | None = None) -> Iterator[tuple[str, str]]:
        """
        Iterate over (file_path, xml_content) tuples for all .dtsx blobs in the
        repository without writing to disk (reads from git object store directly).
        Only works for local repos.
        """
        repo = git.Repo(repo_url_or_path)
        ref = branch or self.branch
        try:
            commit = repo.commit(ref)
        except Exception:
            commit = repo.head.commit

        def _walk_tree(tree: git.Tree, prefix: str = "") -> Iterator[tuple[str, str]]:
            for blob in tree.blobs:
                if blob.name.endswith(".dtsx"):
                    yield f"{prefix}{blob.name}", blob.data_stream.read().decode("utf-8", errors="replace")
            for subtree in tree.trees:
                yield from _walk_tree(subtree, f"{prefix}{subtree.name}/")

        yield from _walk_tree(commit.tree)
