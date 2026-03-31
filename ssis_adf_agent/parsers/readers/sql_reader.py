"""
SQL Server msdb reader — retrieves SSIS package XML stored in SQL Server.

Supports two storage modes:
  1. **MSDB storage**: packages stored in msdb.dbo.sysssispackages (SQL Server 2005-2012 style)
  2. **SSIS Catalog (SSISDB)**: packages stored in SSISDB.catalog.packages (SQL Server 2012+)
"""
from __future__ import annotations

import base64
import zlib
from dataclasses import dataclass

from ..ssis_parser import SSISParser
from ..models import SSISPackage, SqlAgentSchedule

try:
    import pyodbc
    _PYODBC_AVAILABLE = True
except ImportError:
    _PYODBC_AVAILABLE = False


@dataclass
class PackageRef:
    name: str
    folder: str
    project: str
    package_id: str


class SqlServerReader:
    """
    Reads SSIS packages from SQL Server (msdb or SSISDB catalog).

    Usage::

        reader = SqlServerReader(
            server="myserver",
            database="msdb",
            trusted_connection=True,
        )
        packages = reader.read_all()

        # Or for SSISDB:
        reader = SqlServerReader(server="myserver", database="SSISDB", trusted_connection=True)
        packages = reader.read_all_from_catalog(folder="MyFolder", project="MyProject")
    """

    # SQL for msdb storage (pre-catalog)
    _MSDB_LIST_SQL = """
        SELECT [name], [id], [description], [folderid]
        FROM [msdb].[dbo].[sysssispackages]
        ORDER BY [name]
    """

    _MSDB_READ_SQL = """
        SELECT [packagedata]
        FROM [msdb].[dbo].[sysssispackages]
        WHERE [name] = ?
    """

    # SQL for SSISDB catalog
    _CATALOG_LIST_SQL = """
        SELECT p.name, p.package_id, f.name AS folder_name, pr.name AS project_name
        FROM [SSISDB].[catalog].[packages] p
        JOIN [SSISDB].[catalog].[projects] pr ON p.project_id = pr.project_id
        JOIN [SSISDB].[catalog].[folders] f ON pr.folder_id = f.folder_id
        {where_clause}
        ORDER BY f.name, pr.name, p.name
    """

    _CATALOG_READ_SQL = """
        DECLARE @project_binary VARBINARY(MAX);
        SELECT @project_binary = p.project_lsn
        FROM [SSISDB].[catalog].[packages] packages
        WHERE packages.package_id = ?;
        SELECT @project_binary;
    """

    def __init__(
        self,
        server: str,
        database: str = "msdb",
        username: str | None = None,
        password: str | None = None,
        trusted_connection: bool = True,
        driver: str = "ODBC Driver 18 for SQL Server",
        encrypt: bool = True,
        trust_server_certificate: bool = False,
    ) -> None:
        if not _PYODBC_AVAILABLE:
            raise ImportError(
                "pyodbc is required for SqlServerReader. Install with: pip install pyodbc"
            )
        self._parser = SSISParser()
        self._conn_str = self._build_conn_str(
            server, database, username, password,
            trusted_connection, driver, encrypt, trust_server_certificate
        )

    def _build_conn_str(
        self,
        server: str,
        database: str,
        username: str | None,
        password: str | None,
        trusted: bool,
        driver: str,
        encrypt: bool,
        trust_server_cert: bool,
    ) -> str:
        parts = [
            f"DRIVER={{{driver}}}",
            f"SERVER={server}",
            f"DATABASE={database}",
            f"Encrypt={'yes' if encrypt else 'no'}",
            f"TrustServerCertificate={'yes' if trust_server_cert else 'no'}",
        ]
        if trusted:
            parts.append("Trusted_Connection=yes")
        else:
            if username:
                parts.append(f"UID={username}")
            if password:
                parts.append(f"PWD={password}")
        return ";".join(parts)

    def _connect(self) -> "pyodbc.Connection":
        return pyodbc.connect(self._conn_str, timeout=30)

    # ------------------------------------------------------------------
    # msdb storage
    # ------------------------------------------------------------------

    def list_packages(self) -> list[str]:
        """List all package names in msdb.dbo.sysssispackages."""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(self._MSDB_LIST_SQL)
            return [row[0] for row in cursor.fetchall()]

    def read(self, package_name: str) -> SSISPackage:
        """Read and parse a single package by name from msdb."""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(self._MSDB_READ_SQL, (package_name,))
            row = cursor.fetchone()
            if row is None:
                raise ValueError(f"Package '{package_name}' not found in msdb")
            xml = self._decode_package_data(row[0])
            return self._parser.parse_xml(xml, source_identifier=f"msdb://{package_name}")

    def read_all(self) -> list[SSISPackage]:
        """Read and parse all packages from msdb.dbo.sysssispackages."""
        names = self.list_packages()
        packages: list[SSISPackage] = []
        errors: list[str] = []
        for name in names:
            try:
                packages.append(self.read(name))
            except Exception as exc:
                errors.append(f"{name}: {exc}")
        if errors:
            import warnings
            for err in errors:
                warnings.warn(f"Skipped package: {err}", stacklevel=2)
        return packages

    def _decode_package_data(self, data: bytes | bytearray | memoryview) -> str:
        """
        msdb stores package XML as compressed / base64 binary in older formats,
        or as raw XML bytes. Attempt to decode appropriately.
        """
        raw = bytes(data)
        # Try raw UTF-8 / UTF-16 XML first
        for enc in ("utf-8-sig", "utf-16", "utf-8"):
            try:
                text = raw.decode(enc)
                if text.lstrip().startswith("<"):
                    return text
            except (UnicodeDecodeError, ValueError):
                continue

        # Try zlib decompress (some older SQL versions compress the blob)
        try:
            decompressed = zlib.decompress(raw)
            return decompressed.decode("utf-8", errors="replace")
        except zlib.error:
            pass

        # Try base64 decode then decompress
        try:
            decoded = base64.b64decode(raw)
            decompressed = zlib.decompress(decoded)
            return decompressed.decode("utf-8", errors="replace")
        except Exception:
            pass

        # Last resort: treat as raw bytes
        return raw.decode("utf-8", errors="replace")

    # ------------------------------------------------------------------
    # SSIS Catalog (SSISDB) storage
    # ------------------------------------------------------------------

    def list_catalog_packages(
        self,
        folder: str | None = None,
        project: str | None = None,
    ) -> list[PackageRef]:
        """List packages in the SSISDB catalog, optionally filtered by folder/project."""
        conditions: list[str] = []
        params: list[str] = []
        if folder:
            conditions.append("f.name = ?")
            params.append(folder)
        if project:
            conditions.append("pr.name = ?")
            params.append(project)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        sql = self._CATALOG_LIST_SQL.format(where_clause=where)

        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            return [
                PackageRef(
                    name=row[0],
                    package_id=str(row[1]),
                    folder_name=row[2],
                    project_name=row[3],
                )
                for row in cursor.fetchall()
            ]

    def read_all_from_catalog(
        self,
        folder: str | None = None,
        project: str | None = None,
    ) -> list[SSISPackage]:
        """
        Read all packages from SSISDB catalog.

        Note: SSISDB stores project deployment packages as compiled .ispac binaries.
        This method reads the raw XML for package-deployment model packages only.
        For project-deployment model, the .ispac must be extracted separately.
        """
        refs = self.list_catalog_packages(folder, project)
        packages: list[SSISPackage] = []
        errors: list[str] = []

        with self._connect() as conn:
            for ref in refs:
                try:
                    cursor = conn.cursor()
                    # For package deployment model, packagedata is available
                    cursor.execute(
                        "SELECT packagedata FROM [SSISDB].[catalog].[packages] WHERE package_id = ?",
                        (ref.package_id,),
                    )
                    row = cursor.fetchone()
                    if row is None or row[0] is None:
                        errors.append(
                            f"{ref.folder_name}/{ref.project_name}/{ref.name}: "
                            "No packagedata (project-deployment model — extract .ispac instead)"
                        )
                        continue
                    xml = self._decode_package_data(row[0])
                    source = f"ssisdb://{ref.folder_name}/{ref.project_name}/{ref.name}"
                    packages.append(self._parser.parse_xml(xml, source_identifier=source))
                except Exception as exc:
                    errors.append(f"{ref.folder_name}/{ref.name}: {exc}")

        if errors:
            import warnings
            for err in errors:
                warnings.warn(f"Skipped catalog package: {err}", stacklevel=2)

        return packages

    # ------------------------------------------------------------------
    # SQL Agent schedule extraction
    # ------------------------------------------------------------------

    _AGENT_SCHEDULE_SQL = """
        SELECT
            j.name AS job_name,
            s.name AS schedule_name,
            s.freq_type,
            s.freq_interval,
            s.freq_subday_type,
            s.freq_subday_interval,
            s.active_start_time,
            s.active_end_time,
            s.freq_recurrence_factor
        FROM msdb.dbo.sysjobs j
        JOIN msdb.dbo.sysjobschedules js ON j.job_id = js.job_id
        JOIN msdb.dbo.sysschedules s ON js.schedule_id = s.schedule_id
        WHERE j.name = ?
          AND s.enabled = 1
        ORDER BY js.next_run_date DESC
    """

    def read_agent_schedule(self, job_name: str) -> SqlAgentSchedule | None:
        """Read the SQL Agent job schedule for a given job name.

        Returns the first enabled schedule or None if no schedule found.
        """
        try:
            with self._connect() as conn:
                cursor = conn.cursor()
                cursor.execute(self._AGENT_SCHEDULE_SQL, (job_name,))
                row = cursor.fetchone()
                if row is None:
                    return None
                return SqlAgentSchedule(
                    job_name=row[0],
                    schedule_name=row[1],
                    frequency_type=int(row[2]),
                    freq_interval=int(row[3]),
                    freq_subday_type=int(row[4]),
                    freq_subday_interval=int(row[5]),
                    active_start_time=int(row[6]),
                    active_end_time=int(row[7]),
                    freq_recurrence_factor=int(row[8]),
                )
        except Exception:
            return None
