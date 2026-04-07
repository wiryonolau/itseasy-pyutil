import logging

from sqlalchemy.sql import Executable

from itseasy_pyutil.database.formatter import SQLFormatter

logger = logging.getLogger(__name__)


class DryRunResult:
    def fetchone(self):
        return None

    def fetchall(self):
        return []

    def first(self):
        return None

    def scalar(self):
        return None

    def one(self):
        raise Exception("No rows (dry run)")

    def one_or_none(self):
        return None

    def __iter__(self):
        return iter([])


class DryRunConnection:
    def __init__(self, conn):
        self._conn = conn
        self._dialect = conn.dialect
        self._formatter = SQLFormatter()

    def __getattr__(self, name):
        if name in {"execute", "exec_driver_sql"}:
            return getattr(self, name)
        raise AttributeError(f"{name} is not allowed in DryRunConnection")

    @property
    def connection(self):
        raise RuntimeError("DBAPI access is disabled in dry-run mode")

    @property
    def engine(self):
        raise RuntimeError("Engine execution disabled in dry-run mode")

    def scalar(self, *args, **kwargs):
        return None

    # SqlAlchemy
    def _execute_20(self, statement, *args, **kwargs):
        return self.execute(statement, *args, **kwargs)

    def _clean_sql(self, sql: str) -> str:
        return self._formatter.format(sql)

    def _print_sql(self, sql):
        cleaned = self._clean_sql(sql)

        logger.info("\n--\n\n%s", cleaned)

    def _is_read_query(self, sql: str) -> bool:
        sql = sql.lstrip().lower()
        return (
            sql.startswith("select")
            or sql.startswith("show")
            or sql.startswith("describe")
        )

    def begin(self, *args, **kwargs):
        return self

    def commit(self):
        pass

    def rollback(self):
        pass

    def execute(self, statement, *args, **kwargs):
        if isinstance(statement, Executable):
            compiled = statement.compile(
                dialect=self._dialect,
                compile_kwargs={"literal_binds": True},
            )
            sql = str(compiled)
        else:
            sql = str(statement)

        # Skip read queries entirely
        if self._is_read_query(sql):
            return DryRunResult()

        self._print_sql(sql)

        return DryRunResult()

    def exec_driver_sql(self, statement, *args, **kwargs):
        sql = str(statement)

        if self._is_read_query(sql):
            return DryRunResult()

        self._print_sql(sql)
        return DryRunResult()
