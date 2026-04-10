import logging

from sqlalchemy import inspection
from sqlalchemy.sql import Executable

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

    def __enter__(self):
        self._conn.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self._conn.__exit__(exc_type, exc_val, exc_tb)

    # 🔑 CRITICAL: propagate wrapping
    def execution_options(self, *args, **kwargs):
        conn = self._conn.execution_options(*args, **kwargs)
        return DryRunConnection(conn)

    def begin(self, *args, **kwargs):
        self._conn.begin(*args, **kwargs)
        return self

    def commit(self):
        pass

    def rollback(self):
        pass

    @property
    def dialect(self):
        return self._dialect

    @property
    def engine(self):
        return self._conn.engine

    @property
    def connection(self):
        return self._conn.connection

    def _is_read(self, sql: str) -> bool:
        sql = sql.strip().lower()
        return sql.startswith(("select", "show", "describe", "with"))

    def _print(self, sql: str):
        logger.info("\n--\n\n%s", sql)

    def execute(self, statement, *args, **kwargs):
        if isinstance(statement, Executable):
            compiled = statement.compile(
                dialect=self._dialect,
                compile_kwargs={"literal_binds": True},
            )
            sql = str(compiled)
        else:
            sql = str(statement)

        if self._is_read(sql):
            return self._conn.execute(statement, *args, **kwargs)

        self._print(sql)
        return DryRunResult()

    def exec_driver_sql(self, statement, *args, **kwargs):
        sql = str(statement)

        if self._is_read(sql):
            return self._conn.exec_driver_sql(statement, *args, **kwargs)

        self._print(sql)
        return DryRunResult()

    def __getattr__(self, name):
        return getattr(self._conn, name)


@inspection._inspects(DryRunConnection)
def inspect_dry_run_connection(subject):
    return inspection.inspect(subject._conn)
