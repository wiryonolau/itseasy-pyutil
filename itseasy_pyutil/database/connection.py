import logging
import sys

import sqlalchemy as sa
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

    def __getattr__(self, name):
        return getattr(self._conn, name)
