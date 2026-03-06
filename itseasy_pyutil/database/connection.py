import logging
import sys

import sqlalchemy as sa
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
        self.dialect = conn.dialect

    def _print_sql(self, sql):
        logger.info(sql)

    def execute(self, statement, *args, **kwargs):
        if isinstance(statement, Executable):
            compiled = statement.compile(
                dialect=self.dialect,
                compile_kwargs={"literal_binds": True},
            )
            sql = str(compiled)
        else:
            sql = str(statement)

        self._print_sql(sql)

        return DryRunResult()

    def exec_driver_sql(self, statement, *args, **kwargs):
        self._print_sql(statement)

        return DryRunResult()

    def __getattr__(self, name):
        return getattr(self._conn, name)
