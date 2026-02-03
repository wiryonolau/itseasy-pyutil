import sys
from collections import namedtuple
from contextlib import asynccontextmanager

import aiosqlite

from itseasy_pyutil import list_get
from itseasy_pyutil.database import (
    AbstractDatabase,
    Condition,
    ConditionSet,
    Response,
)


class Database(AbstractDatabase):
    async def connect(self):
        self._conn = await aiosqlite.connect(self._db_config["database"])
        self._conn.row_factory = aiosqlite.Row

        # --- REQUIRED PRAGMAs (idempotent) ---
        await self._conn.execute("PRAGMA journal_mode = WAL")
        await self._conn.execute("PRAGMA synchronous = NORMAL")
        await self._conn.execute("PRAGMA foreign_keys = ON")

    async def close(self):
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def reconnect(self):
        await self.close()
        await self.connect()

    @asynccontextmanager
    async def get_conn(self):
        if not self._conn:
            await self.connect()

        try:
            yield self._conn, self._conn
        except Exception:
            raise

    @asynccontextmanager
    async def tx(self):
        if not self._conn:
            await self.connect()

        try:
            await self._conn.execute("BEGIN")
            yield self._conn
            await self._conn.commit()
        except Exception:
            await self._conn.rollback()
            raise

    # ------------------------------------------------------------------
    # fetch
    # ------------------------------------------------------------------
    async def get_rows(self, query, params=()):
        if self._as_dev:
            self._logger.debug(self.get_sql_string(query, params))

        async with self._conn.execute(query, params) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def get_row(self, query, params=()):
        if self._as_dev:
            self._logger.debug(self.get_sql_string(query, params))

        async with self._conn.execute(query, params) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None

    # ------------------------------------------------------------------
    # filters
    # ------------------------------------------------------------------
    async def get_filter_row(self, **kwargs):
        rows = await self.get_filter_rows(limit=1, offset=0, **kwargs)
        return list_get(rows, 0, None)

    async def get_filter_rows(
        self,
        table,
        columns=[],
        joins=[],
        conditions=[],
        orders=[],
        offset=0,
        limit=1000,
    ):
        join_stmt, join_params = self.parse_joins(joins)
        cond_stmt, cond_params = self.parse_conditions(conditions)

        cols = [self.sanitize_identifier(c, allow_star=True) for c in columns]

        query = f"""
            SELECT {",".join(cols) or "*"}
            FROM {self.sanitize_identifier(table)}
            {join_stmt}
            {cond_stmt}
            LIMIT ? OFFSET ?
        """

        params = join_params + cond_params + [limit, offset]

        if self._as_dev:
            self._logger.debug(self.get_sql_string(query, params))

        return await self.get_rows(query, params)

    async def get_count(self, table, joins=[], conditions=[]):
        join_stmt, join_params = self.parse_joins(joins)
        cond_stmt, cond_params = self.parse_conditions(conditions)

        query = f"""
            SELECT COUNT(*) AS total
            FROM {self.sanitize_identifier(table)}
            {join_stmt}
            {cond_stmt}
        """

        row = await self.get_row(query, join_params + cond_params)
        return row["total"] if row else 0

    # ------------------------------------------------------------------
    # execute
    # ------------------------------------------------------------------
    async def execute(self, query, params=(), return_result=False):
        if self._as_dev:
            self._logger.debug(self.get_sql_string(query, params))

        try:
            async with self.tx():
                cur = await self._conn.execute(query, params)

                if return_result:
                    rows = await cur.fetchall()
                    return [dict(r) for r in rows]

                return Response(
                    success=True,
                    lastrowid=cur.lastrowid,
                    data={},
                    error=None,
                )
        except Exception as e:
            return Response(
                success=False, lastrowid=None, data={}, error=str(e)
            )

    async def execute_many(self, statements=[]):
        affected = 0
        try:
            async with self.tx():
                for stmt in statements:
                    query, params = stmt if len(stmt) == 2 else (stmt[0], [])
                    cur = await self._conn.execute(query, params)
                    affected += cur.rowcount

            return Response(success=True, lastrowid=None, data={}, error=None)
        except Exception as e:
            return Response(
                success=False, lastrowid=None, data={}, error=str(e)
            )

    # ------------------------------------------------------------------
    # DML
    # ------------------------------------------------------------------
    async def delete(self, table, conditions=[]):
        cond_sql, params = self.parse_conditions(conditions)

        query = f"""
            DELETE FROM {self.sanitize_identifier(table)}
            {cond_sql}
        """

        return await self.execute(query, params)

    async def insert(self, table, column_values={}):
        cols = list(column_values.keys())
        values = list(column_values.values())

        col_sql = ", ".join(self.sanitize_identifier(c) for c in cols)
        val_sql = ", ".join(["?"] * len(values))

        sql = f"""
            INSERT INTO {self.sanitize_identifier(table)}
            ({col_sql}) VALUES ({val_sql})
        """

        return await self.execute(sql, values)

    async def update(
        self, table, identifiers=[], column_values={}, conditions=[]
    ):
        set_cols = {
            k: v for k, v in column_values.items() if k not in identifiers
        }

        set_sql = ", ".join(
            f"{self.sanitize_identifier(c)}=?" for c in set_cols
        )

        id_conditions = [
            Condition(column=c, value=column_values[c]) for c in identifiers
        ]

        conditions = id_conditions + [ConditionSet(conditions=conditions)]
        cond_sql, cond_params = self.parse_conditions(conditions)

        sql = f"""
            UPDATE {self.sanitize_identifier(table)}
            SET {set_sql}
            {cond_sql}
        """

        params = list(set_cols.values()) + cond_params
        return await self.execute(sql, params)

    async def upsert(
        self, table, identifiers=[], column_values={}, has_auto_id=True
    ):
        UpsertResponse = namedtuple("UpsertResponse", identifiers + ["error"])

        cols = list(column_values.keys())
        values = list(column_values.values())

        conflict_cols = ", ".join(
            self.sanitize_identifier(c) for c in identifiers
        )

        update_cols = [c for c in cols if c not in identifiers]

        update_sql = ", ".join(
            f"{self.sanitize_identifier(c)}=excluded.{self.sanitize_identifier(c)}"
            for c in update_cols
        )

        sql = f"""
            INSERT INTO {self.sanitize_identifier(table)}
            ({",".join(self.sanitize_identifier(c) for c in cols)})
            VALUES ({",".join(["?"] * len(values))})
            ON CONFLICT ({conflict_cols})
            DO UPDATE SET {update_sql}
        """

        try:
            async with self.tx():
                cur = await self._conn.execute(sql, values)

            result = [column_values.get(i) for i in identifiers]
            if has_auto_id and cur.lastrowid:
                result[0] = cur.lastrowid

            result.append(None)
            return UpsertResponse(*result)

        except Exception as e:
            return UpsertResponse(
                *[column_values.get(i) for i in identifiers], str(e)
            )
