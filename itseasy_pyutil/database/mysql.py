import sys
from collections import namedtuple
from contextlib import asynccontextmanager

import aiomysql

from itseasy_pyutil import list_get
from itseasy_pyutil.database import (
    AbstractDatabase,
    Condition,
    ConditionSet,
    Response,
)


class Database(AbstractDatabase):

    # -----------------------------
    # Pool / lifecycle
    # -----------------------------

    async def connect(self):
        self._pool = await aiomysql.create_pool(**self._db_config)

    async def reconnect(self):
        await self.close()
        await self.connect()

    async def close(self):
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()

    # -----------------------------
    # Deprecated compatibility APIs
    # -----------------------------

    async def get_connection(self):
        """
        DEPRECATED: Unsafe. Returns raw connection without lifecycle management.
        Prefer tx() or helper methods.
        """
        if not self._pool:
            await self.connect()

        try:
            async with self._pool.acquire() as conn:
                return conn
        except RuntimeError:
            await self.reconnect()
        except aiomysql.Error:
            await self.reconnect()
        except:
            return None

    @asynccontextmanager
    async def get_conn(self):
        """
        DEPRECATED: Prefer tx() or helper methods.
        """
        conn = await self._pool.acquire()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                yield conn, cursor
        finally:
            self._pool.release(conn)

    # -----------------------------
    # Transaction core (depth safe)
    # -----------------------------

    async def _tx_begin(self, conn):
        depth = getattr(conn, "_tx_depth", 0)
        if depth == 0:
            await conn.begin()
        conn._tx_depth = depth + 1

    async def _tx_commit(self, conn):
        depth = getattr(conn, "_tx_depth", 0)
        if depth <= 1:
            await conn.commit()
            conn._tx_depth = 0
        else:
            conn._tx_depth = depth - 1

    async def _tx_rollback(self, conn):
        await conn.rollback()
        conn._tx_depth = 0

    # -----------------------------
    # tx() context manager
    # -----------------------------

    @asynccontextmanager
    async def tx(self):
        conn = await self._pool.acquire()
        cur = await conn.cursor(aiomysql.DictCursor)

        try:
            await self._tx_begin(conn)
            yield cur
            await self._tx_commit(conn)

        except Exception:
            await self._tx_rollback(conn)
            raise

        finally:
            await cur.close()
            self._pool.release(conn)

    # -----------------------------
    # Read helpers (no tx needed)
    # -----------------------------

    async def get_rows(self, query, params=()):
        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, params)
                return await cursor.fetchall()

    async def get_row(self, query, params=()):
        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, params)
                return await cursor.fetchone()

    async def get_filter_row(
        self, table, columns=[], joins=[], conditions=[], orders=[]
    ):
        response = await self.get_filter_rows(
            table=table,
            columns=columns,
            joins=joins,
            orders=orders,
            conditions=conditions,
            offset=0,
            limit=1,
        )
        return list_get(response, 0, None)

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
        conditions_stmt, conditions_params = self.parse_conditions(conditions)

        query = f"""
            SELECT {",".join(columns) or "*"}
            FROM {self.sanitize_identifier(table)}
            {join_stmt}
            {conditions_stmt}
            LIMIT %s OFFSET %s
        """

        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(
                    query, join_params + conditions_params + [limit, offset]
                )
                return await cursor.fetchall()

    async def get_count(self, table, index=None, joins=[], conditions=[]):
        join_stmt, join_params = self.parse_joins(joins)
        conditions_stmt, conditions_params = self.parse_conditions(conditions)

        query = f"""
            SELECT COUNT({index or "*"}) AS total
            FROM {self.sanitize_identifier(table)}
            {join_stmt}
            {conditions_stmt}
        """

        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, join_params + conditions_params)
                row = await cursor.fetchone()
                return row.get("total", 0)

    # -----------------------------
    # Write helpers (TX SAFE + backward compatible)
    # -----------------------------

    async def execute(self, query, params=(), return_result=False):
        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                try:
                    await self._tx_begin(conn)

                    await cursor.execute(query, params)

                    if return_result:
                        result = await cursor.fetchall()
                    else:
                        result = Response(
                            success=True,
                            lastrowid=cursor.lastrowid or None,
                            data={},
                            error=None,
                        )

                    await self._tx_commit(conn)
                    return result

                except Exception as e:
                    await self._tx_rollback(conn)

                    if getattr(conn, "_tx_depth", 0) > 0:
                        raise

                    return Response(
                        success=False, lastrowid=None, data={}, error=str(e)
                    )

    async def execute_many(self, statements=[]):
        affected = 0

        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                try:
                    await self._tx_begin(conn)

                    for stmt in statements:
                        query, args = stmt if len(stmt) == 2 else (stmt[0], [])
                        await cursor.execute(query, args)
                        affected += cursor.rowcount

                    await self._tx_commit(conn)

                    return Response(
                        success=affected > 0,
                        lastrowid=None,
                        data={},
                        error=None,
                    )

                except Exception as e:
                    await self._tx_rollback(conn)

                    if getattr(conn, "_tx_depth", 0) > 0:
                        raise

                    return Response(
                        success=False, lastrowid=None, data={}, error=str(e)
                    )

    async def delete(self, table, conditions=[]):
        stmt, params = self.parse_conditions(conditions)

        query = f"DELETE FROM {self.sanitize_identifier(table)} {stmt}"

        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                try:
                    await self._tx_begin(conn)
                    await cursor.execute(query, params)
                    await self._tx_commit(conn)

                    return Response(
                        success=cursor.rowcount > 0,
                        lastrowid=None,
                        data={},
                        error=None,
                    )

                except Exception as e:
                    await self._tx_rollback(conn)

                    if getattr(conn, "_tx_depth", 0) > 0:
                        raise

                    return Response(
                        success=False, lastrowid=None, data={}, error=str(e)
                    )

    async def insert(self, table, column_values={}):
        columns = list(column_values.keys())
        values = list(column_values.values())

        sql = f"""
            INSERT INTO {self.sanitize_identifier(table)}
            ({",".join(columns)})
            VALUES ({",".join(["%s"] * len(values))})
        """

        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                try:
                    await self._tx_begin(conn)
                    await cursor.execute(sql, values)
                    await self._tx_commit(conn)

                    return Response(
                        success=True,
                        lastrowid=cursor.lastrowid or None,
                        data={},
                        error=None,
                    )

                except Exception as e:
                    await self._tx_rollback(conn)

                    if getattr(conn, "_tx_depth", 0) > 0:
                        raise

                    return Response(
                        success=False, lastrowid=None, data={}, error=str(e)
                    )

    async def update(
        self, table, identifiers=[], column_values={}, conditions=[]
    ):
        id_conditions = [
            Condition(column=col, value=column_values.get(col))
            for col in identifiers
        ]

        conditions = id_conditions + [ConditionSet(conditions=conditions)]
        stmt, params = self.parse_conditions(conditions)

        updates = {
            k: v for k, v in column_values.items() if k not in identifiers
        }
        set_clause = ", ".join(f"{k}=%s" for k in updates)

        query = f"""
            UPDATE {self.sanitize_identifier(table)}
            SET {set_clause}
            {stmt}
        """

        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                try:
                    await self._tx_begin(conn)
                    await cursor.execute(query, list(updates.values()) + params)
                    await self._tx_commit(conn)

                    return Response(
                        success=cursor.rowcount > 0,
                        lastrowid=None,
                        data={},
                        error=None,
                    )

                except Exception as e:
                    await self._tx_rollback(conn)

                    if getattr(conn, "_tx_depth", 0) > 0:
                        raise

                    return Response(
                        success=False, lastrowid=None, data={}, error=str(e)
                    )

    async def upsert(
        self, table, identifiers=[], column_values={}, has_auto_id=True
    ):
        columns = list(column_values.keys())
        values = list(column_values.values())

        update_cols = [c for c in columns if c not in identifiers]
        update_clause = ", ".join(f"{c}=VALUES({c})" for c in update_cols)

        sql = f"""
            INSERT INTO {self.sanitize_identifier(table)}
            ({",".join(columns)})
            VALUES ({",".join(["%s"] * len(values))})
            ON DUPLICATE KEY UPDATE {update_clause}
        """

        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                try:
                    await self._tx_begin(conn)
                    await cursor.execute(sql, values)
                    await self._tx_commit(conn)

                    UpsertResponse = namedtuple(
                        "UpsertResponse", identifiers + ["error"]
                    )

                    data = [column_values.get(i) for i in identifiers]

                    if has_auto_id and cursor.lastrowid:
                        data[0] = cursor.lastrowid

                    data.append(None)
                    return UpsertResponse(*data)

                except Exception as e:
                    await self._tx_rollback(conn)

                    if getattr(conn, "_tx_depth", 0) > 0:
                        raise

                    UpsertResponse = namedtuple(
                        "UpsertResponse", identifiers + ["error"]
                    )
                    data = [column_values.get(i) for i in identifiers]
                    data.append(str(e))
                    return UpsertResponse(*data)
