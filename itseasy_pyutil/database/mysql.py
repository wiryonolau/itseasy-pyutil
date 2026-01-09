import re
import sys
import traceback
from collections import namedtuple
from contextlib import asynccontextmanager
from typing import NamedTuple, Optional

import aiomysql
import pymysql

from itseasy_pyutil import get_logger, list_get
from itseasy_pyutil.database import (
    ORDER_PATTERN,
    SAFE_IDENTIFIER,
    SAFE_IDENTIFIER_WITH_STAR,
    AbstractDatabase,
    Condition,
    ConditionSet,
    Expression,
    Filter,
    Join,
    Response,
)


class Database(AbstractDatabase):
    async def connect(self):
        self._pool = await aiomysql.create_pool(
            **self._db_config,
        )

    async def get_connection(self):
        if not self._pool:
            await self.connect()

        try:
            async with self._pool.acquire() as conn:
                return conn
        except RuntimeError as e:
            await self.reconnect()
        except aiomysql.Error as e:
            await self.reconnect()
        except:
            return None

    async def reconnect(self):
        await self.close()
        await self.connect()

    async def close(self):
        self._pool.close()
        await self._pool.wait_closed()

    @asynccontextmanager
    async def get_conn(self):
        conn = await self._pool.acquire()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                yield conn, cursor
        finally:
            self._pool.release(conn)

    @asynccontextmanager
    async def tx(self):
        """Transaction context manager: begin, commit, rollback automatically."""
        conn = await self._pool.acquire()
        cur = await conn.cursor(aiomysql.DictCursor)
        try:
            await conn.begin()
            yield cur
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise
        finally:
            await cur.close()
            self._pool.release(conn)

    async def get_rows(self, query, params=()):
        if self._as_dev:
            self._logger.debug(self.get_sql_string(query, params))

        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, params)
                return await cursor.fetchall()

    async def get_row(self, query, params=()):
        if self._as_dev:
            self._logger.debug(self.get_sql_string(query, params))

        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, params)
                return await cursor.fetchone()

    async def get_filter_row(
        self,
        table,
        columns=[],
        joins=[],
        conditions=[],
        orders=[],
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

        columns = [
            self.sanitize_identifier(c, allow_star=True) for c in columns
        ]
        orders = [self.sanitize_order(o) for o in orders if o.strip()]

        query = f"""
            SELECT {",".join(columns) or "*"}
            FROM {self.sanitize_identifier(table)}
            {join_stmt}
            {conditions_stmt}
            {"ORDER BY" if len(orders) else ""}
            {",".join(orders)}
            LIMIT %s OFFSET %s
        """

        if self._as_dev:
            self._logger.debug(
                self.get_sql_string(
                    query, join_params + conditions_params + [limit, offset]
                )
            )

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

        if self._as_dev:
            self._logger.debug(
                self.get_sql_string(query, join_params + conditions_params)
            )

        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, join_params + conditions_params)
                result = await cursor.fetchone()
                return result.get("total", 0)

    async def execute(self, query, params=(), return_result: bool = False):
        if self._as_dev:
            self._logger.debug(self.get_sql_string(query, params))

        try:
            async with self._pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute(query, params)

                    if return_result:
                        return await cursor.fetchall()
                    return Response(
                        success=True,
                        lastrowid=cursor.lastrowid or None,
                        error=None,
                    )
        except Exception as e:
            return Response(success=False, lastrowid=None, error=str(e))

    async def execute_many(self, statements=[]):
        affected_rows = 0
        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await conn.begin()
                try:
                    for stmt in statements:
                        args = []
                        if len(stmt) == 1:
                            query = stmt[0]
                        if len(stmt) == 2:
                            query, args = stmt
                        else:
                            continue

                        if self._as_dev:
                            self._logger.debug(self.get_sql_string(query, args))

                        await cursor.execute(query, args)
                        affected_rows += cursor.rowcount
                    await conn.commit()
                    return Response(
                        success=affected_rows > 0,
                        lastrowid=cursor.lastrowid or None,
                        error=None,
                    )
                except Exception as e:
                    await conn.rollback()
                    self._logger.info(sys.exc_info())
                    return Response(success=False, lastrowid=None, error=str(e))

    async def delete(self, table, conditions=[]):
        conditions, params = self.parse_conditions(conditions)

        query = f"""
        DELETE FROM {self.sanitize_identifier(table)}
        {conditions}
        """

        if self._as_dev:
            self._logger.debug(self.get_sql_string(query, params))

        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await conn.begin()

                try:
                    await cursor.execute(query, params)
                    await conn.commit()
                    return Response(
                        success=cursor.rowcount > 0,
                        lastrowid=cursor.lastrowid or None,
                        error=None,
                    )
                except Exception as e:
                    await conn.rollback()
                    self._logger.info(sys.exc_info())
                    return Response(success=False, lastrowid=None, error=str(e))

    async def insert(self, table, column_values={}):
        """
        Insert without condition check
        """
        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await conn.begin()

                try:
                    # Build columns and values for INSERT
                    columns = [
                        self.sanitize_identifier(c, allow_star=True)
                        for c in column_values.keys()
                    ]
                    columns = ",".join(column_values.keys())
                    values = tuple(column_values.values())

                    # Insert query
                    insert_stmt = f"""
                    INSERT INTO {self.sanitize_identifier(table)} ({columns}) VALUES ({', '.join(['%s'] * len(values))})
                    """

                    if self._as_dev:
                        self._logger.debug(
                            self.get_sql_string(insert_stmt, values)
                        )

                    await cursor.execute(insert_stmt, values)
                    await conn.commit()

                    return Response(
                        success=cursor.rowcount > 0,
                        lastrowid=cursor.lastrowid or None,
                        error=None,
                    )
                except Exception as e:
                    self._logger.debug(f"Transaction failed: {sys.exc_info()}")
                    await conn.rollback()
                    return Response(
                        success=False, lastrowid=None, error=(str(e))
                    )

    async def update(
        self, table, identifiers=[], column_values={}, conditions=[]
    ):
        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await conn.begin()
                condition_by_indentifier = []
                for col in identifiers:
                    condition_by_indentifier.append(
                        Condition(column=col, value=column_values.get(col))
                    )

                conditions = condition_by_indentifier + [
                    ConditionSet(conditions=conditions)
                ]
                conditions, params = self.parse_conditions(conditions)

                update_columns = {
                    key: value
                    for key, value in column_values.items()
                    if key not in identifiers
                }

                set_clause = ", ".join(
                    [
                        f"{self.sanitize_identifier(col)}=%s"
                        for col in update_columns
                    ]
                )

                update_stmt = f"UPDATE {self.sanitize_identifier(table)} SET {set_clause} {conditions}"

                update_values = list(update_columns.values()) + params

                if self._as_dev:
                    self._logger.debug(
                        self.get_sql_string(update_stmt, update_values)
                    )

                try:
                    await cursor.execute(update_stmt, update_values)
                    await conn.commit()
                    return Response(
                        success=cursor.rowcount > 0,
                        lastrowid=cursor.lastrowid or None,
                        error=None,
                    )
                except Exception as e:
                    self._logger.debug(f"Transaction failed: {sys.exc_info()}")
                    await conn.rollback()
                    return Response(success=False, lastrowid=None, error=str(e))

    async def upsert(
        self, table, identifiers=[], column_values={}, has_auto_id=True
    ):
        UpsertResponse = namedtuple("UpsertResponse", identifiers + ["error"])
        response_data = [column_values.get(i) for i in identifiers]

        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await conn.begin()
                try:
                    # Build columns
                    columns = [
                        self.sanitize_identifier(c, allow_star=True)
                        for c in column_values.keys()
                    ]
                    values = list(column_values.values())

                    update_cols = [
                        c for c in column_values.keys() if c not in identifiers
                    ]

                    update_clause = ", ".join(
                        f"{self.sanitize_identifier(c)}=VALUES({self.sanitize_identifier(c)})"
                        for c in update_cols
                    )

                    sql = f"""
                        INSERT INTO {self.sanitize_identifier(table)}
                        ({",".join(columns)})
                        VALUES ({",".join(["%s"]*len(values))})
                        ON DUPLICATE KEY UPDATE {update_clause}
                    """

                    if self._as_dev:
                        self._logger.debug(self.get_sql_string(sql, values))

                    await cursor.execute(sql, values)
                    await conn.commit()

                    if has_auto_id and cursor.lastrowid:
                        response_data[0] = cursor.lastrowid

                    response_data.append(None)
                    return UpsertResponse(*response_data)

                except Exception as e:
                    await conn.rollback()
                    response_data.append(str(e))
                    return UpsertResponse(*response_data)
