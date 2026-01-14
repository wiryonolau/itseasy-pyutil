import asyncio
import re
import sys
from collections import namedtuple
from contextlib import asynccontextmanager

import asyncpg

from itseasy_pyutil import get_logger, list_get
from itseasy_pyutil.database import (
    AbstractDatabase,
    Condition,
    ConditionSet,
    Response,
)


class Database(AbstractDatabase):
    def prepare(self, sql, params):
        has_percent = "%s" in sql
        has_dollar = bool(re.search(r"\$\d+", sql))
        has_named = bool(re.search(r"(?<!:):\w+", sql))  # 👈 FIXED

        # 🚫 mixed placeholder styles
        styles = sum([has_percent, has_dollar, has_named])
        if styles > 1:
            raise ValueError("Mixed SQL placeholder styles are not supported")

        # -------------------------------------------------
        # Case 1: asyncpg native ($1, $2...)
        # -------------------------------------------------
        if has_dollar:
            pass

        # -------------------------------------------------
        # Case 2: MySQL-style %s → $1, $2...
        # -------------------------------------------------
        elif has_percent:
            if not isinstance(params, (list, tuple)):
                raise TypeError("%s placeholders require positional params")

            parts = sql.split("%s")
            sql = (
                "".join(f"{part}${i+1}" for i, part in enumerate(parts[:-1]))
                + parts[-1]
            )

        # -------------------------------------------------
        # Case 3: SQLAlchemy-style :param → $1, $2...
        # -------------------------------------------------
        elif has_named:
            if not isinstance(params, dict):
                raise TypeError(":param placeholders require dict params")

            names = []

            def repl(match):
                name = match.group(1)
                if name not in params:
                    raise KeyError(f"Missing SQL param :{name}")
                names.append(name)
                return f"${len(names)}"

            sql = re.sub(r"(?<!:):(\w+)", repl, sql)  # 👈 FIXED
            values = [params[name] for name in names]

            params = values
        else:
            params = []

        if self._as_dev:
            self._logger.debug(self.get_sql_string(sql, params))
        # -------------------------------------------------
        # No placeholders
        # -------------------------------------------------
        return sql, params

    async def connect(self):
        self._pool = await asyncpg.create_pool(
            **self._db_config,
        )

    # -----------------------------
    # Deprecated compatibility APIs
    # -----------------------------
    async def get_connection(self):
        """
        Acquire a connection from the pool.
        Returns a single connection or None if failed.
        """
        if not self._pool:
            await self.connect()

        try:
            # Note: use `async with` only if you want automatic release;
            # here we return the connection for caller to use
            conn = await self._pool.acquire()
            return conn
        except (asyncpg.PostgresError, RuntimeError) as e:
            await self.reconnect()
            return None
        except Exception:
            return None

    async def reconnect(self):
        """
        Close and recreate the pool
        """
        await self.close()
        await self.connect()

    async def close(self):
        """
        Close the asyncpg pool
        """
        if self._pool:
            await self._pool.close()
            self._pool = None

    @asynccontextmanager
    async def _acquire_pool(self):
        conn = await self._pool.acquire()
        released = False
        try:
            yield conn

        except asyncio.CancelledError:
            try:
                await conn.close()
            finally:
                released = True
            self._logger.error("CancelledError occurred", exc_info=True)
            raise

        except Exception:
            try:
                await conn.close()
            finally:
                released = True
            self._logger.error("Database exception occurred", exc_info=True)
            raise

        finally:
            if not released:
                try:
                    await self._pool.release(conn)
                except Exception:
                    self._logger.error(
                        "Failed to release connection", exc_info=True
                    )

    @asynccontextmanager
    async def tx(self):
        """
        Transaction context manager: begin, commit, rollback automatically.
        Yields the connection for executing queries.
        """
        async with self._acquire_pool() as conn:
            try:
                async with conn.transaction():
                    yield conn
            except Exception:
                raise

    @asynccontextmanager
    async def _get_connection(self, conn=None):
        if conn is not None:
            # We do NOT own this connection
            yield conn
        else:
            # We DO own this connection
            async with self._get_connection(conn=conn) as c:
                yield c

    async def get_rows(self, query, params=(), conn=None):
        sql, params = self.prepare(query, params)
        async with self._get_connection(conn=conn) as conn:
            rows = await conn.fetch(sql, *params)
            return [dict(r) for r in rows]

    async def get_row(self, query, params=(), conn=None):
        sql, params = self.prepare(query, params)
        async with self._get_connection(conn=conn) as conn:
            row = await conn.fetchrow(sql, *params)
            return dict(row) if row else None

    async def get_filter_row(
        self, table, columns=[], joins=[], conditions=[], orders=[], conn=None
    ):
        response = await self.get_filter_rows(
            table=table,
            columns=columns,
            joins=joins,
            orders=orders,
            conditions=conditions,
            offset=0,
            limit=1,
            conn=conn,
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
        conn=None,
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

        all_params = join_params + conditions_params + [limit, offset]
        sql, params = self.prepare(query, all_params)

        async with self._get_connection(conn=conn) as conn:
            rows = await conn.fetch(sql, *params)
            return [dict(r) for r in rows]

    async def get_count(
        self, table, index=None, joins=[], conditions=[], conn=None
    ):
        join_stmt, join_params = self.parse_joins(joins)
        conditions_stmt, conditions_params = self.parse_conditions(conditions)

        query = f"""
            SELECT COUNT({index or "*"}) AS total
            FROM {self.sanitize_identifier(table)}
            {join_stmt}
            {conditions_stmt}
        """

        all_params = join_params + conditions_params
        sql, params = self.prepare(query, all_params)

        async with self._get_connection(conn=conn) as conn:
            row = await conn.fetchrow(sql, *params)
            row = dict(row) if row else {}
            return row.get("total", 0)

    async def execute(
        self, query, params=(), return_result: bool = False, conn=None
    ):
        try:
            async with self._get_connection(conn) as conn:
                query, params = self.prepare(query, params)

                if return_result:
                    return await conn.fetch(query, *params)

                async with conn.transaction():
                    await conn.execute(query, *params)

                return Response(success=True, lastrowid=None, error=None)

        except Exception as e:
            if conn is not None:
                raise

            return Response(success=False, lastrowid=None, error=str(e))

    async def execute_many(self, statements=[], conn=None):
        """
        statements: list of (query,) or (query, params)
        """
        affected_rows = 0
        try:
            async with self._get_connection(conn) as conn:
                async with conn.transaction():

                    for stmt in statements:
                        args = []
                        if len(stmt) == 1:
                            query = stmt[0]
                        elif len(stmt) == 2:
                            query, args = stmt
                        else:
                            continue

                        query, args = self.prepare(query, args)
                        result = await conn.execute(query, *args)

                        affected_rows += int(result.split()[-1])

                return Response(
                    success=affected_rows > 0,
                    lastrowid=None,
                    error=None,
                )
        except Exception as e:
            self._logger.info(sys.exc_info())

            if conn is not None:
                raise

            return Response(success=False, lastrowid=None, error=str(e))

    async def delete(self, table, conditions=[], conn=None):
        conditions_stmt, params = self.parse_conditions(conditions)

        query = f"""
            DELETE FROM {self.sanitize_identifier(table)}
            {conditions_stmt}
        """

        query, params = self.prepare(query, params)

        try:
            async with self._get_connection(conn) as conn:
                async with conn.transaction():
                    result = await conn.execute(query, *params)

                affected = int(result.split()[-1])
                return Response(
                    success=affected > 0, lastrowid=None, error=None
                )

        except Exception as e:
            self._logger.info(sys.exc_info())

            if conn is not None:
                raise

            return Response(success=False, lastrowid=None, error=str(e))

    async def insert(self, table, column_values={}, conn=None):
        async with self._get_connection(conn) as conn:
            try:
                async with conn.transaction():

                    # Build columns and values for INSERT
                    columns = [
                        self.sanitize_identifier(c, allow_star=True)
                        for c in column_values.keys()
                    ]
                    columns = ",".join(columns)
                    values = list(column_values.values())

                    insert_stmt = f"""
                        INSERT INTO {self.sanitize_identifier(table)}
                        ({columns})
                        VALUES ({", ".join(["%s"] * len(values))})
                        RETURNING id
                    """

                    sql, values = self.prepare(insert_stmt, values)

                    row = await conn.fetchrow(sql, *values)

                return Response(
                    success=True,
                    lastrowid=row["id"] if row else None,
                    error=None,
                )

            except Exception as e:
                self._logger.debug(f"Transaction failed: {sys.exc_info()}")

                if conn is not None:
                    raise

                return Response(
                    success=False,
                    lastrowid=None,
                    error=str(e),
                )

    async def update(
        self, table, identifiers=[], column_values={}, conditions=[], conn=None
    ):
        try:
            async with self._get_connection(conn) as conn:
                async with conn.transaction():

                    # Build identifier-based conditions
                    condition_by_identifier = []
                    for col in identifiers:
                        condition_by_identifier.append(
                            Condition(column=col, value=column_values.get(col))
                        )

                    conditions = condition_by_identifier + [
                        ConditionSet(conditions=conditions)
                    ]

                    where_clause, params = self.parse_conditions(conditions)

                    # Columns to update (exclude identifiers)
                    update_columns = {
                        key: value
                        for key, value in column_values.items()
                        if key not in identifiers
                    }

                    set_clause = ", ".join(
                        f"{self.sanitize_identifier(col)}=%s"
                        for col in update_columns
                    )

                    update_stmt = f"""
                        UPDATE {self.sanitize_identifier(table)}
                        SET {set_clause}
                        {where_clause}
                        RETURNING 1
                    """

                    update_values = list(update_columns.values()) + params
                    sql, values = self.prepare(update_stmt, update_values)

                    row = await conn.fetchrow(sql, *values)

                return Response(
                    success=bool(row),
                    lastrowid=None,
                    error=None,
                )

        except Exception as e:
            self._logger.debug(f"Transaction failed: {sys.exc_info()}")

            # CRITICAL PART: do not swallow inside tx()
            if conn is not None:
                raise

            return Response(
                success=False,
                lastrowid=None,
                error=str(e),
            )

    async def upsert(
        self, table, identifiers, column_values, has_auto_id="id", conn=None
    ):
        try:
            # ------------------------------------------------------------
            # 1️⃣ Build INSERT data (omit auto id when None)
            # ------------------------------------------------------------
            insert_data = {
                k: v
                for k, v in column_values.items()
                if not (k == has_auto_id and v is None)
            }

            if not insert_data:
                raise ValueError("Nothing to insert")

            # ------------------------------------------------------------
            # 2️⃣ Sanitize identifiers
            # ------------------------------------------------------------
            identifiers = [
                c
                for c in identifiers
                if c in insert_data and insert_data[c] is not None
            ]

            # ------------------------------------------------------------
            # 3️⃣ SQL parts
            # ------------------------------------------------------------
            insert_cols = [self.sanitize_identifier(c) for c in insert_data]
            placeholders = ["%s"] * len(insert_cols)
            conflict_cols = [self.sanitize_identifier(c) for c in identifiers]

            update_cols = [
                c
                for c in insert_data
                if c not in identifiers and c != has_auto_id
            ]

            # ------------------------------------------------------------
            # 4️⃣ Conflict clause
            # ------------------------------------------------------------
            if conflict_cols:
                if update_cols:
                    update_clause = ", ".join(
                        f"{self.sanitize_identifier(c)} = EXCLUDED.{self.sanitize_identifier(c)}"
                        for c in update_cols
                    )
                    conflict_action = f"""
                        ON CONFLICT ({", ".join(conflict_cols)})
                        DO UPDATE SET {update_clause}
                    """
                else:
                    conflict_action = f"""
                        ON CONFLICT ({", ".join(conflict_cols)})
                        DO NOTHING
                    """
            else:
                conflict_action = ""

            # ------------------------------------------------------------
            # 5️⃣ Final SQL
            # ------------------------------------------------------------
            sql = f"""
                INSERT INTO {self.sanitize_identifier(table)}
                ({", ".join(insert_cols)})
                VALUES ({", ".join(placeholders)})
                {conflict_action}
                RETURNING *
            """

            sql, values = self.prepare(sql, list(insert_data.values()))

            # ------------------------------------------------------------
            # 6️⃣ Execute safely
            # ------------------------------------------------------------
            async with self._get_connection(conn) as conn:
                async with conn.transaction():
                    row = await conn.fetchrow(sql, *values)

            # ------------------------------------------------------------
            # 7️⃣ Success response
            # ------------------------------------------------------------
            fields = list(row.keys()) + ["error"]
            UpsertResponse = namedtuple("UpsertResponse", fields)

            return UpsertResponse(**row, error=None)

        except Exception as exc:
            # IMPORTANT: if inside tx, re-raise
            if conn is not None:
                raise

            UpsertResponse = namedtuple("UpsertResponse", ["error"])
            return UpsertResponse(error=str(exc))
