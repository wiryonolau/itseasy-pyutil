import re
import sys
from collections import namedtuple
from contextlib import asynccontextmanager

import asyncpg

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
    Response,
)


class Database(AbstractDatabase):
    def _prepare(self, sql, params):
        if self._as_dev:
            self._logger.debug(self.get_sql_string(sql, params))

        has_percent = "%s" in sql
        has_dollar = bool(re.search(r"\$\d+", sql))
        has_named = bool(re.search(r":\w+", sql))

        # 🚫 mixed placeholder styles
        styles = sum([has_percent, has_dollar, has_named])
        if styles > 1:
            raise ValueError("Mixed SQL placeholder styles are not supported")

        # -------------------------------------------------
        # Case 1: asyncpg native ($1, $2...)
        # -------------------------------------------------
        if has_dollar:
            return sql, params

        # -------------------------------------------------
        # Case 2: MySQL-style %s → $1, $2...
        # -------------------------------------------------
        if has_percent:
            if not isinstance(params, (list, tuple)):
                raise TypeError("%s placeholders require positional params")

            parts = sql.split("%s")
            sql = (
                "".join(f"{part}${i+1}" for i, part in enumerate(parts[:-1]))
                + parts[-1]
            )

            return sql, params

        # -------------------------------------------------
        # Case 3: SQLAlchemy-style :param → $1, $2...
        # -------------------------------------------------
        if has_named:
            if not isinstance(params, dict):
                raise TypeError(":param placeholders require dict params")

            names = []

            def repl(match):
                name = match.group(1)
                if name not in params:
                    raise KeyError(f"Missing SQL param :{name}")
                names.append(name)
                return f"${len(names)}"

            sql = re.sub(r":(\w+)", repl, sql)
            values = [params[name] for name in names]

            return sql, values

        # -------------------------------------------------
        # No placeholders
        # -------------------------------------------------
        return sql, []

    async def connect(self):
        self._pool = await asyncpg.create_pool(
            min_size=1,
            max_size=5,
            **self._db_config,
        )

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
    async def get_conn(self):
        """
        Acquire a connection from the pool.
        Yields (conn,) because asyncpg doesn't use cursors like aiomysql.
        """
        async with self._pool.acquire() as conn:
            try:
                yield (conn,)
            finally:
                pass  # pool handles release automatically

    @asynccontextmanager
    async def tx(self):
        """
        Transaction context manager: begin, commit, rollback automatically.
        Yields the connection for executing queries.
        """
        async with self._pool.acquire() as conn:
            tr = conn.transaction()
            await tr.start()  # BEGIN
            try:
                yield conn
                await tr.commit()  # COMMIT
            except Exception:
                await tr.rollback()  # ROLLBACK
                raise

    async def get_rows(self, query, params=()):
        sql, params = self._prepare(query, params)
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
            return [dict(r) for r in rows]

    async def get_row(self, query, params=()):
        sql, params = self._prepare(query, params)
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, *params)
            return dict(row) if row else None

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
        orders = [self.sanitize_order(o) for o in orders]

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
        sql, params = self._prepare(query, all_params)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
            return [dict(r) for r in rows]

    async def get_count(self, table, index=None, joins=[], conditions=[]):
        join_stmt, join_params = self.parse_joins(joins)
        conditions_stmt, conditions_params = self.parse_conditions(conditions)

        query = f"""
            SELECT COUNT({index or "*"}) AS total
            FROM {self.sanitize_identifier(table)}
            {join_stmt}
            {conditions_stmt}
        """

        all_params = join_params + conditions_params
        sql, params = self._prepare(query, all_params)

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, *params)
            row = dict(row) if row else {}
            return row.get("total", 0)

    async def execute(self, query, params=(), return_result: bool = False):
        try:
            async with self._pool.acquire() as conn:
                if return_result:
                    rows = await conn.fetch(query, *params)
                    return rows
                else:
                    tr = conn.transaction()
                    await tr.start()
                    try:
                        query, params = self._prepare(query, params)
                        result = await conn.execute(query, *params)
                        await tr.commit()
                        return Response(
                            success=True, lastrowid=None, error=None
                        )
                    except Exception as e:
                        await tr.rollback()
                        return Response(
                            success=False, lastrowid=None, error=str(e)
                        )
        except Exception as e:
            return Response(success=False, lastrowid=None, error=str(e))

    async def execute_many(self, statements=[]):
        """
        statements: list of (query,) or (query, params)
        """
        affected_rows = 0
        async with self._pool.acquire() as conn:
            tr = conn.transaction()
            await tr.start()
            try:
                for stmt in statements:
                    args = []
                    if len(stmt) == 1:
                        query = stmt[0]
                    elif len(stmt) == 2:
                        query, args = stmt
                    else:
                        continue

                    query, args = self._prepare(query, args)

                    result = await conn.execute(query, *args)
                    # asyncpg returns "INSERT 0 1" or "UPDATE 3" etc.
                    affected_rows += int(result.split()[-1])

                await tr.commit()
                return Response(
                    success=affected_rows > 0, lastrowid=None, error=None
                )
            except Exception as e:
                await tr.rollback()
                self._logger.info(sys.exc_info())
                return Response(success=False, lastrowid=None, error=str(e))

    async def delete(self, table, conditions=[]):
        conditions_stmt, params = self.parse_conditions(conditions)

        query = f"""
            DELETE FROM {self.sanitize_identifier(table)}
            {conditions_stmt}
        """

        query, params = self._prepare(query, params)

        async with self._pool.acquire() as conn:
            tr = conn.transaction()
            await tr.start()
            try:
                result = await conn.execute(query, *params)
                await tr.commit()
                affected = int(result.split()[-1])
                return Response(
                    success=affected > 0, lastrowid=None, error=None
                )
            except Exception as e:
                await tr.rollback()
                self._logger.info(sys.exc_info())
                return Response(success=False, lastrowid=None, error=str(e))

    async def insert(self, table, column_values={}):
        """
        Insert without condition check (asyncpg version)
        """
        async with self._pool.acquire() as conn:
            tr = conn.transaction()
            await tr.start()  # BEGIN

            try:
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

                sql, values = self._prepare(insert_stmt, values)

                row = await conn.fetchrow(sql, *values)

                await tr.commit()  # COMMIT

                return Response(
                    success=True,
                    lastrowid=row["id"] if row else None,
                    error=None,
                )

            except Exception as e:
                self._logger.debug(f"Transaction failed: {sys.exc_info()}")
                await tr.rollback()  # ROLLBACK

                return Response(
                    success=False,
                    lastrowid=None,
                    error=str(e),
                )

    async def update(
        self, table, identifiers=[], column_values={}, conditions=[]
    ):
        async with self._pool.acquire() as conn:
            tr = conn.transaction()
            await tr.start()  # BEGIN

            try:
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

                sql, values = self._prepare(update_stmt, update_values)

                row = await conn.fetchrow(sql, *values)

                await tr.commit()  # COMMIT

                return Response(
                    success=bool(row),
                    lastrowid=None,
                    error=None,
                )

            except Exception as e:
                self._logger.debug(f"Transaction failed: {sys.exc_info()}")
                await tr.rollback()  # ROLLBACK

                return Response(
                    success=False,
                    lastrowid=None,
                    error=str(e),
                )

    async def upsert(
        self,
        table,
        identifiers,
        column_values,
        has_auto_id="id",
    ):
        """
        PostgreSQL upsert with correct identity handling.

        - Identity column MUST be omitted from INSERT when None
        - identifiers contains all unique columns (including identity)
        """

        identity_value = column_values.get(has_auto_id)

        # ------------------------------------------------------------
        # 1️⃣ Build INSERT columns
        # ------------------------------------------------------------
        insert_cols = []
        insert_vals = []

        for col, val in column_values.items():
            # 🚫 omit identity column when None
            if col == has_auto_id and val is None:
                continue
            insert_cols.append(col)
            insert_vals.append(val)

        # ------------------------------------------------------------
        # 2️⃣ Conflict target (only identifiers with actual values)
        # ------------------------------------------------------------
        conflict_target = [
            c
            for c in identifiers
            if c != has_auto_id or identity_value is not None
        ]

        # ------------------------------------------------------------
        # 3️⃣ Columns to update (never identifiers)
        # ------------------------------------------------------------
        update_cols = [c for c in column_values.keys() if c not in identifiers]

        # ------------------------------------------------------------
        # 4️⃣ Build SQL
        # ------------------------------------------------------------
        cols_sql = ",".join(self.sanitize_identifier(c) for c in insert_cols)
        placeholders = ",".join(["%s"] * len(insert_vals))

        sql = f"""
            INSERT INTO {self.sanitize_identifier(table)}
            ({cols_sql})
            VALUES ({placeholders})
        """

        if conflict_target:
            if update_cols:
                set_clause = ", ".join(
                    f"{self.sanitize_identifier(c)} = EXCLUDED.{self.sanitize_identifier(c)}"
                    for c in update_cols
                )
                sql += f"""
                    ON CONFLICT ({','.join(self.sanitize_identifier(c) for c in conflict_target)})
                    DO UPDATE SET {set_clause}
                """
            else:
                sql += f"""
                    ON CONFLICT ({','.join(self.sanitize_identifier(c) for c in conflict_target)})
                    DO NOTHING
                """

        sql += " RETURNING *"

        sql, vals = self._prepare(sql, insert_vals)

        # ------------------------------------------------------------
        # 5️⃣ Execute
        # ------------------------------------------------------------
        async with self._pool.acquire() as conn:
            tr = conn.transaction()
            await tr.start()
            try:
                row = await conn.fetchrow(sql, *vals)
                await tr.commit()

                fields = list(column_values.keys()) + ["error"]
                UpsertResponse = namedtuple("UpsertResponse", fields)

                data = []
                for c in column_values.keys():
                    data.append(row[c] if row else column_values.get(c))
                data.append(None)

                return UpsertResponse(*data)

            except Exception as e:
                await tr.rollback()

                fields = list(column_values.keys()) + ["error"]
                UpsertResponse = namedtuple("UpsertResponse", fields)
                return UpsertResponse(
                    *[column_values.get(c) for c in column_values.keys()],
                    str(e),
                )
