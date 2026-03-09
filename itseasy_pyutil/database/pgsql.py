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
    Expression,
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

    def build_filter_query(
        self,
        table,
        columns=None,
        joins=None,
        conditions=None,
        orders=None,
        offset=0,
        limit=1000,
    ):
        columns = columns or []
        joins = joins or []
        conditions = conditions or []
        orders = orders or []

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
            {"ORDER BY " + ",".join(orders) if orders else ""}
            LIMIT %s OFFSET %s
        """

        params = join_params + conditions_params + [limit, offset]

        return query, params

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
            yield conn
            return

        if not self._pool:
            await self.connect()

        async with self._acquire_pool() as c:
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
        query, params = self.build_filter_query(
            table=table,
            columns=columns,
            joins=joins,
            conditions=conditions,
            orders=orders,
            offset=offset,
            limit=limit,
        )

        sql, prepared_params = self.prepare(query, params)

        async with self._get_connection(conn=conn) as conn:
            rows = await conn.fetch(sql, *prepared_params)
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

                return Response(
                    success=True, lastrowid=None, data={}, error=None
                )

        except Exception as e:
            if conn is not None:
                raise

            return Response(
                success=False, lastrowid=None, data={}, error=str(e)
            )

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
                    data={},
                    error=None,
                )
        except Exception as e:
            self._logger.info(sys.exc_info())

            if conn is not None:
                raise

            return Response(
                success=False, lastrowid=None, data={}, error=str(e)
            )

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
                    success=affected > 0, lastrowid=None, data={}, error=None
                )

        except Exception as e:
            self._logger.info(sys.exc_info())

            if conn is not None:
                raise

            return Response(
                success=False, lastrowid=None, data={}, error=str(e)
            )

    async def insert(
        self, table, column_values=None, identifier="id", conn=None
    ):
        column_values = column_values or {}

        async with self._get_connection(conn) as conn:
            try:
                async with conn.transaction():

                    columns = []
                    placeholders = []
                    values = []

                    for col, val in column_values.items():
                        columns.append(
                            self.sanitize_identifier(col, allow_star=True)
                        )

                        if isinstance(val, Expression):
                            placeholders.append(str(val))
                        else:
                            placeholders.append("%s")
                            values.append(val)

                    columns_sql = ",".join(columns)
                    placeholders_sql = ",".join(placeholders)

                    insert_stmt = f"""
                        INSERT INTO {self.sanitize_identifier(table)}
                        ({columns_sql})
                        VALUES ({placeholders_sql})
                        RETURNING *
                    """

                    sql, values = self.prepare(insert_stmt, values)

                    row = await conn.fetchrow(sql, *values)

                    return Response(
                        success=True,
                        lastrowid=row[identifier] if row else None,
                        data=dict(row) if row else {},
                        error=None,
                    )

            except Exception as e:
                self._logger.debug(f"Transaction failed: {sys.exc_info()}")

                if conn is not None:
                    raise

                return Response(
                    success=False,
                    lastrowid=None,
                    data={},
                    error=str(e),
                )

    async def update(
        self,
        table,
        identifiers=None,
        column_values=None,
        conditions=None,
        identity_column="id",
        conn=None,
    ):
        identifiers = identifiers or []
        column_values = column_values or {}
        conditions = conditions or []

        try:
            async with self._get_connection(conn) as conn:
                async with conn.transaction():

                    # Build identifier-based conditions
                    condition_by_identifier = [
                        Condition(column=col, value=column_values.get(col))
                        for col in identifiers
                    ]

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

                    if not update_columns:
                        raise ValueError("No columns to update")

                    set_clause = ", ".join(
                        f"{self.sanitize_identifier(col)} = %s"
                        for col in update_columns
                    )

                    update_stmt = f"""
                        UPDATE {self.sanitize_identifier(table)}
                        SET {set_clause}
                        {where_clause}
                        RETURNING *
                    """

                    update_values = list(update_columns.values()) + params
                    sql, values = self.prepare(update_stmt, update_values)

                    row = await conn.fetchrow(sql, *values)

                    return Response(
                        success=row is not None,
                        lastrowid=(
                            row.get(identity_column)
                            if row and identity_column
                            else None
                        ),
                        data=dict(row) if row else None,
                        error=None,
                    )

        except Exception as e:
            self._logger.debug("Transaction failed", exc_info=True)

            # Do not swallow exceptions inside an outer transaction
            if conn is not None:
                raise

            return Response(
                success=False,
                lastrowid=None,
                data=None,
                error=str(e),
            )

    async def upsert(
        self,
        table: str,
        identifiers: list[str],
        column_values: dict,
        identifier_mode: str = "any",
        has_auto_id: str = "id",
        conn=None,
    ):
        """
        Generic concurrency-safe upsert.

        Behavior
        --------
        1. Filter identifiers whose values are not None.
        2. SELECT ... FOR UPDATE to detect existing row.
        3. If row exists:
                UPDATE if there are non-identifier columns.
                otherwise return existing row.
        4. If row does not exist:
                INSERT
                If insert races (UniqueViolation):
                        UPDATE if update columns exist
                        otherwise SELECT existing row.

        Guarantees
        ----------
        - Identifiers are never skipped silently.
        - SELECT and UPDATE use identical identifier conditions.
        - No invalid UPDATE statements.
        - Safe under concurrent inserts.
        """

        try:

            # ------------------------------------------------------------
            # 1️⃣ Prepare INSERT data
            # ------------------------------------------------------------
            # Remove auto identity column if value is None so the DB
            # can generate it automatically.
            insert_data = {
                k: v
                for k, v in column_values.items()
                if not (k == has_auto_id and v is None)
            }

            if not insert_data:
                raise ValueError("No data to upsert")

            # ------------------------------------------------------------
            # 2️⃣ Build filtered identifiers
            # ------------------------------------------------------------
            # Only identifiers with non-None values can be used to locate
            # existing rows. This prevents skipped identifiers.
            filtered_identifiers = [
                col for col in identifiers if column_values.get(col) is not None
            ]

            if not filtered_identifiers:
                raise ValueError("No usable identifiers provided")

            # ------------------------------------------------------------
            # 3️⃣ Determine columns that can be updated
            # ------------------------------------------------------------
            # Identifier columns must never be updated.
            update_columns = {
                k: v
                for k, v in column_values.items()
                if k not in filtered_identifiers
            }

            # ------------------------------------------------------------
            # 4️⃣ Build identifier conditions
            # ------------------------------------------------------------
            # SELECT and UPDATE must use identical conditions.
            glue = "OR" if identifier_mode == "any" else "AND"

            identifier_conditions = [
                Condition(column=col, value=column_values[col], glue=glue)
                for col in filtered_identifiers
            ]

            # ------------------------------------------------------------
            # 5️⃣ Build SELECT statement (row lock)
            # ------------------------------------------------------------
            select_stmt, select_params = self.select_stmt(
                table=table,
                conditions=identifier_conditions,
                limit=1,
                offset=0,
                for_update=True,
            )

            select_stmt, select_params = self.prepare(
                select_stmt, select_params
            )

            # ------------------------------------------------------------
            # 6️⃣ Build UPDATE statement (only if needed)
            # ------------------------------------------------------------
            update_stmt = None
            update_params = []

            if update_columns:
                update_stmt, update_params = self.update_stmt(
                    table=table,
                    identifiers=filtered_identifiers,
                    column_values=column_values,
                    returning=["*"],
                )

                update_stmt, update_params = self.prepare(
                    update_stmt, update_params
                )

            # ------------------------------------------------------------
            # 7️⃣ Build INSERT statement
            # ------------------------------------------------------------
            insert_stmt, insert_params = self.insert_stmt(
                table=table,
                column_values=insert_data,
                returning=["*"],
            )

            insert_stmt, insert_params = self.prepare(
                insert_stmt, insert_params
            )

            # ------------------------------------------------------------
            # 8️⃣ Execute transaction
            # ------------------------------------------------------------
            async with self._get_connection(conn) as conn:

                async with conn.transaction():

                    result = None
                    inserted = False

                    # ----------------------------------------
                    # Try locating existing row
                    # ----------------------------------------
                    row = await conn.fetchrow(select_stmt, *select_params)

                    # ----------------------------------------
                    # Row exists
                    # ----------------------------------------
                    if row:

                        # Update only if there are non-identifier columns
                        if update_stmt:
                            result = await conn.fetchrow(
                                update_stmt, *update_params
                            )
                        else:
                            # Identifier-only upsert → return existing row
                            result = row

                    # ----------------------------------------
                    # Row does not exist → attempt insert
                    # ----------------------------------------
                    else:

                        try:
                            result = await conn.fetchrow(
                                insert_stmt, *insert_params
                            )
                            inserted = True

                        except asyncpg.exceptions.UniqueViolationError:

                            # Another transaction inserted the row first

                            if update_stmt:
                                result = await conn.fetchrow(
                                    update_stmt, *update_params
                                )
                            else:
                                # Identifier-only case → fetch row
                                result = await conn.fetchrow(
                                    select_stmt.replace("FOR UPDATE", ""),
                                    *select_params,
                                )

                            inserted = False

                if result is None:
                    raise RuntimeError("Insert/Update failed")

                # ------------------------------------------------------------
                # 9️⃣ Build response
                # ------------------------------------------------------------
                data = dict(result)
                data["inserted"] = inserted
                data["error"] = None

                UpsertResponse = namedtuple("UpsertResponse", data.keys())
                return UpsertResponse(**data)

        except Exception as exc:

            # If using external connection, propagate error
            if conn is not None:
                raise

            UpsertResponse = namedtuple("UpsertResponse", ["error"])
            return UpsertResponse(error=str(exc))
