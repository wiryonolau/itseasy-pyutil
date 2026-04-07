import asyncio
import re
import sys
from collections import namedtuple
from contextlib import asynccontextmanager

import asyncpg

from itseasy_pyutil import list_get
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

    async def get_rows(self, query, params=None, conn=None):
        params = params or []

        sql, params = self.prepare(query, params)
        async with self._get_connection(conn=conn) as conn:
            rows = await conn.fetch(sql, *params)
            return [dict(r) for r in rows]

    async def get_row(self, query, params=None, conn=None):
        params = params or []

        sql, params = self.prepare(query, params)
        async with self._get_connection(conn=conn) as conn:
            row = await conn.fetchrow(sql, *params)
            return dict(row) if row else None

    async def get_filter_row(
        self,
        table,
        columns=None,
        joins=None,
        conditions=None,
        orders=None,
        conn=None,
    ):
        columns = columns or []
        joins = joins or []
        conditions = conditions or []
        orders = orders or []

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
        columns=None,
        joins=None,
        conditions=None,
        orders=None,
        offset=0,
        limit=1000,
        conn=None,
    ):
        columns = columns or []
        joins = joins or []
        conditions = conditions or []
        orders = orders or []

        offset = 0 if offset is None else max(0, int(offset))
        limit = 1000 if limit is None else max(1, int(limit))

        if self._max_limit is not None:
            limit = min(limit, self._max_limit)

        # Limit pagination window to reduce deep OFFSET query cost
        if self._max_offset is not None:
            if offset > self._max_offset:
                return []
            remaining = (self._max_offset + limit) - offset
            limit = min(limit, remaining)

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
        self, table, index=None, joins=None, conditions=None, conn=None
    ):
        index = index or "*"
        joins = joins or []
        conditions = conditions or []

        join_stmt, join_params = self.parse_joins(joins)
        conditions_stmt, conditions_params = self.parse_conditions(conditions)

        query = f"""
            SELECT COUNT({index}) AS total
            FROM {self.sanitize_identifier(table)}
            {join_stmt}
            {conditions_stmt}
        """

        all_params = join_params + conditions_params
        sql, params = self.prepare(query, all_params)

        async with self._get_connection(conn=conn) as conn:
            row = await conn.fetchrow(sql, *params)
            row = dict(row) if row else {}
            total = row.get("total", 0)

        # Limit pagination window to reduce deep OFFSET query cost
        if self._max_offset is not None and self._max_limit is not None:
            total = min(total, self._max_offset + self._max_limit)

        return total

    async def execute(
        self, query, params=None, return_result: bool = False, conn=None
    ):
        params = params or []

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

    async def execute_many(self, statements=None, conn=None):
        """
        statements: list of (query,) or (query, params)
        """
        statements = statements or []

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

    async def delete(self, table, conditions=None, conn=None):
        conditions = conditions or []

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
        has_auto_id: str = "id",
        conn=None,
    ):
        try:
            # ------------------------------------------------------------
            # 1️⃣ Remove auto id if None (allow DB to generate)
            # ------------------------------------------------------------
            insert_data = {
                k: v
                for k, v in column_values.items()
                if not (k == has_auto_id and v is None)
            }

            if not insert_data:
                raise ValueError("No data to upsert")

            # ------------------------------------------------------------
            # 2️⃣ Conflict keys come from ORIGINAL payload, not insert_data
            # ------------------------------------------------------------
            conflict_keys = [
                c
                for c in identifiers
                if c in column_values and column_values[c] is not None
            ]

            use_upsert = bool(conflict_keys)

            # ------------------------------------------------------------
            # 3️⃣ Build SQL components
            # ------------------------------------------------------------
            cols = list(insert_data.keys())
            values = list(insert_data.values())

            safe_table = self.sanitize_identifier(table)
            safe_cols = [self.sanitize_identifier(c) for c in cols]
            placeholders = [f"${i}" for i in range(1, len(cols) + 1)]

            # ------------------------------------------------------------
            # 4️⃣ Build SQL
            # ------------------------------------------------------------
            if use_upsert:
                safe_conflicts = [
                    self.sanitize_identifier(c) for c in conflict_keys
                ]

                update_cols = [c for c in cols if c not in conflict_keys]

                if update_cols:
                    update_clause = ", ".join(
                        f"{self.sanitize_identifier(c)} = EXCLUDED.{self.sanitize_identifier(c)}"
                        for c in update_cols
                    )
                else:
                    # Composite-safe no-op update
                    noop = self.sanitize_identifier(conflict_keys[0])
                    update_clause = f"{noop} = {safe_table}.{noop}"

                sql = f"""
                    INSERT INTO {safe_table}
                        ({", ".join(safe_cols)})
                    VALUES
                        ({", ".join(placeholders)})
                    ON CONFLICT ({", ".join(safe_conflicts)})
                    DO UPDATE SET
                        {update_clause}
                    RETURNING *,
                            (xmax = 0) AS inserted
                """
            else:
                # Fallback: pure insert
                sql = f"""
                    INSERT INTO {safe_table}
                        ({", ".join(safe_cols)})
                    VALUES
                        ({", ".join(placeholders)})
                    RETURNING *, true AS inserted
                """

            # ------------------------------------------------------------
            # 5️⃣ Execute
            # ------------------------------------------------------------
            sql, values = self.prepare(sql, values)

            async with self._get_connection(conn) as conn:
                async with conn.transaction():
                    row = await conn.fetchrow(sql, *values)

            if row is None:
                raise RuntimeError("Insert/Upsert failed: no row returned")

            # ------------------------------------------------------------
            # 6️⃣ Return response
            # ------------------------------------------------------------
            data = dict(row)
            data["error"] = None

            UpsertResponse = namedtuple("UpsertResponse", data.keys())
            return UpsertResponse(**data)

        except Exception as exc:
            if conn is not None:
                raise

            UpsertResponse = namedtuple("UpsertResponse", ["error"])
            return UpsertResponse(error=str(exc))

    async def upsert2(
        self,
        table: str,
        identifier_keys: list[str],
        update_keys: list[str],
        values: dict,
        optional_keys: list[str] | None = None,
        conn=None,
    ):
        """
        # upsert2:
        # - Try UPDATE first using `update_keys` columns that have non-None values.
        # - If no row is updated, try INSERT with all `values`.
        # - If INSERT hits a unique conflict, retry UPDATE once.
        #
        # Why this design:
        # - keeps API simple for caller: pass one `values` dict
        # - caller can choose different columns for UPDATE matching (`update_keys`)
        # - `identifier_keys` are treated as identity columns and are never included in UPDATE SET
        # - safer under concurrency than plain select-then-insert
        # - allows database-generated values by skipping `optional_keys`
        #   when their input value is None
        #
        # Arguments:
        # - table: target table name
        # - identifier_keys: identity/unique columns used for response shape on error;
        #   these columns are also excluded from UPDATE SET
        # - update_keys: columns used to match existing row for UPDATE (WHERE)
        # - values: all column values for insert/update decision
        # - optional_keys: columns that can be skipped on INSERT when value is None,
        #   allowing DB defaults to apply
        #
        # Notes:
        # - only `update_keys` columns with non-None values are used in UPDATE WHERE
        # - UPDATE SET uses columns from `values` except active `update_keys` and `identifier_keys`
        # - INSERT uses all `values` except `optional_keys` with None value
        # - returns full row on success, plus error=None
        # - returns `{key: None, ..., error: "..."}`
        #   on standalone failure
        """
        optional_keys = optional_keys or []
        safe_table = self.sanitize_identifier(table)

        active_update_keys = [
            k for k in update_keys if k in values and values[k] is not None
        ]

        set_columns = [
            k
            for k in values.keys()
            if k not in active_update_keys and k not in identifier_keys
        ]

        try:
            async with self._get_connection(conn) as conn:
                async with conn.transaction():
                    row = None

                    # ------------------------------------------------
                    # 1) UPDATE
                    # ------------------------------------------------
                    if active_update_keys and set_columns:
                        set_parts, where_parts, args = [], [], []
                        idx = 1

                        for col in set_columns:
                            set_parts.append(
                                f"{self.sanitize_identifier(col)} = ${idx}"
                            )
                            args.append(values[col])
                            idx += 1

                        for col in active_update_keys:
                            where_parts.append(
                                f"{self.sanitize_identifier(col)} = ${idx}"
                            )
                            args.append(values[col])
                            idx += 1

                        sql = f"""
                            UPDATE {safe_table}
                            SET {", ".join(set_parts)}
                            WHERE {" AND ".join(where_parts)}
                            RETURNING *
                        """
                        sql = " ".join(sql.split())
                        args = list(args)

                        try:
                            sql, args = self.prepare(sql, args)
                        except Exception as e:
                            if "bad escape" not in str(e):
                                raise

                        row = await conn.fetchrow(sql, *args)

                    # ------------------------------------------------
                    # 2) INSERT
                    # ------------------------------------------------
                    if row is None:
                        insert_data = {
                            k: v
                            for k, v in values.items()
                            if not (k in optional_keys and v is None)
                        }

                        if not insert_data:
                            raise ValueError("No data to insert")

                        ins_cols = [
                            self.sanitize_identifier(k)
                            for k in insert_data.keys()
                        ]
                        placeholders = [
                            f"${i}" for i in range(1, len(ins_cols) + 1)
                        ]

                        insert_sql = f"""
                            INSERT INTO {safe_table} ({", ".join(ins_cols)})
                            VALUES ({", ".join(placeholders)})
                            RETURNING *
                        """
                        insert_sql = " ".join(insert_sql.split())
                        insert_vals = list(insert_data.values())

                        try:
                            insert_sql, insert_vals = self.prepare(
                                insert_sql, insert_vals
                            )
                        except Exception as e:
                            if "bad escape" not in str(e):
                                raise

                        try:
                            async with conn.transaction():
                                row = await conn.fetchrow(
                                    insert_sql, *insert_vals
                                )

                        except asyncpg.UniqueViolationError:
                            # ----------------------------------------
                            # 3) RETRY UPDATE
                            # ----------------------------------------
                            if not (active_update_keys and set_columns):
                                raise

                            set_parts, where_parts, args = [], [], []
                            idx = 1

                            for col in set_columns:
                                set_parts.append(
                                    f"{self.sanitize_identifier(col)} = ${idx}"
                                )
                                args.append(values[col])
                                idx += 1

                            for col in active_update_keys:
                                where_parts.append(
                                    f"{self.sanitize_identifier(col)} = ${idx}"
                                )
                                args.append(values[col])
                                idx += 1

                            retry_sql = f"""
                                UPDATE {safe_table}
                                SET {", ".join(set_parts)}
                                WHERE {" AND ".join(where_parts)}
                                RETURNING *
                            """
                            retry_sql = " ".join(retry_sql.split())
                            args = list(args)

                            try:
                                retry_sql, args = self.prepare(retry_sql, args)
                            except Exception as e:
                                if "bad escape" not in str(e):
                                    raise

                            row = await conn.fetchrow(retry_sql, *args)

                    # ------------------------------------------------
                    # FINAL
                    # ------------------------------------------------
                    if row is None:
                        raise RuntimeError(f"Upsert failed on table '{table}'")

                    data = dict(row)
                    data["error"] = None
                    UpsertResponse = namedtuple("UpsertResponse", data.keys())
                    return UpsertResponse(**data)

        except Exception as exc:
            if conn is not None:
                raise

            error_fields = {k: None for k in identifier_keys}
            error_fields["error"] = str(exc)
            UpsertResponse = namedtuple("UpsertResponse", error_fields.keys())
            return UpsertResponse(**error_fields)
