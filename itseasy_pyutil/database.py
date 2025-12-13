import abc
import re
import sys
import traceback
from collections import namedtuple
from contextlib import asynccontextmanager
from typing import NamedTuple, Optional

from itseasy_pyutil import get_logger, list_get

SAFE_IDENTIFIER = re.compile(
    r"""
    ^
    [A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*   # table.column or schema.table.column
    (?:\s+AS\s+[A-Za-z_][A-Za-z0-9_]*)?                   # optional "AS alias"
    $
    """,
    re.IGNORECASE | re.VERBOSE,
)

SAFE_IDENTIFIER_WITH_STAR = re.compile(
    r"""
    ^(
        \* |                                               # just *
        [A-Za-z_][A-Za-z0-9_]*\.\* |                       # alias.*
        [A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)* # normal col/table.col
    )
    (?:\s+AS\s+[A-Za-z_][A-Za-z0-9_]*)?$                   # optional "AS alias"
    """,
    re.IGNORECASE | re.VERBOSE,
)

ORDER_PATTERN = re.compile(
    r"""
    ^
    [A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*   # identifier (schema.table.column)
    (?:\s+IS\s+NULL)?                                     # optional "IS NULL"
    (?:\s+(ASC|DESC))?                                    # optional ASC/DESC
    $
    """,
    re.IGNORECASE | re.VERBOSE,
)


class Filter(NamedTuple):
    """
    Filter format from frontend
    """

    id: str
    value: str = None
    opr: str = "="


class Expression:
    def __init__(self, expr: str):
        # Kill multiple statements
        expr = expr.split(";", 1)[0]
        # Kill inline comments
        expr = expr.split("--", 1)[0].split("/*", 1)[0]
        self._expr = expr.strip()

    @property
    def expr(self):
        return self._expr

    def __str__(self):
        return self._expr


class Condition(NamedTuple):
    column: str
    value: str
    glue: str = "AND"
    opr: str = "="


class ConditionSet(NamedTuple):
    conditions: list = []
    # This is glue against outside condition not inside set
    glue: str = "AND"


class Join(NamedTuple):
    table: str
    conditions: list = []
    join_type: str = "INNER JOIN"


class Response(NamedTuple):
    """
    Response for sql other then SELECT
    """

    success: bool
    lastrowid: Optional[int]
    error: Optional[str]


class DatabaseAware:
    def __init__(self, db: "Database", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._db = db


class AbstractDatabase(abc.ABC):
    def __init__(self, db_config, as_dev: bool = False):
        self._db_config = db_config
        self._pool = None
        self._as_dev = as_dev

        self._logger = get_logger(self.__class__.__name__)

    async def init(self):
        await self.connect()

    def sanitize_order(self, order: str) -> str:
        if isinstance(order, Expression):
            return order.expr

        if not isinstance(order, str):
            raise ValueError("Order clause must be a string")
        order = order.strip()

        if not ORDER_PATTERN.match(order):
            raise ValueError(f"Invalid ORDER BY clause: {order}")
        return order

    def sanitize_identifier(
        self, identifier: str, allow_star: bool = False
    ) -> str:
        if not isinstance(identifier, str):
            self._logger.debug([identifier, allow_star])
            raise ValueError("Identifier must be a string")

        identifier = identifier.strip()
        pattern = SAFE_IDENTIFIER_WITH_STAR if allow_star else SAFE_IDENTIFIER

        if not pattern.match(identifier):
            raise ValueError(f"Invalid SQL identifier: {identifier}")

        return identifier

    def filter_to_conditions(self, filters=[], mapping={}):
        """
        Convert filter from front end to condition, beware of sql injection
        """
        conditions = []

        for f in filters:
            try:
                obj = Filter(**f)

                opr = obj.opr
                value = obj.value

                if obj.opr in ["=", ">", "<", ">=", "<="]:
                    opr = obj.opr
                elif obj.opr == "eq":
                    opr = "="
                elif obj.opr == "neq":
                    opr = "!="
                elif obj.opr == "gt":
                    opr = ">"
                elif obj.opr == "gte":
                    opr = ">="
                elif obj.opr == "lt":
                    opr = "<"
                elif obj.opr == "lte":
                    opr = "<="
                elif obj.opr == "contain":
                    opr = "LIKE"
                    value = f"%{value}%"
                elif obj.opr == "includes":
                    opr = "IN"
                    value = value.split(",")
                elif obj.opr == "startswith":
                    opr = "LIKE"
                    value = f"{value}%"
                elif obj.opr == "empty":
                    opr = "IS"
                    value = None
                elif obj.opr == "notempty":
                    opr = "IS NOT"
                    value = None
                else:
                    continue

                if not len(str(value)):
                    continue

                column = mapping.get(obj.id, obj.id)

                conditions.append(
                    Condition(column=column, value=value, opr=opr)
                )
            except:
                continue
        return conditions

    def get_sql_string(self, sql: str, values=[]):
        # Step 1: Clean SQL (remove empty lines, extra spaces)
        lines = [line.strip() for line in sql.splitlines() if line.strip()]
        cleaned_sql = re.sub(r"\s+", " ", " ".join(lines)).strip()

        if len(values):
            parts = cleaned_sql.split("%s")
            if len(parts) - 1 != len(values):
                raise ValueError(
                    "Number of placeholders (%s) and values do not match."
                )

            result = parts[0]
            for i in range(len(values)):
                val = values[i]
                if isinstance(val, bool):
                    formatted_val = str(int(val))
                elif isinstance(val, int):
                    formatted_val = str(val)
                elif isinstance(val, bytes):
                    formatted_val = val.hex()
                elif val is None:
                    formatted_val = "NULL"
                else:
                    escaped = str(val).replace(
                        "'", "''"
                    )  # Escape single quotes
                    formatted_val = f"'{escaped}'"
                result += formatted_val + parts[i + 1]
            return result
        else:
            return cleaned_sql

    def parse_joins(self, joins=[]):
        params = []

        stmts = []
        for i, join in enumerate(joins):
            if len(join) == 2:
                table, conditions = join
                join_type = "INNER JOIN"
            elif len(join) == 3:
                table, conditions, join_type = join
            else:
                continue

            table = self.sanitize_identifier(table)
            condition_stmt, condition_params = self.parse_conditions(
                conditions, "ON"
            )

            stmts.append(f"{join_type} {table} {condition_stmt}")
            params += condition_params

        return (" ".join(stmts), params)

    def parse_conditions(self, conditions=[], suffix="WHERE"):
        params = []
        statements = []
        for i, condition in enumerate(conditions):
            stmt = ""

            if isinstance(condition, ConditionSet):
                group_stmt, group_params = self.parse_conditions(
                    conditions=condition.conditions, suffix=""
                )
                if not group_stmt:
                    continue

                glue = (
                    condition.glue if len(statements) else ""
                )  # Only prepend if not first

                statements.append(
                    f"{glue} ({group_stmt})" if glue else f"({group_stmt})"
                )
                params += group_params
                continue
            elif not isinstance(condition, Condition):
                if len(condition) == 2:
                    column, value = condition
                    opr = "="
                    glue = "AND"
                elif len(condition) == 3:
                    column, opr, value = condition
                    glue = "AND"
                elif len(condition) == 4:
                    column, opr, value, glue = condition
                else:
                    continue

                if isinstance(value, bool):
                    value = int(value)
                elif isinstance(value, bytes):
                    value = pymysql.Binary(value)

                if not isinstance(column, Expression):
                    column = self.sanitize_identifier(identifier=column)

                condition = Condition(
                    column=column, value=value, opr=opr, glue=glue
                )

            if condition.opr in ["not in", "in", "NOT IN", "IN"]:
                placeholders = ", ".join(["%s"] * len(condition.value))
                stmt = f"{condition.column} {condition.opr} ({placeholders})"
                params += condition.value
            elif condition.opr in ["between", "BETWEEN"]:
                stmt = f"({condition.column} {condition.opr} %s AND %s)"
                params += condition.value
            elif condition.opr == "ref":
                """
                FOR JOIN
                """
                if isinstance(condition.value, Expression):
                    stmt = f"{condition.column} = {condition.value.expr}"
                else:
                    stmt = f"{condition.column} = {condition.value}"
            else:
                if isinstance(condition.value, Expression):
                    stmt = f"{condition.column} {condition.opr} {condition.value.expr}"
                else:
                    stmt = f"{condition.column} {condition.opr} %s"
                    params.append(condition.value)

            glue = condition.glue if len(statements) else ""
            statements.append(f"{glue} {stmt}" if glue else stmt)

        if not len(statements):
            return ("", [])

        statements = " ".join(statements)
        return (f"{suffix} {statements}", params)

    def select_stmt(
        self,
        table,
        columns=[],
        joins=[],
        conditions=[],
        orders=[],
        limit=100,
        offset=0,
    ):
        join_stmt, join_params = self.parse_joins(joins)
        conditions_stmt, conditions_params = self.parse_conditions(conditions)

        columns = [
            self.sanitize_identifier(c, allow_star=True) for c in columns
        ]
        orders = [self.sanitize_order(o) for o in orders]

        stmt = f"""
            SELECT {",".join(columns) or "*"}
            FROM {self.sanitize_identifier(table)}
            {join_stmt}
            {conditions_stmt}
            {"ORDER BY" if len(orders) else ""}
            {",".join(orders)}
            LIMIT %s OFFSET %s
        """
        return (stmt, join_params + conditions_params + [limit, offset])

    def insert_stmt(self, table, column_values={}):
        columns = ",".join(
            [self.sanitize_identifier(c) for c in column_values.keys()]
        )
        placeholders = ",".join(["%s"] * len(column_values))
        values = list(column_values.values())

        # Insert query
        insert_stmt = f"""
        INSERT INTO {self.sanitize_identifier(table)} ({columns}) VALUES ({placeholders})
        """

        return (insert_stmt, values)

    def update_stmt(
        self, table, identifiers=[], conditions=[], column_values={}
    ):
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
            [f"{self.sanitize_identifier(col)}=%s" for col in update_columns]
        )

        update_stmt = f"UPDATE {self.sanitize_identifier(table)} SET {set_clause} {conditions}"

        update_values = list(update_columns.values()) + params

        return (update_stmt, update_values)

    def delete_stmt(self, table, conditions=[]):
        conditions, params = self.parse_conditions(conditions)

        stmt = f"""
        DELETE FROM {self.sanitize_identifier(table)}
        {conditions}
        """

        return (stmt, params)
