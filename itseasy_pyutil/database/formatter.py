import re

import sqlparse


class SQLFormatter:
    def format(self, sql: str) -> str:
        sql = self._normalize(sql)

        statements = sqlparse.split(sql)

        formatted = []
        for stmt in statements:
            stmt = self._format_statement(stmt)
            stmt = self._fix_cte(stmt)
            stmt = self._fix_begin_end(stmt)
            stmt = self._fix_pg_function(stmt)

            formatted.append(stmt.strip())

        return ";\n".join(formatted) + ";"

    # -----------------------------------------------------

    def _normalize(self, sql: str) -> str:
        sql = sql.strip()

        # collapse whitespace
        sql = re.sub(r"[ \t]+", " ", sql)
        sql = re.sub(r"\n+", "\n", sql)

        return sql

    # -----------------------------------------------------

    def _format_statement(self, sql: str) -> str:
        return sqlparse.format(
            sql,
            reindent=True,
            keyword_case="upper",
            indent_width=2,
            use_space_around_operators=True,
            strip_whitespace=True,
        )

    # -----------------------------------------------------
    # Fix: CREATE VIEW ... AS WITH
    # -----------------------------------------------------

    def _fix_cte(self, sql: str) -> str:
        sql = re.sub(
            r"\bAS WITH\b",
            "AS\nWITH",
            sql,
            flags=re.IGNORECASE,
        )

        sql = re.sub(
            r"\)\s*SELECT",
            ")\nSELECT",
            sql,
            flags=re.IGNORECASE,
        )

        return sql

    # -----------------------------------------------------
    # Fix: BEGIN / END blocks
    # -----------------------------------------------------

    def _fix_begin_end(self, sql: str) -> str:
        sql = re.sub(r"\bBEGIN\b", "\nBEGIN\n", sql, flags=re.IGNORECASE)
        sql = re.sub(r"\bEND\b", "\nEND\n", sql, flags=re.IGNORECASE)

        sql = re.sub(r";\s*", ";\n", sql)

        return sql

    # -----------------------------------------------------
    # Fix: PostgreSQL $$ functions
    # -----------------------------------------------------

    def _fix_pg_function(self, sql: str) -> str:
        if "$$" not in sql:
            return sql

        parts = re.split(r"(\$\$.*?\$\$)", sql, flags=re.S)

        result = []

        for part in parts:
            if part.startswith("$$"):
                body = part[2:-2].strip()

                body = re.sub(r";\s*", ";\n", body)
                body = re.sub(r"\bBEGIN\b", "\nBEGIN\n", body, flags=re.I)
                body = re.sub(r"\bEND\b", "\nEND\n", body, flags=re.I)

                body = "\n".join(
                    "  " + line.strip()
                    for line in body.splitlines()
                    if line.strip()
                )

                result.append("$$\n" + body + "\n$$")
            else:
                result.append(part)

        return "".join(result)
