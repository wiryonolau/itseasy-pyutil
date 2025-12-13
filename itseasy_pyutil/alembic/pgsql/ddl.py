import datetime
import importlib.util
import os

import sqlalchemy as sa
from sqlalchemy.sql import text


class DDLManager:
    def __init__(self, conn, modules=[], app_package=None):
        self.conn = conn
        self.app_modules_pkg = None

        if app_package is None:
            raise RuntimeError("DDLManager requires app_package=<package>")

        if isinstance(app_package, str):
            self.app_package = app_package
        else:
            self.app_package = app_package.__name__

        self.app_modules_pkg = self._import_modules_pkg(self.app_package)
        self.load_all_module_ddls(modules=modules)

    # ----------------------------------------------------------------
    # dynamically import "<app_package>.modules"
    # ----------------------------------------------------------------
    def _import_modules_pkg(self, app_package: str):
        full = f"{app_package}.modules"
        mod = __import__(full, fromlist=["*"])
        return mod

    # ----------------------------------------------------------------
    # Load ddl.py for each module dynamically
    # ----------------------------------------------------------------
    def load_all_module_ddls(self, modules):
        modules_base_path = self.app_modules_pkg.__path__[0]

        for module_name in modules:
            file_path = os.path.join(
                modules_base_path,
                module_name,
                "db",
                "ddl.py",
            )

            if not os.path.exists(file_path):
                continue

            spec = importlib.util.spec_from_file_location(
                f"{self.app_package}.modules.{module_name}.db.ddl",
                file_path,
            )
            module = importlib.util.module_from_spec(spec)

            module.__dict__.update(
                {
                    "rename_table": self.rename_table,
                    "rename_column": self.rename_column,
                    "create_partition": self.create_partition,
                    "create_trigger": self.create_trigger,
                    "create_audit_trigger": self.create_audit_trigger,
                    "create_modified_trigger": self.create_modified_trigger,
                    "create_procedure": self.create_procedure,
                    "run_ddl": self.run_ddl,
                }
            )

            spec.loader.exec_module(module)

    # ----------------------------------------------------------------
    # PostgreSQL DDL Handlers
    # ----------------------------------------------------------------

    def rename_table(self, old, new):
        exists_new = self.conn.execute(
            sa.text(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = :name
            """
            ),
            {"name": new},
        ).fetchone()

        if exists_new:
            return

        exists_old = self.conn.execute(
            sa.text(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = :name
            """
            ),
            {"name": old},
        ).fetchone()

        if not exists_old:
            return

        self.conn.execute(sa.text(f'ALTER TABLE "{old}" RENAME TO "{new}";'))

    def rename_column(self, table, old, new):
        exists_new = self.conn.execute(
            sa.text(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = :table
                AND column_name = :col
            """
            ),
            {"table": table, "col": new},
        ).fetchone()

        if exists_new:
            return

        old_col = self.conn.execute(
            sa.text(
                """
                SELECT data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = :table
                AND column_name = :col
            """
            ),
            {"table": table, "col": old},
        ).fetchone()

        if not old_col:
            return

        self.conn.execute(
            sa.text(f'ALTER TABLE "{table}" RENAME COLUMN "{old}" TO "{new}";')
        )

    def create_partition(
        self,
        table,
        column="created_at",
        year=None,
        mode="year",
        months_ahead=12,
    ):
        now = datetime.datetime.utcnow()
        year = year or now.year

        # PostgreSQL requires table already created as PARTITIONED
        # So we assume parent table is created as PARTITION BY RANGE(column)

        if mode == "year":
            pname = f"{table}_p{year}"
            upper = year + 1

            exists = self.conn.execute(
                sa.text(
                    """
                    SELECT 1
                    FROM pg_inherits
                    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
                    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
                    WHERE parent.relname = :table AND child.relname = :pname
                """
                ),
                {"table": table, "pname": pname},
            ).fetchone()

            if not exists:
                sql = f"""
                    CREATE TABLE "{pname}" PARTITION OF "{table}"
                    FOR VALUES FROM ('{year}-01-01') TO ('{upper}-01-01');
                """
                self.conn.execute(text(sql))
            return

        elif mode == "month":
            cur = datetime.date(year, 1, 1)
            for _ in range(months_ahead):
                pname = f"{table}_p{cur.year}{cur.month:02d}"
                next_month = (
                    datetime.date(cur.year + 1, 1, 1)
                    if cur.month == 12
                    else datetime.date(cur.year, cur.month + 1, 1)
                )

                exists = self.conn.execute(
                    sa.text(
                        """
                        SELECT 1
                        FROM pg_inherits
                        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
                        JOIN pg_class child ON pg_inherits.inhrelid = child.oid
                        WHERE parent.relname = :table AND child.relname = :pname
                    """
                    ),
                    {"table": table, "pname": pname},
                ).fetchone()

                if not exists:
                    sql = f"""
                        CREATE TABLE "{pname}" PARTITION OF "{table}"
                        FOR VALUES FROM ('{cur.isoformat()}') TO ('{next_month.isoformat()}');
                    """
                    self.conn.execute(text(sql))

                # next month
                cur = next_month
            return

        else:
            raise ValueError("Unsupported partition mode")

    def create_audit_trigger(
        self, table_name, pk_column, exclude_columns=None, drop=False
    ):
        if exclude_columns is None:
            exclude_columns = []

        rows = self.conn.execute(
            sa.text(
                """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
            AND table_name = :table
            ORDER BY ordinal_position
        """
            ),
            {"table": table_name},
        ).fetchall()

        columns = [r[0] for r in rows]
        diff_cols = [c for c in columns if c not in exclude_columns]

        json_old = ", ".join([f"'{c}', OLD.{c}" for c in diff_cols])
        json_new = ", ".join([f"'{c}', NEW.{c}" for c in diff_cols])
        change_conditions = " OR ".join(
            [f"OLD.{c} IS DISTINCT FROM NEW.{c}" for c in diff_cols]
        )

        trg_a_ins = f"{table_name}_AINS"
        sql_a_ins = f"""
        CREATE OR REPLACE FUNCTION {trg_a_ins}_fn() RETURNS trigger AS $$
        BEGIN
            INSERT INTO audit_log(table_name, row_id, action, user_id, after_data)
            VALUES ('{table_name}', NEW.{pk_column}, 'INSERT', current_user, jsonb_build_object({json_new}));
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        DROP TRIGGER IF EXISTS {trg_a_ins} ON "{table_name}";
        CREATE TRIGGER {trg_a_ins} AFTER INSERT ON "{table_name}"
        FOR EACH ROW EXECUTE FUNCTION {trg_a_ins}_fn();
        """

        trg_a_upd = f"{table_name}_AUPD"
        sql_a_upd = f"""
        CREATE OR REPLACE FUNCTION {trg_a_upd}_fn() RETURNS trigger AS $$
        BEGIN
            IF {change_conditions} THEN
                INSERT INTO audit_log(table_name, row_id, action, user_id, before_data, after_data)
                VALUES ('{table_name}', NEW.{pk_column}, 'UPDATE', current_user,
                        jsonb_build_object({json_old}),
                        jsonb_build_object({json_new}));
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        DROP TRIGGER IF EXISTS {trg_a_upd} ON "{table_name}";
        CREATE TRIGGER {trg_a_upd} AFTER UPDATE ON "{table_name}"
        FOR EACH ROW EXECUTE FUNCTION {trg_a_upd}_fn();
        """

        trg_a_del = f"{table_name}_ADEL"
        sql_a_del = f"""
        CREATE OR REPLACE FUNCTION {trg_a_del}_fn() RETURNS trigger AS $$
        BEGIN
            INSERT INTO audit_log(table_name, row_id, action, user_id, before_data)
            VALUES ('{table_name}', OLD.{pk_column}, 'DELETE', current_user,
                    jsonb_build_object({json_old}));
            RETURN OLD;
        END;
        $$ LANGUAGE plpgsql;

        DROP TRIGGER IF EXISTS {trg_a_del} ON "{table_name}";
        CREATE TRIGGER {trg_a_del} AFTER DELETE ON "{table_name}"
        FOR EACH ROW EXECUTE FUNCTION {trg_a_del}_fn();
        """

        self.conn.execute(text(sql_a_ins))
        self.conn.execute(text(sql_a_upd))
        self.conn.execute(text(sql_a_del))
        print(f"Audit triggers created for table '{table_name}'")

    def create_trigger(self, name, sql, drop=False):
        # in PostgreSQL, triggers are usually managed with CREATE OR REPLACE FUNCTION
        self.conn.execute(sa.text(sql))

    def create_modified_trigger(self, table, drop=False):
        name = f"{table}_BUPD"
        sql = f"""
        CREATE OR REPLACE FUNCTION {name}_fn() RETURNS trigger AS $$
        BEGIN
            NEW.modified_at = CURRENT_TIMESTAMP AT TIME ZONE 'UTC';
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        DROP TRIGGER IF EXISTS {name} ON "{table}";
        CREATE TRIGGER {name} BEFORE UPDATE ON "{table}"
        FOR EACH ROW EXECUTE FUNCTION {name}_fn();
        """
        self.conn.execute(sa.text(sql))

    def create_procedure(self, name, body, drop=False):
        if drop:
            self.conn.execute(sa.text(f"DROP PROCEDURE IF EXISTS {name}"))

        sql = f"CREATE PROCEDURE {name} {body}"
        self.conn.execute(sa.text(sql))

    def run_ddl(self, sql):
        self.conn.execute(sa.text(sql))
