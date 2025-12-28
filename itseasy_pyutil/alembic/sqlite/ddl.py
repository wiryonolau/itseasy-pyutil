import importlib.util
import os

import sqlalchemy as sa


class DDLManager:
    def __init__(self, conn, modules=[], app_package=None):
        self.conn = conn

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
        return __import__(full, fromlist=["*"])

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
                "sqlite",
                "ddl.py",
            )

            if not os.path.exists(file_path):
                continue

            spec = importlib.util.spec_from_file_location(
                f"{self.app_package}.modules.{module_name}.db.sqlite.ddl",
                file_path,
            )
            module = importlib.util.module_from_spec(spec)

            module.__dict__.update(
                {
                    "rename_table": self.rename_table,
                    "rename_column": self.rename_column,
                    "create_trigger": self.create_trigger,
                    "create_audit_trigger": self.create_audit_trigger,
                    "create_modified_trigger": self.create_modified_trigger,
                    "run_ddl": self.run_ddl,
                }
            )

            spec.loader.exec_module(module)

    # ----------------------------------------------------------------
    # helpers
    # ----------------------------------------------------------------
    def _table_exists(self, table):
        row = self.conn.execute(
            sa.text(
                """
            SELECT 1 FROM sqlite_master
            WHERE type='table' AND name=:name
        """
            ),
            {"name": table},
        ).fetchone()
        return bool(row)

    def _trigger_exists(self, name):
        row = self.conn.execute(
            sa.text(
                """
            SELECT 1 FROM sqlite_master
            WHERE type='trigger' AND name=:name
        """
            ),
            {"name": name},
        ).fetchone()
        return bool(row)

    def _table_columns(self, table):
        rows = self.conn.execute(
            sa.text(f'PRAGMA table_info("{table}")')
        ).fetchall()
        return [r[1] for r in rows]

    # ----------------------------------------------------------------
    # DDL
    # ----------------------------------------------------------------
    def rename_table(self, old, new):
        if not self._table_exists(old):
            return
        if self._table_exists(new):
            return

        self.conn.execute(sa.text(f'ALTER TABLE "{old}" RENAME TO "{new}"'))

    def rename_column(self, table, old, new):
        if not self._table_exists(table):
            return

        cols = self._table_columns(table)
        if old not in cols or new in cols:
            return

        self.conn.execute(
            sa.text(f'ALTER TABLE "{table}" RENAME COLUMN "{old}" TO "{new}"')
        )

    # ----------------------------------------------------------------
    # Triggers
    # ----------------------------------------------------------------
    def create_trigger(self, name, sql, drop=False):
        if self._trigger_exists(name):
            if not drop:
                return
            self.conn.execute(sa.text(f'DROP TRIGGER IF EXISTS "{name}"'))

        self.conn.execute(sa.text(sql))

    def create_modified_trigger(self, table, drop=False):
        if not self._table_exists(table):
            return

        name = f"{table}_BUPD"

        if self._trigger_exists(name):
            if not drop:
                return
            self.conn.execute(sa.text(f'DROP TRIGGER IF EXISTS "{name}"'))

        sql = f"""
        CREATE TRIGGER "{name}"
        BEFORE UPDATE ON "{table}"
        FOR EACH ROW
        BEGIN
            SELECT CURRENT_TIMESTAMP;
            SET NEW.modified_at = CURRENT_TIMESTAMP;
        END;
        """

        self.conn.execute(sa.text(sql))

    def create_audit_trigger(
        self,
        table_name,
        pk_column,
        exclude_columns=None,
        drop=False,
    ):
        if exclude_columns is None:
            exclude_columns = []

        if not self._table_exists(table_name):
            return

        columns = [
            c
            for c in self._table_columns(table_name)
            if c not in exclude_columns
        ]

        cols_csv = ", ".join(columns)
        new_vals = ", ".join([f"NEW.{c}" for c in columns])
        old_vals = ", ".join([f"OLD.{c}" for c in columns])

        #
        # INSERT
        #
        trg_ins = f"{table_name}_AINS"
        sql_ins = f"""
        CREATE TRIGGER "{trg_ins}"
        AFTER INSERT ON "{table_name}"
        BEGIN
            INSERT INTO audit_log (
                table_name, row_id, action, after_data
            )
            VALUES (
                '{table_name}',
                NEW.{pk_column},
                'INSERT',
                json_object({", ".join([f"'{c}', NEW.{c}" for c in columns])})
            );
        END;
        """

        #
        # UPDATE (full snapshot)
        #
        trg_upd = f"{table_name}_AUPD"
        sql_upd = f"""
        CREATE TRIGGER "{trg_upd}"
        AFTER UPDATE ON "{table_name}"
        BEGIN
            INSERT INTO audit_log (
                table_name, row_id, action, before_data, after_data
            )
            VALUES (
                '{table_name}',
                NEW.{pk_column},
                'UPDATE',
                json_object({", ".join([f"'{c}', OLD.{c}" for c in columns])}),
                json_object({", ".join([f"'{c}', NEW.{c}" for c in columns])})
            );
        END;
        """

        #
        # DELETE
        #
        trg_del = f"{table_name}_ADEL"
        sql_del = f"""
        CREATE TRIGGER "{trg_del}"
        AFTER DELETE ON "{table_name}"
        BEGIN
            INSERT INTO audit_log (
                table_name, row_id, action, before_data
            )
            VALUES (
                '{table_name}',
                OLD.{pk_column},
                'DELETE',
                json_object({", ".join([f"'{c}', OLD.{c}" for c in columns])})
            );
        END;
        """

        self.create_trigger(trg_ins, sql_ins, drop)
        self.create_trigger(trg_upd, sql_upd, drop)
        self.create_trigger(trg_del, sql_del, drop)

    # ----------------------------------------------------------------
    # raw
    # ----------------------------------------------------------------
    def run_ddl(self, sql):
        self.conn.execute(sa.text(sql))
