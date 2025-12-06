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

        # dynamically import app_package.modules
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

            # inject functions
            module.__dict__.update({
                "rename_table": self.rename_table,
                "rename_column": self.rename_column,
                "create_partition": self.create_partition,
                "create_trigger": self.create_trigger,
                "create_audit_trigger" : self.create_audit_trigger,
                "create_modified_trigger": self.create_modified_trigger,
                "create_procedure": self.create_procedure,
                "run_ddl": self.run_ddl,
            })

            spec.loader.exec_module(module)

    # ----------------------------------------------------------------
    # ALL DDL HANDLERS (same as before)
    # ----------------------------------------------------------------

    def rename_table(self, old, new):
        exists_new = self.conn.execute(
            sa.text("""
                SELECT 1 FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = :name
            """),
            {"name": new},
        ).fetchone()

        if exists_new:
            return

        exists_old = self.conn.execute(
            sa.text("""
                SELECT 1 FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = :name
            """),
            {"name": old},
        ).fetchone()

        if not exists_old:
            return

        self.conn.execute(sa.text(f"RENAME TABLE `{old}` TO `{new}`;"))


    def rename_column(self, table, old, new):
        exists_new = self.conn.execute(
            sa.text("""
                SELECT 1 FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = :table
                AND COLUMN_NAME = :col
            """),
            {"table": table, "col": new},
        ).fetchone()

        if exists_new:
            return

        old_col = self.conn.execute(
            sa.text("""
                SELECT COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = :table
                AND COLUMN_NAME = :col
            """),
            {"table": table, "col": old},
        ).fetchone()

        if not old_col:
            return

        column_type = old_col.COLUMN_TYPE
        nullable = "NULL" if old_col.IS_NULLABLE == "YES" else "NOT NULL"
        default = (
            f"DEFAULT {old_col.COLUMN_DEFAULT}"
            if old_col.COLUMN_DEFAULT is not None
            else ""
        )

        sql = f"""
            ALTER TABLE `{table}`
            CHANGE COLUMN `{old}` `{new}`
            {column_type} {nullable} {default};
        """
        self.conn.execute(sa.text(sql))


    def create_partition(self, table, column="created_at", year=None, mode="year", months_ahead=12):
        conn = self.conn
        now = datetime.datetime.utcnow()

        # --- 0. Detect if table is already partitioned ---
        rows = conn.execute(text("""
            SELECT PARTITION_NAME
            FROM information_schema.PARTITIONS
            WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = :table
        """), {"table": table}).fetchall()

        # MariaDB 11 returns NULL rows for non-partitioned tables → detect properly
        is_partitioned = any(row[0] is not None for row in rows)

        # Already existing partitions (skip NULL)
        existing = {row[0] for row in rows if row[0]}

        # ----------------------------------------------------------------------
        # 1. If NOT partitioned → convert table → create FIRST *correct* partition
        # ----------------------------------------------------------------------
        if not is_partitioned:
            if mode == "year":
                start_year = year or now.year
                upper = start_year + 1
                pname = f"p{start_year}"

                sql = f"""
                ALTER TABLE `{table}`
                PARTITION BY RANGE (YEAR(`{column}`)) (
                    PARTITION `{pname}` VALUES LESS THAN ({upper})
                );
                """
                conn.execute(text(sql))
                existing = {pname}

            elif mode == "month":
                # first month: year-Jan if year provided, else current month
                if year:
                    cur = datetime.datetime(year, 1, 1)
                else:
                    cur = now.replace(day=1)

                pname = f"p{cur.year}{cur.month:02d}"

                # compute next month boundary
                if cur.month == 12:
                    next_month = datetime.date(cur.year + 1, 1, 1)
                else:
                    next_month = datetime.date(cur.year, cur.month + 1, 1)

                sql = f"""
                ALTER TABLE `{table}`
                PARTITION BY RANGE (TO_DAYS(`{column}`)) (
                    PARTITION `{pname}` VALUES LESS THAN (TO_DAYS('{next_month.isoformat()}'))
                );
                """
                conn.execute(text(sql))
                existing = {pname}

            elif mode == "datetime":
                start_year = year or now.year
                pname = f"p{start_year}"
                upper_date = datetime.date(start_year + 1, 1, 1).isoformat()

                sql = f"""
                ALTER TABLE `{table}`
                PARTITION BY RANGE COLUMNS (`{column}`) (
                    PARTITION `{pname}` VALUES LESS THAN ('{upper_date}')
                );
                """
                conn.execute(text(sql))
                existing = {pname}

            else:
                raise ValueError("Unsupported partition mode")

        # ----------------------------------------------------------------------
        # From this point, table is partitioned and `existing` contains partitions
        # ----------------------------------------------------------------------

        # === YEAR partitions ===
        if mode == "year":
            start_year = year or now.year
            pname = f"p{start_year}"

            if pname not in existing:
                upper = start_year + 1
                sql = (
                    f"ALTER TABLE `{table}` ADD PARTITION ("
                    f"PARTITION `{pname}` VALUES LESS THAN ({upper})"
                    f");"
                )
                conn.execute(text(sql))

            return

        # === MONTH partitions ===
        elif mode == "month":
            cur = datetime.datetime(year, 1, 1) if year else now.replace(day=1)

            for _ in range(months_ahead):
                pname = f"p{cur.year}{cur.month:02d}"

                if pname not in existing:
                    if cur.month == 12:
                        next_month = datetime.date(cur.year + 1, 1, 1)
                    else:
                        next_month = datetime.date(cur.year, cur.month + 1, 1)

                    sql = (
                        f"ALTER TABLE `{table}` ADD PARTITION ("
                        f"PARTITION `{pname}` "
                        f"VALUES LESS THAN (TO_DAYS('{next_month.isoformat()}'))"
                        f");"
                    )
                    conn.execute(text(sql))

                # increment month
                if cur.month == 12:
                    cur = cur.replace(year=cur.year+1, month=1)
                else:
                    cur = cur.replace(month=cur.month+1)

            return

        # === DATETIME partitions ===
        elif mode == "datetime":
            start_year = year or now.year
            pname = f"p{start_year}"

            if pname not in existing:
                upper_date = datetime.date(start_year + 1, 1, 1).isoformat()
                sql = (
                    f"ALTER TABLE `{table}` "
                    f"ADD PARTITION (PARTITION `{pname}` VALUES LESS THAN ('{upper_date}'))"
                )
                conn.execute(text(sql))

            return

        else:
            raise ValueError("Unsupported partition mode")



    def create_audit_trigger(self, table_name, pk_column, exclude_columns=None):
        if exclude_columns is None:
            exclude_columns = []

        # 1. Load columns for diff
        rows = self.conn.execute(sa.text("""
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = :table
            ORDER BY ORDINAL_POSITION
        """), {"table": table_name}).fetchall()

        columns = [r[0] for r in rows]
        diff_cols = [c for c in columns if c not in exclude_columns]

        # 2. Build JSON OBJECT pairs
        json_old = ", ".join([f"'{c}', OLD.{c}" for c in diff_cols])
        json_new = ", ".join([f"'{c}', NEW.{c}" for c in diff_cols])

        # 3. Detect changed columns
        change_conditions = " OR ".join(
            [f"NOT (OLD.{c} <=> NEW.{c})" for c in diff_cols]
        )

        #
        # ==== AFTER INSERT : AINS ====
        #
        trg_a_ins = f"{table_name}_AINS"
        sql_a_ins = f"""
        CREATE TRIGGER {trg_a_ins}
        AFTER INSERT ON {table_name}
        FOR EACH ROW
        BEGIN
            INSERT INTO audit_log (table_name, row_id, action, user_id, after_data)
            VALUES (
                '{table_name}',
                NEW.{pk_column},
                'INSERT',
                @current_user_id,
                JSON_OBJECT({json_new})
            );
        END;
        """

        #
        # ==== AFTER UPDATE : AUPD ====
        #
        trg_a_upd = f"{table_name}_AUPD"
        sql_a_upd = f"""
        CREATE TRIGGER {trg_a_upd}
        AFTER UPDATE ON {table_name}
        FOR EACH ROW
        BEGIN
            IF {change_conditions} THEN
                INSERT INTO audit_log (
                    table_name, row_id, action, user_id,
                    before_data, after_data
                )
                VALUES (
                    '{table_name}',
                    NEW.{pk_column},
                    'UPDATE',
                    @current_user_id,
                    JSON_OBJECT({json_old}),
                    JSON_OBJECT({json_new})
                );
            END IF;
        END;
        """

        #
        # ==== AFTER DELETE : ADEL ====
        #
        trg_a_del = f"{table_name}_ADEL"
        sql_a_del = f"""
        CREATE TRIGGER {trg_a_del}
        AFTER DELETE ON {table_name}
        FOR EACH ROW
        BEGIN
            INSERT INTO audit_log (
                table_name, row_id, action, user_id, before_data
            )
            VALUES (
                '{table_name}',
                OLD.{pk_column},
                'DELETE',
                @current_user_id,
                JSON_OBJECT({json_old})
            );
        END;
        """

        #
        # Create triggers immediately
        #
        self.create_trigger(trg_a_ins, sql_a_ins)
        self.create_trigger(trg_a_upd, sql_a_upd)
        self.create_trigger(trg_a_del, sql_a_del)

        print(f"Audit triggers created for table '{table_name}'")


    def create_trigger(self, name, sql):
        print(f"Create trigger for {name}")
        exists = self.conn.execute(
            sa.text("""
                SELECT 1 FROM information_schema.TRIGGERS
                WHERE TRIGGER_SCHEMA = DATABASE()
                AND TRIGGER_NAME = :name
            """),
            {"name": name},
        ).fetchone()

        if exists:
            return

        self.conn.execute(sa.text(sql))


    def create_modified_trigger(self, table):
        name = f"{table}_BUPD"
        print(f"Create modified trigger {name} for {table}")

        exists = self.conn.execute(
            sa.text("""
                SELECT 1 FROM information_schema.TRIGGERS
                WHERE TRIGGER_SCHEMA = DATABASE()
                AND TRIGGER_NAME = :name
            """),
            {"name": name},
        ).fetchone()

        if exists:
            return

        sql = f"""
            CREATE TRIGGER {name}
            BEFORE UPDATE ON `{table}`
            FOR EACH ROW
            BEGIN
                SET NEW.modified_at = UTC_TIMESTAMP();
            END
        """

        self.conn.execute(sa.text(sql))


    def create_procedure(self, name, body):
        exists = self.conn.execute(
            sa.text("""
                SELECT 1 FROM information_schema.ROUTINES
                WHERE ROUTINE_SCHEMA = DATABASE()
                AND ROUTINE_NAME = :name
                AND ROUTINE_TYPE = 'PROCEDURE'
            """),
            {"name": name},
        ).fetchone()

        if exists:
            return

        sql = f"CREATE PROCEDURE {name} {body}"
        self.conn.execute(sa.text(sql))


    def run_ddl(self, sql):
        self.conn.execute(sa.text(sql))
