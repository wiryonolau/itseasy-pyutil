import os
import sys
from logging.config import fileConfig

from alembic import context
from sqlalchemy import (
    CheckConstraint,
    ForeignKeyConstraint,
    PrimaryKeyConstraint,
    UniqueConstraint,
    create_engine,
    inspect,
    text,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.sql.schema import DefaultClause, ScalarElementColumnDefault

from itseasy_pyutil.util import floatval


def normalize_column_type(mysql_type):
    """Normalize MySQL column types to match SQLAlchemy types."""
    type_mapping = {
        "bigint": "BigInteger",
        "bit": "Boolean",
        "blob": "LargeBinary",
        "boolean": "Boolean",
        "char": "String",
        "date": "Date",
        "datetime": "DateTime",
        "decimal": "Numeric",
        "float": "Float",
        "int": "Integer",
        "json": "JSON",
        "smallint": "SmallInteger",
        "text": "Text",
        "time": "Time",
        "tinyint": "Boolean",
        "varchar": "String",
        "enum": "Enum",
    }
    return type_mapping.get(mysql_type, mysql_type.upper())


def normalize_sa_type(sa_type):
    """Normalize SQLAlchemy type to MySQL"""
    type_mapping = {
        "Boolean": "tinyint",  # MySQL uses tinyint(1) for boolean
        "Integer": "int",
        "SmallInteger": "smallint",
        "BigInteger": "bigint",
        "String": "varchar",
        "Text": "text",
        "Float": "float",
        "Numeric": "decimal",
        "DateTime": "datetime",
        "Date": "date",
        "Time": "time",
        "JSON": "longtext",
        "LargeBinary": "blob",
        "Enum": "enum",
    }

    return type_mapping.get(sa_type, sa_type.lower())


def normalize_default_value(default_value):
    """Normalize default values for comparison."""
    if isinstance(default_value, ScalarElementColumnDefault):
        default_value = str(default_value.arg)

    if isinstance(default_value, DefaultClause):
        default_value = str(default_value.arg)

    if isinstance(default_value, TextClause):
        default_value = str(default_value)

    if isinstance(default_value, bool):
        default_value = "1" if default_value else "0"

    if isinstance(default_value, str):
        default_value = default_value.lower()

    if default_value in [True, "true"]:
        return "1"

    if default_value in [False, "false"]:
        return "0"

    if default_value == "null":
        return None

    if default_value is None:
        return None

    return default_value


def column_info(connection, table_name, column_name):
    """Retrieve column details from INFORMATION_SCHEMA."""
    query = text(
        """
        SELECT 
            COLUMN_DEFAULT, COLUMN_TYPE, 
            IS_NULLABLE, DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH, 
            NUMERIC_PRECISION, 
            NUMERIC_SCALE, 
            EXTRA,
            CASE
                WHEN COLUMN_TYPE LIKE '%unsigned%' THEN TRUE
                ELSE FALSE 
            END AS IS_UNSIGNED
        FROM information_schema.tables AS t
        INNER JOIN information_schema.columns AS c
            ON c.table_schema = t.table_schema
                AND c.table_name = t.table_name
        WHERE t.table_type = "BASE TABLE"
            AND t.table_name = :table
            AND c.column_name = :column
        """
    )
    result = connection.execute(
        query, {"table": table_name, "column": column_name}
    ).fetchone()

    if result:
        (
            col_default,
            col_type,
            is_nullable,
            data_type,
            char_length,
            num_precision,
            num_scale,
            extra,
            is_unsigned,
        ) = result

        erratas = {
            "auto_increment": True if extra == "auto_increment" else False
        }

        return (
            data_type,
            col_type,
            is_nullable.lower(),
            normalize_default_value(col_default),
            char_length if char_length is not None else None,
            num_precision if num_precision is not None else None,
            num_scale if num_scale is not None else None,
            erratas,
            is_unsigned,
        )
    return None


def remove_old_tables(connection, metadata):
    """Find and drop tables that are not in the schema."""
    inspector = inspect(connection)
    existing_tables = set(inspector.get_table_names())
    model_tables = set(metadata.tables.keys())

    tables_to_drop = existing_tables - model_tables
    for table in tables_to_drop:
        print(f"Dropping old table: {table}")  # Debugging log
        try:
            connection.execute(text(f"DROP TABLE IF EXISTS `{table}`"))
        except SQLAlchemyError as e:
            print(f"Error dropping {table}: {e}")


def remove_old_columns(connection, metadata):
    """Find and drop columns that are not in the schema."""
    inspector = inspect(connection)

    for table_name, table_obj in metadata.tables.items():
        existing_columns = {
            col["name"] for col in inspector.get_columns(table_name)
        }
        model_columns = set(table_obj.columns.keys())

        columns_to_drop = existing_columns - model_columns
        for column_name in columns_to_drop:
            alter_stmt = (
                f"ALTER TABLE `{table_name}` DROP COLUMN `{column_name}`"
            )
            print(f"Dropping column: {alter_stmt}")  # Debugging log

            try:
                connection.execute(text(alter_stmt))
            except SQLAlchemyError as e:
                print(
                    f"Error dropping column {column_name} in {table_name}: {e}"
                )


def column_has_update(existing, new):
    (
        existing_type,
        existing_col_type,
        existing_nullable,
        existing_default,
        existing_length,
        existing_precision,
        existing_scale,
        existing_erratas,
        existing_is_unsigned,
    ) = existing

    (
        new_type,
        new_col_type,
        new_nullable,
        new_default,
        new_length,
        new_precision,
        new_scale,
        new_erratas,
        new_is_unsigned,
    ) = new

    if existing_default:
        existing_default = existing_default.strip("'\"").strip("'")

    if existing_nullable != new_nullable:
        return True

    existing_type = normalize_sa_type(existing_type)
    new_type = normalize_sa_type(new_type)

    if existing_type != new_type:
        return True

    if new_default:
        new_default = new_default.strip("'")

    if new_default is not None and existing_default != str(new_default).lower():
        if existing_type in ["smallint", "int", "bigint", "float", "decimal"]:
            existing_default = floatval(existing_default)
            new_default = floatval(existing_default)

            if existing_default == new_default:
                return False
        return True

    if new_is_unsigned != existing_is_unsigned:
        return True

    if new_length is not None and existing_length != new_length:
        return True

    if new_precision is not None and existing_precision != new_precision:
        return True

    if new_scale is not None and existing_scale != new_scale:
        return True

    if new_type == "enum" and existing_col_type != new_col_type:
        return True

    for k, v in existing_erratas.items():
        if new_erratas.get(k, None) != v:
            return True

    return False


def modify_column_if_needed(connection, table_name, column_name, column_obj):
    """Modify column only if differences exist in type, nullability, default, length, or precision."""
    existing_info = column_info(connection, table_name, column_name)

    new_type = type(column_obj.type).__name__

    new_col_type = new_type
    if new_type.lower() == "enum":
        enum_class = getattr(column_obj.type, "enum_class")
        enum_name = ["'{}'".format(e.value) for e in enum_class]
        new_col_type = "enum({})".format(",".join(enum_name))

    new_nullable = "yes" if column_obj.nullable else "no"
    new_default = normalize_default_value(
        column_obj.default or column_obj.server_default
    )
    new_length = getattr(column_obj.type, "length", None)
    new_precision = getattr(column_obj.type, "precision", None)
    new_scale = getattr(column_obj.type, "scale", None)
    new_erratas = {
        "auto_increment": True if column_obj.autoincrement == True else False
    }
    new_is_unsigned = getattr(column_obj.type, "unsigned", False)

    new_info = (
        new_type,
        new_col_type,
        new_nullable,
        new_default,
        new_length,
        new_precision,
        new_scale,
        new_erratas,
        new_is_unsigned,
    )

    if existing_info:
        if column_has_update(existing_info, new_info):
            print(
                f"Checking {column_name} in {table_name} -> Existing: {existing_info}, New: {new_info}"
            )
            new_type = normalize_sa_type(new_type)

            if new_type == "enum":
                alter_stmt = f"ALTER TABLE `{table_name}` MODIFY COLUMN `{column_name}` {new_col_type}"
            else:
                alter_stmt = f"ALTER TABLE `{table_name}` MODIFY COLUMN `{column_name}` {new_type}"
        else:
            print(f"Skipping {table_name}.{column_name} : No changes detected.")
            return
    else:
        print(
            f"Column {column_name} does not exist in {table_name}. Creating column."
        )
        new_type = normalize_sa_type(new_type)

        if new_type == "enum":
            alter_stmt = f"ALTER TABLE `{table_name}` ADD COLUMN `{column_name}` {new_col_type}"
        else:
            alter_stmt = f"ALTER TABLE `{table_name}` ADD COLUMN `{column_name}` {new_type}"

    if new_type != "enum" and new_length is not None:
        alter_stmt += f"({new_length})"
    elif new_precision is not None and new_scale is not None:
        alter_stmt += f"({new_precision}, {new_scale})"
    if new_is_unsigned:
        alter_stmt += f" UNSIGNED"
    if new_erratas.get("auto_increment"):
        alter_stmt += f" AUTO_INCREMENT"
    if new_nullable == "no":
        alter_stmt += " NOT NULL"
    if new_default is not None:
        if isinstance(new_default, bool):
            new_default = 1 if new_default else 0
        alter_stmt += f" DEFAULT {new_default}"

    try:
        print(alter_stmt)
        remove_column_fk(
            connection=connection,
            table_name=table_name,
            column_name=column_name,
        )
        connection.execute(text(alter_stmt))
    except SQLAlchemyError as e:
        label = "Modifying" if existing_info else "Adding"
        print(f"Error {label} {table_name}.{column_name}: {e}")


def remove_column_fk(connection, table_name, column_name):
    fks = connection.execute(
        text(
            f"""
        SELECT CONSTRAINT_NAME, TABLE_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE REFERENCED_TABLE_SCHEMA = DATABASE()
          AND REFERENCED_TABLE_NAME = :table
          AND REFERENCED_COLUMN_NAME = :column
    """
        ),
        {"table": table_name, "column": column_name},
    ).fetchall()

    for fk in fks:
        connection.execute(
            text(
                f"ALTER TABLE `{fk.TABLE_NAME}` DROP FOREIGN KEY `{fk.CONSTRAINT_NAME}`"
            )
        )


def sync_primary_keys(connection, metadata):
    inspector = inspect(connection)

    for table_name, table_obj in metadata.tables.items():
        existing_pk = inspector.get_pk_constraint(table_name)
        existing_pk_columns = (
            tuple(existing_pk["constrained_columns"])
            if existing_pk and existing_pk.get("constrained_columns")
            else tuple()
        )

        model_pk_columns = tuple(
            col.name for col in table_obj.primary_key.columns
        )

        # PK already matches → skip
        if existing_pk_columns == model_pk_columns:
            continue

        cols_info = inspector.get_columns(table_name)
        auto_inc_cols = {
            col["name"]
            for col in cols_info
            if col.get("autoincrement") in (True, "auto")
        }

        try:
            # Safety: AUTO_INCREMENT must be part of PK
            if auto_inc_cols and not auto_inc_cols.issubset(
                set(model_pk_columns)
            ):
                raise RuntimeError(
                    f"AUTO_INCREMENT column(s) {auto_inc_cols} must be part of PRIMARY KEY "
                    f"in table {table_name}"
                )

            if auto_inc_cols and model_pk_columns:
                # Must do in one statement
                pk_cols = ", ".join(f"`{c}`" for c in model_pk_columns)
                sql = (
                    f"ALTER TABLE `{table_name}` "
                    f"DROP PRIMARY KEY, ADD PRIMARY KEY ({pk_cols})"
                )
                connection.execute(text(sql))

            else:
                # Drop PK first
                if existing_pk_columns:
                    connection.execute(
                        text(f"ALTER TABLE `{table_name}` DROP PRIMARY KEY")
                    )

                # Add new PK
                if model_pk_columns:
                    pk_cols = ", ".join(f"`{c}`" for c in model_pk_columns)
                    connection.execute(
                        text(
                            f"ALTER TABLE `{table_name}` ADD PRIMARY KEY ({pk_cols})"
                        )
                    )

            print(f"PK synced for table {table_name}")

        except SQLAlchemyError as e:
            print(f"Error syncing PK for {table_name}: {e}")
            continue


def sync_indexes(connection, metadata):
    inspector = inspect(connection)

    for table_name, table_obj in metadata.tables.items():
        existing_indexes = {
            idx["name"]: idx
            for idx in inspector.get_indexes(table_name)
            if idx.get("name")
        }

        for idx in table_obj.indexes:
            name = idx.name
            if not name:
                continue

            model_cols = tuple(col.name for col in idx.columns)
            model_unique = bool(idx.unique)

            existing = existing_indexes.get(name)

            if not existing:
                print(f"Creating index {name} on {table_name}")
                idx.create(connection)
                continue

            existing_cols = tuple(existing["column_names"])
            existing_unique = bool(existing.get("unique"))

            if (existing_cols, existing_unique) == (model_cols, model_unique):
                continue

            print(
                f"Rebuilding index {name} on {table_name} "
                f"(cols: {existing_cols} → {model_cols}, "
                f"unique: {existing_unique} → {model_unique})"
            )

            connection.execute(text(f"DROP INDEX `{name}` ON `{table_name}`"))
            idx.create(connection)


def normalize_fk_action(action):
    """
    Normalize MySQL-equivalent FK actions.

    MySQL treats the following as identical:
    - None
    - NO ACTION
    - RESTRICT
    """
    if not action:
        return "RESTRICT"

    action = action.upper()

    if action in ("NO ACTION", "RESTRICT"):
        return "RESTRICT"

    return action


def sync_foreign_keys(connection, metadata):
    inspector = inspect(connection)

    for table_name, table_obj in metadata.tables.items():
        print(f"Checking foreign keys for table: {table_name}")

        # ---------- existing FKs ----------
        existing_fks = {}
        for fk in inspector.get_foreign_keys(table_name):
            identity = (
                tuple(fk["constrained_columns"]),
                fk["referred_table"],
                tuple(fk["referred_columns"]),
                normalize_fk_action(fk.get("options", {}).get("ondelete")),
                normalize_fk_action(fk.get("options", {}).get("onupdate")),
            )
            existing_fks[identity] = fk["name"]

        # ---------- model FKs ----------
        model_fks = {}
        for constraint in table_obj.constraints:
            if not isinstance(constraint, ForeignKeyConstraint):
                continue

            identity = (
                tuple(constraint.column_keys),
                constraint.elements[0].column.table.name,
                tuple(elem.column.name for elem in constraint.elements),
                normalize_fk_action(constraint.ondelete),
                normalize_fk_action(constraint.onupdate),
            )

            model_fks[identity] = constraint.name

            print(
                f"  Model FK: {identity[0]} → "
                f"{identity[1]}{identity[2]} "
                f"ON DELETE {identity[3]} "
                f"ON UPDATE {identity[4]}"
            )

        # ---------- drop obsolete ----------
        for identity, fk_name in existing_fks.items():
            if identity not in model_fks:
                print(f"Dropping FK {fk_name} on {table_name}")
                connection.execute(
                    text(
                        f"ALTER TABLE `{table_name}` "
                        f"DROP FOREIGN KEY `{fk_name}`"
                    )
                )

        # ---------- add missing ----------
        for identity, fk_name in model_fks.items():
            if identity in existing_fks:
                continue

            cols, ref_table, ref_cols, ondelete, onupdate = identity

            col_sql = ", ".join(f"`{c}`" for c in cols)
            ref_col_sql = ", ".join(f"`{c}`" for c in ref_cols)

            sql = (
                f"ALTER TABLE `{table_name}` "
                f"ADD CONSTRAINT `{fk_name}` "
                f"FOREIGN KEY ({col_sql}) "
                f"REFERENCES `{ref_table}` ({ref_col_sql})"
            )

            # Only emit clauses if non-default
            if ondelete != "RESTRICT":
                sql += f" ON DELETE {ondelete}"
            if onupdate != "RESTRICT":
                sql += f" ON UPDATE {onupdate}"

            print(f"Adding FK: {sql}")
            connection.execute(text(sql))


def sync_unique_constraints(connection, metadata):
    inspector = inspect(connection)

    for table_name, table_obj in metadata.tables.items():

        # ---- existing unique constraints ----
        existing_uniques = {}
        for uc in inspector.get_unique_constraints(table_name):
            if not uc.get("column_names"):
                continue

            identity = tuple(uc["column_names"])  # ORDERED tuple
            existing_uniques[identity] = uc["name"]

        # ---- model unique constraints ----
        model_uniques = {}
        for c in table_obj.constraints:
            if not isinstance(c, UniqueConstraint):
                continue
            if not c.columns:
                continue

            identity = tuple(c.columns.keys())  # ORDERED tuple

            if not c.name:
                raise RuntimeError(
                    f"UniqueConstraint on {table_name}{identity} must be named"
                )

            model_uniques[identity] = c.name

        # ---- drop obsolete or changed uniques ----
        for identity, name in existing_uniques.items():
            if identity not in model_uniques:
                print(f"Dropping unique constraint {name} on {table_name}")
                connection.execute(
                    text(f"ALTER TABLE `{table_name}` " f"DROP INDEX `{name}`")
                )

        # ---- add missing uniques ----
        for identity, name in model_uniques.items():
            if identity in existing_uniques:
                continue

            cols_sql = ", ".join(f"`{c}`" for c in identity)
            sql = (
                f"ALTER TABLE `{table_name}` "
                f"ADD UNIQUE INDEX `{name}` ({cols_sql})"
            )

            print(f"Adding unique constraint {name} on {table_name}")
            connection.execute(text(sql))


def sync_check_constraints(connection, metadata):
    inspector = inspect(connection)

    # ---- detect engine ----
    version = connection.execute(text("SELECT VERSION()")).scalar()
    is_mariadb = "mariadb" in version.lower()

    if is_mariadb:
        print("Skipping CHECK constraints (MariaDB ignores them)")
        return

    for table_name, table_obj in metadata.tables.items():
        print(f"Checking CHECK constraints for table: {table_name}")

        # ---- existing CHECK constraints ----
        existing_checks = {}
        for c in inspector.get_check_constraints(table_name):
            if not c.get("name"):
                continue

            identity = (c["name"], c["sqltext"])
            existing_checks[identity] = c["name"]

        # ---- model CHECK constraints ----
        model_checks = {}
        for c in table_obj.constraints:
            if not hasattr(c, "sqltext"):
                continue
            if not c.name:
                raise RuntimeError(
                    f"CHECK constraint on {table_name} must be named"
                )

            identity = (c.name, str(c.sqltext))
            model_checks[identity] = c.name

        # ---- drop obsolete / changed CHECKs ----
        for identity, name in existing_checks.items():
            if identity not in model_checks:
                print(f"Dropping CHECK constraint {name} on {table_name}")
                connection.execute(
                    text(f"ALTER TABLE `{table_name}` " f"DROP CHECK `{name}`")
                )

        # ---- add missing CHECKs ----
        for identity, name in model_checks.items():
            if identity in existing_checks:
                continue

            _, sqltext = identity
            sql = (
                f"ALTER TABLE `{table_name}` "
                f"ADD CONSTRAINT `{name}` CHECK ({sqltext})"
            )

            print(f"Adding CHECK constraint {name} on {table_name}")
            connection.execute(text(sql))


def sync_schema(engine, metadata):
    """Ensure schema consistency."""
    with engine.begin() as conn:
        conn.execute(text("SET SESSION sql_mode = ''"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))

        for table in metadata.tables.values():
            table.create(conn, checkfirst=True)

        remove_old_tables(conn, metadata)
        remove_old_columns(conn, metadata)

        for table_name, table_obj in metadata.tables.items():
            for column_name, column_obj in table_obj.columns.items():
                modify_column_if_needed(
                    conn, table_name, column_name, column_obj
                )

        sync_primary_keys(conn, metadata)
        sync_indexes(conn, metadata)
        sync_foreign_keys(conn, metadata)
        sync_unique_constraints(conn, metadata)
        sync_check_constraints(conn, metadata)

        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
