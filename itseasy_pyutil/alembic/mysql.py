import os
import sys
from logging.config import fileConfig

from alembic import context
from sqlalchemy import (CheckConstraint, ForeignKeyConstraint,
                        PrimaryKeyConstraint, UniqueConstraint, create_engine,
                        inspect, text)
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

    if new_type == "enum" and existing_col_type.lower() != new_col_type.lower():
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
        enum_name = ["'{}'".format(e.name) for e in enum_class]
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
        existing_pk_columns = set(existing_pk["constrained_columns"]) if existing_pk else set()
        model_pk_columns = set(col.name for col in table_obj.primary_key.columns)

        # PK already matches → skip
        if existing_pk_columns == model_pk_columns:
            continue

        # Get auto-increment columns
        cols_info = inspector.get_columns(table_name)
        auto_inc_cols = {col["name"] for col in cols_info if col.get("autoincrement") in (True, "auto")}

        try:
            if auto_inc_cols:
                # Must drop/add PK in one step to avoid 1075
                pk_cols = ", ".join(f"`{col}`" for col in model_pk_columns)
                sql = f"ALTER TABLE `{table_name}` DROP PRIMARY KEY, ADD PRIMARY KEY ({pk_cols})"
                connection.execute(text(sql))
            else:
                # Drop PK first
                if existing_pk_columns:
                    connection.execute(text(f"ALTER TABLE `{table_name}` DROP PRIMARY KEY"))
                # Add new PK
                if model_pk_columns:
                    pk_cols = ", ".join(f"`{col}`" for col in model_pk_columns)
                    connection.execute(text(f"ALTER TABLE `{table_name}` ADD PRIMARY KEY ({pk_cols})"))

            print(f"PK synced for table {table_name}")

        except SQLAlchemyError as e:
            print(f"Error syncing PK for {table_name}: {e}")
            continue


def sync_indexes(connection, metadata):
    inspector = inspect(connection)

    for table_name, table_obj in metadata.tables.items():
        # get existing indexes
        existing_indexes = {
            idx["name"]: idx
            for idx in inspector.get_indexes(table_name)
        }

        for idx in table_obj.indexes:
            name = idx.name

            # SQLAlchemy Index → column names
            defined_cols = [col.name for col in idx.columns]

            existing = existing_indexes.get(name)

            if not existing:
                print(f"Creating index {name} on {table_name}")
                idx.create(connection)
                continue

            existing_cols = existing["column_names"]

            # compare ordered list, not set
            if existing_cols != defined_cols:
                print(
                    f"Rebuilding index {name} on {table_name} "
                    f"(old: {existing_cols}, new: {defined_cols})"
                )
                # drop index safely
                connection.execute(
                    text(f"DROP INDEX `{name}` ON `{table_name}`")
                )
                # recreate
                idx.create(connection)


def sync_foreign_keys(connection, metadata):
    """Ensure foreign keys match the schema without dropping valid ones."""
    inspector = inspect(connection)

    for table_name, table_obj in metadata.tables.items():
        print(f"Checking foreign keys for table: {table_name}")

        # Get existing foreign keys from the database
        existing_foreign_keys = {
            tuple(fk["constrained_columns"]): {
                "referred_table": fk["referred_table"],
                "referred_columns": tuple(fk["referred_columns"]),
                "name": fk["name"],
            }
            for fk in inspector.get_foreign_keys(table_name)
        }

        # Get foreign keys from the model definition
        model_foreign_keys = {}
        for constraint in table_obj.constraints:
            if isinstance(constraint, ForeignKeyConstraint):
                columns = tuple(constraint.column_keys)
                ref_table = constraint.elements[0].column.table.name
                ref_columns = tuple(
                    elem.column.name for elem in constraint.elements
                )
                model_foreign_keys[columns] = (ref_table, ref_columns)

                print(
                    f"  Found FK in model: {columns} → {ref_table}({ref_columns})"
                )

        # Add missing foreign keys
        for columns, (ref_table, ref_columns) in model_foreign_keys.items():
            if columns not in existing_foreign_keys:
                col_str = ", ".join(columns)
                ref_col_str = ", ".join(ref_columns)
                add_stmt = f"ALTER TABLE `{table_name}` ADD FOREIGN KEY ({col_str}) REFERENCES `{ref_table}`({ref_col_str})"
                print(f"Adding missing foreign key: {add_stmt}")

                try:
                    connection.execute(text(add_stmt))
                except SQLAlchemyError as e:
                    print(f"Error adding foreign key on {table_name}: {e}")

        # Drop obsolete foreign keys
        for columns, fk_info in existing_foreign_keys.items():
            if columns not in model_foreign_keys:
                fk_name = fk_info["name"]
                drop_stmt = (
                    f"ALTER TABLE `{table_name}` DROP FOREIGN KEY `{fk_name}`"
                )
                print(f"Dropping outdated foreign key: {drop_stmt}")

                try:
                    connection.execute(text(drop_stmt))
                except SQLAlchemyError as e:
                    print(f"Error dropping foreign key on {table_name}: {e}")


def sync_unique_constraints(connection, metadata):
    """Ensure unique constraints match the schema."""
    inspector = inspect(connection)

    for table_name, table_obj in metadata.tables.items():
        existing_unique_constraints = {
            constraint["name"]: set(constraint["column_names"])
            for constraint in inspector.get_unique_constraints(table_name)
        }

        model_unique_constraints = {
            constraint.name: set(constraint.columns.keys())
            for constraint in table_obj.constraints
            if isinstance(constraint, UniqueConstraint) and constraint.name
        }

        # Drop unique constraints not in model
        for constraint_name in (
            existing_unique_constraints.keys() - model_unique_constraints.keys()
        ):
            drop_stmt = (
                f"ALTER TABLE `{table_name}` DROP INDEX `{constraint_name}`"
            )
            print(f"Dropping unique constraint: {drop_stmt}")
            try:
                connection.execute(text(drop_stmt))
            except SQLAlchemyError as e:
                print(
                    f"Error dropping unique constraint {constraint_name} on {table_name}: {e}"
                )

        # Add missing unique constraints
        for constraint_name, columns in model_unique_constraints.items():
            if constraint_name not in existing_unique_constraints:
                add_stmt = f"ALTER TABLE `{table_name}` ADD CONSTRAINT `{constraint_name}` UNIQUE ({', '.join(columns)})"
                print(f"Adding unique constraint: {add_stmt}")
                try:
                    connection.execute(text(add_stmt))
                except SQLAlchemyError as e:
                    print(
                        f"Error adding unique constraint {constraint_name} on {table_name}: {e}"
                    )


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

        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
