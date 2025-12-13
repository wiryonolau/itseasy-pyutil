import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import ForeignKeyConstraint, UniqueConstraint, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.schema import DefaultClause
from sqlalchemy.sql.elements import TextClause

from itseasy_pyutil.util import floatval

# ---------- Helpers ----------

def normalize_default_value(value):
    if value is None:
        return None
    if isinstance(value, DefaultClause):
        value = value.arg
    if isinstance(value, TextClause):
        value = str(value).lower()
    if isinstance(value, bool):
        value = "true" if value else "false"
    if isinstance(value, str):
        value = value.lower()
    return value


def normalize_sa_type(sa_type):
    mapping = {
        "Integer": "integer",
        "SmallInteger": "smallint",
        "BigInteger": "bigint",
        "Boolean": "boolean",
        "String": "character varying",
        "Text": "text",
        "Float": "double precision",
        "Numeric": "numeric",
        "DateTime": "timestamp without time zone",
        "Date": "date",
        "Time": "time without time zone",
        "JSON": "jsonb",
        "LargeBinary": "bytea",
        "Enum": "enum",
    }
    return mapping.get(sa_type, sa_type.lower())


# ---------- Column inspection ----------

def column_info(connection, table_name, column_name):
    inspector = inspect(connection)
    for col in inspector.get_columns(table_name):
        if col["name"] != column_name:
            continue

        seq_query = text(
            """
            SELECT c.is_identity
            FROM information_schema.columns c
            WHERE c.table_name = :table AND c.column_name = :column
            """
        )
        seq_res = connection.execute(seq_query, {"table": table_name, "column": column_name}).fetchone()
        is_identity = seq_res[0] == "YES" if seq_res else False

        return (
            str(col["type"]),
            str(col["type"]),
            "yes" if col["nullable"] else "no",
            normalize_default_value(col.get("default")),
            getattr(col["type"], "length", None),
            getattr(col["type"], "precision", None),
            getattr(col["type"], "scale", None),
            {"extra": None},
            is_identity
        )
    return None


# ---------- Column diff ----------

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
        existing_is_identity,
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
        new_is_identity,
    ) = new

    if existing_default:
        existing_default = str(existing_default).strip("'\"").lower()
    if new_default:
        new_default = str(new_default).strip("'\"").lower()

    if existing_nullable != new_nullable:
        return True

    existing_type = normalize_sa_type(existing_type)
    new_type = normalize_sa_type(new_type)

    if existing_type != new_type:
        return True

    if existing_default != new_default:
        if existing_type in ["smallint", "integer", "bigint", "double precision", "numeric"]:
            try:
                if floatval(existing_default) == floatval(new_default):
                    return False
            except Exception:
                pass
        return True

    if new_length is not None and existing_length != new_length:
        return True
    if new_precision is not None and existing_precision != new_precision:
        return True
    if new_scale is not None and existing_scale != new_scale:
        return True

    if new_type == "enum" and existing_col_type != new_col_type:
        return True

    if new_is_identity != existing_is_identity:
        return True

    for k, v in existing_erratas.items():
        if new_erratas.get(k, None) != v:
            return True

    return False


# ---------- Column sync ----------
def modify_column_if_needed(connection, table_name, column_name, column_obj):
    """
    Create or modify a column in PostgreSQL.
    Always removes any FK referencing the column before altering.
    Handles type, length/precision, nullable, default, enum, identity.
    Idempotent for multiple runs.
    """
    existing_info = column_info(connection, table_name, column_name)

    new_type = type(column_obj.type).__name__
    new_col_type = new_type
    if new_type.lower() == "enum":
        enum_class = getattr(column_obj.type, "enum_class")
        new_col_type = ",".join([e.value for e in enum_class])

    new_nullable = "yes" if column_obj.nullable else "no"
    new_default = normalize_default_value(column_obj.default or column_obj.server_default)
    new_length = getattr(column_obj.type, "length", None)
    new_precision = getattr(column_obj.type, "precision", None)
    new_scale = getattr(column_obj.type, "scale", None)
    new_erratas = {"extra": getattr(column_obj, "extra", None)}
    new_is_identity = getattr(column_obj, "autoincrement", False)

    new_info = (
        new_type,
        new_col_type,
        new_nullable,
        new_default,
        new_length,
        new_precision,
        new_scale,
        new_erratas,
        new_is_identity,
    )

    if existing_info:
        if column_has_update(existing_info, new_info):
            print(f"Checking {column_name} in {table_name} -> Existing: {existing_info}, New: {new_info}")

            # Always remove any FK referencing this column first
            remove_column_fk(connection, table_name, column_name)

            # Handle enums
            if new_type.lower() == "enum":
                enum_type_name = f"{table_name}_{column_name}_enum"
                existing_vals = connection.execute(
                    text(
                        "SELECT enumlabel FROM pg_enum JOIN pg_type ON pg_enum.enumtypid = pg_type.oid "
                        "WHERE typname=:name ORDER BY enumsortorder"
                    ),
                    {"name": enum_type_name}
                ).fetchall()
                existing_vals = [v[0] for v in existing_vals]
                for val in enum_class:
                    if val.value not in existing_vals:
                        connection.execute(
                            text(f'ALTER TYPE "{enum_type_name}" ADD VALUE IF NOT EXISTS \'{val.value}\'')
                        )

            # Handle incompatible type with identity: drop identity first
            if existing_info[8] and existing_info[1] != new_col_type:
                connection.execute(
                    text(f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" DROP IDENTITY IF EXISTS')
                )

            # ALTER type
            col_type_sql = normalize_sa_type(new_type)
            if new_length:
                col_type_sql += f"({new_length})"
            elif new_precision and new_scale:
                col_type_sql += f"({new_precision},{new_scale})"

            connection.execute(
                text(f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" TYPE {col_type_sql} USING "{column_name}"::{col_type_sql}')
            )

            # ALTER nullability
            if existing_info[2] != new_nullable:
                action = "DROP NOT NULL" if new_nullable == "yes" else "SET NOT NULL"
                connection.execute(text(f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" {action}'))

            # ALTER default
            if existing_info[3] != new_default:
                if new_default is None:
                    connection.execute(text(f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" DROP DEFAULT'))
                else:
                    connection.execute(text(f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" SET DEFAULT {new_default}'))

            # ALTER identity
            if existing_info[8] != new_is_identity or (existing_info[1] != new_col_type and new_is_identity):
                if new_is_identity:
                    connection.execute(
                        text(f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" ADD GENERATED BY DEFAULT AS IDENTITY IF NOT EXISTS')
                    )
                    # Ensure sequence exists and owned properly
                    sync_sequence(connection, table_name, column_name)
                else:
                    connection.execute(
                        text(f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" DROP IDENTITY IF EXISTS')
                    )
        else:
            print(f"Skipping {table_name}.{column_name} : No changes detected.")
            return

    else:
        print(f"Column {column_name} does not exist in {table_name}. Creating column.")

        # Drop FK just in case (safe no-op)
        remove_column_fk(connection, table_name, column_name)

        col_type_sql = normalize_sa_type(new_type)
        if new_length:
            col_type_sql += f"({new_length})"
        elif new_precision and new_scale:
            col_type_sql += f"({new_precision},{new_scale})"

        alter_stmt = f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {col_type_sql}'
        if not column_obj.nullable:
            alter_stmt += " NOT NULL"
        if new_default is not None:
            alter_stmt += f" DEFAULT {new_default}"
        if new_is_identity:
            alter_stmt += " GENERATED BY DEFAULT AS IDENTITY"

        connection.execute(text(alter_stmt))
        if new_is_identity:
            sync_sequence(connection, table_name, column_name)



def remove_column_fk(connection, table_name, column_name):
    """
    Drop any foreign key constraints that reference the given column.
    Safe to run even if no FKs exist.
    """
    query = text("""
        SELECT
            tc.constraint_name,
            tc.table_name
        FROM
            information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
        WHERE
            tc.constraint_type = 'FOREIGN KEY'
            AND kcu.column_name = :column
            AND kcu.table_name = :table
    """)
    fks = connection.execute(query, {"table": table_name, "column": column_name}).fetchall()

    for fk in fks:
        print(f'Dropping FK {fk.constraint_name} on table {fk.table_name} for column {column_name}')
        connection.execute(text(f'ALTER TABLE "{fk.table_name}" DROP CONSTRAINT "{fk.constraint_name}"'))


# ---------- Schema helpers ----------

def remove_old_tables(connection, metadata):
    inspector = inspect(connection)
    existing_tables = set(inspector.get_table_names())
    model_tables = set(metadata.tables.keys())

    tables_to_drop = existing_tables - model_tables
    for table in tables_to_drop:
        print(f"Dropping old table: {table}")
        try:
            connection.execute(text(f'DROP TABLE IF EXISTS "{table}" CASCADE'))
        except SQLAlchemyError as e:
            print(f"Error dropping {table}: {e}")


def remove_old_columns(connection, metadata):
    inspector = inspect(connection)

    for table_name, table_obj in metadata.tables.items():
        existing_columns = {col["name"] for col in inspector.get_columns(table_name)}
        model_columns = set(table_obj.columns.keys())

        columns_to_drop = existing_columns - model_columns
        for column_name in columns_to_drop:
            alter_stmt = f'ALTER TABLE "{table_name}" DROP COLUMN "{column_name}" CASCADE'
            print(f"Dropping column: {alter_stmt}")
            try:
                connection.execute(text(alter_stmt))
            except SQLAlchemyError as e:
                print(f"Error dropping column {column_name} in {table_name}: {e}")


def sync_primary_keys(connection, metadata):
    inspector = inspect(connection)

    for table in metadata.tables.values():
        table_name = table.name
        schema = table.schema

        fq_table = (
            f'"{schema}"."{table_name}"'
            if schema else f'"{table_name}"'
        )

        existing_pk = inspector.get_pk_constraint(table_name, schema=schema)
        existing_pk_columns = (
            tuple(existing_pk["constrained_columns"])
            if existing_pk and existing_pk.get("constrained_columns")
            else tuple()
        )

        model_pk_columns = tuple(col.name for col in table.primary_key.columns)

        if existing_pk_columns == model_pk_columns:
            continue

        try:
            # Drop existing PK if present
            if existing_pk_columns and existing_pk.get("name"):
                connection.execute(
                    text(
                        f'ALTER TABLE {fq_table} '
                        f'DROP CONSTRAINT "{existing_pk["name"]}"'
                    )
                )

            # Add new PK if defined in model
            if model_pk_columns:
                pk_cols = ", ".join(f'"{c}"' for c in model_pk_columns)
                connection.execute(
                    text(
                        f'ALTER TABLE {fq_table} '
                        f'ADD PRIMARY KEY ({pk_cols})'
                    )
                )

            print(f"PK synced for table {fq_table}")

        except SQLAlchemyError as e:
            print(f"Error syncing PK for {fq_table}: {e}")


def sync_indexes(connection, metadata):
    inspector = inspect(connection)

    for table in metadata.tables.values():
        table_name = table.name
        schema = table.schema

        fq_table = (
            f'"{schema}"."{table_name}"'
            if schema else f'"{table_name}"'
        )

        # Existing indexes in DB (exclude PK & unique constraints)
        existing_indexes = {
            idx["name"]: idx
            for idx in inspector.get_indexes(table_name, schema=schema)
        }

        for idx in table.indexes:
            name = idx.name
            model_cols = tuple(col.name for col in idx.columns)
            model_unique = idx.unique

            existing = existing_indexes.get(name)

            needs_rebuild = (
                not existing
                or tuple(existing["column_names"]) != model_cols
                or bool(existing.get("unique")) != bool(model_unique)
            )

            if not needs_rebuild:
                continue

            print(f"Rebuilding index {name} on {fq_table}")

            # Drop existing index if present
            if existing:
                fq_index = (
                    f'"{schema}"."{name}"'
                    if schema else f'"{name}"'
                )
                connection.execute(
                    text(f'DROP INDEX {fq_index}')
                )

            # Build CREATE INDEX statement
            cols_sql = ", ".join(f'"{c}"' for c in model_cols)
            unique_sql = "UNIQUE " if model_unique else ""

            connection.execute(
                text(
                    f'CREATE {unique_sql}INDEX "{name}" '
                    f'ON {fq_table} ({cols_sql})'
                )
            )




def sync_foreign_keys(connection, metadata):
    inspector = inspect(connection)

    for table in metadata.tables.values():
        table_name = table.name
        schema = table.schema

        fq_table = (
            f'"{schema}"."{table_name}"'
            if schema else f'"{table_name}"'
        )

        # ---- EXISTING FKs (from DB) ----
        existing = {}
        for fk in inspector.get_foreign_keys(table_name, schema=schema):
            key = (
                tuple(fk["constrained_columns"]),
                fk["referred_schema"],
                fk["referred_table"],
                tuple(fk["referred_columns"]),
            )
            existing[key] = fk["name"]

        # ---- MODEL FKs ----
        model = {}
        for constraint in table.constraints:
            if isinstance(constraint, ForeignKeyConstraint):
                local_cols = tuple(constraint.column_keys)

                elem = constraint.elements[0]
                ref_table = elem.column.table
                ref_schema = ref_table.schema
                ref_table_name = ref_table.name
                ref_cols = tuple(e.column.name for e in constraint.elements)

                key = (
                    local_cols,
                    ref_schema,
                    ref_table_name,
                    ref_cols,
                )
                model[key] = constraint.name

        # ---- DROP missing or changed FKs ----
        for key, fk_name in existing.items():
            if key not in model:
                connection.execute(
                    text(
                        f'ALTER TABLE {fq_table} '
                        f'DROP CONSTRAINT "{fk_name}"'
                    )
                )

        # ---- ADD missing FKs ----
        for key, fk_name in model.items():
            if key not in existing:
                local_cols, ref_schema, ref_table, ref_cols = key

                local_sql = ", ".join(f'"{c}"' for c in local_cols)
                ref_sql = ", ".join(f'"{c}"' for c in ref_cols)

                fq_ref = (
                    f'"{ref_schema}"."{ref_table}"'
                    if ref_schema else f'"{ref_table}"'
                )

                connection.execute(
                    text(
                        f'ALTER TABLE {fq_table} '
                        f'ADD CONSTRAINT "{fk_name}" '
                        f'FOREIGN KEY ({local_sql}) '
                        f'REFERENCES {fq_ref} ({ref_sql})'
                    )
                )


def sync_unique_constraints(connection, metadata):
    inspector = inspect(connection)

    for table in metadata.tables.values():
        table_name = table.name
        schema = table.schema

        fq_table = (
            f'"{schema}"."{table_name}"'
            if schema else f'"{table_name}"'
        )

        existing = {
            c["name"]: tuple(c["column_names"])
            for c in inspector.get_unique_constraints(table_name, schema=schema)
            if c["name"]
        }

        model = {
            c.name: tuple(c.columns.keys())
            for c in table.constraints
            if isinstance(c, UniqueConstraint) and c.name
        }

        # 1️⃣ Drop constraints that are missing OR different
        for name, cols in existing.items():
            if name not in model or model[name] != cols:
                connection.execute(
                    text(
                        f'ALTER TABLE {fq_table} DROP CONSTRAINT "{name}"'
                    )
                )

        # 2️⃣ Add constraints that are missing OR replaced
        for name, cols in model.items():
            if name not in existing or existing.get(name) != cols:
                col_str = ", ".join(f'"{c}"' for c in cols)
                connection.execute(
                    text(
                        f'ALTER TABLE {fq_table} '
                        f'ADD CONSTRAINT "{name}" UNIQUE ({col_str})'
                    )
                )

def sync_check_constraints(connection, metadata):
    inspector = inspect(connection)

    for table in metadata.tables.values():
        table_name = table.name
        schema = table.schema

        fq_table = (
            f'"{schema}"."{table_name}"'
            if schema else f'"{table_name}"'
        )

        # Existing checks in DB
        existing = {
            c["name"]: c["sqltext"]
            for c in inspector.get_check_constraints(table_name, schema=schema)
            if c.get("name")
        }

        # Model checks
        model = {
            c.name: str(c.sqltext)
            for c in table.constraints
            if getattr(c, "sqltext", None) is not None and c.name
        }

        # Drop checks that are missing or changed
        for name, sqltext in existing.items():
            if name not in model or model[name] != sqltext:
                print(f"Dropping check constraint {name} on {fq_table}")
                connection.execute(
                    text(
                        f'ALTER TABLE {fq_table} DROP CONSTRAINT "{name}"'
                    )
                )

        # Add checks that are missing or replaced
        for name, sqltext in model.items():
            if name not in existing or existing.get(name) != sqltext:
                print(f"Adding check constraint {name} on {fq_table}")
                connection.execute(
                    text(
                        f'ALTER TABLE {fq_table} '
                        f'ADD CONSTRAINT "{name}" CHECK ({sqltext})'
                    )
                )

def sync_sequence(connection, table, column):
    """
    Ensure the sequence for a SERIAL or IDENTITY column exists and
    is owned by the column. PostgreSQL-safe and idempotent.
    """

    table_name = table.name
    schema = table.schema

    fq_table = (
        f'"{schema}"."{table_name}"'
        if schema else f'"{table_name}"'
    )

    # 1️⃣ Check identity status
    identity = connection.execute(
        text(
            """
            SELECT is_identity
            FROM information_schema.columns
            WHERE table_name = :table
              AND column_name = :column
              AND (:schema IS NULL OR table_schema = :schema)
            """
        ),
        {
            "table": table_name,
            "column": column,
            "schema": schema,
        },
    ).scalar()

    # 2️⃣ Find sequence (works for SERIAL)
    seq_name = connection.execute(
        text(
            """
            SELECT pg_get_serial_sequence(
                :fq_table, :column
            )
            """
        ),
        {
            "fq_table": f"{schema}.{table_name}" if schema else table_name,
            "column": column,
        },
    ).scalar()

    # 3️⃣ If neither IDENTITY nor SERIAL → create identity
    if identity != "YES" and seq_name is None:
        print(f"Creating identity for {fq_table}.{column}")

        # Drop default if present (SERIAL leftovers)
        connection.execute(
            text(
                f'ALTER TABLE {fq_table} '
                f'ALTER COLUMN "{column}" DROP DEFAULT'
            )
        )

        connection.execute(
            text(
                f'ALTER TABLE {fq_table} '
                f'ALTER COLUMN "{column}" '
                f'ADD GENERATED BY DEFAULT AS IDENTITY'
            )
        )
        return

    # 4️⃣ Ensure sequence ownership (SERIAL only)
    if seq_name:
        # seq_name is already schema-qualified, DO NOT quote it
        connection.execute(
            text(
                f'ALTER SEQUENCE {seq_name} '
                f'OWNED BY {fq_table}."{column}"'
            )
        )


# ---------- Main schema sync ----------

def sync_schema(engine, metadata):
    """Ensure schema consistency."""
    with engine.begin() as conn:
        # Optional: defer FK checks during transaction
        conn.execute(text("SET CONSTRAINTS ALL DEFERRED"))

        # Create tables if not exists
        for table in metadata.tables.values():
            table.create(conn, checkfirst=True)

        remove_old_tables(conn, metadata)
        remove_old_columns(conn, metadata)

        # Sync columns
        for table_name, table_obj in metadata.tables.items():
            for column_name, column_obj in table_obj.columns.items():
                modify_column_if_needed(conn, table_name, column_name, column_obj)

        # Sync PKs, indexes, FKs, unique and check constraints
        sync_primary_keys(conn, metadata)
        sync_indexes(conn, metadata)
        sync_foreign_keys(conn, metadata)
        sync_unique_constraints(conn, metadata)
        sync_check_constraints(conn, metadata)

