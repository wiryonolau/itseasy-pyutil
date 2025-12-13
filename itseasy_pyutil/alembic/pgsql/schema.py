from sqlalchemy import (
    BigInteger,
    ForeignKeyConstraint,
    Integer,
    MetaData,
    Table,
    UniqueConstraint,
    inspect,
    text,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.sql.schema import DefaultClause, ScalarElementColumnDefault


# ---------- Normalize default ----------
def normalize_default_value(default_value):
    if default_value is None:
        return None
    if isinstance(default_value, ScalarElementColumnDefault):
        default_value = default_value.arg
    if isinstance(default_value, DefaultClause):
        default_value = default_value.arg
    if isinstance(default_value, TextClause):
        return str(default_value)
    if isinstance(default_value, bool):
        return "TRUE" if default_value else "FALSE"
    if isinstance(default_value, str):
        return f"'{default_value}'"
    return str(default_value)


# ---------- Normalize SQLAlchemy types ----------
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
        seq_res = connection.execute(
            seq_query, {"table": table_name, "column": column_name}
        ).fetchone()
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
            is_identity,
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

    # ---------- Normalize types ----------
    existing_type_norm = normalize_sa_type(str(existing_type).split("(")[0])
    new_type_norm = normalize_sa_type(str(new_type).split("(")[0])

    # ---------- Normalize nullable ----------
    existing_nullable = bool(existing_nullable)
    new_nullable = bool(new_nullable)

    # ---------- Normalize identity ----------
    existing_is_identity = bool(existing_is_identity)
    new_is_identity = str(new_is_identity).lower() in (
        "true",
        "yes",
        "auto",
        "identity",
    )

    # ---------- Normalize defaults ----------
    existing_default = normalize_default_value(existing_default)
    new_default = normalize_default_value(new_default)

    if existing_default is not None:
        existing_default = (
            str(existing_default).split("::")[0].strip("'\"").lower()
        )
    if new_default is not None:
        new_default = str(new_default).split("::")[0].strip("'\"").lower()

    # ---------- Compare ----------
    if existing_nullable != new_nullable:
        return True
    if existing_type_norm != new_type_norm:
        return True
    if existing_default != new_default:
        return True
    if new_length is not None and existing_length != new_length:
        return True
    if new_precision is not None and existing_precision != new_precision:
        return True
    if new_scale is not None and existing_scale != new_scale:
        return True
    if new_type_norm == "enum" and existing_col_type != new_col_type:
        return True
    if new_is_identity != existing_is_identity:
        return True

    return False


# ---------- Remove column FK ----------
def remove_column_fk(connection, table_name, column_name):
    query = text(
        """
        SELECT tc.constraint_name, tc.table_name
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND kcu.column_name = :column
          AND kcu.table_name = :table
        """
    )
    fks = connection.execute(
        query, {"table": table_name, "column": column_name}
    ).fetchall()
    for fk in fks:
        print(
            f"Dropping FK {fk.constraint_name} on table {fk.table_name} for column {column_name}"
        )
        connection.execute(
            text(
                f'ALTER TABLE "{fk.table_name}" DROP CONSTRAINT "{fk.constraint_name}"'
            )
        )


# ---------- Sync sequence ----------
def sync_sequence(connection, table_name, column):
    """
    Ensure the sequence for a SERIAL or IDENTITY column exists and
    is owned by the column. PostgreSQL-safe and idempotent.
    Schema handling is removed.
    """

    # 1️⃣ Check if the column is identity
    identity = connection.execute(
        text(
            """
            SELECT is_identity
            FROM information_schema.columns
            WHERE table_name = :table
              AND column_name = :column
            """
        ),
        {"table": table_name, "column": column},
    ).scalar()

    # 2️⃣ Get the sequence associated with this column (SERIAL)
    seq_name = connection.execute(
        text(
            """
            SELECT pg_get_serial_sequence(:table, :column)
            """
        ),
        {"table": table_name, "column": column},
    ).scalar()

    # 3️⃣ If column is not identity and sequence doesn't exist → create identity
    if identity != "YES" and not seq_name:
        print(f"Creating identity for {fq_table}.{column}")
        connection.execute(
            text(f'ALTER TABLE {fq_table} ALTER COLUMN "{column}" DROP DEFAULT')
        )
        connection.execute(
            text(
                f'ALTER TABLE {fq_table} ALTER COLUMN "{column}" '
                f"ADD GENERATED BY DEFAULT AS IDENTITY"
            )
        )
        return

    # 4️⃣ Only set OWNED BY if sequence exists and column is not identity
    if seq_name and identity != "YES":
        connection.execute(
            text(f'ALTER SEQUENCE {seq_name} OWNED BY {fq_table}."{column}"')
        )


# ---------- Modify column ----------
def modify_column_if_needed(connection, table_name, column_name, column_obj):
    """
    Create or modify a column in PostgreSQL.
    Handles:
      - Column creation if missing
      - Enum types creation with default table_column_enum naming
      - Adding missing enum values
      - Type changes
      - Nullable changes
      - Defaults (booleans, strings, numerics, enums)
      - Identity/autoincrement (only for integer primary keys)
      - Idempotent
    """
    existing_info = column_info(connection, table_name, column_name)

    # Basic column info
    new_type = type(column_obj.type).__name__
    new_nullable = "yes" if column_obj.nullable else "no"
    new_default = normalize_default_value(
        column_obj.default or column_obj.server_default
    )
    new_length = getattr(column_obj.type, "length", None)
    new_precision = getattr(column_obj.type, "precision", None)
    new_scale = getattr(column_obj.type, "scale", None)
    new_is_identity = getattr(column_obj, "autoincrement", False)

    # Enum handling
    enum_class = None
    enum_type_name = None
    if new_type.lower() == "enum":
        enum_class = getattr(column_obj.type, "enum_class")
        enum_type_name = f"{table_name}_{column_name}_enum"

    new_col_type = enum_type_name if enum_class else normalize_sa_type(new_type)
    new_erratas = getattr(column_obj, "extra", None)

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

    # COLUMN EXISTS: update
    if existing_info:
        if column_has_update(existing_info, new_info):
            print(f"Updating column {table_name}.{column_name}")
            remove_column_fk(connection, table_name, column_name)

            # Add missing enum values
            if enum_class:
                existing_vals = connection.execute(
                    text(
                        "SELECT enumlabel FROM pg_enum "
                        "JOIN pg_type ON pg_enum.enumtypid = pg_type.oid "
                        "WHERE typname=:name ORDER BY enumsortorder"
                    ),
                    {"name": enum_type_name},
                ).fetchall()
                existing_vals = [v[0] for v in existing_vals]
                for val in enum_class:
                    if val.value not in existing_vals:
                        connection.execute(
                            text(
                                f"ALTER TYPE \"{enum_type_name}\" ADD VALUE IF NOT EXISTS '{val.value}'"
                            )
                        )

            # Drop identity if type changed
            if existing_info[8] and existing_info[1] != new_col_type:
                connection.execute(
                    text(
                        f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" DROP IDENTITY IF EXISTS'
                    )
                )

            # ALTER TYPE
            col_type_sql = new_col_type
            if new_length:
                col_type_sql += f"({new_length})"
            elif new_precision and new_scale:
                col_type_sql += f"({new_precision},{new_scale})"

            connection.execute(
                text(
                    f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" TYPE {col_type_sql} USING "{column_name}"::{col_type_sql}'
                )
            )

            # ALTER nullability
            if existing_info[2] != new_nullable:
                action = (
                    "DROP NOT NULL" if new_nullable == "yes" else "SET NOT NULL"
                )
                connection.execute(
                    text(
                        f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" {action}'
                    )
                )

            # ALTER default
            if existing_info[3] != new_default:
                if new_default is None:
                    connection.execute(
                        text(
                            f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" DROP DEFAULT'
                        )
                    )
                else:
                    default_val = (
                        new_default
                        if isinstance(new_default, str)
                        else str(new_default)
                    )
                    connection.execute(
                        text(
                            f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" SET DEFAULT {default_val}'
                        )
                    )

            # ALTER identity (only integer primary keys)
            if (
                new_is_identity
                and isinstance(column_obj.type, (Integer, BigInteger))
                and getattr(column_obj, "primary_key", False)
                and not column_obj.nullable
            ):
                if not existing_info[8]:
                    connection.execute(
                        text(
                            f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" ADD GENERATED BY DEFAULT AS IDENTITY'
                        )
                    )
                    sync_sequence(connection, table_name, column_name)
            elif existing_info[8] and not new_is_identity:
                connection.execute(
                    text(
                        f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" DROP IDENTITY IF EXISTS'
                    )
                )

        else:
            print(f"Skipping {table_name}.{column_name}: no changes detected.")
            return

    # COLUMN DOES NOT EXIST: create
    else:
        print(
            f"Column {column_name} does not exist in {table_name}. Creating column."
        )
        remove_column_fk(connection, table_name, column_name)

        # CREATE ENUM TYPE if needed
        if enum_class:
            escaped_values = [v.value.replace("'", "''") for v in enum_class]
            enum_values_sql = ", ".join(f"'{v}'" for v in escaped_values)
            sql = f"""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = '{enum_type_name}') THEN
                    CREATE TYPE {enum_type_name} AS ENUM ({enum_values_sql});
                END IF;
            END
            $$;
            """
            connection.execute(text(sql))

        # Column type
        col_type_sql = new_col_type
        if new_length:
            col_type_sql += f"({new_length})"
        elif new_precision and new_scale:
            col_type_sql += f"({new_precision},{new_scale})"

        # Build ADD COLUMN statement
        alter_stmt = f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {col_type_sql}'
        if not column_obj.nullable:
            alter_stmt += " NOT NULL"
        if new_default is not None:
            default_val = (
                new_default
                if isinstance(new_default, str)
                else str(new_default)
            )
            alter_stmt += f" DEFAULT {default_val}"

        # Only add identity for integer primary keys
        if (
            new_is_identity
            and isinstance(column_obj.type, (Integer, BigInteger))
            and getattr(column_obj, "primary_key", False)
            and not column_obj.nullable
        ):
            alter_stmt += " GENERATED BY DEFAULT AS IDENTITY"

        connection.execute(text(alter_stmt))

        if (
            new_is_identity
            and isinstance(column_obj.type, (Integer, BigInteger))
            and getattr(column_obj, "primary_key", False)
            and not column_obj.nullable
        ):
            sync_sequence(connection, table_name, column_name)


# ---------- Remove old tables/columns ----------
def remove_old_tables(connection, metadata):
    inspector = inspect(connection)
    existing_tables = set(inspector.get_table_names())
    model_tables = set(metadata.tables.keys())
    for table in existing_tables - model_tables:
        print(f"Dropping old table: {table}")
        try:
            connection.execute(text(f'DROP TABLE IF EXISTS "{table}" CASCADE'))
        except SQLAlchemyError as e:
            print(f"Error dropping {table}: {e}")


def remove_old_columns(connection, metadata):
    inspector = inspect(connection)
    for table_name, table_obj in metadata.tables.items():
        existing_columns = {
            col["name"] for col in inspector.get_columns(table_name)
        }
        model_columns = set(table_obj.columns.keys())
        for column_name in existing_columns - model_columns:
            print(f"Dropping column {column_name} in table {table_name}")
            try:
                connection.execute(
                    text(
                        f'ALTER TABLE "{table_name}" DROP COLUMN "{column_name}" CASCADE'
                    )
                )
            except SQLAlchemyError as e:
                print(
                    f"Error dropping column {column_name} in {table_name}: {e}"
                )


# ---------- Sync PK, indexes, FKs, unique, check ----------
def sync_primary_keys(connection, metadata):
    inspector = inspect(connection)
    for table in metadata.tables.values():
        table_name = table.name

        existing_pk = inspector.get_pk_constraint(table_name)
        existing_pk_columns = (
            tuple(existing_pk["constrained_columns"]) if existing_pk else ()
        )
        model_pk_columns = tuple(col.name for col in table.primary_key.columns)
        if existing_pk_columns != model_pk_columns:
            if existing_pk and existing_pk.get("name"):
                connection.execute(
                    text(
                        f'ALTER TABLE {table_name} DROP CONSTRAINT "{existing_pk["name"]}"'
                    )
                )
            if model_pk_columns:
                cols_sql = ", ".join(f'"{c}"' for c in model_pk_columns)
                connection.execute(
                    text(
                        f"ALTER TABLE {table_name} ADD PRIMARY KEY ({cols_sql})"
                    )
                )


def sync_indexes(connection, metadata):
    inspector = inspect(connection)
    for table in metadata.tables.values():
        table_name = table.name

        existing_indexes = {
            idx["name"]: idx for idx in inspector.get_indexes(table_name)
        }
        for idx in table.indexes:
            name = idx.name
            model_cols = tuple(col.name for col in idx.columns)
            model_unique = idx.unique
            existing = existing_indexes.get(name)
            if (
                existing
                and tuple(existing["column_names"]) == model_cols
                and bool(existing.get("unique")) == bool(model_unique)
            ):
                continue
            if existing:
                connection.execute(text(f'DROP INDEX "{name}"'))
            cols_sql = ", ".join(f'"{c}"' for c in model_cols)
            unique_sql = "UNIQUE " if model_unique else ""
            connection.execute(
                text(
                    f'CREATE {unique_sql}INDEX "{name}" ON {table_name} ({cols_sql})'
                )
            )


def sync_foreign_keys(connection, metadata):
    inspector = inspect(connection)
    for table in metadata.tables.values():
        table_name = table.name

        existing = {
            tuple(fk["constrained_columns"]): fk
            for fk in inspector.get_foreign_keys(table_name)
        }
        model = {}
        for constraint in table.constraints:
            if isinstance(constraint, ForeignKeyConstraint):
                local_cols = tuple(constraint.column_keys)
                ref_table = constraint.elements[0].column.table.name
                ref_cols = tuple(e.column.name for e in constraint.elements)
                model[local_cols] = (ref_table, ref_cols, constraint.name)
        # Drop missing FKs
        for cols, fk in existing.items():
            if cols not in model:
                connection.execute(
                    text(
                        f'ALTER TABLE {table_name} DROP CONSTRAINT "{fk["name"]}"'
                    )
                )
        # Add missing FKs
        for cols, (ref_table, ref_cols, fk_name) in model.items():
            if cols not in existing:
                local_sql = ", ".join(f'"{c}"' for c in cols)
                ref_sql = ", ".join(f'"{c}"' for c in ref_cols)
                fq_ref = f'"{ref_table}"'
                connection.execute(
                    text(
                        f'ALTER TABLE {table_name} ADD CONSTRAINT "{fk_name}" FOREIGN KEY ({local_sql}) REFERENCES {fq_ref} ({ref_sql})'
                    )
                )


def sync_unique_constraints(connection, metadata):
    inspector = inspect(connection)
    for table in metadata.tables.values():
        table_name = table.name

        existing = {
            c["name"]: tuple(c["column_names"])
            for c in inspector.get_unique_constraints(table_name)
            if c.get("name")
        }
        model = {
            c.name: tuple(c.columns.keys())
            for c in table.constraints
            if isinstance(c, UniqueConstraint) and c.name
        }
        for name, cols in existing.items():
            if name not in model or model[name] != cols:
                connection.execute(
                    text(f'ALTER TABLE {table_name} DROP CONSTRAINT "{name}"')
                )
        for name, cols in model.items():
            if name not in existing or existing.get(name) != cols:
                col_str = ", ".join(f'"{c}"' for c in cols)
                connection.execute(
                    text(
                        f'ALTER TABLE {table_name} ADD CONSTRAINT "{name}" UNIQUE ({col_str})'
                    )
                )


def sync_check_constraints(connection, metadata):
    inspector = inspect(connection)
    for table in metadata.tables.values():
        table_name = table.name

        existing = {
            c["name"]: c["sqltext"]
            for c in inspector.get_check_constraints(table_name)
            if c.get("name")
        }
        model = {
            c.name: str(c.sqltext)
            for c in table.constraints
            if getattr(c, "sqltext", None) is not None and c.name
        }
        for name, sqltext in existing.items():
            if name not in model or model[name] != sqltext:
                connection.execute(
                    text(f'ALTER TABLE {table_name} DROP CONSTRAINT "{name}"')
                )
        for name, sqltext in model.items():
            if name not in existing or existing.get(name) != sqltext:
                connection.execute(
                    text(
                        f'ALTER TABLE {table_name} ADD CONSTRAINT "{name}" CHECK ({sqltext})'
                    )
                )


# ---------- Main schema sync ----------
def sync_schema(engine, metadata):
    with engine.begin() as conn:
        conn.execute(text("SET CONSTRAINTS ALL DEFERRED"))
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
