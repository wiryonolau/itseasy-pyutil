import re

from sqlalchemy import (
    BigInteger,
    DateTime,
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

from itseasy_pyutil.util import boolval


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
    if sa_type is None:
        return None

    # Detect timezone if DateTime
    if (
        hasattr(sa_type, "timezone")
        and sa_type.__class__.__name__.lower() == "datetime"
    ):
        if sa_type.timezone:
            return "timestamp with time zone"
        else:
            return "timestamp without time zone"

    t = str(sa_type).lower()
    if "(" in t:
        t = t.split("(")[0]

    mapping = {
        "integer": "integer",
        "smallinteger": "smallint",
        "biginteger": "bigint",
        "boolean": "boolean",
        "string": "character varying",
        "varchar": "character varying",
        "character varying": "character varying",
        "text": "text",
        "float": "double precision",
        "numeric": "numeric",
        "timestamp": "timestamp without time zone",
        "date": "date",
        "time": "time without time zone",
        "json": "jsonb",
        "jsonb": "jsonb",
        "largebinary": "bytea",
        "bytea": "bytea",
        "enum": "enum",
    }

    return mapping.get(t, t)


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
    # Encode timezone into string for new_type if DateTime
    existing_type_str = str(existing_type).lower()
    new_type_str = str(new_type).lower()

    # If you can detect timezone from the new_type string, append "with time zone" (or pass here if you already encoded)
    # Otherwise leave as-is for backward compatibility
    existing_type_norm = normalize_sa_type(existing_type_str)
    new_type_norm = normalize_sa_type(new_type_str)

    # ---------- Normalize nullable ----------
    existing_nullable = boolval(existing_nullable)
    new_nullable = boolval(new_nullable)

    # ---------- Normalize identity ----------
    existing_is_identity = boolval(existing_is_identity)
    new_is_identity = str(new_is_identity).lower() in (
        "true",
        "yes",
        "auto",
        "identity",
    )

    # ---------- Normalize defaults ----------
    existing_default = normalize_default_value(existing_default)
    new_default = normalize_default_value(new_default)

    # Normalize PostgreSQL defaults
    type_cast_regex = re.compile(r"::\s*[a-zA-Z_ ]+")
    if existing_default is not None:
        existing_default = (
            type_cast_regex.sub("", str(existing_default)).strip("'\"").lower()
        )
    if new_default is not None:
        new_default = (
            type_cast_regex.sub("", str(new_default)).strip("'\"").lower()
        )

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
    Sync SERIAL / IDENTITY for columns that are explicitly autoincrement
    in the SQLAlchemy model.
    """

    column_name = column.name

    # 1️⃣ Column must want autoincrement
    if not (column.primary_key or column.autoincrement is True):
        return

    # 2️⃣ Identity requires NOT NULL
    if column.nullable:
        return

    # 3️⃣ Only integer types
    if column.type.__class__.__name__.lower() not in (
        "smallinteger",
        "integer",
        "biginteger",
    ):
        return

    # 4️⃣ Inspect DB state
    row = connection.execute(
        text(
            """
            SELECT is_identity
            FROM information_schema.columns
            WHERE table_name = :table
              AND column_name = :column
            """
        ),
        {"table": table_name, "column": column_name},
    ).fetchone()

    if not row:
        return

    is_identity = row[0] == "YES"

    seq_name = connection.execute(
        text("SELECT pg_get_serial_sequence(:table, :column)"),
        {"table": table_name, "column": column_name},
    ).scalar()

    # 5️⃣ Already identity → nothing to do
    if is_identity:
        return

    # 6️⃣ SERIAL → migrate to identity
    if seq_name:
        print(f"[SEQ][MIGRATE] {table_name}.{column_name}")

        connection.execute(
            text(
                f'ALTER TABLE "{table_name}" '
                f'ALTER COLUMN "{column_name}" '
                f"ADD GENERATED BY DEFAULT AS IDENTITY"
            )
        )

        connection.execute(
            text(
                f"ALTER SEQUENCE {seq_name} "
                f'OWNED BY "{table_name}"."{column_name}"'
            )
        )


# ---------- Modify column ----------
def modify_column_if_needed(connection, table_name, column_name, column_obj):
    """
    Decide whether to create or update a column.
    """
    existing_info = column_info(connection, table_name, column_name)

    if existing_info:
        update_column_if_needed(
            connection, table_name, column_name, column_obj, existing_info
        )
    else:
        create_column_if_missing(
            connection, table_name, column_name, column_obj
        )


def compute_column_type(column_obj):
    """Return PostgreSQL type string including timezone if needed."""
    col_type_name = type(column_obj.type).__name__
    if col_type_name.lower() == "enum":
        return None  # will handle enum separately
    elif isinstance(column_obj.type, DateTime):
        return (
            "timestamp with time zone"
            if getattr(column_obj.type, "timezone", False)
            else "timestamp without time zone"
        )
    else:
        return normalize_sa_type(col_type_name)


def create_column_if_missing(connection, table_name, column_name, column_obj):
    """
    Create a new column in PostgreSQL, handling enums, identity, default, nullable.
    """
    new_col_type = compute_column_type(column_obj)
    enum_class = None
    enum_type_name = None

    # Enum handling
    if type(column_obj.type).__name__.lower() == "enum":
        enum_class = getattr(column_obj.type, "enum_class")
        enum_type_name = f"{table_name}_{column_name}_enum"
        new_col_type = enum_type_name

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

    # Build type SQL
    col_type_sql = new_col_type
    if getattr(column_obj.type, "length", None):
        col_type_sql += f"({column_obj.type.length})"
    elif getattr(column_obj.type, "precision", None) and getattr(
        column_obj.type, "scale", None
    ):
        col_type_sql += f"({column_obj.type.precision},{column_obj.type.scale})"

    # Build ADD COLUMN statement
    alter_stmt = (
        f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {col_type_sql}'
    )
    if not column_obj.nullable:
        alter_stmt += " NOT NULL"

    default_val = normalize_default_value(
        column_obj.default or column_obj.server_default
    )
    if default_val is not None:
        alter_stmt += f" DEFAULT {default_val}"

    # Identity for integer primary keys
    if (
        getattr(column_obj, "autoincrement", False)
        and isinstance(column_obj.type, (Integer, BigInteger))
        and getattr(column_obj, "primary_key", False)
        and not column_obj.nullable
    ):
        alter_stmt += " GENERATED BY DEFAULT AS IDENTITY"

    connection.execute(text(alter_stmt))

    # Sync sequence check
    sync_sequence(connection, table_name, column_obj)


def update_column_if_needed(
    connection, table_name, column_name, column_obj, existing_info
):
    """
    Update an existing column if type, default, nullable, identity, or enum values differ.
    Only removes FKs if column changes.
    """
    new_col_type = compute_column_type(column_obj)
    new_type = type(column_obj.type).__name__
    new_nullable = "yes" if column_obj.nullable else "no"
    new_default = normalize_default_value(
        column_obj.default or column_obj.server_default
    )
    new_length = getattr(column_obj.type, "length", None)
    new_precision = getattr(column_obj.type, "precision", None)
    new_scale = getattr(column_obj.type, "scale", None)
    new_is_identity = (
        getattr(column_obj, "autoincrement", False)
        and isinstance(column_obj.type, (Integer, BigInteger))
        and getattr(column_obj, "primary_key", False)
        and not column_obj.nullable
    )

    enum_class = None
    if new_type.lower() == "enum":
        enum_class = getattr(column_obj.type, "enum_class")
        new_col_type = f"{table_name}_{column_name}_enum"

    new_info = (
        new_type,
        new_col_type,
        new_nullable,
        new_default,
        new_length,
        new_precision,
        new_scale,
        getattr(column_obj, "extra", None),
        new_is_identity,
    )

    if column_has_update(existing_info, new_info):
        print(f"Updating column {table_name}.{column_name}")

        # Remove FKs only when there is an actual change
        remove_column_fk(connection, table_name, column_name)

        # Enums: add missing values
        if enum_class:
            stmt = text(
                "SELECT enumlabel FROM pg_enum "
                "JOIN pg_type ON pg_enum.enumtypid = pg_type.oid "
                "WHERE typname=:name ORDER BY enumsortorder"
            )
            print("SQL:", stmt, "PARAMS:", {"name": new_col_type})

            existing_vals = connection.execute(
                stmt,
                {"name": new_col_type},
            ).fetchall()
            existing_vals = [v[0] for v in existing_vals]

            for val in enum_class:
                if val.value not in existing_vals:
                    sql = f"ALTER TYPE \"{new_col_type}\" ADD VALUE IF NOT EXISTS '{val.value}'"
                    print("SQL:", sql)
                    connection.execute(text(sql))

        # ALTER TYPE
        col_type_sql = new_col_type
        if new_length:
            col_type_sql += f"({new_length})"
        elif new_precision and new_scale:
            col_type_sql += f"({new_precision},{new_scale})"

        sql = (
            f'ALTER TABLE "{table_name}" '
            f'ALTER COLUMN "{column_name}" '
            f'TYPE {col_type_sql} USING "{column_name}"::{col_type_sql}'
        )
        print("SQL:", sql)
        connection.execute(text(sql))

        # ALTER nullable
        if existing_info[2] != new_nullable:
            action = (
                "DROP NOT NULL" if new_nullable == "yes" else "SET NOT NULL"
            )
            sql = f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" {action}'
            print("SQL:", sql)
            connection.execute(text(sql))

        # ALTER default
        if existing_info[3] != new_default:
            if new_default is None:
                sql = f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" DROP DEFAULT'
                print("SQL:", sql)
                connection.execute(text(sql))
            else:
                sql = (
                    f'ALTER TABLE "{table_name}" '
                    f'ALTER COLUMN "{column_name}" SET DEFAULT {new_default}'
                )
                print("SQL:", sql)
                connection.execute(text(sql))

        # ALTER identity
        if new_is_identity and not existing_info[8]:
            sql = (
                f'ALTER TABLE "{table_name}" '
                f'ALTER COLUMN "{column_name}" ADD GENERATED BY DEFAULT AS IDENTITY'
            )
            print("SQL:", sql)
            connection.execute(text(sql))
            sync_sequence(connection, table_name, column_obj)

        elif existing_info[8] and not new_is_identity:
            sql = (
                f'ALTER TABLE "{table_name}" '
                f'ALTER COLUMN "{column_name}" DROP IDENTITY IF EXISTS'
            )
            print("SQL:", sql)
            connection.execute(text(sql))
    else:
        print(f"Skipping {table_name}.{column_name}: no changes detected.")


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

        existing = inspector.get_pk_constraint(table_name) or {}
        existing_cols = tuple(existing.get("constrained_columns") or ())
        existing_name = existing.get("name")

        model_cols = tuple(col.name for col in table.primary_key.columns)

        if existing_cols == model_cols:
            print(f"[PK][NOOP] {table_name}")
            continue

        if existing_cols and existing_name:
            sql = (
                f'ALTER TABLE "{table_name}" DROP CONSTRAINT "{existing_name}"'
            )
            print(f"[PK][DROP] {sql}")
            connection.execute(text(sql))

        if model_cols:
            cols_sql = ", ".join(f'"{c}"' for c in model_cols)
            sql = f'ALTER TABLE "{table_name}" ADD PRIMARY KEY ({cols_sql})'
            print(f"[PK][ADD] {sql}")
            connection.execute(text(sql))


def sync_indexes(connection, metadata):
    inspector = inspect(connection)

    for table in metadata.tables.values():
        table_name = table.name

        # 1️⃣ Explicit Index() objects ONLY (authority)
        model_indexes = {
            idx.name: {
                "cols": tuple(col.name for col in idx.columns),
                "unique": boolval(idx.unique),
                "where": (
                    str(idx.dialect_options["postgresql"].get("where"))
                    if idx.dialect_options["postgresql"].get("where")
                    is not None
                    else None
                ),
            }
            for idx in table.indexes
            if idx.name
        }

        # 2️⃣ Read existing DB index predicates (Postgres only)
        predicate_sql = text(
            """
            SELECT
                i.relname AS index_name,
                pg_get_expr(ix.indpred, ix.indrelid) AS predicate
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            WHERE t.relname = :table
              AND ix.indpred IS NOT NULL
        """
        )

        db_predicates = {
            row.index_name: row.predicate
            for row in connection.execute(predicate_sql, {"table": table_name})
        }

        # 3️⃣ Existing DB indexes (including predicate)
        db_indexes = {
            idx["name"]: {
                "cols": tuple(idx["column_names"]),
                "unique": boolval(idx.get("unique")),
                "where": db_predicates.get(idx["name"]),
            }
            for idx in inspector.get_indexes(table_name)
            if idx.get("name")
        }

        changed = False

        # 4️⃣ Drop / recreate ONLY model-managed indexes
        for name, m in model_indexes.items():
            if name in db_indexes and db_indexes[name] == m:
                continue

            changed = True

            if name in db_indexes:
                # 🚫 Safety: never drop UNIQUE unless model explicitly owns it
                if db_indexes[name]["unique"] and not m["unique"]:
                    print(f"[INDEX][SKIP][UNIQUE] {table_name}.{name}")
                    continue

                sql = f'DROP INDEX "{name}"'
                print(f"[INDEX][DROP] {sql}")
                connection.execute(text(sql))

            cols_sql = ", ".join(f'"{c}"' for c in m["cols"])
            unique_sql = "UNIQUE " if m["unique"] else ""
            where_sql = f" WHERE {m['where']}" if m["where"] else ""

            sql = (
                f'CREATE {unique_sql}INDEX "{name}" '
                f'ON "{table_name}" ({cols_sql})'
                f"{where_sql}"
            )

            print(f"[INDEX][CREATE] {sql}")
            connection.execute(text(sql))

        if not changed:
            print(f"[INDEX][NOOP] {table_name}")


def get_existing_fks(connection, table_name, schema="public"):
    """
    Return dict mapping constrained columns tuple -> FK constraint name.
    Works reliably for lower-case table/column names in PostgreSQL.
    """
    sql = text(
        """
        SELECT
            con.conname AS constraint_name,
            array_agg(att.attname ORDER BY att.attnum) AS constrained_columns
        FROM
            pg_constraint con
            JOIN pg_class rel ON rel.oid = con.conrelid
            JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
            JOIN unnest(con.conkey) AS attnum(attnum) ON TRUE
            JOIN pg_attribute att
              ON att.attrelid = rel.oid AND att.attnum = attnum.attnum
        WHERE
            con.contype = 'f'
            AND nsp.nspname = :schema
            AND rel.relname = :table
        GROUP BY con.conname
        """
    )
    result = (
        connection.execute(sql, {"schema": schema, "table": table_name})
        .mappings()
        .all()
    )
    fks = {}
    for row in result:
        fks[tuple(row["constrained_columns"])] = row["constraint_name"]
    return fks


def sync_foreign_keys(connection, metadata, schema="public"):
    """
    Synchronize foreign keys between SQLAlchemy metadata and PostgreSQL database.
    """
    for table in metadata.tables.values():
        table_name = table.name

        # Fetch existing FKs
        existing_fks_map = get_existing_fks(
            connection, table_name, schema=schema
        )
        print(f"[FK][EXISTING] {table_name}: {existing_fks_map}")

        # Extract model FKs
        model_fks_map = {}
        for c in table.constraints:
            if isinstance(c, ForeignKeyConstraint):
                cols = tuple(c.column_keys)
                ref_table = c.elements[0].column.table.name
                ref_cols = tuple(e.column.name for e in c.elements)
                model_fks_map[cols] = (ref_table, ref_cols, c.name)

        # DROP FKs not in metadata
        for cols, fk_name in existing_fks_map.items():
            if cols not in model_fks_map:
                sql = f'ALTER TABLE "{schema}"."{table_name}" DROP CONSTRAINT "{fk_name}"'
                print(f"[FK][DROP] {sql}")
                connection.execute(text(sql))

        # ADD FKs missing in database
        for cols, (ref_table, ref_cols, name) in model_fks_map.items():
            if cols not in existing_fks_map:
                local_cols = ", ".join(f'"{c}"' for c in cols)
                remote_cols = ", ".join(f'"{c}"' for c in ref_cols)
                sql = (
                    f'ALTER TABLE "{schema}"."{table_name}" '
                    f'ADD CONSTRAINT "{name}" FOREIGN KEY ({local_cols}) '
                    f'REFERENCES "{schema}"."{ref_table}" ({remote_cols})'
                )
                print(f"[FK][ADD] {sql}")
                connection.execute(text(sql))

        if not existing_fks_map and not model_fks_map:
            print(f"[FK][NOOP] {table_name}")


def sync_unique_constraints(connection, metadata):
    # --- sanity: ensure PG 15+
    version = int(connection.execute(text("SHOW server_version_num")).scalar())
    if version < 150000:
        raise RuntimeError("sync_unique_constraints requires PostgreSQL >= 15")

    for table in metadata.tables.values():
        table_name = table.name

        # --- DB unique constraints (portable detection)
        db_uniques = {
            row["conname"]: {
                "columns": tuple(row["columns"]),
                "nulls_not_distinct": "NULLS NOT DISTINCT" in row["definition"],
            }
            for row in connection.execute(
                text(
                    """
                    SELECT
                        c.conname,
                        pg_get_constraintdef(c.oid) AS definition,
                        array_agg(a.attname ORDER BY u.ordinality) AS columns
                    FROM pg_constraint c
                    JOIN unnest(c.conkey) WITH ORDINALITY AS u(attnum, ordinality)
                        ON TRUE
                    JOIN pg_attribute a
                        ON a.attnum = u.attnum
                       AND a.attrelid = c.conrelid
                    WHERE c.contype = 'u'
                      AND c.conrelid = to_regclass(:table)
                    GROUP BY c.conname, c.oid
                """
                ),
                {"table": table_name},
            ).mappings()
        }

        # --- model unique constraints
        model_uniques = {}
        for c in table.constraints:
            if isinstance(c, UniqueConstraint) and c.name:
                model_uniques[c.name] = {
                    "columns": tuple(col.name for col in c.columns),
                    "nulls_not_distinct": c.dialect_options["postgresql"].get(
                        "nulls_not_distinct", False
                    ),
                }

        # --- add missing constraints (safe)
        for name, model in model_uniques.items():
            if name in db_uniques:
                continue

            cols = ", ".join(f'"{c}"' for c in model["columns"])
            if model["nulls_not_distinct"]:
                sql = (
                    f'ALTER TABLE "{table_name}" '
                    f'ADD CONSTRAINT "{name}" '
                    f"UNIQUE NULLS NOT DISTINCT ({cols})"
                )
            else:
                sql = (
                    f'ALTER TABLE "{table_name}" '
                    f'ADD CONSTRAINT "{name}" UNIQUE ({cols})'
                )

            print(f"[UNIQUE][ADD] {table_name}.{name}")
            connection.execute(text(sql))

        # --- replace only if semantics differ (atomic)
        for name, db in db_uniques.items():
            model = model_uniques.get(name)
            if not model:
                continue

            if (
                model["columns"] == db["columns"]
                and model["nulls_not_distinct"] == db["nulls_not_distinct"]
            ):
                continue

            tmp = f"{name}__tmp"
            cols = ", ".join(f'"{c}"' for c in model["columns"])

            if model["nulls_not_distinct"]:
                create = (
                    f'ALTER TABLE "{table_name}" '
                    f'ADD CONSTRAINT "{tmp}" '
                    f"UNIQUE NULLS NOT DISTINCT ({cols})"
                )
            else:
                create = (
                    f'ALTER TABLE "{table_name}" '
                    f'ADD CONSTRAINT "{tmp}" UNIQUE ({cols})'
                )

            drop_old = (
                f'ALTER TABLE "{table_name}" ' f'DROP CONSTRAINT "{name}"'
            )
            rename = (
                f'ALTER TABLE "{table_name}" '
                f'RENAME CONSTRAINT "{tmp}" TO "{name}"'
            )

            print(f"[UNIQUE][REPLACE] {table_name}.{name}")
            connection.execute(text(create))
            connection.execute(text(drop_old))
            connection.execute(text(rename))


def sync_check_constraints(connection, metadata):
    inspector = inspect(connection)

    for table in metadata.tables.values():
        table_name = table.name

        # constraints that exist in DB
        existing = {
            c["name"]
            for c in inspector.get_check_constraints(table_name)
            if c.get("name")
        }

        # constraints defined in SQLAlchemy model
        model = {
            c.name: str(c.sqltext)
            for c in table.constraints
            if getattr(c, "sqltext", None) is not None and c.name
        }

        # ------------------------------
        # 1. Drop all constraints that exist in DB but are not in the model
        # ------------------------------
        for name in existing - model.keys():  # <- fix: drop orphan
            sql = f'ALTER TABLE "{table_name}" DROP CONSTRAINT "{name}"'
            print(f"[CHECK] dropping orphan {sql}")
            connection.execute(text(sql))

        # ------------------------------
        # 2. Drop constraints that exist in both (recreate)
        # ------------------------------
        for name in existing & model.keys():
            sql = f'ALTER TABLE "{table_name}" DROP CONSTRAINT "{name}"'
            print(f"[CHECK] recreating {sql}")
            connection.execute(text(sql))

        # ------------------------------
        # 3. Add model constraints with NOT VALID
        # ------------------------------
        for name, sqltext in model.items():
            sql = (
                f'ALTER TABLE "{table_name}" '
                f'ADD CONSTRAINT "{name}" CHECK ({sqltext}) NOT VALID'
            )
            print(f"[CHECK] adding {sql}")
            connection.execute(text(sql))

        # ------------------------------
        # 4. Validate all model constraints
        # ------------------------------
        for name in model.keys():
            sql = f'ALTER TABLE "{table_name}" VALIDATE CONSTRAINT "{name}"'
            print(f"[CHECK] validating {sql}")
            connection.execute(text(sql))

        if not existing and not model:
            print(f'[CHECK] no managed constraints on "{table_name}"')


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
        sync_unique_constraints(conn, metadata)
        sync_foreign_keys(conn, metadata)
        sync_check_constraints(conn, metadata)
