from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------
def table_exists(conn, table_name):
    row = conn.execute(
        text(
            "SELECT 1 FROM sqlite_master " "WHERE type='table' AND name=:name"
        ),
        {"name": table_name},
    ).fetchone()
    return bool(row)


def existing_columns(conn, table_name):
    rows = conn.execute(text(f'PRAGMA table_info("{table_name}")')).fetchall()
    return {r[1] for r in rows}


# ----------------------------------------------------------------------
# schema sync
# ----------------------------------------------------------------------
def sync_schema(engine, metadata):
    """
    SQLite-safe schema synchronization.

    Guarantees:
    - tables exist
    - columns exist
    - indexes exist

    Does NOT:
    - modify column types
    - drop columns
    - alter PK/FK/constraints
    """

    with engine.begin() as conn:

        # SQLite requires this per connection
        conn.execute(text("PRAGMA foreign_keys = ON"))

        inspector = inspect(conn)

        # --------------------------------------------------------------
        # 1. create tables
        # --------------------------------------------------------------
        for table in metadata.tables.values():
            if not table_exists(conn, table.name):
                print(f"Creating table: {table.name}")
                table.create(conn)
            else:
                print(f"Table exists: {table.name}")

        # --------------------------------------------------------------
        # 2. add missing columns
        # --------------------------------------------------------------
        for table_name, table in metadata.tables.items():
            db_cols = existing_columns(conn, table_name)

            for col in table.columns:
                if col.name in db_cols:
                    continue

                col_type = col.type.compile(engine.dialect)

                stmt = (
                    f'ALTER TABLE "{table_name}" '
                    f'ADD COLUMN "{col.name}" {col_type}'
                )

                if not col.nullable:
                    stmt += " NOT NULL"

                if col.server_default is not None:
                    stmt += f" DEFAULT {col.server_default.arg}"

                print(f"Adding column: {table_name}.{col.name}")
                try:
                    conn.execute(text(stmt))
                except SQLAlchemyError as e:
                    print(f"Failed to add column {table_name}.{col.name}: {e}")

        # --------------------------------------------------------------
        # 3. create indexes (includes unique constraints)
        # --------------------------------------------------------------
        for table_name, table in metadata.tables.items():
            existing_indexes = {
                idx["name"]
                for idx in inspector.get_indexes(table_name)
                if idx.get("name")
            }

            for idx in table.indexes:
                if not idx.name:
                    continue

                if idx.name in existing_indexes:
                    continue

                print(f"Creating index {idx.name} on {table_name}")
                try:
                    idx.create(conn)
                except SQLAlchemyError as e:
                    print(f"Failed to create index {idx.name}: {e}")

        print("SQLite schema sync complete")
