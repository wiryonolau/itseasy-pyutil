from itseasy_pyutil.database.connection import DryRunConnection


class DryRunEngine:
    def __init__(self, engine):
        self._engine = engine

    @staticmethod
    def patch_dialect(dialect):
        orig = dialect.has_table

        def wrapped(connection, *args, **kwargs):
            if isinstance(connection, DryRunConnection):
                connection = connection._conn
            return orig(connection, *args, **kwargs)

        dialect.has_table = wrapped

    def connect(self, *args, **kwargs):
        conn = self._engine.connect(*args, **kwargs)
        return DryRunConnection(conn)

    def execution_options(self, *args, **kwargs):
        eng = self._engine.execution_options(*args, **kwargs)
        return DryRunEngine(eng)

    def begin(self, *args, **kwargs):
        conn = self.connect()
        return conn.begin(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._engine, name)
