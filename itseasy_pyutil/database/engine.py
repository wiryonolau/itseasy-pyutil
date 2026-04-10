from itseasy_pyutil.database.connection import DryRunConnection


class DryRunEngine:
    def __init__(self, engine):
        self._engine = engine

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
