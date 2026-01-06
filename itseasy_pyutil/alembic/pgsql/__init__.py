from sqlalchemy.dialects.postgresql import base
from sqlalchemy.types import UserDefinedType


class LTREE(UserDefinedType):
    cache_ok = True

    def get_col_spec(self, **kw):
        return "ltree"


# One-time registration on import
if "ltree" not in base.ischema_names:
    base.ischema_names["ltree"] = LTREE


from itseasy_pyutil.alembic.pgsql.ddl import DDLManager
from itseasy_pyutil.alembic.pgsql.schema import sync_schema
