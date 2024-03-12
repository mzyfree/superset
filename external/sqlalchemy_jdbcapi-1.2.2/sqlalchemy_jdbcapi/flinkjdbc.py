from abc import ABC
from contextlib import closing
from typing import (Any, Dict)
from typing import List

from sqlalchemy import util, types
from sqlalchemy.engine import Engine
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql import sqltypes
from sqlalchemy.types import TypeEngine

from .base import MixedBinary, BaseDialect

colspecs = util.update_copy(
    DefaultDialect.colspecs, {sqltypes.LargeBinary: MixedBinary},
)


def _driver_kwargs():
    return {}


def execute_sql(sql: List[str], engine: Engine) -> List[tuple]:
    result_set = None
    with closing(engine.raw_connection()) as conn:
        cursor = conn.cursor()
        for i, statement in enumerate(sql):
            print("Query start ", str(i), statement)
            try:
                cursor.execute(statement)
                # db_engine_spec.handle_cursor(cursor, query, session)
                result_set = cursor.fetchall()
            except Exception as ex:
                print("Query failed ", statement, ex)
                return []
    return result_set


def convert_type(origin: str) -> TypeEngine:
    if origin.upper() == "BINARY":
        return types.BINARY
    elif origin.upper() in ["TINYINT", "SMALLINT", "INT", "BIGINT"]:
        return types.INTEGER
    elif origin.upper() in ["DECIMAL", "FLOAT", "DOUBLE"]:
        return types.FLOAT
    elif origin.upper() == "ARRAY":
        return types.ARRAY
    elif origin.upper() == "MAP":
        return types.JSON
    else:
        return types.String


class FlinkJDBCDialect(BaseDialect, DefaultDialect, ABC):
    jdbc_db_name = "flink"
    jdbc_driver_name = "com.ververica.flink.table.jdbc.FlinkDriver"
    colspecs = colspecs

    def initialize(self, connection):
        super(FlinkJDBCDialect, self).initialize(connection)

    def create_connect_args(self, url):
        if url is None:
            return
        # dialects expect jdbc url e.g.
        # if sqlalchemy create_engine() url is passed e.g.
        # it is parsed wrong
        # restore original url
        s: str = str(url)
        # get jdbc url
        jdbc_url: str = s.split("//", 1)[-1]
        # add driver information
        if not jdbc_url.startswith("jdbc"):
            jdbc_url = f"jdbc:flink://{jdbc_url}"
        print('use jdbc url is ' + jdbc_url)
        kwargs = {
            "jclassname": self.jdbc_driver_name,
            "url": jdbc_url,
            # pass driver args via JVM System settings
            "driver_args": []
        }
        return (), kwargs

    def do_commit(self, dbapi_connection):
        pass

    def do_rollback(self, dbapi_connection):
        pass

    def get_default_isolation_level(self, dbapi_conn):
        return None

    def _check_unicode_returns(self, connection, additional_tests=None):
        return None

    def _check_unicode_description(self, connection):
        return None

    def get_columns(self, connection: Engine,
                    table_name: str, schema=None, **kw) -> List[Dict[str, Any]]:
        catalog = "iceberg"
        sql = [
            "use catalog " + catalog,
            "use " + schema,
            "describe " + table_name
        ]
        temp = execute_sql(sql, connection)
        result: List[Dict[str, Any]] = []
        for line in temp:
            result.append({
                'name': line[0],
                'type': convert_type(line[1]),
                'nullable': line[2],
                'primary_key': line[3]
            })
        return result


dialect = FlinkJDBCDialect
