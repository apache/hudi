# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""SQLAlchemy dialect for Spark Connect using the Foundry ADBC driver."""

import re
from urllib.parse import unquote, urlencode

from sqlalchemy import types
from sqlalchemy.engine import default
from sqlalchemy.sql.compiler import IdentifierPreparer


class SparkIdentifierPreparer(IdentifierPreparer):
    def __init__(self, dialect):
        super().__init__(dialect, initial_quote="`", escape_quote="`")


def spark_type(name):
    name = name.strip().lower()
    if name.startswith("decimal("):
        match = re.fullmatch(r"decimal\((\d+),\s*(\d+)\)", name)
        if match:
            return types.Numeric(int(match.group(1)), int(match.group(2)))
    base = name.split("(", 1)[0]
    return {
        "boolean": types.Boolean, "tinyint": types.SmallInteger,
        "smallint": types.SmallInteger, "int": types.Integer,
        "integer": types.Integer, "bigint": types.BigInteger,
        "float": types.Float, "double": types.Float,
        "string": types.String, "varchar": types.String, "char": types.String,
        "date": types.Date, "timestamp": types.DateTime,
        "timestamp_ntz": types.DateTime, "binary": types.LargeBinary,
    }.get(base, types.NullType)()


class SparkConnectDialect(default.DefaultDialect):
    name = "sparkconnect"
    driver = "adbc"
    preparer = SparkIdentifierPreparer
    supports_statement_cache = False
    supports_native_boolean = True
    default_paramstyle = "qmark"

    @classmethod
    def dbapi(cls):
        from adbc_driver_manager import dbapi
        return dbapi

    def create_connect_args(self, url):
        if not url.host:
            raise ValueError("Spark Connect host is required")
        if url.username or url.password:
            raise ValueError("Credentials in SQLAlchemy URL are not supported")
        options = dict(url.query)
        driver = options.pop("driver", "spark")
        options.setdefault("api", "connect")
        options.setdefault("auth_type", "none")
        if options["api"] != "connect":
            raise ValueError("Only api=connect is supported")
        host = f"[{url.host}]" if ":" in url.host else url.host
        authority = f"{host}:{url.port or 15002}"
        uri = f"spark://{authority}?{urlencode(options)}"
        return [], {"driver": driver, "db_kwargs": {"uri": uri}, "autocommit": True,
                    "_schema": unquote(url.database) if url.database else None}

    def connect(self, *args, **kwargs):
        schema = kwargs.pop("_schema", None)
        connection = self.dbapi.connect(*args, **kwargs)
        if schema:
            try:
                with connection.cursor() as cursor:
                    cursor.execute("USE " + self.identifier_preparer.quote_identifier(schema))
            except BaseException:
                connection.close()
                raise
        return connection

    def do_execute(self, cursor, statement, parameters, context=None):
        if parameters:
            raise NotImplementedError("Spark Connect ADBC parameter binding is not supported")
        cursor.execute(statement)

    def do_executemany(self, cursor, statement, parameters, context=None):
        raise NotImplementedError("Spark Connect ADBC parameter binding is not supported")

    def do_rollback(self, dbapi_connection):
        # Spark does not support SQL transactions; connections use autocommit.
        pass

    def _get_default_schema_name(self, connection):
        return connection.exec_driver_sql("SELECT current_database()").scalar()

    def get_schema_names(self, connection, **kw):
        return [row[0] for row in connection.exec_driver_sql("SHOW DATABASES")]

    def _qualified(self, table, schema=None):
        quote = self.identifier_preparer.quote_identifier
        return ".".join(quote(part) for part in (schema, table) if part is not None)

    def get_table_names(self, connection, schema=None, **kw):
        sql = "SHOW TABLES"
        if schema:
            sql += " IN " + self.identifier_preparer.quote_identifier(schema)
        views = set(self.get_view_names(connection, schema=schema))
        return [row[1] for row in connection.exec_driver_sql(sql) if not row[2] and row[1] not in views]

    def get_view_names(self, connection, schema=None, **kw):
        sql = "SHOW VIEWS"
        if schema:
            sql += " IN " + self.identifier_preparer.quote_identifier(schema)
        return [row[1] for row in connection.exec_driver_sql(sql) if not row[2]]

    def get_columns(self, connection, table_name, schema=None, **kw):
        rows = connection.exec_driver_sql("DESCRIBE TABLE " + self._qualified(table_name, schema))
        columns = []
        for row in rows:
            name = row[0]
            # DESCRIBE separates metadata sections with rows that have no type.
            # A real, quoted column can contain whitespace or start with '#'.
            if not name or not row[1]:
                break
            columns.append({"name": name, "type": spark_type(row[1]), "nullable": True, "default": None})
        return columns

    def has_table(self, connection, table_name, schema=None, **kw):
        if schema is None:
            # Local temporary views are session-scoped and cannot be qualified
            # with a database name, even though SHOW TABLES IN lists them.
            if any(row[1] == table_name and row[2]
                   for row in connection.exec_driver_sql("SHOW TABLES")):
                return True
        return (table_name in self.get_table_names(connection, schema=schema)
                or table_name in self.get_view_names(connection, schema=schema))

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        # Spark does not expose enforced SQL primary key constraints.
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        return []

    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        return []

    def get_table_comment(self, connection, table_name, schema=None, **kw):
        return {"text": None}
