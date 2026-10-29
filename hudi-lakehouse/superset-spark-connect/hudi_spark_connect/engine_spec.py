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
"""Superset integration for Spark SQL over ADBC Connect."""

from urllib.parse import quote, unquote

from superset.db_engine_specs.base import BaseEngineSpec
from superset.db_engine_specs.spark import time_grain_expressions
from superset.sql.parse import SQLGLOT_DIALECTS

# Superset maps SQL parser dialects by engine name.
SQLGLOT_DIALECTS["sparkconnect"] = SQLGLOT_DIALECTS["spark"]


class SparkConnectEngineSpec(BaseEngineSpec):
    engine = "sparkconnect"
    engine_name = "Apache Spark Connect (ADBC)"
    supports_dynamic_schema = True
    supports_file_upload = False
    _time_grain_expressions = time_grain_expressions
    metadata = {
        "description": "Apache Spark SQL over Spark Connect using ADBC.",
        "logo": "apache-spark.png",
        "homepage_url": "https://spark.apache.org/sql/",
        "connection_string": "sparkconnect://{hostname}:{port}/{database}?api=connect",
        "default_port": 15002,
    }

    @classmethod
    def convert_dttm(cls, target_type, dttm, db_extra=None):
        target = target_type.strip().upper()
        if target == "DATE":
            return f"CAST('{dttm.date().isoformat()}' AS DATE)"
        if target in ("DATETIME", "TIMESTAMP", "TIMESTAMP_NTZ"):
            value = dttm.isoformat(sep=" ", timespec="microseconds")
            spark_type = "TIMESTAMP" if target == "DATETIME" else target
            return f"CAST('{value}' AS {spark_type})"
        return None

    @classmethod
    def epoch_to_dttm(cls):
        return "TIMESTAMP_SECONDS({col})"

    @classmethod
    def epoch_ms_to_dttm(cls):
        return "TIMESTAMP_MILLIS({col})"

    @classmethod
    def adjust_engine_params(cls, uri, connect_args, catalog=None, schema=None):
        if catalog:
            raise ValueError("Selecting a catalog is not supported")
        if schema:
            uri = uri.set(database=quote(schema, safe=""))
        return uri, connect_args

    @classmethod
    def get_schema_from_engine_params(cls, sqlalchemy_uri, connect_args):
        return unquote(sqlalchemy_uri.database) if sqlalchemy_uri.database else None
