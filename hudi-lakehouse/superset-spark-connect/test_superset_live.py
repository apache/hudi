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
"""Opt-in SQL Lab API test. The target connection must allow DML."""
import os
import unittest
import uuid

URL = os.environ.get("HUDI_SUPERSET_URL")
USER = os.environ.get("HUDI_SUPERSET_USER")
PASSWORD = os.environ.get("HUDI_SUPERSET_PASSWORD")
DATABASE = os.environ.get("HUDI_SUPERSET_DATABASE_ID")
SCHEMA = os.environ.get("HUDI_SUPERSET_SCHEMA", "default")


@unittest.skipUnless(all((URL, USER, PASSWORD, DATABASE)), "set Superset URL, user, password and database ID")
class SupersetLiveTest(unittest.TestCase):
    def test_temporal_chart_filters_and_epoch_grouping(self):
        import requests

        session = requests.Session()
        base = URL.rstrip("/")
        response = session.post(base + "/api/v1/security/login", json={
            "username": USER, "password": PASSWORD, "provider": "db"}, timeout=30)
        response.raise_for_status()
        session.headers["Authorization"] = "Bearer " + response.json()["access_token"]
        response = session.get(base + "/api/v1/security/csrf_token/", timeout=30)
        response.raise_for_status()
        session.headers.update({"X-CSRFToken": response.json()["result"], "Referer": base + "/"})
        table = "temporal_adapter_" + uuid.uuid4().hex
        dataset_id = None
        created = False

        def run(sql):
            response = session.post(base + "/api/v1/sqllab/execute/", json={
                "database_id": int(DATABASE), "sql": sql, "schema": SCHEMA,
                "runAsync": False, "client_id": uuid.uuid4().hex[:11]}, timeout=180)
            response.raise_for_status()
            result = response.json()
            self.assertEqual(result.get("status"), "success", result)
            return result["data"]

        def chart(query):
            response = session.post(base + "/api/v1/chart/data", json={
                "datasource": {"id": dataset_id, "type": "table"}, "queries": [query],
                "result_format": "json", "result_type": "full", "force": True}, timeout=120)
            response.raise_for_status()
            result = response.json()["result"][0]
            self.assertEqual(result["status"], "success", result)
            return result

        try:
            run(f"CREATE TABLE {table} (id INT, day DATE, event_ts TIMESTAMP, "
                "epoch_s BIGINT, epoch_ms BIGINT) USING parquet")
            created = True
            run(f"INSERT INTO {table} VALUES "
                "(1, DATE '2023-11-14', TIMESTAMP '2023-11-14 22:13:20', 1700000000, 1700000000000), "
                "(2, DATE '2023-11-15', TIMESTAMP '2023-11-15 22:13:20', 1700086400, 1700086400000)")
            response = session.post(base + "/api/v1/dataset/", json={
                "database": int(DATABASE), "schema": SCHEMA, "table_name": table}, timeout=120)
            response.raise_for_status()
            dataset_id = response.json()["id"]
            for name in ("day", "event_ts"):
                result = chart({"columns": ["id"], "filters": [
                    {"col": name, "op": "==", "val": 1700000000000}], "row_limit": 100})
                self.assertEqual(result["data"], [{"id": 1}])

            response = session.get(base + f"/api/v1/dataset/{dataset_id}", timeout=30)
            response.raise_for_status()
            columns = response.json()["result"]["columns"]
            for column in columns:
                if column["column_name"] in ("epoch_s", "epoch_ms"):
                    column["is_dttm"] = True
                    column["python_date_format"] = column["column_name"]
            keys = ("id", "column_name", "is_dttm", "python_date_format", "type", "groupby", "filterable")
            response = session.put(base + f"/api/v1/dataset/{dataset_id}", json={
                "columns": [{key: column[key] for key in keys} for column in columns]}, timeout=30)
            response.raise_for_status()
            for name in ("epoch_s", "epoch_ms"):
                result = chart({"columns": [{"columnType": "BASE_AXIS", "sqlExpression": name,
                                             "label": name, "timeGrain": "P1D"}],
                                "metrics": [{"expressionType": "SQL", "sqlExpression": "COUNT(*)",
                                             "label": "count"}], "row_limit": 100})
                self.assertEqual(sorted((row[name], row["count"]) for row in result["data"]),
                                 [(1699920000000, 1), (1700006400000, 1)])
            result = chart({"columns": ["id"], "orderby": [["id", True]],
                            "row_offset": 1, "row_limit": None})
            self.assertEqual(result["data"], [{"id": 2}])
        finally:
            try:
                if dataset_id is not None:
                    session.delete(base + f"/api/v1/dataset/{dataset_id}", timeout=30).raise_for_status()
            finally:
                if created:
                    run(f"DROP TABLE IF EXISTS {table} PURGE")
                session.close()

    def test_sql_lab_queries_and_writes(self):
        import requests

        session = requests.Session()
        base = URL.rstrip("/")
        response = session.post(base + "/api/v1/security/login", json={
            "username": USER, "password": PASSWORD, "provider": "db"}, timeout=30)
        response.raise_for_status()
        session.headers["Authorization"] = "Bearer " + response.json()["access_token"]
        response = session.get(base + "/api/v1/security/csrf_token/", timeout=30)
        response.raise_for_status()
        session.headers.update({"X-CSRFToken": response.json()["result"], "Referer": base + "/"})
        table = "connect_adapter_" + uuid.uuid4().hex

        def run(sql):
            response = session.post(base + "/api/v1/sqllab/execute/", json={
                "database_id": int(DATABASE), "sql": sql, "schema": SCHEMA,
                "runAsync": False, "client_id": uuid.uuid4().hex[:11]}, timeout=180)
            response.raise_for_status()
            result = response.json()
            self.assertEqual(result.get("status"), "success", result)
            return result["data"]

        created = False
        dataset_id = None
        attempted = False
        try:
            attempted = True
            run(f"CREATE TABLE {table} (id INT, region STRING, amount INT, ts BIGINT) USING hudi "
                "TBLPROPERTIES(type='cow', primaryKey='id', preCombineField='ts')")
            created = True
            run(f"INSERT INTO {table} VALUES (1,'East',100,1), (2,'West',200,1)")
            self.assertEqual(run(f"SELECT SUM(amount) AS total FROM {table}"), [{"total": 300}])
            self.assertEqual(run(f"SELECT id FROM {table} WHERE region='East'"), [{"id": 1}])
            response = session.post(base + "/api/v1/dataset/", json={
                "database": int(DATABASE), "schema": SCHEMA, "table_name": table}, timeout=120)
            response.raise_for_status()
            dataset_id = response.json()["id"]
            payload = {"datasource": {"id": dataset_id, "type": "table"},
                       "queries": [{"metrics": [{"expressionType": "SQL",
                           "sqlExpression": "SUM(amount)", "label": "total"}],
                           "columns": ["region"], "row_limit": 100}],
                       "result_format": "json", "result_type": "full", "force": True}

            def chart_data():
                response = session.post(base + "/api/v1/chart/data", json=payload, timeout=120)
                response.raise_for_status()
                result = response.json()["result"][0]
                self.assertEqual(result["status"], "success", result)
                return {row["region"]: row["total"] for row in result["data"]}

            self.assertEqual(chart_data(), {"East": 100, "West": 200})
            run(f"UPDATE {table} SET amount=150, ts=2 WHERE id=1")
            self.assertEqual(run(f"SELECT id, amount FROM {table} WHERE id=1"), [{"id": 1, "amount": 150}])
            self.assertEqual(chart_data(), {"East": 150, "West": 200})
            run(f"MERGE INTO {table} t USING ("
                "SELECT 1 AS id, 'East' AS region, 175 AS amount, CAST(3 AS BIGINT) AS ts "
                "UNION ALL SELECT 3, 'North', 300, CAST(3 AS BIGINT)) s "
                "ON t.id=s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
            self.assertEqual(run(f"SELECT id, region, amount, ts FROM {table} ORDER BY id"), [
                {"id": 1, "region": "East", "amount": 175, "ts": 3},
                {"id": 2, "region": "West", "amount": 200, "ts": 1},
                {"id": 3, "region": "North", "amount": 300, "ts": 3},
            ])
            run(f"DELETE FROM {table} WHERE id=2")
            self.assertEqual(run(f"SELECT count(*) AS n FROM {table}"), [{"n": 2}])
        finally:
            try:
                if dataset_id is not None:
                    try:
                        response = session.delete(base + f"/api/v1/dataset/{dataset_id}", timeout=30)
                        response.raise_for_status()
                    except Exception:
                        print(f"Cleanup failed; remove test dataset {dataset_id} manually.")
                        raise
            finally:
                try:
                    if created:
                        try:
                            run(f"DROP TABLE IF EXISTS {table} PURGE")
                        except Exception:
                            print(f"Cleanup failed; remove test table {SCHEMA}.{table} manually.")
                            raise
                    elif attempted:
                        print(f"CREATE did not report success; check for test table {SCHEMA}.{table} before cleanup.")
                finally:
                    session.close()
