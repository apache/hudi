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

"""Offline checks of smoke-test SQL flow; these do not replace live ADBC tests."""
import argparse
import contextlib
import importlib.util
import io
import pathlib
import sys
import tempfile
import types
import unittest
from unittest.mock import MagicMock, patch

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "local-dev/example/connect_smoke.py"


class ConnectSmokeTest(unittest.TestCase):
    def setUp(self):
        self.dbapi = MagicMock()
        fake_module = types.ModuleType("adbc_driver_manager")
        fake_module.dbapi = self.dbapi
        with patch.dict(sys.modules, {"adbc_driver_manager": fake_module}):
            spec = importlib.util.spec_from_file_location("connect_smoke", SCRIPT)
            self.smoke = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(self.smoke)
        self.cursor = self.dbapi.connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value

    def invoke(self, phase, *args):
        with patch.object(sys, "argv", [str(SCRIPT), phase, "--database", "connect_smoke_test", *args]):
            with contextlib.redirect_stdout(io.StringIO()):
                self.smoke.main()

    def test_identifiers_are_restricted(self):
        self.assertEqual(self.smoke.identifier("connect_smoke_123"), "connect_smoke_123")
        for invalid in ["default", "other_db", "connect_smoke_a;DROP TABLE t", "connect_smoke_a.b"]:
            with self.subTest(identifier=invalid), self.assertRaises(argparse.ArgumentTypeError):
                self.smoke.identifier(invalid)

    def test_prepare_reads_back_each_write_without_fetching_dml(self):
        self.cursor.fetchall.side_effect = [
            [(1, 100), (2, 200)], [(1, 150), (2, 200)], [(1, 150)], [(1, 175), (3, 300)],
        ]
        self.invoke("prepare")
        sqls = [call.args[0] for call in self.cursor.execute.call_args_list]
        self.assertEqual(sqls[0], "CREATE DATABASE connect_smoke_test")
        self.assertEqual(len(sqls), 10)
        self.assertEqual(self.cursor.fetchall.call_count, 4)
        self.assertTrue(sqls[2].startswith("INSERT"))
        self.assertTrue(sqls[4].startswith("UPDATE"))
        self.assertTrue(sqls[6].startswith("DELETE"))
        self.assertTrue(sqls[8].startswith("MERGE INTO"))
        self.assertNotIn("hoodie.write.table.version", " ".join(sqls))
        self.assertNotIn("hoodie.write.auto.upgrade", " ".join(sqls))
        self.assertTrue(self.dbapi.connect.call_args.kwargs["autocommit"])

    def test_write_error_is_not_retried(self):
        self.cursor.execute.side_effect = [None, None, RuntimeError("unknown write outcome")]
        with self.assertRaisesRegex(RuntimeError, "unknown write outcome"):
            self.invoke("prepare")
        self.assertEqual(self.cursor.execute.call_count, 3)
        self.cursor.fetchall.assert_not_called()

    def test_ownership_marker_survives_failure_after_database_creation(self):
        for completed_statements in (1, 2):
            with self.subTest(completed_statements=completed_statements), tempfile.TemporaryDirectory() as directory:
                self.cursor.execute.side_effect = [None] * completed_statements + [RuntimeError("prepare failed")]
                marker = pathlib.Path(directory) / "database-created"
                with self.assertRaisesRegex(RuntimeError, "prepare failed"):
                    self.invoke("prepare", "--created-marker", str(marker))
                self.assertEqual(marker.read_text(), "connect_smoke_test")

    def test_failed_database_creation_does_not_record_ownership(self):
        self.cursor.execute.side_effect = RuntimeError("database already exists")
        with tempfile.TemporaryDirectory() as directory:
            marker = pathlib.Path(directory) / "database-created"
            with self.assertRaisesRegex(RuntimeError, "database already exists"):
                self.invoke("prepare", "--created-marker", str(marker))
            self.assertFalse(marker.exists())
        self.assertEqual(self.cursor.execute.call_count, 1)

    def test_existing_marker_is_rejected_before_creating_database(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = pathlib.Path(directory) / "database-created"
            marker.write_text("previous run")
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                self.invoke("prepare", "--created-marker", str(marker))
            self.assertEqual(marker.read_text(), "previous run")
        self.dbapi.connect.assert_not_called()

    def test_verify_checks_persisted_values(self):
        self.cursor.fetchall.return_value = [(1, "East", 175, 3), (3, "North", 300, 3)]
        self.invoke("verify")
        self.assertEqual(self.cursor.execute.call_count, 1)
        self.cursor.fetchall.return_value = []
        with self.assertRaises(AssertionError):
            self.invoke("verify")

    def test_cleanup_does_not_cascade_or_fetch_results(self):
        self.invoke("cleanup")
        self.assertEqual([call.args[0] for call in self.cursor.execute.call_args_list], [
            "DROP TABLE IF EXISTS connect_smoke_test.freshness_orders PURGE",
            "DROP TABLE IF EXISTS connect_smoke_test.mor_orders PURGE",
            "DROP TABLE IF EXISTS connect_smoke_test.partitioned_orders PURGE",
            "DROP TABLE IF EXISTS connect_smoke_test.orders PURGE",
            "DROP DATABASE IF EXISTS connect_smoke_test",
        ])
        self.cursor.fetchall.assert_not_called()

    def test_sql_matrix_covers_services_incremental_partition_sync_and_versions(self):
        with patch.object(self.smoke, "query", side_effect=[
            [("20261022000100000",)],
            [("20261022000200000", 1, "COMPLETED")],
            *[[('hoodie.table.version', '8')]] * 3,
        ]) as query, patch.object(self.smoke, "run") as run:
            self.invoke("sql", "--expected-table-version", "8")
        sqls = "\n".join(call.args[1] for call in run.call_args_list)
        for clause in ("TIMESTAMP AS OF '20261022000100000'", "hudi_table_changes", "PARTITIONED BY (region)",
                       "'hoodie.compact.inline' = 'true'", "'read_optimized'"):
            self.assertIn(clause, sqls)
        self.assertNotIn("hoodie.write.table.version", sqls)
        partitioned_ddl = next(call.args[1] for call in run.call_args_list
                               if call.args[1].startswith("CREATE TABLE") and "PARTITIONED BY" in call.args[1])
        self.assertNotIn("hoodie.datasource.meta.sync", partitioned_ddl)
        self.assertNotIn("hoodie.datasource.hive_sync", partitioned_ddl)
        self.assertEqual(query.call_count, 5)

    def test_sql_matrix_rejects_missing_compaction_or_wrong_version(self):
        for compactions in ([], [("20261022000200000", 1, "COMPLETED")]):
            version = "9" if compactions else "8"
            with self.subTest(compactions=compactions), patch.object(self.smoke, "query", side_effect=[
                [("20261022000100000",)], compactions,
                *[[("hoodie.table.version", version)]] * 3,
            ]), patch.object(self.smoke, "run"), self.assertRaises(AssertionError):
                self.invoke("sql", "--expected-table-version", "8")

    def test_freshness_keeps_one_connection_open_and_refresh_is_explicit(self):
        for refresh in (False, True):
            with self.subTest(refresh=refresh):
                self.dbapi.reset_mock()
                with patch.object(self.smoke, "query", side_effect=[[("before",)], [("after",)]]), \
                        patch.object(self.smoke, "run") as run, \
                        patch.object(self.smoke.subprocess, "run") as writer:
                    def check_connection_open(*args, **kwargs):
                        self.dbapi.connect.return_value.__exit__.assert_not_called()
                    writer.side_effect = check_connection_open
                    self.invoke("freshness", *(["--refresh"] if refresh else []),
                                "--external-writer", "python", "/fixture/writer.py")
                self.assertEqual(self.dbapi.connect.call_count, 1)
                self.assertEqual([c.args[0] for c in writer.call_args_list], [
                    ["python", "/fixture/writer.py", "connect_smoke_test", "commit-schema"],
                    ["python", "/fixture/writer.py", "connect_smoke_test", "recreate"],
                ])
                refreshes = [c.args[1] for c in run.call_args_list if c.args[1].startswith("REFRESH")]
                self.assertEqual(len(refreshes), 2 if refresh else 0)
                reads = [c.args[2] for c in run.call_args_list if c.args[1].startswith("SELECT")]
                self.assertEqual(reads, [[(1, 100)], [(1, 100, None), (2, 200, "external")], [(9, "recreated")]])

    def test_failed_external_writer_does_not_refresh_or_retry(self):
        with patch.object(self.smoke, "query", return_value=[("before",)]), \
                patch.object(self.smoke, "run") as run, \
                patch.object(self.smoke.subprocess, "run", side_effect=RuntimeError("writer failed")) as writer:
            with self.assertRaisesRegex(RuntimeError, "writer failed"):
                self.invoke("freshness", "--refresh", "--external-writer", "writer")
        self.assertEqual(writer.call_count, 1)
        self.assertFalse(any(c.args[1].startswith("REFRESH") for c in run.call_args_list))

    def test_trino_compares_results_not_just_command_success(self):
        outputs = ['{"id":1,"amount":175}\n{"id":3,"amount":300}\n',
                   '{"id":1,"region":"East","amount":100}\n{"id":2,"region":"West","amount":200}\n']
        with patch.object(self.smoke.subprocess, "run", side_effect=[
            types.SimpleNamespace(stdout=output) for output in outputs
        ]) as client:
            self.invoke("trino")
        self.dbapi.connect.assert_not_called()
        self.assertIn("hudi.connect_smoke_test.partitioned_orders", client.call_args.args[0][-1])
        with patch.object(self.smoke.subprocess, "run", return_value=types.SimpleNamespace(stdout="")), \
                self.assertRaises(AssertionError):
            self.invoke("trino")

    def test_unsafe_metadata_literals_are_rejected(self):
        for value in ("bad'quote", "bad\\escape", "bad\nline"):
            with self.assertRaises(ValueError):
                self.smoke.sql_literal(value)


if __name__ == "__main__":
    unittest.main()
