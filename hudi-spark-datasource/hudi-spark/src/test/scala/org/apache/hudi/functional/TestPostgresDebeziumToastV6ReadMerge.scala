/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.functional

import org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

/**
 * Regression test: a Postgres CDC TOAST sentinel must be backfilled at read time on a
 * table-version-6 MOR table using [[PostgresDebeziumAvroPayload]].
 *
 * A Postgres TOAST column that did not change is emitted by Debezium as the sentinel
 * `__debezium_unavailable_value`; the payload merge is supposed to restore the prior column value.
 * On a v6 table the 1.x read path uses the CUSTOM payload merger ([[org.apache.hudi.common.model.HoodieAvroRecordMerger]]).
 * That payload backfills by mutating the incoming Avro record in place and returning the same
 * reference; the merger previously short-circuited `if (updatedRecord == newerAvroRecord) return newer`
 * and returned the engine-native record, which never received the in-place backfill — so the raw
 * sentinel leaked to readers. The fix removes that shortcut and rebuilds the result from the merged
 * Avro record.
 *
 * Scenario (single id, MOR base + log slice; `_event_lsn` is the ordering field):
 *   base   : _event_lsn=100, description='Original description'
 *   update : _event_lsn=200, description='Updated description'
 *   update : _event_lsn=300, description=`__debezium_unavailable_value` (TOAST — column unchanged)
 *
 * The winning record by ordering is _event_lsn=300; its TOAST sentinel must be backfilled from the
 * base record, so the read must return 'Original description' (v6 / payload semantics). Before the
 * fix this returned the raw sentinel.
 */
class TestPostgresDebeziumToastV6ReadMerge extends HoodieSparkSqlTestBase {

  test("v6 MOR PostgresDebeziumAvroPayload backfills TOAST sentinel at read") {
    withTempDir { tmp =>
      withTable(generateTableName) { tableName =>
        val tableLocation = s"${tmp.getCanonicalPath}/$tableName"

        // Table-version-6 MOR table with the Postgres Debezium payload. `hoodie.write.table.version`
        // = '6' forces the same table version production tables created before the Hudi 1.x upgrade
        // carry, which is the combination that exercises the CUSTOM payload read-merge path.
        spark.sql(
          s"""
             |CREATE TABLE $tableName (
             |  id INT,
             |  name STRING,
             |  description STRING,
             |  _change_operation_type STRING,
             |  _event_lsn BIGINT
             |) USING hudi
             |LOCATION '$tableLocation'
             |TBLPROPERTIES (
             |  type = 'mor',
             |  primaryKey = 'id',
             |  preCombineField = '_event_lsn',
             |  payloadClass = '${classOf[PostgresDebeziumAvroPayload].getName}',
             |  'hoodie.spark.sql.insert.into.operation' = 'upsert',
             |  'hoodie.write.table.version' = '6'
             |)
             |""".stripMargin)

        // Batch 1: base snapshot (lsn=100, description='Original description') -> base file.
        spark.sql(
          s"""
             |INSERT INTO $tableName
             |SELECT id, data.* FROM (
             |  SELECT 1 AS id, named_struct(
             |    'name', 'John',
             |    'description', 'Original description',
             |    '_change_operation_type', 'r',
             |    '_event_lsn', CAST(100 AS BIGINT)
             |  ) AS data
             |) t
             |""".stripMargin)

        // Batch 2: two CDC updates for id=1 in one upsert -> MOR log file.
        //   lsn=200: description='Updated description'
        //   lsn=300: TOAST sentinel (description unchanged in Postgres, so Debezium omits it).
        spark.sql(
          s"""
             |INSERT INTO $tableName
             |SELECT id, data.* FROM (
             |  SELECT 1 AS id, named_struct(
             |    'name', 'John Updated',
             |    'description', 'Updated description',
             |    '_change_operation_type', 'u',
             |    '_event_lsn', CAST(200 AS BIGINT)
             |  ) AS data
             |  UNION ALL
             |  SELECT 1, named_struct(
             |    'name', 'John Updated Again',
             |    'description', '${PostgresDebeziumAvroPayload.DEBEZIUM_TOASTED_VALUE}',
             |    '_change_operation_type', 'u',
             |    '_event_lsn', CAST(300 AS BIGINT)
             |  )
             |) t
             |""".stripMargin)

        // The base+log read merge must backfill the TOAST sentinel from the base record. The winning
        // record is lsn=300 (name='John Updated Again'), and its description must be restored to the
        // prior value 'Original description' rather than the raw sentinel.
        checkAnswer(s"SELECT id, name, description, _event_lsn FROM $tableName WHERE id = 1")(
          Seq(1, "John Updated Again", "Original description", 300L)
        )
      }
    }
  }
}
