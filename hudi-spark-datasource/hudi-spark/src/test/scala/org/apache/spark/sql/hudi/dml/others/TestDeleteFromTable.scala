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

package org.apache.spark.sql.hudi.dml.others

import org.apache.hudi.common.config.RecordMergeMode

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

class TestDeleteFromTable extends HoodieSparkSqlTestBase {

  test("Test deleting from table") {
    withSparkSqlSessionConfig("hoodie.merge.small.file.group.candidates.limit" -> "0") {
      withRecordType()(withTempDir { tmp =>
        Seq("cow,COMMIT_TIME_ORDERING", "cow,EVENT_TIME_ORDERING",
          "mor,COMMIT_TIME_ORDERING", "mor,EVENT_TIME_ORDERING").foreach { argString =>
          val args = argString.split(',')
          val tableType = args(0)
          val mergeMode = args(1)
          val preCombineClause =
            if (RecordMergeMode.valueOf(mergeMode) == RecordMergeMode.EVENT_TIME_ORDERING) {
              "preCombineField = 'ts',"
            } else {
              ""
            }
          val tableName = generateTableName
          spark.sql(
            s"""
               |CREATE TABLE $tableName (
               |  id int,
               |  dt string,
               |  name string,
               |  price double,
               |  ts long
               |) USING hudi
               | tblproperties (
               |    primaryKey = 'id',
               |    $preCombineClause
               |    type = '$tableType',
               |    recordMergeMode = '$mergeMode'
               | )
               | PARTITIONED BY (dt)
               | LOCATION '${tmp.getCanonicalPath}/$tableName'
         """.stripMargin)

          // NOTE: Do not write the field alias, the partition field must be placed last.
          spark.sql(
            s"""
               | INSERT INTO $tableName VALUES
               | (1, 'a1', 10, 1000, "2021-01-05"),
               | (2, 'a2', 20, 2000, "2021-01-06"),
               | (3, 'a3', 30, 3000, "2021-01-07")
                """.stripMargin)

          checkAnswer(s"SELECT id, name, price, ts, dt FROM $tableName")(
            Seq(1, "a1", 10.0, 1000, "2021-01-05"),
            Seq(2, "a2", 20.0, 2000, "2021-01-06"),
            Seq(3, "a3", 30.0, 3000, "2021-01-07")
          )

          // Delete single row
          spark.sql(s"DELETE FROM $tableName WHERE id = 1")

          checkAnswer(s"SELECT id, name, price, ts, dt FROM $tableName")(
            Seq(2, "a2", 20.0, 2000, "2021-01-06"),
            Seq(3, "a3", 30.0, 3000, "2021-01-07")
          )

          // Try deleting non-existent row
          spark.sql(s"DELETE FROM $tableName WHERE id = 1")

          checkAnswer(s"SELECT id, name, price, ts, dt FROM $tableName")(
            Seq(2, "a2", 20.0, 2000, "2021-01-06"),
            Seq(3, "a3", 30.0, 3000, "2021-01-07")
          )

          // Delete record identified by some field other than the primary-key
          spark.sql(s"DELETE FROM $tableName WHERE name = 'a2'")

          checkAnswer(s"SELECT id, name, price, ts, dt FROM $tableName")(
            Seq(3, "a3", 30.0, 3000, "2021-01-07")
          )
        }
      })
    }
  }

  test("Test DELETE on VECTOR column preserves custom-type metadata") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id bigint,
           |  embedding VECTOR(3)
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  type = 'cow',
           |  primaryKey = 'id'
           | )
         """.stripMargin)

      spark.sql(
        s"""
           |insert into $tableName values
           |  (1, array(cast(0.1 as float), cast(0.2 as float), cast(0.3 as float))),
           |  (2, array(cast(0.4 as float), cast(0.5 as float), cast(0.6 as float)))
           """.stripMargin)

      spark.sql(s"delete from $tableName where id = 1")

      checkAnswer(s"select id from $tableName")(Seq(2L))
    }
  }

  test("Test DELETE on BLOB column preserves custom-type metadata") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id bigint,
           |  payload BLOB
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  type = 'cow',
           |  primaryKey = 'id'
           | )
         """.stripMargin)

      spark.sql(
        s"""
           |insert into $tableName values
           |  (1, named_struct(
           |        'type', 'OUT_OF_LINE',
           |        'data', cast(null as binary),
           |        'reference', named_struct(
           |          'external_path', 'blobs/a',
           |          'offset', 0L,
           |          'length', 3L,
           |          'managed', false))),
           |  (2, named_struct(
           |        'type', 'OUT_OF_LINE',
           |        'data', cast(null as binary),
           |        'reference', named_struct(
           |          'external_path', 'blobs/b',
           |          'offset', 0L,
           |          'length', 5L,
           |          'managed', false)))
           """.stripMargin)

      spark.sql(s"delete from $tableName where id = 1")

      checkAnswer(s"select id, payload.reference.external_path from $tableName")(
        Seq(2L, "blobs/b"))
    }
  }
}
