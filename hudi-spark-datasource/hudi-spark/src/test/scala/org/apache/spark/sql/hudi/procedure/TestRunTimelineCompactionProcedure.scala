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

package org.apache.spark.sql.hudi.procedure

class TestRunTimelineCompactionProcedure extends HoodieSparkProcedureTestBase {

  test("Test Call run_timeline_compaction Procedure by Table") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           | id int,
           | name string,
           | price double,
           | ts long
           | ) using hudi
           | location '${tmp.getCanonicalPath}'
           | tblproperties (
           |   primaryKey = 'id',
           |   type = 'cow',
           |   orderingFields = 'ts',
           |   hoodie.metadata.enable = 'false',
           |   hoodie.keep.min.commits = '2',
           |   hoodie.keep.max.commits = '3',
           |   hoodie.cleaner.commits.retained = '1',
           |   hoodie.timeline.compaction.batch.size = '2'
           | )
           |""".stripMargin)

      // Generate enough commits so that subsequent archive_commits calls produce
      // multiple L0 archived timeline files, which is the input that the timeline
      // compaction procedure will then merge.
      for (i <- 1 to 8) {
        spark.sql(s"insert into $tableName values($i, 'a$i', ${i * 10}, ${i * 1000})")
      }
      spark.sql(s"call archive_commits(table => '$tableName'" +
        s", min_commits => 2, max_commits => 3, retain_commits => 1, enable_metadata => false)")

      val result = spark.sql(s"call run_timeline_compaction(table => '$tableName')").collect()
      assertResult(1)(result.length)
      assert(result(0).getString(0) != null && result(0).getString(0).nonEmpty,
        s"start_compaction_time should be a non-empty instant string, got: ${result(0).getString(0)}")
      assert(result(0).getLong(1) >= 0L,
        s"time_taken_in_millis should be non-negative, got: ${result(0).getLong(1)}")

      // Calling the procedure again on a quiesced table should still succeed
      // (it is effectively a no-op when there is nothing left to compact).
      val resultIdempotent = spark.sql(s"call run_timeline_compaction(table => '$tableName')").collect()
      assertResult(1)(resultIdempotent.length)
    }
  }

  test("Test Call run_timeline_compaction Procedure by Path") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           | id int,
           | name string,
           | price double,
           | ts long
           | ) using hudi
           | location '${tmp.getCanonicalPath}'
           | tblproperties (
           |   primaryKey = 'id',
           |   type = 'cow',
           |   orderingFields = 'ts',
           |   hoodie.metadata.enable = 'false'
           | )
           |""".stripMargin)

      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")

      val result = spark.sql(
        s"call run_timeline_compaction(path => '${tmp.getCanonicalPath}')").collect()
      assertResult(1)(result.length)
      assert(result(0).getString(0) != null && result(0).getString(0).nonEmpty)
      assert(result(0).getLong(1) >= 0L)
    }
  }
}
