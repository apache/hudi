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

class TestArchiveCommitsProcedure extends HoodieSparkProcedureTestBase {

  test("Test Call archive_commits Procedure by Table") {
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
           |   hoodie.metadata.enable = "false"
           | )
           |""".stripMargin)

      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 20, 2000)")
      spark.sql(s"insert into $tableName values(3, 'a3', 30, 3000)")
      spark.sql(s"insert into $tableName values(4, 'a4', 40, 4000)")
      spark.sql(s"insert into $tableName values(5, 'a5', 50, 5000)")
      spark.sql(s"insert into $tableName values(6, 'a6', 60, 6000)")

      val result1 = spark.sql(s"call archive_commits(table => '$tableName'" +
        s", min_commits => 2, max_commits => 3, retain_commits => 1, enable_metadata => false)")
        .collect()
        .map(row => Seq(row.getInt(0)))
      assertResult(1)(result1.length)
      assertResult(0)(result1(0).head)

      // collect active commits for table
      val commits = spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
      assertResult(2) {
        commits.length
      }

      // collect archived commits for table
      val endTs = commits(0).get(0).toString
      val archivedCommits = spark.sql(s"""call show_archived_commits(table => '$tableName', end_ts => '$endTs')""").collect()
      assertResult(4) {
        archivedCommits.length
      }
    }
  }
}
