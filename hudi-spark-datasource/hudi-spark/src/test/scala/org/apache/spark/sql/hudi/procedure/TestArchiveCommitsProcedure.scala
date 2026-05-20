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

  /**
   * Helper: create a fresh COW table at the given location with `numCommits`
   * insert commits already written. Returns the table name.
   */
  private def createTableWithCommits(location: String, numCommits: Int): String = {
    val tableName = generateTableName
    spark.sql(
      s"""
         |create table $tableName (
         | id int,
         | name string,
         | price double,
         | ts long
         | ) using hudi
         | location '$location'
         | tblproperties (
         |   primaryKey = 'id',
         |   type = 'cow',
         |   orderingFields = 'ts',
         |   hoodie.metadata.enable = "false"
         | )
         |""".stripMargin)

    (1 to numCommits).foreach { i =>
      spark.sql(s"insert into $tableName values($i, 'a$i', ${i * 10}, ${i * 1000})")
    }
    tableName
  }

  test("Test Call archive_commits Procedure with named parameters") {
    withTempDir { tmp =>
      val tableName = createTableWithCommits(tmp.getCanonicalPath, 6)

      val result = spark.sql(
        s"call archive_commits(table => '$tableName'," +
          " min_commits => 2, max_commits => 3, retain_commits => 1, enable_metadata => false)")
        .collect()
        .map(row => Seq(row.getInt(0)))
      assertResult(1)(result.length)
      assertResult(0)(result(0).head)

      val commits = spark.sql(s"call show_commits(table => '$tableName', limit => 10)").collect()
      assertResult(2)(commits.length)

      val endTs = commits(0).get(0).toString
      val archived = spark.sql(
        s"call show_archived_commits(table => '$tableName', end_ts => '$endTs')").collect()
      assertResult(4)(archived.length)
    }
  }

  test("Test Call archive_commits Procedure driven only by options") {
    withTempDir { tmp =>
      val tableName = createTableWithCommits(tmp.getCanonicalPath, 6)

      // No min/max named params — archival behavior must come from `options` alone.
      // This used to fail (Expected 2, but got 6) because withArchivalConfig#putAll
      // would overwrite hoodie.keep.min.commits/hoodie.keep.max.commits from
      // user props with the procedure's named-default min=20/max=30.
      val result = spark.sql(
        s"call archive_commits(table => '$tableName'," +
          " retain_commits => 1," +
          " options => 'hoodie.keep.min.commits=2,hoodie.keep.max.commits=3," +
          "hoodie.commits.archival.batch=1,hoodie.metadata.enable=false')")
        .collect()
        .map(row => Seq(row.getInt(0)))
      assertResult(1)(result.length)
      assertResult(0)(result(0).head)

      val commits = spark.sql(s"call show_commits(table => '$tableName', limit => 10)").collect()
      assertResult(2)(commits.length)

      val endTs = commits(0).get(0).toString
      val archived = spark.sql(
        s"call show_archived_commits(table => '$tableName', end_ts => '$endTs')").collect()
      assertResult(4)(archived.length)
    }
  }

  test("Test Call archive_commits Procedure: named parameters override options") {
    withTempDir { tmp =>
      val tableName = createTableWithCommits(tmp.getCanonicalPath, 6)

      // options requests min=10/max=20 (would archive nothing for 6 commits),
      // but named min_commits=2/max_commits=3 must take precedence.
      val result = spark.sql(
        s"call archive_commits(table => '$tableName'," +
          " min_commits => 2, max_commits => 3, retain_commits => 1, enable_metadata => false," +
          " options => 'hoodie.keep.min.commits=10,hoodie.keep.max.commits=20')")
        .collect()
        .map(row => Seq(row.getInt(0)))
      assertResult(1)(result.length)
      assertResult(0)(result(0).head)

      val commits = spark.sql(s"call show_commits(table => '$tableName', limit => 10)").collect()
      // named params won → archival happened, only 2 active commits left
      assertResult(2)(commits.length)

      val endTs = commits(0).get(0).toString
      val archived = spark.sql(
        s"call show_archived_commits(table => '$tableName', end_ts => '$endTs')").collect()
      assertResult(4)(archived.length)
    }
  }

  test("Test Call archive_commits Procedure: invalid options string fails fast") {
    withTempDir { tmp =>
      val tableName = createTableWithCommits(tmp.getCanonicalPath, 2)

      val ex = intercept[IllegalArgumentException] {
        spark.sql(
          s"call archive_commits(table => '$tableName', options => 'invalid_token')")
          .collect()
      }
      assert(ex.getMessage.contains("Invalid options format"))
    }
  }
}