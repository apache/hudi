/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.procedure

class TestRestoreProcedure extends HoodieSparkProcedureTestBase {

  private def createTableAndInsertData(tableName: String, tablePath: String, tableType: String = "cow"): Array[String] = {
    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long
         |) using hudi
         | location '$tablePath'
         | tblproperties (
         |  primaryKey = 'id',
         |  preCombineField = 'ts',
         |  type = '$tableType'
         | )
       """.stripMargin)
    spark.sql(s"insert into $tableName select 1, 'a1', 10.0, 1000")
    spark.sql(s"insert into $tableName select 2, 'a2', 20.0, 1500")
    spark.sql(s"insert into $tableName select 3, 'a3', 30.0, 2000")
    spark.sql(s"insert into $tableName select 4, 'a4', 40.0, 2500")
    spark.sql(s"call show_commits(table => '$tableName')").collect()
      .map(_.getString(0)).sorted
  }

  test("Test restore_to_instant basic CoW") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName
      val commits = createTableAndInsertData(tableName, tablePath)
      assertResult(4)(commits.length)

      // restore to after the 2nd commit — only commits(0) and commits(1) should remain
      val result = spark.sql(
        s"call restore_to_instant(table => '$tableName', instant_time => '${commits(1)}')"
      ).collect()

      assertResult(1)(result.length)
      assertResult(true)(result(0).getBoolean(0))     // restore_result
      assert(result(0).getString(1) != null)          // start_restore_time (dynamic timestamp)
      assert(result(0).getLong(2) >= 0)               // time_taken_in_millis
      assert(result(0).getLong(3) >= 0L)              // instants_rolled_back
      assertResult(true)(result(0).isNullAt(4))       // audit_result = null (audit not requested)

      // verify data reverted to 2 records
      val count = spark.sql(s"select count(*) from $tableName").collect()(0).getLong(0)
      assertResult(2)(count)
    }
  }

  test("Test restore_to_instant basic MoR") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName
      val commits = createTableAndInsertData(tableName, tablePath, tableType = "mor")
      assertResult(4)(commits.length)

      val result = spark.sql(
        s"call restore_to_instant(table => '$tableName', instant_time => '${commits(1)}')"
      ).collect()

      assertResult(1)(result.length)
      assertResult(true)(result(0).getBoolean(0))
      assert(result(0).getString(1) != null)
      assert(result(0).getLong(2) >= 0)
      assert(result(0).getLong(3) >= 0L)
      assertResult(true)(result(0).isNullAt(4))

      val count = spark.sql(s"select count(*) from $tableName").collect()(0).getLong(0)
      assertResult(2)(count)
    }
  }

  test("Test restore_to_instant using path parameter") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName
      val commits = createTableAndInsertData(tableName, tablePath)
      assertResult(4)(commits.length)

      val result = spark.sql(
        s"call restore_to_instant(path => '$tablePath', instant_time => '${commits(1)}')"
      ).collect()

      assertResult(1)(result.length)
      assertResult(true)(result(0).getBoolean(0))
      assert(result(0).isNullAt(4))

      val count = spark.sql(s"select count(*) from $tableName").collect()(0).getLong(0)
      assertResult(2)(count)
    }
  }

  test("Test restore_to_instant with audit_post_restore") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName
      val commits = createTableAndInsertData(tableName, tablePath)
      assertResult(4)(commits.length)

      val result = spark.sql(
        s"""call restore_to_instant(
           |  table => '$tableName',
           |  instant_time => '${commits(1)}',
           |  audit_post_restore => true
           |)""".stripMargin
      ).collect()

      assertResult(1)(result.length)
      assertResult(true)(result(0).getBoolean(0))     // restore_result
      assert(result(0).getString(1) != null)          // start_restore_time
      // audit_result should be "PASSED": all rolled-back files are absent
      assertResult(false)(result(0).isNullAt(4))
      assertResult("PASSED")(result(0).getString(4))

      val count = spark.sql(s"select count(*) from $tableName").collect()(0).getLong(0)
      assertResult(2)(count)
    }
  }

  test("Test restore_to_instant audit_only mode") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName
      val commits = createTableAndInsertData(tableName, tablePath)
      assertResult(4)(commits.length)

      // Round 1: perform the restore and capture the restore operation's own timeline timestamp
      val restoreRows = spark.sql(
        s"call restore_to_instant(table => '$tableName', instant_time => '${commits(1)}')"
      ).collect()
      assertResult(true)(restoreRows(0).getBoolean(0))
      // start_restore_time is the restore instant's timeline timestamp — NOT commits(1)
      val restoreInstantTs = restoreRows(0).getString(1)
      assert(restoreInstantTs != null)
      assert(restoreInstantTs != commits(1))

      // Round 2: audit_only using restore_instant_time (the original target commit is not needed)
      val auditRows = spark.sql(
        s"""call restore_to_instant(
           |  table => '$tableName',
           |  audit_only => true,
           |  restore_instant_time => '$restoreInstantTs'
           |)""".stripMargin
      ).collect()

      assertResult(1)(auditRows.length)
      assertResult(true)(auditRows(0).isNullAt(0))    // restore_result = null (no restore performed)
      assertResult(true)(auditRows(0).isNullAt(1))    // start_restore_time = null
      assertResult(true)(auditRows(0).isNullAt(2))    // time_taken_in_millis = null
      assertResult(true)(auditRows(0).isNullAt(3))    // instants_rolled_back = null
      assertResult(false)(auditRows(0).isNullAt(4))   // audit_result is present
      assertResult("PASSED")(auditRows(0).getString(4))
    }
  }

  test("Test restore_to_instant audit_only with non-existent restore instant throws") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName
      createTableAndInsertData(tableName, tablePath)

      // restore_instant_time pointing at a timestamp that has never been a restore instant.
      assertThrows[Exception] {
        spark.sql(
          s"""call restore_to_instant(
             |  table => '$tableName',
             |  audit_only => true,
             |  restore_instant_time => '19700101000000000'
             |)""".stripMargin
        ).collect()
      }
    }
  }

  test("Test restore_to_instant cross-validation: audit_only=true requires restore_instant_time") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName
      createTableAndInsertData(tableName, tablePath)

      assertThrows[Exception] {
        spark.sql(
          s"call restore_to_instant(table => '$tableName', audit_only => true)"
        ).collect()
      }
    }
  }

  test("Test restore_to_instant cross-validation: audit_only=false rejects restore_instant_time") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName
      val commits = createTableAndInsertData(tableName, tablePath)

      assertThrows[Exception] {
        spark.sql(
          s"""call restore_to_instant(
             |  table => '$tableName',
             |  instant_time => '${commits(1)}',
             |  restore_instant_time => '20990101000000000'
             |)""".stripMargin
        ).collect()
      }
    }
  }

  test("Test restore_to_instant cross-validation: audit_only=true rejects instant_time") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName
      val commits = createTableAndInsertData(tableName, tablePath)

      assertThrows[Exception] {
        spark.sql(
          s"""call restore_to_instant(
             |  table => '$tableName',
             |  audit_only => true,
             |  instant_time => '${commits(1)}',
             |  restore_instant_time => '20990101000000000'
             |)""".stripMargin
        ).collect()
      }
    }
  }

  test("Test restore_to_instant cross-validation: audit_only=false requires instant_time") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName
      createTableAndInsertData(tableName, tablePath)

      assertThrows[Exception] {
        spark.sql(s"call restore_to_instant(table => '$tableName')").collect()
      }
    }
  }
}
