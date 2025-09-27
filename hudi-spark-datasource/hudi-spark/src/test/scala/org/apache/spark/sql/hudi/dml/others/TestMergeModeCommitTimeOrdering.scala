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

import org.apache.hudi.common.config.RecordMergeMode.COMMIT_TIME_ORDERING
import org.apache.hudi.common.model.HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableVersion}
import org.apache.hudi.common.testutils.HoodieTestUtils

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase.validateTableConfig

class TestMergeModeCommitTimeOrdering extends HoodieSparkSqlTestBase {

  Seq(
    "cow,current,false,false", "cow,current,false,true", "cow,current,true,false",
    "mor,current,false,false", "mor,current,false,true", "mor,current,true,false",
    "cow,6,true,false", "cow,6,true,true", "mor,6,true,true",
    "cow,8,true,false", "cow,8,true,true", "mor,8,true,true").foreach { args =>
    val argList = args.split(',')
    val tableType = argList(0)
    val tableVersion = if (argList(1).equals("current")) {
      String.valueOf(HoodieTableVersion.current().versionCode())
    } else {
      argList(1)
    }
    val setRecordMergeConfigs = argList(2).toBoolean
    val setUpsertOperation = argList(3).toBoolean
    val isUpsert = setUpsertOperation || (tableVersion.toInt != 6 && setRecordMergeConfigs)
    val storage = HoodieTestUtils.getDefaultStorage
    val mergeConfigClause = if (setRecordMergeConfigs) {
      // with precombine field set, UPSERT operation is used automatically
      if (tableVersion.toInt == 6) {
        // Table version 6
        s", payloadClass = '${classOf[OverwriteWithLatestAvroPayload].getName}'"
      } else {
        // Current table version >= 8
        ", preCombineField = 'ts',\nhoodie.record.merge.mode = 'COMMIT_TIME_ORDERING'"
      }
    } else {
      // By default, the COMMIT_TIME_ORDERING is used if not specified by the user
      ""
    }
    val writeTableVersionClause = if (tableVersion.toInt < HoodieTableVersion.current().versionCode()) {
      s"hoodie.write.table.version = $tableVersion,"
    } else {
      ""
    }
    val expectedMergeConfigs: Map[String, String] = tableVersion.toInt match {
      case 6 =>
        Map(
          HoodieTableConfig.VERSION.key -> "6",
          HoodieTableConfig.PAYLOAD_CLASS_NAME.key -> classOf[OverwriteWithLatestAvroPayload].getName
        )
      case 8 =>
        Map(
          HoodieTableConfig.VERSION.key -> tableVersion,
          HoodieTableConfig.RECORD_MERGE_MODE.key -> COMMIT_TIME_ORDERING.name(),
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key -> COMMIT_TIME_BASED_MERGE_STRATEGY_UUID
        )
      case _ =>
        Map(
          HoodieTableConfig.VERSION.key -> tableVersion,
          HoodieTableConfig.RECORD_MERGE_MODE.key -> COMMIT_TIME_ORDERING.name()
        )
    }
    val nonExistentConfigs = if (tableVersion.toInt == 6) {
      Seq(HoodieTableConfig.RECORD_MERGE_MODE.key, HoodieTableConfig.ORDERING_FIELDS.key)
    } else {
      if (setRecordMergeConfigs) {
        Seq()
      } else {
        Seq(HoodieTableConfig.ORDERING_FIELDS.key)
      }
    }

    test(s"Test $tableType table with COMMIT_TIME_ORDERING (tableVersion=$tableVersion,"
      + s"setRecordMergeConfigs=$setRecordMergeConfigs,setUpsertOperation=$setUpsertOperation)") {
      withSparkSqlSessionConfigWithCondition(
        ("hoodie.merge.small.file.group.candidates.limit" -> "0", true),
        ("hoodie.spark.sql.insert.into.operation" -> "upsert", setUpsertOperation),
        // TODO(HUDI-8820): enable MDT after supporting MDT with table version 6
        ("hoodie.metadata.enable" -> "false", tableVersion.toInt == 6)
      ) {
        withRecordType()(withTempDir { tmp =>
          val tableName = generateTableName
          // Create table with COMMIT_TIME_ORDERING
          spark.sql(
            s"""
               | create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               | ) using hudi
               | tblproperties (
               |  $writeTableVersionClause
               |  type = '$tableType',
               |  primaryKey = 'id'
               |  $mergeConfigClause
               | )
               | location '${tmp.getCanonicalPath}'
             """.stripMargin)
          validateTableConfig(
            storage, tmp.getCanonicalPath, expectedMergeConfigs, nonExistentConfigs)
          // Insert initial records with ts=100
          spark.sql(
            s"""
               | insert into $tableName
               | select 1 as id, 'A' as name, 10.0 as price, 100 as ts
               | union all
               | select 2, 'B', 20.0, 100
             """.stripMargin)

          // Verify inserting records with the same ts value are visible (COMMIT_TIME_ORDERING)
          spark.sql(
            s"""
               | insert into $tableName
               | select 1 as id, 'A_equal' as name, 60.0 as price, 100 as ts
               | union all
               | select 2, 'B_equal', 70.0, 100
            """.stripMargin)
          validateTableConfig(
            storage, tmp.getCanonicalPath, expectedMergeConfigs, nonExistentConfigs)
          checkAnswer(s"select id, name, price, ts from $tableName order by id")(
            (if (isUpsert) {
              // With UPSERT operation, there is no duplicate
              Seq(
                Seq(1, "A_equal", 60.0, 100),
                Seq(2, "B_equal", 70.0, 100))
            } else {
              // With INSERT operation, there are duplicates
              Seq(
                Seq(1, "A", 10.0, 100),
                Seq(1, "A_equal", 60.0, 100),
                Seq(2, "B", 20.0, 100),
                Seq(2, "B_equal", 70.0, 100))
            }): _*)

          if (isUpsert) {
            // Verify updating records with the same ts value are visible (COMMIT_TIME_ORDERING)
            spark.sql(
              s"""
                 | update $tableName
                 | set price = 50.0, ts = 100
                 | where id = 1
             """.stripMargin)
            validateTableConfig(
              storage, tmp.getCanonicalPath, expectedMergeConfigs, nonExistentConfigs)
            checkAnswer(s"select id, name, price, ts from $tableName order by id")(
              Seq(1, "A_equal", 50.0, 100),
              Seq(2, "B_equal", 70.0, 100))

            // Verify inserting records with a lower ts value are visible (COMMIT_TIME_ORDERING)
            spark.sql(
              s"""
                 | insert into $tableName
                 | select 1 as id, 'A' as name, 30.0 as price, 99 as ts
                 | union all
                 | select 2, 'B', 40.0, 99
             """.stripMargin)

            checkAnswer(s"select id, name, price, ts from $tableName order by id")(
              Seq(1, "A", 30.0, 99),
              Seq(2, "B", 40.0, 99))

            // Verify updating records with a lower ts value are visible (COMMIT_TIME_ORDERING)
            spark.sql(
              s"""
                 | update $tableName
                 | set price = 50.0, ts = 98
                 | where id = 1
             """.stripMargin)

            checkAnswer(s"select id, name, price, ts from $tableName order by id")(
              Seq(1, "A", 50.0, 98),
              Seq(2, "B", 40.0, 99))

            // Verify inserting records with a higher ts value are visible (COMMIT_TIME_ORDERING)
            spark.sql(
              s"""
                 | insert into $tableName
                 | select 1 as id, 'A' as name, 30.0 as price, 101 as ts
                 | union all
                 | select 2, 'B', 40.0, 101
             """.stripMargin)

            // Verify records with ts=101 are visible
            checkAnswer(s"select id, name, price, ts from $tableName order by id")(
              Seq(1, "A", 30.0, 101),
              Seq(2, "B", 40.0, 101)
            )

            // Verify updating records with a higher ts value are visible (COMMIT_TIME_ORDERING)
            spark.sql(
              s"""
                 | update $tableName
                 | set price = 50.0, ts = 102
                 | where id = 1
             """.stripMargin)

            // Verify final state after all operations
            checkAnswer(s"select id, name, price, ts from $tableName order by id")(
              Seq(1, "A", 50.0, 102),
              Seq(2, "B", 40.0, 101)
            )

            // Delete record
            spark.sql(s"delete from $tableName where id = 1")
            if (tableType == "mor") {
              HoodieSparkSqlTestBase.validateDeleteLogBlockPrecombineNullOrZero(tmp.getCanonicalPath)
            }
            validateTableConfig(
              storage, tmp.getCanonicalPath, expectedMergeConfigs, nonExistentConfigs)
            // Verify deletion
            checkAnswer(s"select id, name, price, ts from $tableName order by id")(
              Seq(2, "B", 40.0, 101)
            )
          }
        })
      }
    }

    // TODO(HUDI-8468): add COW test after supporting COMMIT_TIME_ORDERING in MERGE INTO for COW
    test(s"Test merge operations with COMMIT_TIME_ORDERING for $tableType table "
      + s"(tableVersion=$tableVersion,setRecordMergeConfigs=$setRecordMergeConfigs,"
      + s"setUpsertOperation=$setUpsertOperation)") {
      withSparkSqlSessionConfigWithCondition(
        ("hoodie.merge.small.file.group.candidates.limit" -> "0", true),
        ("hoodie.spark.sql.insert.into.operation" -> "upsert", setUpsertOperation),
        // TODO(HUDI-8820): enable MDT after supporting MDT with table version 6
        ("hoodie.metadata.enable" -> "false", tableVersion.toInt == 6)
      ) {
        withRecordType()(withTempDir { tmp =>
          val tableName = generateTableName
          // Create table with COMMIT_TIME_ORDERING
          spark.sql(
            s"""
               | create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               | ) using hudi
               | tblproperties (
               |  $writeTableVersionClause
               |  type = '$tableType',
               |  primaryKey = 'id'
               |  $mergeConfigClause
               | )
               | location '${tmp.getCanonicalPath}'
           """.stripMargin)
          validateTableConfig(
            storage, tmp.getCanonicalPath, expectedMergeConfigs, nonExistentConfigs)

          // Insert initial records
          spark.sql(
            s"""
               | insert into $tableName
               | select 1 as id, 'A' as name, 10.0 as price, 100L as ts union all
               | select 0, 'X', 20.0, 100L union all
               | select 2, 'B', 20.0, 100L union all
               | select 3, 'C', 30.0, 100L union all
               | select 4, 'D', 40.0, 100L union all
               | select 5, 'E', 50.0, 100L union all
               | select 6, 'F', 60.0, 100L
           """.stripMargin)
          validateTableConfig(
            storage, tmp.getCanonicalPath, expectedMergeConfigs, nonExistentConfigs)

          // TODO(HUDI-8840): enable MERGE INTO with deletes
          val shouldTestMergeIntoDelete = setRecordMergeConfigs && (tableVersion.toInt >= 8)
          // Merge operation - delete with higher, lower and equal ordering field value, all should take effect.
          if (shouldTestMergeIntoDelete) {
            spark.sql(
              s"""
                 | merge into $tableName t
                 | using (
                 |   select 1 as id, 'B2' as name, 25.0 as price, 101L as ts union all
                 |   select 2, '', 55.0, 99L as ts union all
                 |   select 0, '', 55.0, 100L as ts
                 | ) s
                 | on t.id = s.id
                 | when matched then delete
           """.stripMargin)
          }

          // Merge operation - update with mixed ts values
          spark.sql(
            s"""
               | merge into $tableName t
               | using (
               |   select 4 as id, 'D2' as name, 45.0 as price, 101L as ts union all
               |   select 5, 'E2', 55.0, 99L as ts union all
               |   select 6, 'F2', 65.0, 100L as ts
               | ) s
               | on t.id = s.id
               | when matched then update set *
           """.stripMargin)

          // Verify state after merges
          validateTableConfig(
            storage, tmp.getCanonicalPath, expectedMergeConfigs, nonExistentConfigs)
          val nonDeletedRows: Seq[Seq[Any]] = if (shouldTestMergeIntoDelete) {
            Seq()
          } else {
            Seq(Seq(0, "X", 20.0, 100),
              Seq(1, "A", 10.0, 100),
              Seq(2, "B", 20.0, 100))
          }
          checkAnswer(s"select id, name, price, ts from $tableName order by id")(
            (nonDeletedRows ++ Seq(
              Seq(3, "C", 30.0, 100),
              Seq(4, "D2", 45.0, 101),
              Seq(5, "E2", 55.0, 99),
              Seq(6, "F2", 65.0, 100)
            )): _*)

          // Insert new records through merge
          spark.sql(
            s"""
               | merge into $tableName t
               | using (
               |   select 7 as id, 'D2' as name, 45.0 as price, 100L as ts union all
               |   select 8, 'E2', 55.0, 100L as ts
               | ) s
               | on t.id = s.id
               | when not matched then insert *
           """.stripMargin)

          // Verify final state
          validateTableConfig(
            storage, tmp.getCanonicalPath, expectedMergeConfigs, nonExistentConfigs)
          checkAnswer(s"select id, name, price, ts from $tableName order by id")(
            (nonDeletedRows ++ Seq(
              Seq(3, "C", 30.0, 100),
              Seq(4, "D2", 45.0, 101),
              Seq(5, "E2", 55.0, 99),
              Seq(6, "F2", 65.0, 100),
              Seq(7, "D2", 45.0, 100),
              Seq(8, "E2", 55.0, 100)
            )): _*)
        })
      }
    }
  }
}
