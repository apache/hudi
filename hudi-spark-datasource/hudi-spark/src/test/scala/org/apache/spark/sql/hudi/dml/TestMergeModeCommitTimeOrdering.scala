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

package org.apache.spark.sql.hudi.dml

import org.apache.hudi.common.config.RecordMergeMode.COMMIT_TIME_ORDERING
import org.apache.hudi.common.model.HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.testutils.HoodieTestUtils

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase.validateTableConfig

class TestMergeModeCommitTimeOrdering extends HoodieSparkSqlTestBase {

  // TODO(HUDI-8938): add "mor,true,true,6" after the fix
  Seq("mor,6,true,true").foreach { args =>
    val argList = args.split(',')
    val tableType = argList(0)
    val tableVersion = argList(1)
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
        // Current table version (8)
        ", preCombineField = 'ts',\nhoodie.record.merge.mode = 'COMMIT_TIME_ORDERING'"
      }
    } else {
      // By default, the COMMIT_TIME_ORDERING is used if not specified by the user
      ""
    }
    val writeTableVersionClause = if (tableVersion.toInt == 6) {
      s"hoodie.write.table.version = $tableVersion,"
    } else {
      ""
    }
    val expectedMergeConfigs = if (tableVersion.toInt == 6) {
      Map(
        HoodieTableConfig.VERSION.key -> "6",
        HoodieTableConfig.PAYLOAD_CLASS_NAME.key -> classOf[OverwriteWithLatestAvroPayload].getName)
    } else {
      Map(
        HoodieTableConfig.VERSION.key -> "8",
        HoodieTableConfig.RECORD_MERGE_MODE.key -> COMMIT_TIME_ORDERING.name(),
        HoodieTableConfig.PAYLOAD_CLASS_NAME.key -> classOf[OverwriteWithLatestAvroPayload].getName,
        HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key -> COMMIT_TIME_BASED_MERGE_STRATEGY_UUID)
    }
    val nonExistentConfigs = if (tableVersion.toInt == 6) {
      Seq(HoodieTableConfig.RECORD_MERGE_MODE.key, HoodieTableConfig.PRECOMBINE_FIELD.key)
    } else {
      if (setRecordMergeConfigs) {
        Seq()
      } else {
        Seq(HoodieTableConfig.PRECOMBINE_FIELD.key)
      }
    }

    test(s"Test $tableType table with COMMIT_TIME_ORDERING (tableVersion=$tableVersion,"
      + s"setRecordMergeConfigs=$setRecordMergeConfigs,setUpsertOperation=$setUpsertOperation)") {
      withSparkSqlSessionConfigWithCondition(
        ("hoodie.merge.small.file.group.candidates.limit" -> "0", true),
        ("hoodie.spark.sql.insert.into.operation" -> "upsert", setUpsertOperation),
        // TODO(HUDI-8820): enable MDT after supporting MDT with table version 6
        ("hoodie.metadata.enable" -> "false", tableVersion.toInt == 6),
        ("hoodie.file.group.reader.enabled" -> "true", true),
        ("hoodie.merge.use.record.positions" -> "false", true)
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
                 | where id = 2
             """.stripMargin)
            validateTableConfig(
              storage, tmp.getCanonicalPath, expectedMergeConfigs, nonExistentConfigs)
            checkAnswer(s"select id, name, price, ts from $tableName order by id")(
              Seq(1, "A_equal", 50.0, 100),
              Seq(2, "B_equal", 70.0, 100))
          }
        })
      }
    }
  }
}
