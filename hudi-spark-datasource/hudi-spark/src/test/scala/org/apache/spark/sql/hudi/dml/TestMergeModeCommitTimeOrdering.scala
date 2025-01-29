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

  /*
  "cow,false,false,8", "cow,false,true,8", "cow,true,false,8",
    "cow,true,false,6", "cow,true,true,6",
    "mor,false,false,8", "mor,false,true,8", "mor,true,false,8",
    "mor,true,false,6", "mor,true,true,6"
   */
  //"cow,true,false,6",
  Seq(
//    "cow,false,false,8",
//    "cow,false,true,8",
//    "cow,true,false,8",
//    "cow,true,false,6",
//    "cow,true,true,6",
//    "mor,false,false,8",
//    "mor,false,true,8",
//    "mor,true,false,8",
    "mor,true,false,6", // ============== for this 2 dimensions, if enable MDT there are data loss.
    "mor,true,true,6").foreach { args =>
    val argList = args.split(',')
    val tableType = argList(0)
    val setRecordMergeConfigs = argList(1).toBoolean
    val setUpsertOperation = argList(2).toBoolean
    val tableVersion = argList(3)
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

    // TODO(HUDI-8468): add COW test after supporting COMMIT_TIME_ORDERING in MERGE INTO for COW
    test(s"Test merge operations with COMMIT_TIME_ORDERING for $tableType table "
      + s"(tableVersion=$tableVersion,setRecordMergeConfigs=$setRecordMergeConfigs,"
      + s"setUpsertOperation=$setUpsertOperation)") {
      withSparkSqlSessionConfig("hoodie.merge.small.file.group.candidates.limit" -> "0") {
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
               |  hoodie.metadata.enable = 'false',
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
               | select 3 as id, 'C' as name, 30.0 as price, 100L as ts union all
               | select 4, 'D', 40.0, 100L
           """.stripMargin)
          validateTableConfig(
            storage, tmp.getCanonicalPath, expectedMergeConfigs, nonExistentConfigs)

          // Merge operation - update with mixed ts values
          spark.sql(
            s"""
               | merge into $tableName t
               | using (
               |   select 4 as id, 'D2' as name, 45.0 as price, 101L as ts
               | ) s
               | on t.id = s.id
               | when matched then update set *
           """.stripMargin)

          // Verify state after merges
          validateTableConfig(
            storage, tmp.getCanonicalPath, expectedMergeConfigs, nonExistentConfigs)
          checkAnswer(s"select id, name, price, ts from $tableName order by id")(
            Seq(3, "C", 30.0, 100),
            Seq(4, "D2", 45.0, 101)
          )
          // With MDT on, after MIT update, only record with id 4 is returned.
        })
      }
    }
  }
}
