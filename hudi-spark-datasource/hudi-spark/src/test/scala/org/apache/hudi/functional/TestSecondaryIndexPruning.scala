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

import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieSparkUtils}
import org.apache.spark.sql.Row
import org.junit.jupiter.api.{Tag, Test}
import org.scalatest.Assertions.assertResult

/**
 * Test cases for secondary index
 */
@Tag("functional")
class TestSecondaryIndexPruning extends SecondaryIndexTestBase {

  @Test
  def testSecondaryIndexWithFilters(): Unit = {
    if (HoodieSparkUtils.gteqSpark3_2) {
      var hudiOpts = commonOpts
      hudiOpts = hudiOpts + (
        DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.COPY_ON_WRITE.name(),
        DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true")

      spark.sql(
        s"""
           |create table $tableName (
           |  ts bigint,
           |  record_key_col string,
           |  not_record_key_col string,
           |  partition_key_col string
           |) using hudi
           | options (
           |  primaryKey ='record_key_col',
           |  hoodie.metadata.enable = 'true',
           |  hoodie.metadata.record.index.enable = 'true',
           |  hoodie.datasource.write.recordkey.field = 'record_key_col',
           |  hoodie.enable.data.skipping = 'true'
           | )
           | partitioned by(partition_key_col)
           | location '$basePath'
       """.stripMargin)
      spark.sql(s"insert into $tableName values(1, 'row1', 'abc', 'p1')")
      spark.sql(s"insert into $tableName values(2, 'row2', 'cde', 'p2')")
      spark.sql(s"insert into $tableName values(3, 'row3', 'def', 'p2')")
      // create secondary index
      spark.sql(s"create index idx_not_record_key_col on $tableName using secondary_index(not_record_key_col)")
      // validate index created successfully
      metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(HoodieTestUtils.getDefaultStorageConf)
        .build()
      assert(metaClient.getTableConfig.getMetadataPartitions.contains("secondary_index_idx_not_record_key_col"))
      // validate data skipping
      verifyQueryPredicate(hudiOpts, "not_record_key_col")
      // validate the secondary index records themselves
      checkAnswer(s"select key, SecondaryIndexMetadata.recordKey from hudi_metadata('$basePath') where type=7")(
        Seq("abc", "row1"),
        Seq("cde", "row2"),
        Seq("def", "row3")
      )

      // create another secondary index on non-string column
      spark.sql(s"create index idx_ts on $tableName using secondary_index(ts)")
      // validate index created successfully
      metaClient = HoodieTableMetaClient.reload(metaClient)
      assert(metaClient.getTableConfig.getMetadataPartitions.contains("secondary_index_idx_ts"))
      // validate data skipping
      verifyQueryPredicate(hudiOpts, "ts")
      // validate the secondary index records themselves
      checkAnswer(s"select key, SecondaryIndexMetadata.recordKey from hudi_metadata('$basePath') where type=7")(
        Seq("1", "row1"),
        Seq("2", "row2"),
        Seq("3", "row3"),
        Seq("abc", "row1"),
        Seq("cde", "row2"),
        Seq("def", "row3")
      )
    }
  }

  private def checkAnswer(sql: String)(expects: Seq[Any]*): Unit = {
    assertResult(expects.map(row => Row(row: _*)).toArray.sortBy(_.toString()))(spark.sql(sql).collect().sortBy(_.toString()))
  }
}
