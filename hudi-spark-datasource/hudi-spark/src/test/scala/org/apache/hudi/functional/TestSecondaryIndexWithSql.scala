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
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{Tag, Test}
import org.scalatest.Assertions.assertResult

/**
 * Test cases for secondary index
 */
@Tag("functional")
class TestSecondaryIndexWithSql extends SecondaryIndexTestBase {

  @Test
  def testSecondaryIndexWithSQL(): Unit = {
    if (HoodieSparkUtils.gteqSpark3_2) {
      spark.sql(
        s"""
           |create table $tableName (
           |  ts bigint,
           |  id string,
           |  rider string,
           |  driver string,
           |  fare int,
           |  city string,
           |  state string
           |) using hudi
           | options (
           |  primaryKey ='id',
           |  type = 'mor',
           |  preCombineField = 'ts',
           |  hoodie.metadata.enable = 'true',
           |  hoodie.metadata.record.index.enable = 'true',
           |  hoodie.metadata.index.secondary.enable = 'true',
           |  hoodie.datasource.write.recordkey.field = 'id'
           | )
           | partitioned by(state)
           | location '$basePath'
       """.stripMargin)
      spark.sql(
        s"""
           | insert into $tableName
           | values
           | (1695159649087, '334e26e9-8355-45cc-97c6-c31daf0df330', 'rider-A', 'driver-K', 19, 'san_francisco', 'california'),
           | (1695091554787, 'e96c4396-3fad-413a-a942-4cb36106d720', 'rider-B', 'driver-M', 27, 'austin', 'texas')
           | """.stripMargin
      )

      // validate record_index created successfully
      val metadataDF = spark.sql(s"select key from hudi_metadata('$basePath') where type=5")
      assert(metadataDF.count() == 2)

      var metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(HoodieTestUtils.getDefaultStorageConf)
        .build()
      assert(metaClient.getTableConfig.getMetadataPartitions.contains("record_index"))
      // create secondary index
      spark.sql(s"create index idx_city on $tableName using secondary_index(city)")
      metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(HoodieTestUtils.getDefaultStorageConf)
        .build()
      assert(metaClient.getTableConfig.getMetadataPartitions.contains("secondary_index_idx_city"))
      assert(metaClient.getTableConfig.getMetadataPartitions.contains("record_index"))

      checkAnswer(s"select key, SecondaryIndexMetadata.recordKey from hudi_metadata('$basePath') where type=7")(
        Seq("austin", "e96c4396-3fad-413a-a942-4cb36106d720"),
        Seq("san_francisco", "334e26e9-8355-45cc-97c6-c31daf0df330")
      )
    }
  }

  private def checkAnswer(sql: String)(expects: Seq[Any]*): Unit = {
    assertResult(expects.map(row => Row(row: _*)).toArray.sortBy(_.toString()))(spark.sql(sql).collect().sortBy(_.toString()))
  }

  @Test
  def testSecondaryIndexWithInFilter(): Unit = {
    if (HoodieSparkUtils.gteqSpark3_2) {
      var hudiOpts = commonOpts
      hudiOpts = hudiOpts + (
        DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.COPY_ON_WRITE.name(),
        DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true")

      spark.sql(
        s"""
           |create table $tableName (
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
      spark.sql(s"insert into $tableName values('row1', 'abc', 'p1')")
      spark.sql(s"insert into $tableName values('row2', 'cde', 'p2')")
      spark.sql(s"insert into $tableName values('row3', 'def', 'p2')")
      // create secondary index
      spark.sql(s"create index idx_not_record_key_col on $tableName using secondary_index(not_record_key_col)")

      assertEquals(0, spark.read.format("hudi").options(hudiOpts).load(basePath).filter("not_record_key_col in ('abc', 'cde')").count())
    }
  }
}
