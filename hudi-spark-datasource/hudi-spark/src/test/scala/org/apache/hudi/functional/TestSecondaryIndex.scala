/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL
import org.apache.hudi.common.model.HoodieTableType
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

class TestSecondaryIndex extends RecordLevelIndexTestBase {
  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testSIInitialization(tableType: HoodieTableType): Unit = {
    //create a new table
    val tableName = "trips_table"
    val basePath = "file:///tmp/trips_table"
    val columns = Seq("ts", "uuid", "rider", "driver", "fare", "city", "state")
    val data =
      Seq((1695159649087L, "334e26e9-8355-45cc-97c6-c31daf0df330", "rider-A", "driver-K", 19.10, "san_francisco", "california"),
        (1695091554787L, "e96c4396-3fad-413a-a942-4cb36106d720", "rider-B", "driver-M", 27.70, "sao_paulo", "texas"),
        (1695091554788L, "e96c4396-3fad-413a-a942-4cb36106d721", "rider-C", "driver-K", 27.70, "san_francisco", "california"),
        (1695046462179L, "9909a8b1-2d15-4d3d-8ec9-efc48c536a00", "rider-D", "driver-L", 33.90, "san_francisco", "california"),
        (1695516137016L, "e3cf430c-889d-4015-bc98-59bdce1e530c", "rider-E", "driver-P", 34.15, "sao_paulo", "texas"),
        (1695115999911L, "c8abbe79-8d89-47ea-b4ce-4d224bae5bfa", "rider-F", "driver-T", 17.85, "chennai", "tamil-nadu"));

    val inserts = spark.createDataFrame(data).toDF(columns: _*)
    inserts.write.format("hudi").
      option(DataSourceWriteOptions.TABLE_TYPE.key, tableType.name()).
      option("hoodie.datasource.write.operation", "insert").
      options(commonOptsWithSecondaryIndexSITest).
      mode(SaveMode.Overwrite).
      save(basePath)

    val tripsDF = spark.read.format("hudi").load(basePath)
    assertEquals(tripsDF.count(), data.length)
    tripsDF.show()

    val metadataDF = spark.sql("select * from  hudi_metadata('file:///tmp/trips_table') where type = 6")
    metadataDF.show(false)

    //TODO (vinay): Fix this assert once the modified HFile reader is available
    assertEquals(metadataDF.count(), tripsDF.select("city").distinct().count())
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testSIUpsert(tableType: HoodieTableType): Unit = {
    val tableName = "trips_table"
    val basePath = "file:///tmp/trips_table"
    val columns = Seq("ts", "uuid", "rider", "driver", "fare", "city", "state")
    val data1 =
      Seq((1695159649087L, "334e26e9-8355-45cc-97c6-c31daf0df330", "rider-A", "driver-K", 19.10, "san_francisco", "california"),
        (1695091554787L, "e96c4396-3fad-413a-a942-4cb36106d720", "rider-B", "driver-M", 27.70, "sao_paulo", "texas"),
        (1695091554788L, "e96c4396-3fad-413a-a942-4cb36106d721", "rider-C", "driver-K", 27.70, "delhi", "delhi"),
        (1695115999911L, "c8abbe79-8d89-47ea-b4ce-4d224bae5bfa", "rider-F", "driver-T", 17.85, "chennai", "tamil-nadu"));

    var inserts = spark.createDataFrame(data1).toDF(columns: _*)
    inserts.write.format("hudi").
      option(DataSourceWriteOptions.TABLE_TYPE.key, tableType.name()).
      option("hoodie.datasource.write.operation", "insert").
      options(commonOptsNewTableSITest).
      mode(SaveMode.Overwrite).
      save(basePath)

    val metadataDF = spark.sql("select * from  hudi_metadata('file:///tmp/trips_table') where type=6")
    metadataDF.show(false)
    assertEquals(metadataDF.count(), 0)

    val data2 =
      Seq((1695046462179L, "9909a8b1-2d15-4d3d-8ec9-efc48c536a00", "rider-D", "driver-L", 33.90, "london", "greater-london"),
        (1695516137016L, "e3cf430c-889d-4015-bc98-59bdce1e530c", "rider-E", "driver-P", 34.15, "austin", "texas"));

    val inserts2 = spark.createDataFrame(data2).toDF(columns: _*)
    inserts2.write.format("hudi").
      option(DataSourceWriteOptions.TABLE_TYPE.key, tableType.name()).
      option("hoodie.datasource.write.operation", "upsert").
      options(commonOptsWithSecondaryIndexSITest).
      mode(SaveMode.Append).
      save(basePath)

    val tripsDF2 = spark.read.format("hudi").load(basePath)
    tripsDF2.show()
    assertEquals(tripsDF2.count(), data1.length + data2.length)

    val metadataDF2 = spark.sql("select * from  hudi_metadata('file:///tmp/trips_table') where type = 6")
    metadataDF2.show(false)
    assertEquals(metadataDF2.count(), tripsDF2.count())
  }

  @Test
  def testSIWithDelete(): Unit = {
    val tableName = "trips_table2"
    val basePath = "file:///tmp/trips_table2"
    val columns = Seq("ts", "uuid", "rider", "driver", "fare", "city", "state")
    val data1 =
      Seq((1695159649087L, "334e26e9-8355-45cc-97c6-c31daf0df330", "rider-A", "driver-K", 19.10, "san_francisco", "california"),
        (1695091554787L, "e96c4396-3fad-413a-a942-4cb36106d720", "rider-B", "driver-M", 27.70, "sao_paulo", "texas"),
        (1695091554788L, "e96c4396-3fad-413a-a942-4cb36106d721", "rider-C", "driver-K", 27.70, "delhi", "delhi"),
        (1695115999911L, "c8abbe79-8d89-47ea-b4ce-4d224bae5bfa", "rider-F", "driver-T", 17.85, "chennai", "tamil-nadu"));

    var inserts = spark.createDataFrame(data1).toDF(columns: _*)
    inserts.write.format("hudi").
      option(DataSourceWriteOptions.TABLE_TYPE.key, HoodieTableType.COPY_ON_WRITE.name()).
      option("hoodie.datasource.write.operation", "insert").
      options(commonOptsWithSecondaryIndexSITest).
      mode(SaveMode.Overwrite).
      save(basePath)

    var metadataDF = spark.sql("select key, type, recordIndexMetadata, SecondaryIndexMetadata  from  hudi_metadata('file:///tmp/trips_table2') where type in (5,6)")
    metadataDF.show(false)

/*    val data2 =
      Seq((1695159649088L, "334e26e9-8355-45cc-97c6-c31daf0df331", "rider-A", "driver-K", 19.10, "los-angeles", "california"))
    var inserts2 = spark.createDataFrame(data2).toDF(columns: _*)
    inserts2.write.format("hudi").
      option(DataSourceWriteOptions.TABLE_TYPE.key, HoodieTableType.COPY_ON_WRITE.name()).
      option("hoodie.datasource.write.operation", "insert").
      options(commonOptsWithSecondaryIndexSITest).
      mode(SaveMode.Append).
      save(basePath)*/

    val deleteDf = inserts.limit(1)
    deleteDf.show(false)
    deleteDf.write.format("org.apache.hudi")
      .option(DataSourceWriteOptions.TABLE_TYPE.key, HoodieTableType.COPY_ON_WRITE.name())
      .option(DataSourceWriteOptions.OPERATION.key, DELETE_OPERATION_OPT_VAL)
      .options(commonOptsWithSecondaryIndexSITest)
      .mode(SaveMode.Append)
      .save(basePath)

    metadataDF = spark.sql("select key, type, recordIndexMetadata, SecondaryIndexMetadata from  hudi_metadata('file:///tmp/trips_table2') where type in (5,6)")
    metadataDF.show(false)
  }
}
