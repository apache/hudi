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

package org.apache.hudi.error

import org.apache.hudi.common.model.OverwriteWithLatestAvroSchemaPayload
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructField, _}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

class TestErrorTableDataSource extends HoodieClientTestBase {
  var spark: SparkSession = null
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.RECORDKEY_FIELD.key() -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key() -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key() -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key() -> "hoodie_test"
  )

  @BeforeEach override def setUp() {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initFileSystem()
  }

  @AfterEach override def tearDown() = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  @Test
  def testErrorTable(): Unit = {

    val schema1 = StructType(StructField("_row_key", StringType, true) :: StructField("name", StringType, true)::
      StructField("timestamp", IntegerType, true):: StructField("age", StringType, true)  :: StructField("partition", IntegerType, true)::Nil)
    val records1 = Array("{\"_row_key\":\"1\",\"name\":\"lisi\",\"age\":\"31\",\"timestamp\":1,\"partition\":1}",
      "{\"_row_key\":\"2\",\"name\":\"liujinhui\",\"age\":\"26\",\"timestamp\":2,\"partition\":1}")
    val inputDF1 = spark.read.schema(schema1.toDDL).json(spark.sparkContext.parallelize(records1, 1))

    inputDF1.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key(), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
      .option("hoodie.write.error.table.enabled", true)
      .option("hoodie.memory.writestatus.failure.fraction", 1.0)
      .mode(SaveMode.Append)
      .save(basePath)

    val schema2 = StructType(StructField("_row_key", StringType, true) :: StructField("name", StringType, true)::
       StructField("timestamp", IntegerType, true) :: StructField("age", StringType, true)  :: StructField("partition", IntegerType, true)::Nil)
    val records2 = Array("{\"_row_key\":\"1\",\"name\":\"lisi\",\"age\":\"31\",\"partition\":1}",
      "{\"_row_key\":\"2\",\"name\":\"liujinhui\",\"age\":\"26\",\"partition\":1}")
    val inputDF2 = spark.read.schema(schema2.toDDL).json(spark.sparkContext.parallelize(records2, 1))

    inputDF2.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key(), DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
      .option("hoodie.write.error.table.enabled", true)
      .option("hoodie.memory.writestatus.failure.fraction", 1.0)
      .option("hoodie.datasource.write.payload.class", classOf[OverwriteWithLatestAvroSchemaPayload].getName)
      .mode(SaveMode.Append)
      .save(basePath)

    val hudiRODF1 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/" + HoodieTableMetaClient.ERROR_TABLE_FOLDER_NAME  + "/*/*/*/")
      assertEquals(2, hudiRODF1.count())


    val hudiRODF2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*")
    assertEquals(2, hudiRODF2.count())
  }
}
