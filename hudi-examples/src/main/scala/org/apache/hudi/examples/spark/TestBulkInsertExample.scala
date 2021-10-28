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

package org.apache.hudi.examples.spark

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.{HoodieIndexConfig, HoodieStorageConfig, HoodieWriteConfig}
import org.apache.hudi.HoodieWriterUtils
import org.apache.hudi.index.HoodieIndex
import org.apache.spark.sql.SparkSession

// TODO More general example
object TestBulkInsertExample {

  def main(args: Array[String]): Unit = {
    val dbPath = args(0)
    val dbName = args(1)
    val tableName = args(2)
    val sql = args(3)
    val path = dbPath + tableName

    val spark = SparkSession.builder.master("yarn")
      .appName(s"bulk_insert_$tableName")
      .config("spark.yarn.maxAppAttempts", "1")
      .config("test.tablePath", path)
      .config("test.table", tableName)
      .enableHiveSupport().getOrCreate()
    
    //create a new table
    val fooTableModifier = Map(
      "path" -> path,
      HoodieWriteConfig.BASE_PATH_PROP -> path,
      HoodieWriteConfig.TABLE_NAME -> tableName,
      "hoodie.bulkinsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.OPERATION_OPT_KEY -> DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL,
      DataSourceWriteOptions.ENABLE_ROW_WRITER_OPT_KEY -> "true",
      DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> "user_id",
      DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> "local_time_ms",
      DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> "date,hour,app,event",
      DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY -> "true",
      DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY -> "org.apache.hudi.keygen.ComplexKeyGenerator",
      DataSourceWriteOptions.TABLE_TYPE_OPT_KEY -> DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL,
      DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY -> "true",
      DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY -> dbName,
      DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY -> "date,hour,app,event",
      DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY -> "org.apache.hudi.hive.MultiPartKeysValueExtractor",
      DataSourceWriteOptions.HIVE_USE_JDBC_OPT_KEY -> "false",
      HoodieStorageConfig.PARQUET_FILE_MAX_BYTES -> "2147483648",
      HoodieIndexConfig.INDEX_TYPE_PROP -> HoodieIndex.IndexType.NON_INDEX.toString)
    val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)

    spark.sql(sql)
      .write
      .format("org.apache.hudi")
      .options(fooTableParams)
      .mode(org.apache.spark.sql.SaveMode.Append)
      .save(path)
  }
}
