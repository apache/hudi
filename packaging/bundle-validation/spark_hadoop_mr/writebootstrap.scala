/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConverters._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.bootstrap.SparkParquetBootstrapDataProvider
import org.apache.hudi.client.bootstrap.selector.BootstrapRegexModeSelector
import org.apache.hudi.{DataSourceWriteOptions, HoodieDataSourceHelpers}
import org.apache.hudi.config.{HoodieBootstrapConfig, HoodieWriteConfig}
import org.apache.hudi.keygen.SimpleKeyGenerator
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._
import java.nio.file.{Paths, Files}


val database = "default"
val tableName = "jonvex_trips_bootstrap"
val bootstrapBasePath = "file:///tmp/hudi-bundles/tests/bootstrap_base_path"
val basePath = "file:///tmp/hudi-bundles/tests/" + tableName
val dataGen = new DataGenerator
val inserts = convertToStringList(dataGen.generateInserts(10)).asScala.toSeq
val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
df.show(100,false)
df.write.partitionBy("partitionpath").parquet(bootstrapBasePath)

val bootstrapDF = spark.emptyDataFrame
bootstrapDF.write.format("hudi").
  option(HoodieWriteConfig.TABLE_NAME, tableName).
  option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL).
  option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key, "false").
  option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL).
  option(HoodieBootstrapConfig.BOOTSTRAP_BASE_PATH_PROP, bootstrapBasePath).
  mode(SaveMode.Overwrite).
  save(basePath)

spark.read.format("hudi").load(basePath).show(100,false)

spark.sql("desc " + tableName).show


val updates = convertToStringList(dataGen.generateUpdates(1)).asScala.toSeq
val dfUpdates = spark.read.json(spark.sparkContext.parallelize(updates, 2))
dfUpdates.show(100,false)
dfUpdates.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(TABLE_NAME, tableName).
  option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL).
  option("hoodie.datasource.meta.sync.enable", "true").
  option("hoodie.datasource.hive_sync.database", database).
  option("hoodie.datasource.hive_sync.table", tableName).
  option("hoodie.datasource.hive_sync.partition_extractor_class", "org.apache.hudi.hive.SinglePartPartitionValueExtractor").
  option("hoodie.datasource.hive_sync.mode", "hms").
  mode(Append).
  save(basePath)

spark.read.format("hudi").load(basePath).show(100,false)

spark.sql("desc " + tableName).show

System.exit(0)