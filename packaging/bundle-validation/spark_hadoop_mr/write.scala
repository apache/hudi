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

val expected = 10
val database = "default"
val tableName = "trips"
val basePath = "file:///tmp/hudi-bundles/tests/" + tableName
val dataGen = new DataGenerator
val inserts = convertToStringList(dataGen.generateInserts(expected)).asScala.toSeq
val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
df.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(TABLE_NAME, tableName).
  option("hoodie.datasource.meta.sync.enable", "true").
  option("hoodie.datasource.hive_sync.database", database).
  option("hoodie.datasource.hive_sync.table", tableName).
  option("hoodie.datasource.hive_sync.partition_extractor_class", "org.apache.hudi.hive.SinglePartPartitionValueExtractor").
  option("hoodie.datasource.hive_sync.mode", "hms").
  mode(Overwrite).
  save(basePath)

spark.sql("desc " + tableName).show

System.exit(0)
