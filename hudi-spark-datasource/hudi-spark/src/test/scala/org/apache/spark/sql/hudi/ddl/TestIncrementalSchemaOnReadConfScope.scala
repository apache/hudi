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

package org.apache.spark.sql.hudi.ddl

import org.apache.hudi.client.utils.SparkInternalSchemaConverter
import org.apache.hudi.common.config.HoodieReaderConfig
import org.apache.hudi.common.schema.internal.{InternalSchema, Types}
import org.apache.hudi.common.schema.internal.utils.SerDeHelper
import org.apache.hudi.common.table.HoodieTableVersion

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.parquet.LegacyHoodieParquetFileFormat
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/**
 * Schema-on-read settings of an incremental read must stay scoped to that query instead of landing in the
 * session-wide Hadoop configuration, where later reads of other tables would pick them up.
 */
class TestIncrementalSchemaOnReadConfScope extends HoodieSparkSqlTestBase {

  private val schemaOnReadKeys = Seq(
    SparkInternalSchemaConverter.HOODIE_QUERY_SCHEMA,
    SparkInternalSchemaConverter.HOODIE_TABLE_PATH,
    SparkInternalSchemaConverter.HOODIE_VALID_COMMITS_LIST)

  Seq(HoodieTableVersion.SIX, HoodieTableVersion.current()).foreach { tableVersion =>
    test(s"Streaming incremental read leaves the session Hadoop conf untouched (tableVersion=${tableVersion.versionCode()})") {
      withTempDir { tmp =>
        val isV6 = tableVersion == HoodieTableVersion.SIX
        val tablePath = s"${tmp.getCanonicalPath}/evolved"
        val tableName = generateTableName
        val writeOptions = Map(
          "hoodie.table.name" -> tableName,
          "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
          "hoodie.datasource.write.recordkey.field" -> "id",
          "hoodie.datasource.write.precombine.field" -> "ts",
          "hoodie.datasource.write.partitionpath.field" -> "",
          "hoodie.datasource.write.keygenerator.class" -> "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
          "hoodie.schema.on.read.enable" -> "true",
          "hoodie.datasource.write.reconcile.schema" -> "true",
          "hoodie.parquet.small.file.limit" -> "0",
          "hoodie.metadata.enable" -> (!isV6).toString,
          "hoodie.write.table.version" -> tableVersion.versionCode().toString)
        import spark.implicits._
        Seq((1, "a1", 1000L), (2, "a2", 1000L)).toDF("id", "name", "ts")
          .write.format("hudi").options(writeOptions).mode("overwrite").save(tablePath)
        Seq((3, "a3", 1000L, 300)).toDF("id", "name", "ts", "qty")
          .write.format("hudi").options(writeOptions).mode("append").save(tablePath)

        val hadoopConf = spark.sparkContext.hadoopConfiguration
        schemaOnReadKeys.foreach(key => assert(hadoopConf.get(key) == null, s"$key is set before the read"))
        val queryName = s"${tableName}_stream"
        ExecutorMetaFolderAccessFileSystem.recordingTaskAccesses(hadoopConf) {
          val query = spark.readStream.format("hudi")
            .option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key, "false")
            .load(tablePath)
            .select("id", "name", "qty")
            .writeStream.format("memory").queryName(queryName)
            .trigger(Trigger.Once())
            .start()
          try {
            query.processAllAvailable()
          } finally {
            query.stop()
          }
        }

        checkAnswer(s"select id, name, qty from $queryName order by id")(
          Seq(1, "a1", null), Seq(2, "a2", null), Seq(3, "a3", 300))
        schemaOnReadKeys.foreach(key =>
          assert(hadoopConf.get(key) == null, s"$key leaked into the session Hadoop conf: ${hadoopConf.get(key)}"))
        ExecutorMetaFolderAccessFileSystem.assertNoTaskAccesses()
      }
    }
  }

  test("Legacy parquet format takes schema-on-read settings passed as reader options") {
    val querySchema = new InternalSchema(Types.RecordType.get(Types.Field.get(0, false, "id", Types.IntType.get())))
    val options = CaseInsensitiveMap(Map(
      SparkInternalSchemaConverter.HOODIE_QUERY_SCHEMA -> SerDeHelper.toJson(querySchema),
      SparkInternalSchemaConverter.HOODIE_TABLE_PATH -> "/tmp/table"))
    // Spark copies the reader options into the scan's conf the same way, with lowercased keys
    val hadoopConf = new Configuration(false)
    options.foreach { case (key, value) => hadoopConf.set(key, value) }
    val schema = StructType(Seq(StructField("id", IntegerType, nullable = false)))

    new LegacyHoodieParquetFileFormat().buildReaderWithPartitionValues(
      spark, schema, StructType(Nil), schema, Nil, options, hadoopConf)

    assert(hadoopConf.get(SparkInternalSchemaConverter.HOODIE_TABLE_PATH) == "/tmp/table")
    assert(SerDeHelper.fromJson(hadoopConf.get(SparkInternalSchemaConverter.HOODIE_QUERY_SCHEMA)).isPresent)
  }
}
