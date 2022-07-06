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

package org.apache.spark.sql.hudi.procedure

import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline
import org.apache.hudi.common.testutils.{HoodieTestDataGenerator, HoodieTestUtils}
import org.apache.hudi.testutils.HoodieClientTestUtils
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.hudi.HoodieSparkSqlTestBase
import org.junit.jupiter.api.Assertions.assertTrue

import java.io.IOException
import java.util
import java.util.Objects
import java.util.concurrent.TimeUnit

class TestHdfsParquetImportProcedure extends HoodieSparkSqlTestBase {

  test("Test Call hdfs_parquet_import Procedure with insert operation") {
    withTempDir { tmp =>
      val fs: FileSystem = FSUtils.getFs(tmp.getCanonicalPath, spark.sparkContext.hadoopConfiguration)
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + Path.SEPARATOR + tableName
      val sourcePath = new Path(tmp.getCanonicalPath, "source")
      val targetPath = new Path(tablePath)
      val schemaFile = new Path(tmp.getCanonicalPath, "file.schema").toString

      // create schema file
      val schemaFileOS = fs.create(new Path(schemaFile))
      try schemaFileOS.write(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA.getBytes)
      finally if (schemaFileOS != null) schemaFileOS.close()

      val insertData: util.List[GenericRecord] = createInsertRecords(sourcePath)

      // Check required fields
      checkExceptionContain(s"""call hdfs_parquet_import(tableType => 'mor')""")(
        s"Argument: table is required")

      checkAnswer(
        s"""call hdfs_parquet_import(
           |table => '$tableName', tableType => '${HoodieTableType.COPY_ON_WRITE.name}',
           |srcPath => '$sourcePath', targetPath => '$targetPath',
           |rowKey => '_row_key', partitionKey => 'timestamp',
           |schemaFilePath => '$schemaFile')""".stripMargin) {
        Seq(0)
      }

      verifyResultData(insertData, fs, tablePath)
    }
  }

  test("Test Call hdfs_parquet_import Procedure with upsert operation") {
    withTempDir { tmp =>
      val fs: FileSystem = FSUtils.getFs(tmp.getCanonicalPath, spark.sparkContext.hadoopConfiguration)
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + Path.SEPARATOR + tableName
      val sourcePath = new Path(tmp.getCanonicalPath, "source")
      val targetPath = new Path(tablePath)
      val schemaFile = new Path(tmp.getCanonicalPath, "file.schema").toString

      // create schema file
      val schemaFileOS = fs.create(new Path(schemaFile))
      try schemaFileOS.write(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA.getBytes)
      finally if (schemaFileOS != null) schemaFileOS.close()

      val insertData: util.List[GenericRecord] = createUpsertRecords(sourcePath)

      // Check required fields
      checkExceptionContain(s"""call hdfs_parquet_import(tableType => 'mor')""")(
        s"Argument: table is required")

      checkAnswer(
        s"""call hdfs_parquet_import(
           |table => '$tableName', tableType => '${HoodieTableType.COPY_ON_WRITE.name}',
           |srcPath => '$sourcePath', targetPath => '$targetPath',
           |rowKey => '_row_key', partitionKey => 'timestamp',
           |schemaFilePath => '$schemaFile', command => 'upsert')""".stripMargin) {
        Seq(0)
      }

      verifyResultData(insertData, fs, tablePath)
    }
  }

  @throws[ParseException]
  @throws[IOException]
  def createInsertRecords(srcFolder: Path): util.List[GenericRecord] = {
    import scala.collection.JavaConversions._
    val srcFile: Path = new Path(srcFolder.toString, "file1.parquet")
    val startTime: Long = HoodieActiveTimeline.parseDateFromInstantTime("20170203000000").getTime / 1000
    val records: util.List[GenericRecord] = new util.ArrayList[GenericRecord]
    for (recordNum <- 0 until 96) {
      records.add(new HoodieTestDataGenerator().generateGenericRecord(recordNum.toString,
        "0", "rider-" + recordNum, "driver-" + recordNum, startTime + TimeUnit.HOURS.toSeconds(recordNum)))
    }
    try {
      val writer: ParquetWriter[GenericRecord] = AvroParquetWriter.builder[GenericRecord](srcFile)
        .withSchema(HoodieTestDataGenerator.AVRO_SCHEMA).withConf(HoodieTestUtils.getDefaultHadoopConf).build
      try {
        for (record <- records) {
          writer.write(record)
        }
      } finally {
        if (writer != null) writer.close()
      }
    }
    records
  }

  @throws[ParseException]
  @throws[IOException]
  def createUpsertRecords(srcFolder: Path): util.List[GenericRecord] = {
    import scala.collection.JavaConversions._
    val srcFile = new Path(srcFolder.toString, "file1.parquet")
    val startTime = HoodieActiveTimeline.parseDateFromInstantTime("20170203000000").getTime / 1000
    val records = new util.ArrayList[GenericRecord]
    // 10 for update
    val dataGen = new HoodieTestDataGenerator
    for (recordNum <- 0 until 11) {
      records.add(dataGen.generateGenericRecord(recordNum.toString, "0", "rider-upsert-" + recordNum, "driver-upsert" + recordNum, startTime + TimeUnit.HOURS.toSeconds(recordNum)))
    }
    // 4 for insert
    for (recordNum <- 96 until 100) {
      records.add(dataGen.generateGenericRecord(recordNum.toString, "0", "rider-upsert-" + recordNum, "driver-upsert" + recordNum, startTime + TimeUnit.HOURS.toSeconds(recordNum)))
    }
    try {
      val writer = AvroParquetWriter.builder[GenericRecord](srcFile).withSchema(HoodieTestDataGenerator.AVRO_SCHEMA).withConf(HoodieTestUtils.getDefaultHadoopConf).build
      try {
        for (record <- records) {
          writer.write(record)
        }
      } finally {
        if (writer != null) writer.close()
      }
    }
    records
  }

  private def verifyResultData(expectData: util.List[GenericRecord], fs: FileSystem, tablePath: String): Unit = {
    import scala.collection.JavaConversions._
    val jsc = new JavaSparkContext(spark.sparkContext)
    val ds = HoodieClientTestUtils.read(jsc, tablePath, spark.sqlContext, fs, tablePath + "/*/*/*/*")
    val readData = ds.select("timestamp", "_row_key", "rider", "driver", "begin_lat", "begin_lon", "end_lat", "end_lon").collectAsList()
    val result = readData.toList.map((row: Row) =>
      new HoodieTripModel(row.getLong(0), row.getString(1),
        row.getString(2), row.getString(3), row.getDouble(4), row.getDouble(5), row.getDouble(6), row.getDouble(7))
    )
    val expected = expectData.toList.map((g: GenericRecord) => new HoodieTripModel(Long.unbox(g.get("timestamp")),
      g.get("_row_key").toString, g.get("rider").toString, g.get("driver").toString, g.get("begin_lat").toString.toDouble,
      g.get("begin_lon").toString.toDouble, g.get("end_lat").toString.toDouble, g.get("end_lon").toString.toDouble))

    assertTrue(expected.size == result.size || (result.containsAll(expected) && expected.containsAll(result)))
  }

  class HoodieTripModel(
     var timestamp: Long,
     var rowKey: String,
     var rider: String,
     var driver: String,
     var beginLat: Double,
     var beginLon: Double,
     var endLat: Double,
     var endLon: Double) {
    override def equals(o: Any): Boolean = {
      if (this == o) {
        true
      } else if (o == null || (getClass ne o.getClass)) {
        false
      } else {
        val other = o.asInstanceOf[HoodieTripModel]
        timestamp == other.timestamp && rowKey == other.rowKey && rider == other.rider &&
          driver == other.driver && beginLat == other.beginLat && beginLon == other.beginLon &&
          endLat == other.endLat && endLon == other.endLon
      }
    }

    override def hashCode: Int = Objects.hashCode(timestamp, rowKey, rider, driver, beginLat, beginLon, endLat, endLon)
  }
}
