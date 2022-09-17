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

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieFileFormat
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.testutils.{HoodieTestDataGenerator, SchemaTestUtil}
import org.apache.hudi.testutils.HoodieSparkWriteableTestTable
import org.apache.spark.api.java.JavaSparkContext

import java.io.IOException
import java.net.URL
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters.asScalaIteratorConverter

class TestRepairsProcedure extends HoodieSparkProcedureTestBase {

  test("Test Call repair_add_partition_meta Procedure") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '$tablePath'
           | tblproperties (
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)
      // create commit instant
      Files.createFile(Paths.get(tablePath, ".hoodie", "100.commit"))

      val metaClient = HoodieTableMetaClient.builder
        .setConf(new JavaSparkContext(spark.sparkContext).hadoopConfiguration())
        .setBasePath(tablePath)
        .build

      // create partition path
      val partition1 = Paths.get(tablePath, "2016/03/15").toString
      val partition2 = Paths.get(tablePath, "2015/03/16").toString
      val partition3 = Paths.get(tablePath, "2015/03/17").toString
      assertResult(metaClient.getFs.mkdirs(new Path(partition1))) {true}
      assertResult(metaClient.getFs.mkdirs(new Path(partition2))) {true}
      assertResult(metaClient.getFs.mkdirs(new Path(partition3))) {true}

      // default is dry run
      val dryResult = spark.sql(s"""call repair_add_partition_meta(table => '$tableName')""").collect()
      assertResult(3) {
        dryResult.length
      }

      // real run
      val realRunResult = spark.sql(s"""call repair_add_partition_meta(table => '$tableName', dry_run => false)""").collect()
      assertResult(3) {
        realRunResult.length
      }
    }
  }

  test("Test Call repair_overwrite_hoodie_props Procedure") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '$tablePath'
           | tblproperties (
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)
      // create commit instant
      val newProps: URL = this.getClass.getClassLoader.getResource("table-config.properties")

      // overwrite hoodie props
      val Result = spark.sql(s"""call repair_overwrite_hoodie_props(table => '$tableName', new_props_file_path => '${newProps.getPath}')""").collect()
      assertResult(15) {
        Result.length
      }
    }
  }

  test("Test Call repair_corrupted_clean_files Procedure") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '$tablePath'
           | tblproperties (
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)
      var metaClient = HoodieTableMetaClient.builder
        .setConf(new JavaSparkContext(spark.sparkContext).hadoopConfiguration())
        .setBasePath(tablePath)
        .build

      // Create four requested files
      for (i <- 100 until 104) {
        val timestamp = String.valueOf(i)
        // Write corrupted requested Clean File
        createEmptyCleanRequestedFile(tablePath, timestamp, metaClient.getHadoopConf)
      }

      // reload meta client
      metaClient = HoodieTableMetaClient.reload(metaClient)
      // first, there are four instants
      assertResult(4) {
        metaClient.getActiveTimeline.filterInflightsAndRequested.getInstants.count
      }

      checkAnswer(s"""call repair_corrupted_clean_files(table => '$tableName')""")(Seq(true))

      // reload meta client
      metaClient = HoodieTableMetaClient.reload(metaClient)
      // after clearing, there should be 0 instant
      assertResult(0) {
        metaClient.getActiveTimeline.filterInflightsAndRequested.getInstants.count
      }
    }
  }

  private var duplicatedPartitionPath: String = null
  private var duplicatedPartitionPathWithUpdates: String = null
  private var duplicatedPartitionPathWithUpserts: String = null
  private var repairedOutputPath: String = null
  private var fileFormat: HoodieFileFormat = null

  test("Test Call repair_deduplicate Procedure with insert") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val bashPath = tmp.getCanonicalPath
      val tablePath = s"$bashPath/$tableName"
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  name string,
           |  favorite_number int,
           |  favorite_color string
           |) using hudi
           | location '$tablePath'
           | tblproperties (
           |  primaryKey = 'name',
           |  type = 'cow'
           | )
       """.stripMargin)
      var metaClient = HoodieTableMetaClient.builder
        .setConf(new JavaSparkContext(spark.sparkContext).hadoopConfiguration())
        .setBasePath(tablePath)
        .build

      generateRecords(tablePath, bashPath, metaClient)

      // reload meta client
      metaClient = HoodieTableMetaClient.reload(metaClient)

      // get fs and check number of latest files
      val fsView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline.getCommitTimeline.filterCompletedInstants,
        metaClient.getFs.listStatus(new Path(duplicatedPartitionPath)))
      val filteredStatuses = fsView.getLatestBaseFiles.iterator().asScala.map(value => value.getPath).toList
      // there should be 3 files
      assertResult(3) {
        filteredStatuses.size
      }

      // before deduplicate, all files contain 210 records
      var files = filteredStatuses.toArray
      var recordCount = getRecordCount(files)
      assertResult(210){recordCount}

      val partitionPath = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH
      val result = spark.sql(
        s"""call repair_deduplicate(table => '$tableName',
           | duplicated_partition_path => '$partitionPath',
           | repaired_output_path => '$repairedOutputPath')""".stripMargin).collect()
      assertResult(1) {
        result.length
      }

      // after deduplicate, there are 200 records
      val fileStatus = metaClient.getFs.listStatus(new Path(repairedOutputPath))
      files = fileStatus.map((status: FileStatus) => status.getPath.toString)
      recordCount = getRecordCount(files)
      assertResult(200){recordCount}
    }
  }

  test("Test Call repair_deduplicate Procedure with update") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val bashPath = tmp.getCanonicalPath
      val tablePath = s"$bashPath/$tableName"
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  name string,
           |  favorite_number int,
           |  favorite_color string
           |) using hudi
           | location '$tablePath'
           | tblproperties (
           |  primaryKey = 'name',
           |  type = 'cow'
           | )
       """.stripMargin)
      var metaClient = HoodieTableMetaClient.builder
        .setConf(new JavaSparkContext(spark.sparkContext).hadoopConfiguration())
        .setBasePath(tablePath)
        .build

      generateRecords(tablePath, bashPath, metaClient)

      // reload meta client
      metaClient = HoodieTableMetaClient.reload(metaClient)

      // get fs and check number of latest files
      val fsView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline.getCommitTimeline.filterCompletedInstants,
        metaClient.getFs.listStatus(new Path(duplicatedPartitionPathWithUpdates)))
      val filteredStatuses = fsView.getLatestBaseFiles.iterator().asScala.map(value => value.getPath).toList
      // there should be 2 files
      assertResult(2) {
        filteredStatuses.size
      }

      // before deduplicate, all files contain 110 records
      var files = filteredStatuses.toArray
      var recordCount = getRecordCount(files)
      assertResult(110){recordCount}

      val partitionPath = HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH
      val result = spark.sql(
        s"""call repair_deduplicate(table => '$tableName',
           | duplicated_partition_path => '$partitionPath',
           | repaired_output_path => '$repairedOutputPath',
           | dedupe_type => 'update_type')""".stripMargin).collect()
      assertResult(1) {
        result.length
      }

      // after deduplicate, there are 100 records
      val fileStatus = metaClient.getFs.listStatus(new Path(repairedOutputPath))
      files = fileStatus.map((status: FileStatus) => status.getPath.toString)
      recordCount = getRecordCount(files)
      assertResult(100){recordCount}
    }
  }

  test("Test Call repair_deduplicate Procedure with upsert") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val bashPath = tmp.getCanonicalPath
      val tablePath = s"$bashPath/$tableName"
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  name string,
           |  favorite_number int,
           |  favorite_color string
           |) using hudi
           | location '$tablePath'
           | tblproperties (
           |  primaryKey = 'name',
           |  type = 'cow'
           | )
       """.stripMargin)
      var metaClient = HoodieTableMetaClient.builder
        .setConf(new JavaSparkContext(spark.sparkContext).hadoopConfiguration())
        .setBasePath(tablePath)
        .build

      generateRecords(tablePath, bashPath, metaClient)

      // reload meta client
      metaClient = HoodieTableMetaClient.reload(metaClient)

      // get fs and check number of latest files
      val fsView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline.getCommitTimeline.filterCompletedInstants,
        metaClient.getFs.listStatus(new Path(duplicatedPartitionPathWithUpserts)))
      val filteredStatuses = fsView.getLatestBaseFiles.iterator().asScala.map(value => value.getPath).toList
      // there should be 3 files
      assertResult(3) {
        filteredStatuses.size
      }

      // before deduplicate, all files contain 120 records
      var files = filteredStatuses.toArray
      var recordCount = getRecordCount(files)
      assertResult(120){recordCount}

      val partitionPath = HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH
      val result = spark.sql(
        s"""call repair_deduplicate(table => '$tableName',
           | duplicated_partition_path => '$partitionPath',
           | repaired_output_path => '$repairedOutputPath',
           | dedupe_type => 'upsert_type')""".stripMargin).collect()
      assertResult(1) {
        result.length
      }

      // after deduplicate, there are 100 records
      val fileStatus = metaClient.getFs.listStatus(new Path(repairedOutputPath))
      files = fileStatus.map((status: FileStatus) => status.getPath.toString)
      recordCount = getRecordCount(files)
      assertResult(100){recordCount}
    }
  }

  test("Test Call repair_deduplicate Procedure with real") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val bashPath = tmp.getCanonicalPath
      val tablePath = s"$bashPath/$tableName"
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  name string,
           |  favorite_number int,
           |  favorite_color string
           |) using hudi
           | location '$tablePath'
           | tblproperties (
           |  primaryKey = 'name',
           |  type = 'cow'
           | )
       """.stripMargin)
      var metaClient = HoodieTableMetaClient.builder
        .setConf(new JavaSparkContext(spark.sparkContext).hadoopConfiguration())
        .setBasePath(tablePath)
        .build

      generateRecords(tablePath, bashPath, metaClient)

      // reload meta client
      metaClient = HoodieTableMetaClient.reload(metaClient)

      // get fs and check number of latest files
      val fsView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline.getCommitTimeline.filterCompletedInstants,
        metaClient.getFs.listStatus(new Path(duplicatedPartitionPath)))
      val filteredStatuses = fsView.getLatestBaseFiles.iterator().asScala.map(value => value.getPath).toList
      // there should be 3 files
      assertResult(3) {
        filteredStatuses.size
      }

      // before deduplicate, all files contain 210 records
      var files = filteredStatuses.toArray
      var recordCount = getRecordCount(files)
      assertResult(210){recordCount}

      val partitionPath = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH
      val result = spark.sql(
        s"""call repair_deduplicate(table => '$tableName',
           | duplicated_partition_path => '$partitionPath',
           | repaired_output_path => '$repairedOutputPath',
           | dry_run => false)""".stripMargin).collect()
      assertResult(1) {
        result.length
      }

      // after deduplicate, there are 200 records
      val fileStatus = metaClient.getFs.listStatus(new Path(duplicatedPartitionPath))
      files = fileStatus.map((status: FileStatus) => status.getPath.toString).filter(p => p.endsWith(".parquet"))
      recordCount = getRecordCount(files)
      assertResult(200){recordCount}
    }
  }

  test("Test Call repair_migrate_partition_meta Procedure") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | partitioned by (ts)
           | tblproperties (
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $tableName select 2, 'a2', 20, 1500")

      // default is dry run
      var result = spark.sql(s"""call repair_migrate_partition_meta(table => '$tableName')""").collect()
      assertResult(2) {
        result.length
      }

      // real run
      result = spark.sql(s"""call repair_migrate_partition_meta(table => '$tableName', dry_run => false)""").collect()
      assertResult(2) {
        result.length
      }
    }
  }

  private def generateRecords(tablePath: String, bashpath: String, metaClient: HoodieTableMetaClient): Unit ={
    duplicatedPartitionPath = Paths.get(tablePath, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH).toString
    duplicatedPartitionPathWithUpdates = Paths.get(tablePath, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH).toString
    duplicatedPartitionPathWithUpserts = Paths.get(tablePath, HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH).toString
    repairedOutputPath = Paths.get(bashpath, "tmp").toString

    // generate 200 records
    val schema: Schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema)
    val testTable: HoodieSparkWriteableTestTable = HoodieSparkWriteableTestTable.of(metaClient, schema)

    val hoodieRecords1 = SchemaTestUtil.generateHoodieTestRecords(0, 100, schema)
    val hoodieRecords2 = SchemaTestUtil.generateHoodieTestRecords(100, 100, schema)
    testTable.addCommit("20160401010101")
      .withInserts(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "1", hoodieRecords1)
    testTable.withInserts(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "2", hoodieRecords2)
    testTable.getFileIdWithLogFile(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)

    testTable.withInserts(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, "4", hoodieRecords1)
    testTable.withInserts(HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH, "6", hoodieRecords1)

    // read records and get 10 to generate duplicates
    val dupRecords = hoodieRecords1.subList(0, 10)
    testTable.withInserts(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, "5", dupRecords)
    testTable.addCommit("20160401010202")
      .withInserts(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "3", dupRecords)
    testTable.withInserts(HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH, "7", dupRecords)
    testTable.withInserts(HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH, "8", dupRecords)

    fileFormat = metaClient.getTableConfig.getBaseFileFormat
  }

  private def getRecordCount(files: Array[String]): Long = {
    var recordCount: Long = 0
    for (file <- files){
      if (HoodieFileFormat.PARQUET == fileFormat){
        recordCount += spark.sqlContext.read.parquet(file).count()
      } else if (HoodieFileFormat.ORC == fileFormat) {
        recordCount += spark.sqlContext.read.orc(file).count()
      } else {
        throw new UnsupportedOperationException(fileFormat.name + " format not supported yet.")
      }
    }
    recordCount
  }

  @throws[IOException]
  def createEmptyCleanRequestedFile(basePath: String, instantTime: String, configuration: Configuration): Unit = {
    val commitFilePath = new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + HoodieTimeline.makeRequestedCleanerFileName(instantTime))
    val fs = FSUtils.getFs(basePath, configuration)
    val os = fs.create(commitFilePath, true)
    os.close()
  }
}
