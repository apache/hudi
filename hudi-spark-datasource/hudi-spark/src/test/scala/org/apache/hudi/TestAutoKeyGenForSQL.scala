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

package org.apache.hudi

import org.apache.hudi.TestAutoKeyGenForSQL.randomString
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.view.{FileSystemViewManager, HoodieTableFileSystemView}
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness.getSparkSqlConf

import org.apache.spark.SparkConf
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, EnumSource}

import scala.util.Random

class TestAutoKeyGenForSQL extends SparkClientFunctionalTestHarness {
  override def conf: SparkConf = conf(getSparkSqlConf)

  @ParameterizedTest
  @CsvSource(value = Array("MERGE_ON_READ", "COPY_ON_WRITE"))
  def testAutoKeyGen(tableType: String): Unit = {
    // No record key is set, which should trigger auto key gen.
    val tableName = "hoodie_test_" + tableType
    spark.sql(
      s"""
         |create table $tableName (
         | ts BIGINT,
         | uuid STRING,
         | rider STRING,
         | driver STRING,
         | fare DOUBLE,
         | city STRING
         |) using hudi
         | options (
         |  hoodie.metadata.enable = 'true',
         |  hoodie.enable.data.skipping = 'true',
         |  hoodie.datasource.write.payload.class = 'org.apache.hudi.common.model.OverwriteWithLatestAvroPayload'
         | )
         | partitioned by(city)
         | location '$basePath'
         | TBLPROPERTIES (hoodie.datasource.write.table.type='$tableType')
       """.stripMargin)
    // Initial data.
    spark.sql(
      s"""
         |INSERT INTO $tableName VALUES
         |  (1695159649087,'334e26e9-8355-45cc-97c6-c31daf0df330','rider-A','driver-K',19.10,'san_francisco'),
         |  (1695091554788,'e96c4396-3fad-413a-a942-4cb36106d721','rider-C','driver-M',27.70 ,'san_francisco'),
         |  (1695046462179,'9909a8b1-2d15-4d3d-8ec9-efc48c536a00','rider-D','driver-L',33.90 ,'san_francisco'),
         |  (1695332066204,'1dced545-862b-4ceb-8b43-d2a568f6616b','rider-E','driver-O',93.50,'san_francisco'),
         |  (1695516137016,'e3cf430c-889d-4015-bc98-59bdce1e530c','rider-F','driver-P',34.15,'sao_paulo'),
         |  (1695376420876,'7a84095f-737f-40bc-b62f-6b69664712d2','rider-G','driver-Q',43.40 ,'sao_paulo'),
         |  (1695173887231,'3eeb61f7-c2b0-4636-99bd-5d7a5a1d2c04','rider-I','driver-S',41.06 ,'chennai'),
         |  (1695115999911,'c8abbe79-8d89-47ea-b4ce-4d224bae5bfa','rider-J','driver-T',17.85,'chennai');
      """.stripMargin)
    spark.sql(s"UPDATE $tableName SET fare = 25.0 WHERE rider = 'rider-D';")
    spark.sql(s"DELETE FROM $tableName WHERE uuid = '334e26e9-8355-45cc-97c6-c31daf0df330';")
    spark.sql(s"DELETE FROM $tableName WHERE uuid = '9909a8b1-2d15-4d3d-8ec9-efc48c536a00';")

    // Validate: data integrity.
    val columns = Seq("ts","uuid","rider","driver","fare","city")
    val actualDf = spark.sql(s"SELECT * FROM $tableName WHERE city = 'san_francisco';")
      .select("ts","uuid","rider","driver","fare","city").sort("uuid")
    val expected = Seq(
      (1695091554788L,"e96c4396-3fad-413a-a942-4cb36106d721","rider-C","driver-M",27.70,"san_francisco"),
      (1695332066204L,"1dced545-862b-4ceb-8b43-d2a568f6616b","rider-E","driver-O",93.50,"san_francisco"))
    val expectedDf = spark.createDataFrame(expected).toDF(columns: _*)
    val expectedMinusActual = expectedDf.except(actualDf)
    val actualMinusExpected = actualDf.except(expectedDf)
    assertTrue(expectedMinusActual.isEmpty && actualMinusExpected.isEmpty)
    // Validate: table property.
    val metaClient: HoodieTableMetaClient = HoodieTableMetaClient
      .builder()
      .setBasePath(basePath)
      .setConf(new HadoopStorageConfiguration(spark.sparkContext.hadoopConfiguration))
      .build()
    // Record key fields should be empty.
    assertTrue(metaClient.getTableConfig.getRecordKeyFields.isEmpty)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testAutoKeyGenForImmutableWorkflow(tableType: HoodieTableType): Unit = {
    // No record key is set, which should trigger auto key gen.
    val tableName = "hoodie_immutable_" + tableType
    val compactionEnabled = if (tableType == HoodieTableType.MERGE_ON_READ) "true" else "false"
    spark.sql(
      s"""
         |CREATE TABLE $tableName (
         | ts BIGINT,
         | uuid STRING,
         | rider STRING,
         | driver STRING,
         | fare DOUBLE,
         | city STRING )
         | USING hudi
         | OPTIONS (
         |  hoodie.metadata.enable = 'true',
         |  hoodie.enable.data.skipping = 'true',
         |  hoodie.write.record.merge.mode = 'COMMIT_TIME_ORDERING',
         |  hoodie.clean.commits.retained = '5',
         |  hoodie.keep.max.commits = '3',
         |  hoodie.keep.min.commits = '2',
         |  hoodie.clustering.inline = 'true',
         |  hoodie.clustering.inline.max.commits = '2',
         |  hoodie.compact.inline = '$compactionEnabled')
         | PARTITIONED BY(city)
         | LOCATION '$basePath'
         | TBLPROPERTIES (hoodie.datasource.write.table.type='${tableType.name}')
       """.stripMargin)
    spark.sql(
      s"""
         |INSERT INTO $tableName VALUES
         |  (1695159649087,'334e26e9-8355-45cc-97c6-c31daf0df330','rider-A','driver-K',19.10,'san_francisco'),
         |  (1695091554788,'e96c4396-3fad-413a-a942-4cb36106d721','rider-C','driver-M',27.70 ,'san_francisco'),
         |  (1695046462179,'9909a8b1-2d15-4d3d-8ec9-efc48c536a00','rider-D','driver-L',33.90 ,'san_francisco'),
         |  (1695332066204,'1dced545-862b-4ceb-8b43-d2a568f6616b','rider-E','driver-O',93.50,'san_francisco'),
         |  (1695516137016,'e3cf430c-889d-4015-bc98-59bdce1e530c','rider-F','driver-P',34.15,'sao_paulo'),
         |  (1695376420876,'7a84095f-737f-40bc-b62f-6b69664712d2','rider-G','driver-Q',43.40 ,'sao_paulo'),
         |  (1695173887231,'3eeb61f7-c2b0-4636-99bd-5d7a5a1d2c04','rider-I','driver-S',41.06 ,'chennai'),
         |  (1695115999911,'c8abbe79-8d89-47ea-b4ce-4d224bae5bfa','rider-J','driver-T',17.85,'chennai');
      """.stripMargin)

    import java.util.UUID
    import scala.util.Random
    for (i <- 0 until 30) {
      val ts: Long = 1695115999911L + i + 1
      val uuid: String = UUID.randomUUID.toString
      val rider: String = s"rider-$i"
      val driver: String = s"driver-$i"
      val fare: Float = Random.nextFloat
      val city: String = randomString(8)

      spark.sql(
        s"""
           |INSERT INTO $tableName VALUES
           |($ts, '$uuid', '$rider', '$driver', $fare, '$city');
        """.stripMargin)
    }

    // Validate: data integrity
    val noRecords = spark.sql(s"SELECT * FROM $tableName").count()
    assertEquals(38, noRecords)
    // Validate: table property.
    val metaClient: HoodieTableMetaClient = HoodieTableMetaClient
      .builder()
      .setBasePath(basePath)
      .setConf(new HadoopStorageConfiguration(spark.sparkContext.hadoopConfiguration))
      .build()
    // Validate: record key fields should be empty.
    assertTrue(metaClient.getTableConfig.getRecordKeyFields.isEmpty)
    // Validate: table services are triggered.
    assertFalse(metaClient.getActiveTimeline.getCleanerTimeline.getInstants.isEmpty)
    assertFalse(metaClient.getArchivedTimeline.getInstants.isEmpty)
    assertFalse(metaClient.getActiveTimeline.getCompletedReplaceTimeline.getInstants.isEmpty)
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      assertFalse(metaClient.getActiveTimeline.getCommitsAndCompactionTimeline.empty())
      val fsv: HoodieTableFileSystemView = FileSystemViewManager.createInMemoryFileSystemView(
        context, metaClient, HoodieMetadataConfig.newBuilder.enable(true).build)
      fsv.loadAllPartitions()
      assertFalse(fsv.getAllFileGroups.flatMap(_.getAllFileSlices).anyMatch(_.hasLogFiles))
    }
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testAutoKeyGenForMutableWorkflow(tableType: HoodieTableType): Unit = {
    // No record key is set, which should trigger auto key gen.
    val tableName = "hoodie_mutable__" + tableType
    val compactionEnabled = if (tableType == HoodieTableType.MERGE_ON_READ) "true" else "false"
    spark.sql(
      s"""
         |CREATE TABLE $tableName (
         | ts BIGINT,
         | uuid STRING,
         | rider STRING,
         | driver STRING,
         | fare DOUBLE,
         | city STRING )
         | USING hudi
         | OPTIONS (
         |  hoodie.metadata.enable = 'true',
         |  hoodie.enable.data.skipping = 'true',
         |  hoodie.write.record.merge.mode = 'COMMIT_TIME_ORDERING',
         |  hoodie.clean.commits.retained = '5',
         |  hoodie.keep.max.commits = '3',
         |  hoodie.keep.min.commits = '2',
         |  hoodie.clustering.inline = 'true',
         |  hoodie.clustering.inline.max.commits = '2',
         |  hoodie.compact.inline = '$compactionEnabled')
         | PARTITIONED BY(city)
         | LOCATION '$basePath'
         | TBLPROPERTIES (hoodie.datasource.write.table.type='${tableType.name}')
       """.stripMargin)
    spark.sql(
      s"""
         |INSERT INTO $tableName VALUES
         |  (1695159649087,'334e26e9-8355-45cc-97c6-c31daf0df330','rider-A','driver-K',19.10,'san_francisco'),
         |  (1695091554788,'e96c4396-3fad-413a-a942-4cb36106d721','rider-B','driver-M',27.70 ,'san_francisco'),
         |  (1695046462179,'9909a8b1-2d15-4d3d-8ec9-efc48c536a00','rider-C','driver-L',33.90 ,'san_francisco'),
         |  (1695332066204,'1dced545-862b-4ceb-8b43-d2a568f6616b','rider-D','driver-O',93.50,'san_francisco'),
         |  (1695516137016,'e3cf430c-889d-4015-bc98-59bdce1e530c','rider-E','driver-P',34.15,'sao_paulo'),
         |  (1695376420876,'7a84095f-737f-40bc-b62f-6b69664712d2','rider-F','driver-Q',43.40 ,'sao_paulo'),
         |  (1695173887231,'3eeb61f7-c2b0-4636-99bd-5d7a5a1d2c04','rider-G','driver-S',41.06 ,'chennai'),
         |  (1695115999911,'c8abbe79-8d89-47ea-b4ce-4d224bae5bfa','rider-H','driver-T',17.85,'chennai');
      """.stripMargin)

    import java.util.UUID
    for (i <- 0 until 10) {
      val ts: Long = 1695115999911L + i + 1
      val uuid: String = UUID.randomUUID.toString
      val rider: String = s"rider-$i"
      val driver: String = s"driver-$i"
      val fare: Float = Random.nextFloat
      val city: String = randomString(8)
      spark.sql(
        s"""
           |INSERT INTO $tableName VALUES
           |($ts, '$uuid', '$rider', '$driver', $fare, '$city');
        """.stripMargin)
    }

    for (i <- 0 until 10) {
      val ts: Long = 1695115999911L + i + 1
      val rider: String = s"rider-${'A' + new Random().nextInt(8)}"
      spark.sql(
        s"""
           |UPDATE $tableName
           |SET ts = $ts
           |WHERE rider = '$rider'
        """.stripMargin)
    }

    for (i <- 0 until 2) {
      val rider: String = s"rider-${('A' + new Random().nextInt(8)).toChar}"
      spark.sql(
        s"""
           |DELETE FROM $tableName
           |WHERE rider = '$rider'
        """.stripMargin)
    }

    // Validate: data integrity
    val noRecords = spark.sql(s"SELECT * FROM $tableName").count()
    assertEquals(16, noRecords)
    // Validate: table property.
    val metaClient: HoodieTableMetaClient = HoodieTableMetaClient
      .builder()
      .setBasePath(basePath)
      .setConf(new HadoopStorageConfiguration(spark.sparkContext.hadoopConfiguration))
      .build()
    // Validate: record key fields should be empty.
    assertTrue(metaClient.getTableConfig.getRecordKeyFields.isEmpty)
    // Validate: table services are triggered.
    assertFalse(metaClient.getActiveTimeline.getCleanerTimeline.getInstants.isEmpty)
    assertFalse(metaClient.getArchivedTimeline.getInstants.isEmpty)
    assertFalse(metaClient.getActiveTimeline.getCompletedReplaceTimeline.getInstants.isEmpty)
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      assertFalse(metaClient.getActiveTimeline.getCommitsAndCompactionTimeline.empty())
      val fsv: HoodieTableFileSystemView = FileSystemViewManager.createInMemoryFileSystemView(
        context, metaClient, HoodieMetadataConfig.newBuilder.enable(true).build)
      fsv.loadAllPartitions()
      assertTrue(fsv.getAllFileGroups.flatMap(_.getAllFileSlices).anyMatch(_.hasLogFiles))
    }
  }
}

object TestAutoKeyGenForSQL {
  val chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

  def randomString(length: Int): String = {
    (1 to length).map(_ => chars(Random.nextInt(chars.length))).mkString
  }
}
