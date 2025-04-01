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

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness.getSparkSqlConf

import org.apache.spark.SparkConf
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

class TestAutoKeyGenForSQL extends SparkClientFunctionalTestHarness {
  override def conf: SparkConf = conf(getSparkSqlConf)

  @ParameterizedTest
  @CsvSource(value = Array("MERGE_ON_READ", "COPY_ON_WRITE"))
  def testAutoKeyGen(tableType: String): Unit = {
    // No record key is set, which should trigger auto key gen.
    // MOR table is used to generate log files.
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
    // Create the first log file by update.
    spark.sql(s"UPDATE $tableName SET fare = 25.0 WHERE rider = 'rider-D';")
    // Create the second log file by delete.
    spark.sql(s"DELETE FROM $tableName WHERE uuid = '334e26e9-8355-45cc-97c6-c31daf0df330';")
    // Create the third log file by delete.
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
}
