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

import org.apache.hudi.common.config.HoodieConfig
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR
import org.apache.hudi.common.util.{BinaryUtil, ConfigUtils, StringUtils}
import org.apache.hudi.metadata.MetadataPartitionType
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase.NAME_FORMAT_0_X
import org.junit.jupiter.api.Assertions.assertTrue

import java.io.IOException
import java.time.Instant

import scala.collection.JavaConverters._

class TestUpgradeOrDowngradeProcedure extends HoodieSparkProcedureTestBase {

  ignore("[HUDI-9700] Test Call downgrade_table and upgrade_table Procedure") {
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
      // Check required fields
      checkExceptionContain(s"""call downgrade_table(table => '$tableName')""")(
        s"Argument: to_version is required")

      var metaClient = createMetaClient(spark, tablePath)

      // verify hoodie.table.version of the original table
      assertResult(HoodieTableVersion.current().versionCode()) {
        metaClient.getTableConfig.getTableVersion.versionCode()
      }
      assertTableVersionFromPropertyFile(
        metaClient, HoodieTableVersion.current().versionCode())

      // downgrade table to ZERO
      checkAnswer(s"""call downgrade_table(table => '$tableName', to_version => 'ZERO')""")(Seq(true))

      // verify the downgraded hoodie.table.version
      metaClient = HoodieTableMetaClient.reload(metaClient)
      assertResult(HoodieTableVersion.ZERO.versionCode) {
        metaClient.getTableConfig.getTableVersion.versionCode()
      }
      assertTableVersionFromPropertyFile(metaClient, HoodieTableVersion.ZERO.versionCode)

      // upgrade table to ONE
      checkAnswer(s"""call upgrade_table(table => '$tableName', to_version => 'ONE')""")(Seq(true))

      // verify the upgraded hoodie.table.version
      metaClient = HoodieTableMetaClient.reload(metaClient)
      assertResult(HoodieTableVersion.ONE.versionCode) {
        metaClient.getTableConfig.getTableVersion.versionCode()
      }
      assertTableVersionFromPropertyFile(metaClient, HoodieTableVersion.ONE.versionCode)
    }
  }

  ignore("[HUDI-9700] Test Call upgrade_table from version three") {
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

      // downgrade table to THREE
      checkAnswer(s"""call downgrade_table(table => '$tableName', to_version => 'THREE')""")(Seq(true))
      var metaClient = createMetaClient(spark, tablePath)
      val storage = metaClient.getStorage
      // verify hoodie.table.version of the table is THREE
      assertResult(HoodieTableVersion.THREE.versionCode) {
        metaClient.getTableConfig.getTableVersion.versionCode()
      }
      val metaPathDir = new StoragePath(metaClient.getBasePath, HoodieTableMetaClient.METAFOLDER_NAME)
      // delete checksum from hoodie.properties
      val props = ConfigUtils.fetchConfigs(
        storage,
        metaPathDir,
        HoodieTableConfig.HOODIE_PROPERTIES_FILE,
        HoodieTableConfig.HOODIE_PROPERTIES_FILE_BACKUP,
        1,
        1000)
      props.remove(HoodieTableConfig.TABLE_CHECKSUM.key)
      try {
        val outputStream = storage.create(new StoragePath(metaPathDir, HoodieTableConfig.HOODIE_PROPERTIES_FILE))
        props.store(outputStream, "Updated at " + Instant.now)
        outputStream.close()
      } catch {
        case e: Exception => fail(e)
      }
      // verify hoodie.table.checksum is deleted from hoodie.properties
      metaClient = HoodieTableMetaClient.reload(metaClient)
      assertResult(false) {metaClient.getTableConfig.contains(HoodieTableConfig.TABLE_CHECKSUM)}
      // upgrade table to SIX
      checkAnswer(s"""call upgrade_table(table => '$tableName', to_version => 'SIX')""")(Seq(true))
      metaClient = HoodieTableMetaClient.reload(metaClient)
      assertResult(HoodieTableVersion.SIX.versionCode) {
        metaClient.getTableConfig.getTableVersion.versionCode()
      }
      val expectedCheckSum = BinaryUtil.generateChecksum(StringUtils.getUTF8Bytes(tableName))
      assertResult(expectedCheckSum) {
        metaClient.getTableConfig.getLong(HoodieTableConfig.TABLE_CHECKSUM)
      }
    }
  }

  test("Test downgrade table to version six") {
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
           | options (
           |  type = 'mor',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)

      spark.sql("set hoodie.merge.small.file.group.candidates.limit=0")
      spark.sql("set hoodie.compact.inline=true")
      spark.sql("set hoodie.compact.inline.max.delta.commits=4")
      spark.sql("set hoodie.clean.commits.retained = 2")
      spark.sql("set hoodie.keep.min.commits = 3")
      spark.sql("set hoodie.keep.min.commits = 4")
      spark.sql("set hoodie.metadata.record.index.enable = true")

      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"update $tableName set name = 'a2' where id = 1")
      spark.sql(s"update $tableName set name = 'a3' where id = 1")

      var metaClient = createMetaClient(spark, tablePath)
      val numCompactionInstants = metaClient.getActiveTimeline.filterCompletedOrMajorOrMinorCompactionInstants.countInstants
      // Disabling record index should not affect downgrade
      spark.sql("set hoodie.metadata.record.index.enable = false")
      // downgrade table to version six
      checkAnswer(s"""call downgrade_table(table => '$tableName', to_version => 'SIX')""")(Seq(true))
      metaClient = createMetaClient(spark, tablePath)
      assertResult(numCompactionInstants + 1)(metaClient.getActiveTimeline.filterCompletedOrMajorOrMinorCompactionInstants.countInstants)
      assertResult(HoodieTableVersion.SIX.versionCode) {
        metaClient.getTableConfig.getTableVersion.versionCode()
      }
      // Verify whether the naming format of instant files is consistent with 0.x
      metaClient.reloadActiveTimeline().getInstants.iterator().asScala.forall(f => NAME_FORMAT_0_X.matcher(INSTANT_FILE_NAME_GENERATOR.getFileName(f)).find())
      checkAnswer(s"select id, name, price, ts from $tableName")(
        Seq(1, "a3", 10.0, 1000)
      )
      // Ensure files and record index partition are available after downgrade
      assertTrue(metaClient.getTableConfig.isMetadataTableAvailable)
      assertTrue(metaClient.getTableConfig.isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX))
    }
  }

  @throws[IOException]
  private def assertTableVersionFromPropertyFile(metaClient: HoodieTableMetaClient, versionCode: Int): Unit = {
    val propertyFile = new StoragePath(metaClient.getMetaPath + "/" + HoodieTableConfig.HOODIE_PROPERTIES_FILE)
    // Load the properties and verify
    val fsDataInputStream = metaClient.getStorage.open(propertyFile)
    val config = new HoodieConfig
    config.getProps.load(fsDataInputStream)
    fsDataInputStream.close()
    assertResult(Integer.toString(versionCode)) {
      config.getString(HoodieTableConfig.VERSION)
    }
  }
}
