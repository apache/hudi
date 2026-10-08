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

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.config.HoodieConfig
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR
import org.apache.hudi.common.util.{BinaryUtil, ConfigUtils, Option => HOption, StringUtils}
import org.apache.hudi.functional.ComplexKeyGenFixtures._
import org.apache.hudi.keygen.constant.ComplexKeyGenEncoding
import org.apache.hudi.metadata.MetadataPartitionType
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.table.upgrade.TestUpgradeDowngrade.getFixtureName
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.SaveMode
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
      assertResult(false) {
        metaClient.getTableConfig.contains(HoodieTableConfig.TABLE_CHECKSUM)
      }
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

  /**
   * The version 6 complex keygen fixtures predate the encoding property, so `upgrade_table` has to read the
   * table's data to record it. A table below version 8 is still on the version 1 timeline layout.
   */
  test("Test upgrade_table on a version 6 single-field complex keygen table records its key encoding") {
    Seq((COMPLEX_KEYGEN_FIXTURE_SUFFIX, ComplexKeyGenEncoding.FIELD_PREFIXED),
      (COMPLEX_KEYGEN_BARE_FIXTURE_SUFFIX, ComplexKeyGenEncoding.VALUE_ONLY)).foreach { case (suffix, expectedEncoding) =>
      withTempDir { tmp =>
        val fixtureName = getFixtureName(HoodieTableVersion.SIX, suffix)
        HoodieTestUtils.extractZipToDirectory(COMPLEX_KEYGEN_FIXTURES_PATH + fixtureName, tmp.toPath, getClass)
        val fixtureTableName = fixtureName.replace(".zip", "")
        val tablePath = tmp.toPath.resolve(fixtureTableName).toString
        val tableName = generateTableName
        spark.sql(s"create table $tableName using hudi location '$tablePath'")
        val storedKeys = spark.read.format("hudi").load(tablePath).select("_hoodie_record_key").collect().map(_.getString(0)).sorted

        checkAnswer(s"call upgrade_table(table => '$tableName', to_version => '${HoodieTableVersion.current().name()}')")(Seq(true))
        val metaClient = createMetaClient(spark, tablePath)
        assertResult(HoodieTableVersion.current())(metaClient.getTableConfig.getTableVersion)
        assertResult(HOption.of(expectedEncoding))(metaClient.getTableConfig.getComplexKeyGenEncoding)

        // upserting the existing records must update them in place, with the keys they are stored under
        val ts = 10000L // above every ordering value the fixture holds
        fixtureRows(spark, FIXTURE_IDS, ts).write.format("hudi")
          .options(fixtureWriteOpts(fixtureTableName))
          .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
          .mode(SaveMode.Append)
          .save(tablePath)
        val afterUpsert = spark.read.format("hudi").load(tablePath)
        assertResult(storedKeys.toSeq)(afterUpsert.select("_hoodie_record_key").collect().map(_.getString(0)).sorted.toSeq)
        assertResult(FIXTURE_IDS.size)(afterUpsert.filter(s"ts = $ts").count())
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
      withSQLConf(
        "hoodie.merge.small.file.group.candidates.limit" -> "0",
        "hoodie.compact.inline" -> "true",
        "hoodie.compact.inline.max.delta.commits" -> "4",
        "hoodie.clean.commits.retained" -> "2",
        "hoodie.keep.min.commits" -> "3",
        "hoodie.keep.max.commits" -> "4",
        "hoodie.metadata.record.index.enable" -> "true"
      ) {
        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"update $tableName set name = 'a2' where id = 1")
        spark.sql(s"update $tableName set name = 'a3' where id = 1")

        var metaClient = createMetaClient(spark, tablePath)
        val numCompactionInstants = metaClient.getActiveTimeline.filterCompletedOrMajorOrMinorCompactionInstants.countInstants
        // Disabling record index should not affect downgrade
        withSQLConf("hoodie.metadata.record.index.enable" -> "false") {
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
