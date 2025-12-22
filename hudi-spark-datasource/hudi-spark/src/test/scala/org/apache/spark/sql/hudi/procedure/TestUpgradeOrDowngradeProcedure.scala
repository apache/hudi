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
import org.apache.hudi.common.util.{BinaryUtil, StringUtils}
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import java.io.IOException
import java.time.Instant

class TestUpgradeOrDowngradeProcedure extends HoodieSparkProcedureTestBase {

  test("Test Call downgrade_table and upgrade_table Procedure") {
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
      assertResult(HoodieTableVersion.SIX.versionCode) {
        metaClient.getTableConfig.getTableVersion.versionCode()
      }
      assertTableVersionFromPropertyFile(metaClient, HoodieTableVersion.SIX.versionCode)

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

  test("Test Call upgrade_table from version three") {
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
      val props = HoodieTableConfig.fetchConfigs(storage, metaPathDir.toString)
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
