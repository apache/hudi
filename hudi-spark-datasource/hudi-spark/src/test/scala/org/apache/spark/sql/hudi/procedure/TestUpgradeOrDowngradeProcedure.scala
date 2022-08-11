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

import org.apache.hadoop.fs.Path
import org.apache.hudi.common.config.HoodieConfig
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion}
import org.apache.spark.api.java.JavaSparkContext

import java.io.IOException

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

      var metaClient = HoodieTableMetaClient.builder
        .setConf(new JavaSparkContext(spark.sparkContext).hadoopConfiguration())
        .setBasePath(tablePath)
        .build

      // verify hoodie.table.version of the original table
      assertResult(HoodieTableVersion.FOUR.versionCode) {
        metaClient.getTableConfig.getTableVersion.versionCode()
      }
      assertTableVersionFromPropertyFile(metaClient, HoodieTableVersion.FOUR.versionCode)

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

  @throws[IOException]
  private def assertTableVersionFromPropertyFile(metaClient: HoodieTableMetaClient, versionCode: Int): Unit = {
    val propertyFile = new Path(metaClient.getMetaPath + "/" + HoodieTableConfig.HOODIE_PROPERTIES_FILE)
    // Load the properties and verify
    val fsDataInputStream = metaClient.getFs.open(propertyFile)
    val hoodieConfig = HoodieConfig.create(fsDataInputStream)
    fsDataInputStream.close()
    assertResult(Integer.toString(versionCode)) {
      hoodieConfig.getString(HoodieTableConfig.VERSION)
    }
  }
}
