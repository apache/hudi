/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.junit.jupiter.api.Assertions.assertEquals

class TestWriteTableVersionConfig extends HoodieSparkSqlTestBase {

  test("Test create table with various write version") {
    Seq(6, 8).foreach { tableVersion =>
      withTempDir { tmp =>
        val tableName = generateTableName
        val basePath = tmp.getCanonicalPath
        spark.sql(
          s"""
             | create table $tableName (
             |  id int,
             |  ts long,
             |  dt string
             | ) using hudi
             | tblproperties (
             |  type = 'mor',
             |  primaryKey = 'id',
             |  orderingFields = 'ts',
             |  hoodie.write.table.version = $tableVersion
             | )
             | partitioned by(dt)
             | location '$basePath'
         """.stripMargin)
        val metaClient = HoodieTableMetaClient.builder().setBasePath(basePath)
          .setConf(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf())).build()
        assertEquals(metaClient.getTableConfig.getTableVersion.versionCode(), tableVersion)
      }
    }
  }
}
