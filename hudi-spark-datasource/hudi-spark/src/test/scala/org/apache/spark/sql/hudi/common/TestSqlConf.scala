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

package org.apache.spark.sql.hudi.common

import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.common.config.DFSPropertiesConfiguration
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.storage.{HoodieStorageUtils, StoragePath}
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.scalatest.BeforeAndAfter

import java.io.File
import java.nio.file.{Files, Paths}

class TestSqlConf extends HoodieSparkSqlTestBase with BeforeAndAfter {

  def setEnv(key: String, value: String): String = {
    val field = System.getenv().getClass.getDeclaredField("m")
    field.setAccessible(true)
    val map = field.get(System.getenv()).asInstanceOf[java.util.Map[java.lang.String, java.lang.String]]
    map.put(key, value)
  }

  test("Test Hudi Conf") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath
      val partitionVal = "2021"
      // Create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  year string
           |) using hudi
           | partitioned by (year)
           | location '$tablePath'
           | options (
           |  primaryKey ='id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)

      // First insert a new record
      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000, $partitionVal)")

      val metaClient = createMetaClient(spark, tablePath)
      val commitCompletionTime1 = metaClient.getActiveTimeline.filterCompletedInstants().lastInstant().get().getCompletionTime

      // Then insert another new record
      spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000, $partitionVal)")

      checkAnswer(s"select id, name, price, ts, year from $tableName")(
        Seq(1, "a1", 10.0, 1000, partitionVal),
        Seq(2, "a2", 10.0, 1000, partitionVal)
      )

      // By default, Spark DML would set table type to COW and use Hive style partitioning, here we
      // set table type to MOR and disable Hive style partitioning in the hudi conf file, and check
      // if Hudi DML can load these configs correctly
      assertResult(true)(Files.exists(Paths.get(s"$tablePath/$partitionVal")))
      assertResult(HoodieTableType.MERGE_ON_READ)(new HoodieTableConfig(
        HoodieStorageUtils.getStorage(tablePath, HoodieTestUtils.getDefaultStorageConf),
        new StoragePath(tablePath, HoodieTableMetaClient.METAFOLDER_NAME)).getTableType)

      // Manually pass incremental configs to global configs to make sure Hudi query is able to load the
      // global configs
      DFSPropertiesConfiguration.addToGlobalProps(QUERY_TYPE.key, QUERY_TYPE_INCREMENTAL_OPT_VAL)
      DFSPropertiesConfiguration.addToGlobalProps(START_COMMIT.key, commitCompletionTime1)
      spark.catalog.refreshTable(tableName)
      checkAnswer(s"select id, name, price, ts, year from $tableName")(
        Seq(2, "a2", 10.0, 1000, partitionVal)
      )

      // delete the record
      spark.sql(s"delete from $tableName where year = $partitionVal")
      val cnt = spark.sql(s"select * from $tableName where year = $partitionVal").count()
      assertResult(0)(cnt)

      // check that schema evolution is enabled (from hudi-defaults.conf),
      // so no exception is thrown on alter table change column type
      spark.sql(s"alter table $tableName change column price price string")
    }
  }

  before {
    val testPropsFilePath = new File("src/test/resources/external-config").getAbsolutePath
    setEnv(DFSPropertiesConfiguration.CONF_FILE_DIR_ENV_NAME, testPropsFilePath)
    DFSPropertiesConfiguration.refreshGlobalProps()
  }

  after {
    DFSPropertiesConfiguration.clearGlobalProps()
  }
}
