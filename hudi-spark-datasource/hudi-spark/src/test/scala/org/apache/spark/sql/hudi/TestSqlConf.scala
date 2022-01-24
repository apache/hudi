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

package org.apache.spark.sql.hudi

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.common.config.DFSPropertiesConfiguration
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}

import java.io.File
import java.nio.file.{Files, Paths}

import org.scalatest.BeforeAndAfter

class TestSqlConf extends TestHoodieSqlBase with BeforeAndAfter {

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
           |  preCombineField = 'ts'
           | )
       """.stripMargin)

      // First merge with a extra input field 'flag' (insert a new record)
      spark.sql(
        s"""
           | merge into $tableName
           | using (
           |  select 1 as id, 'a1' as name, 10 as price, 1000 as ts, '1' as flag, $partitionVal as year
           | ) s0
           | on s0.id = $tableName.id
           | when matched and flag = '1' then update set
           | id = s0.id, name = s0.name, price = s0.price, ts = s0.ts, year = s0.year
           | when not matched and flag = '1' then insert *
       """.stripMargin)
      checkAnswer(s"select id, name, price, ts, year from $tableName")(
        Seq(1, "a1", 10.0, 1000, partitionVal)
      )

      // By default, Spark DML would set table type to COW and use Hive style partitioning, here we
      // set table type to MOR and disable Hive style partitioning in the hudi conf file, and check
      // if Hudi DML can load these configs correctly
      assertResult(true)(Files.exists(Paths.get(s"$tablePath/$partitionVal")))
      assertResult(HoodieTableType.MERGE_ON_READ)(new HoodieTableConfig(
        new Path(tablePath).getFileSystem(new Configuration),
        s"$tablePath/" + HoodieTableMetaClient.METAFOLDER_NAME,
        HoodieTableConfig.PAYLOAD_CLASS_NAME.defaultValue).getTableType)

      // delete the record
      spark.sql(s"delete from $tableName where year = $partitionVal")
      val cnt = spark.sql(s"select * from $tableName where year = $partitionVal").count()
      assertResult(0)(cnt)
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
