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
import org.apache.spark.sql.DataFrameUtil
import org.junit.jupiter.api.Assertions.assertTrue

class TestTruncateTableProcedure extends HoodieSparkProcedureTestBase {

  test("Test Call truncate_table Procedure") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName
      //Step1: create table and insert data
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

      spark.sql(s"insert into $tableName select 1, 'a1', 10.0, 1000L")
      spark.sql(s"insert into $tableName select 2, 'a2', 20.0, 1500L")
      spark.sql(s"insert into $tableName select 3, 'a3', 30.0, 2000L")
      spark.sql(s"insert into $tableName select 4, 'a4', 40.0, 2500L")

      //Step2: call truncate_table procedure
      spark.sql(s"""call truncate_table(table => '$tableName')""")

      val fs = new Path(tablePath).getFileSystem(spark.sparkContext.hadoopConfiguration)
      val files = fs.listStatus(new Path(tablePath))

      //Step3: check number of directories under tablePath, only .hoodie
      assertTrue(files.size == 1)
    }
  }
}
