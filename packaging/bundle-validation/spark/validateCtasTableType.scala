/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils

val database = "default"

if (HoodieSqlCommonUtils.isUsingHiveCatalog(spark)) {
  val tableName1 = "test_ctas_1"
  spark.sql(
    s"""
       | create table $tableName1 using hudi
       | tblproperties(
       |    primaryKey = 'id'
       | )
       | AS
       | select 1 as id, 'a1' as name, 10 as price, 1000 as ts
  """.stripMargin)
  if (spark.catalog.getTable(tableName1).tableType.equals("MANAGED")) {
    val tableName2 = "test_ctas_2"
    spark.sql(
      s"""
         | create table $tableName2 using hudi
         | tblproperties(
         |    primaryKey = 'id'
         | )
         | location '/tmp/$tableName2'
         | AS
         | select 1 as id, 'a1' as name, 10 as price, 1000 as ts
  """.stripMargin)
    if (spark.catalog.getTable(tableName2).tableType.equals("EXTERNAL")) {
      System.out.println("CTAS hive table type validation passed.")
      System.exit(0)
    } else {
      System.err.println(s"CTAS hive table type validation failed:\n\tThe table type of $database.$tableName2 should be EXTERNAL")
      System.exit(1)
    }
  } else {
    System.err.println(s"CTAS hive table type validation failed:\n\tThe table type of $database.$tableName1 should be MANAGED")
    System.exit(1)
  }
} else {
  System.err.println(s"CTAS hive table type validation failed:\n\tSpark should us Hive as Session's Catalog")
  System.exit(1)
}

