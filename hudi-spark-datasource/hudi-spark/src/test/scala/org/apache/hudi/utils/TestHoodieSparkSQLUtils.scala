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

package org.apache.hudi.utils

import org.apache.hudi.HoodieSparkSQLUtils

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

import scala.collection.JavaConverters._

class TestHoodieSparkSQLUtils extends HoodieSparkSqlTestBase {

  test("Test getBasePathFromTableName APIs") {
    withTable(generateTableName) { tableName =>
      val fullTableName = s"default.$tableName"
      createHudiTable(tableName)

      val expectedBasePath = getTableLocation(fullTableName)
      assertEquals(expectedBasePath, HoodieSparkSQLUtils.getBasePathFromTableName(spark, fullTableName))

      val jsc = JavaSparkContext.fromSparkContext(spark.sparkContext)
      assertEquals(expectedBasePath, HoodieSparkSQLUtils.getBasePathFromTableName(jsc, fullTableName))
    }
  }

  test("Test loadHoodiePathsFromHive APIs with and without prefix") {
    val tableName1 = generateTableName
    val tableName2 = generateTableName
    val otherPrefixTable = generateTableName
    val tableNamePrefix = tableName1.take(6)

    withTable(tableName1) { _ =>
      withTable(tableName2) { _ =>
        withTable(otherPrefixTable) { _ =>
          createHudiTable(tableName1)
          createHudiTable(tableName2)
          createHudiTable(otherPrefixTable)

          val fullTableName1 = s"default.$tableName1"
          val fullTableName2 = s"default.$tableName2"
          val fullOtherTableName = s"default.$otherPrefixTable"

          val allPairs = HoodieSparkSQLUtils.loadHoodiePathsFromHive(spark, "default", false).asScala
          val allTableNames = allPairs.map(_.getLeft).toSet
          assertTrue(allTableNames.contains(fullTableName1))
          assertTrue(allTableNames.contains(fullTableName2))
          assertTrue(allTableNames.contains(fullOtherTableName))

          val prefixPairs = HoodieSparkSQLUtils
            .loadHoodiePathsFromHive(spark, "default", false, tableNamePrefix)
            .asScala
          assertTrue(prefixPairs.forall(_.getLeft.split("\\.")(1).startsWith(tableNamePrefix)))
        }
      }
    }
  }

  test("Test loadHoodiePathsFromHive filterHudiDatasets mode") {
    withTable(generateTableName) { tableName =>
      createHudiTable(tableName)
      val filteredPairs = HoodieSparkSQLUtils.loadHoodiePathsFromHive(spark, "default", true).asScala
      // The current filter checks InputFormat value with startsWith("Hoodie").
      assertTrue(filteredPairs.isEmpty)
    }
  }

  private def createHudiTable(tableName: String): Unit = {
    spark.sql(
      s"""
         | create table $tableName (
         |  id int,
         |  name string,
         |  ts long
         | ) using hudi
         | tblproperties (
         |  primaryKey = 'id',
         |  orderingFields = 'ts'
         | )
         |""".stripMargin)
    spark.sql(s"insert into $tableName values (1, 'a1', 1000)")
  }

  private def getTableLocation(fullTableName: String): String = {
    spark.sql(s"desc formatted $fullTableName")
      .collect()
      .find(row => "Location".equals(row.getString(0)))
      .map(row => row.getString(1))
      .get
  }
}
