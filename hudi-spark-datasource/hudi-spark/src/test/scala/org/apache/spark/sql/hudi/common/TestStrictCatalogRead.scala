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

import org.apache.hudi.DataSourceWriteOptions.{COW_TABLE_TYPE_OPT_VAL, MOR_TABLE_TYPE_OPT_VAL, RECORDKEY_FIELD, TABLE_TYPE}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog.DEFAULT_DATABASE
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}

class TestStrictCatalogRead extends HoodieSparkSqlTestBase {

  private val expectedRows = Seq(Row(1, "a1", 10.0, 1000L), Row(2, "a2", 20.0, 2000L))

  test("Test strict catalog rejects table operations on the default database") {
    val tableName = generateTableName
    val e = intercept[StrictDefaultDatabaseException] {
      spark.catalog.tableExists(DEFAULT_DATABASE, tableName)
    }
    assertTrue(e.getMessage.contains(s"'$DEFAULT_DATABASE'"), e.getMessage)
    assertTrue(spark.catalog.databaseExists(DEFAULT_DATABASE))
    assertEquals(testDatabase, spark.catalog.currentDatabase)
  }

  test("Test qualified read from the default database of a table with no recorded database name") {
    Seq(COW_TABLE_TYPE_OPT_VAL, MOR_TABLE_TYPE_OPT_VAL).foreach { tableType =>
      withTempDir { tmp =>
        val tableName = writeAndRegisterTable(tmp.getCanonicalPath, tableType, Map.empty)
        assertEquals(expectedRows, selectFromDefaultSession(newDefaultSession(), tableName))
      }
    }
  }

  /**
   * A read that declares a bucket index still consults the catalog for the bucket options, and with no recorded
   * database name it probes `default`. On this branch that lookup is best-effort (a catalog error is logged and the
   * merge skipped), so the strict catalog cannot surface the probe and the test only pins that the read succeeds.
   * The identity-keyed lookup in the follow-up change removes the probe entirely.
   */
  test("Test qualified read from the default database of a bucket-indexed table with no recorded database name") {
    Seq(COW_TABLE_TYPE_OPT_VAL, MOR_TABLE_TYPE_OPT_VAL).foreach { tableType =>
      withTempDir { tmp =>
        val bucketOptions = Map(
          HoodieIndexConfig.INDEX_TYPE.key -> "BUCKET",
          HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key -> "2")
        val tableName = writeAndRegisterTable(tmp.getCanonicalPath, tableType, bucketOptions)
        val defaultSession = newDefaultSession()
        defaultSession.sql(s"set ${HoodieIndexConfig.INDEX_TYPE.key}=BUCKET")
        assertEquals(expectedRows, selectFromDefaultSession(defaultSession, tableName))
      }
    }
  }

  private def writeAndRegisterTable(dir: String, tableType: String, extraOptions: Map[String, String]): String = {
    val tableName = generateTableName
    val basePath = s"$dir/$tableName"
    val session = spark
    import session.implicits._
    expectedRows.map(r => (r.getInt(0), r.getString(1), r.getDouble(2), r.getLong(3))).toDF("id", "name", "price", "ts")
      .write.format("hudi")
      .option(HoodieWriteConfig.TBL_NAME.key, tableName)
      .option(TABLE_TYPE.key, tableType)
      .option(RECORDKEY_FIELD.key, "id")
      .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
      .options(extraOptions)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    spark.sql(s"create table $testDatabase.$tableName using hudi location '$basePath'")
    assertFalse(createMetaClient(spark, basePath).getTableConfig.contains(HoodieTableConfig.DATABASE_NAME))
    tableName
  }

  // A new session starts in `default`, like a production session connected to a catalog that only
  // allows connecting to `default`.
  private def newDefaultSession(): SparkSession = {
    val session = spark.newSession()
    assertEquals(DEFAULT_DATABASE, session.catalog.currentDatabase)
    session
  }

  private def selectFromDefaultSession(session: SparkSession, tableName: String): Seq[Row] =
    session.sql(s"select id, name, price, ts from $testDatabase.$tableName order by id").collect().toSeq
}
