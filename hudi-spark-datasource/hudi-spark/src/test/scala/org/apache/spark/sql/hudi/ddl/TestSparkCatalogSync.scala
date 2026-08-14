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

package org.apache.spark.sql.hudi.ddl

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.{HiveStylePartitionValueExtractor, HiveSyncConfigHolder, HiveSyncTool}
import org.apache.hudi.hive.ddl.HiveSyncMode
import org.apache.hudi.sync.common.HoodieSyncConfig.{META_SYNC_BASE_PATH, META_SYNC_DATABASE_NAME, META_SYNC_PARTITION_EXTRACTOR_CLASS, META_SYNC_PARTITION_FIELDS, META_SYNC_TABLE_NAME}

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}

class TestSparkCatalogSync extends HoodieSparkSqlTestBase {

  private val sparkCatalogSyncKey = HiveSyncConfigHolder.HIVE_SYNC_USE_SPARK_CATALOG.key()

  test("Test Spark catalog sync with partition lifecycle") {
    withTempDir { tmp =>
      import spark.implicits._

      val tableName = generateTableName
      val databaseName = "testdb"
      val basePath = s"${tmp.getCanonicalPath}/$tableName"

      val syncProps = buildSyncProps(databaseName, tableName, basePath)
      syncProps.setProperty(sparkCatalogSyncKey, "true")

      try {
        spark.sql(s"create database if not exists $databaseName")
        writeToHudi(
          Seq((1, "a1", 1000L, "2024-01-01"), (2, "a2", 1001L, "2024-01-02"))
            .toDF("id", "name", "ts", "dt"),
          tableName,
          basePath,
          SaveMode.Overwrite)

        assertFalse(spark.catalog.tableExists(databaseName, tableName), "Table should not exist before sync")
        syncOnce(syncProps)
        assertTrue(spark.catalog.tableExists(databaseName, tableName), "Table should exist after sync")
        assertTrue(spark.sql(s"show partitions $databaseName.$tableName").count() == 2, "Initial partitions should be registered")

        writeToHudi(
          Seq((3, "a3", 1002L, "2024-01-03")).toDF("id", "name", "ts", "dt"),
          tableName,
          basePath,
          SaveMode.Append)
        syncOnce(syncProps)
        assertTrue(spark.sql(s"show partitions $databaseName.$tableName").count() == 3, "New partition should be registered after append")

        spark.sql(s"alter table $databaseName.$tableName drop partition (dt='2024-01-03')")
        val partitionRows = spark.sql(s"show partitions $databaseName.$tableName").collect().map(_.getString(0))
        assertFalse(partitionRows.contains("dt=2024-01-03"), "Dropped partition should not exist in catalog")
      } finally {
        spark.sql(s"drop table if exists $databaseName.$tableName")
      }
    }
  }

  test("Test Spark catalog sync with schema evolution") {
    withTempDir { tmp =>
      import spark.implicits._

      val tableName = generateTableName
      val databaseName = "testdb"
      val basePath = s"${tmp.getCanonicalPath}/$tableName"

      val syncProps = buildSyncProps(databaseName, tableName, basePath)
      syncProps.setProperty(sparkCatalogSyncKey, "true")

      try {
        spark.sql(s"create database if not exists $databaseName")
        writeToHudi(
          Seq((1, "a1", 1000L, "2024-01-01")).toDF("id", "name", "ts", "dt"),
          tableName,
          basePath,
          SaveMode.Overwrite)

        assertFalse(spark.catalog.tableExists(databaseName, tableName), "Table should not exist before sync")
        syncOnce(syncProps)
        assertFalse(hasColumn(databaseName, tableName, "age"), "Initial schema should not contain age")

        writeToHudi(
          Seq((2, "a2", 1001L, "2024-01-02", 21)).toDF("id", "name", "ts", "dt", "age"),
          tableName,
          basePath,
          SaveMode.Append,
          reconcileSchema = true)

        syncOnce(syncProps)
        assertTrue(hasColumn(databaseName, tableName, "age"), "Catalog schema should be updated with new column age")
      } finally {
        spark.sql(s"drop table if exists $databaseName.$tableName")
      }
    }
  }

  private def syncOnce(syncProps: TypedProperties): Unit = {
    val syncTool = new HiveSyncTool(syncProps, new HiveConf())
    try {
      syncTool.syncHoodieTable()
    } finally {
      syncTool.close()
    }
  }

  private def buildSyncProps(databaseName: String, tableName: String, basePath: String): TypedProperties = {
    val syncProps = new TypedProperties()
    syncProps.setProperty(META_SYNC_DATABASE_NAME.key, databaseName)
    syncProps.setProperty(META_SYNC_TABLE_NAME.key, tableName)
    syncProps.setProperty(META_SYNC_BASE_PATH.key, basePath)
    syncProps.setProperty(META_SYNC_PARTITION_FIELDS.key, "dt")
    syncProps.setProperty(META_SYNC_PARTITION_EXTRACTOR_CLASS.key, classOf[HiveStylePartitionValueExtractor].getName)
    syncProps.setProperty(org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_MODE.key(), HiveSyncMode.HMS.name())
    syncProps.setProperty(sparkCatalogSyncKey, "true")
    syncProps
  }

  private def writeToHudi(df: DataFrame,
                          tableName: String,
                          basePath: String,
                          mode: SaveMode,
                          reconcileSchema: Boolean = false): Unit = {
    val writer = df.write
      .format("hudi")
      .option(HoodieWriteConfig.TBL_NAME.key, tableName)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "id")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "ts")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "dt")
      .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key, "true")
      .option(DataSourceWriteOptions.TABLE_TYPE.key, "COPY_ON_WRITE")

    val finalWriter = if (reconcileSchema) {
      writer.option("hoodie.datasource.write.reconcile.schema", "true")
    } else {
      writer
    }
    finalWriter.mode(mode).save(basePath)
  }

  private def hasColumn(databaseName: String, tableName: String, columnName: String): Boolean = {
    spark.catalog.listColumns(databaseName, tableName).collect().exists(_.name.equalsIgnoreCase(columnName))
  }
}
