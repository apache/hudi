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

import org.apache.hudi.HoodieSparkSqlWriterInternal

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

import java.io.File

/**
 * Tests the post-sync Spark catalog cache refresh: it must target the table in the sync database
 * (never a same-named table in the session's current database), and it must never fail a write
 * that has already been committed and synced.
 */
class TestSparkCatalogCacheRefresh extends HoodieSparkSqlTestBase {

  test("Post-sync catalog refresh targets the sync db and never fails a committed write") {
    withTempDir { tmp =>
      val syncDb = "catalog_refresh_sync_db"
      val tableName = "refresh_t"
      val pathDefault = new File(tmp, "default_refresh_t").getCanonicalPath
      val pathSyncDb = new File(tmp, "syncdb_refresh_t").getCanonicalPath
      try {
        spark.sql(s"create database if not exists $syncDb")
        // Same table name in two databases, backed by different locations.
        Seq(s"default.$tableName" -> pathDefault, s"$syncDb.$tableName" -> pathSyncDb).foreach {
          case (qualifiedName, path) =>
            spark.sql(
              s"""create table $qualifiedName (id int, name string, ts long) using hudi
                 | location '$path'
                 | tblproperties (primaryKey = 'id', preCombineField = 'ts')""".stripMargin)
            spark.sql(s"insert into $qualifiedName values (1, 'a', 1)")
        }

        // Cache the sync db relation, then append a row straight to storage so the cache is stale.
        spark.sql("use default")
        assertResult(1)(spark.table(s"$syncDb.$tableName").count())
        spark.sql("select 2 as id, 'b' as name, cast(2 as bigint) as ts")
          .write.format("hudi")
          .option("hoodie.datasource.write.recordkey.field", "id")
          .option("hoodie.datasource.write.precombine.field", "ts")
          .option("hoodie.datasource.write.partitionpath.field", "")
          .option("hoodie.table.name", tableName)
          .mode(SaveMode.Append)
          .save(pathSyncDb)
        assertResult(1)(spark.table(s"$syncDb.$tableName").count())

        // Break the same-named table in the current database. A refresh that resolved the bare name
        // would hit it, have its failure swallowed, and leave the sync db relation stale.
        FileUtils.deleteDirectory(new File(pathDefault))
        HoodieSparkSqlWriterInternal.refreshSparkCatalogTableCache(spark, syncDb, Seq(tableName))
        assertResult(2)(spark.table(s"$syncDb.$tableName").count())

        // Best-effort: a refresh whose target storage is gone must not throw.
        FileUtils.deleteDirectory(new File(pathSyncDb))
        HoodieSparkSqlWriterInternal.refreshSparkCatalogTableCache(spark, syncDb, Seq(tableName))
      } finally {
        spark.sql("use default")
        spark.sql(s"drop table if exists default.$tableName")
        spark.sql(s"drop table if exists $syncDb.$tableName")
        spark.sql(s"drop database if exists $syncDb cascade")
      }
    }
  }
}
