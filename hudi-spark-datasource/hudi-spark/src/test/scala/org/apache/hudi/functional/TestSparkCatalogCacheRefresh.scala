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
 * Regression test for HUDI-18139.
 *
 * After a successful write + meta-sync, Hudi invalidates the Spark catalog relation cache for the
 * synced table so later reads in the same session see the new data. Two failure modes are covered:
 *
 *  1. The refresh must target the table in the SYNC database, never a same-named table in the
 *     session's current/`default` database (which, in the reported issue, pointed at a bucket the
 *     writer could not access and produced an AccessDenied).
 *  2. The refresh is best-effort: even if it fails (e.g. the table's storage is momentarily
 *     inaccessible), it must NOT fail the write, which has already been committed and synced.
 */
class TestSparkCatalogCacheRefresh extends HoodieSparkSqlTestBase {

  test("HUDI-18139: post-sync catalog refresh targets the sync db and never fails a committed write") {
    withTempDir { tmp =>
      val syncDb = "hudi_18139_sync_db"
      val tableName = "refresh_t"
      val pathDefault = new File(tmp, "default_refresh_t").getCanonicalPath
      val pathSyncDb = new File(tmp, "syncdb_refresh_t").getCanonicalPath
      try {
        spark.sql(s"create database if not exists $syncDb")
        // Same table name `refresh_t` in two databases, backed by different locations.
        spark.sql(
          s"""create table default.$tableName (id int, name string, ts long) using hudi
             | location '$pathDefault'
             | tblproperties (primaryKey = 'id', preCombineField = 'ts')""".stripMargin)
        spark.sql(s"insert into default.$tableName values (1, 'a', 1)")
        spark.sql(
          s"""create table $syncDb.$tableName (id int, name string, ts long) using hudi
             | location '$pathSyncDb'
             | tblproperties (primaryKey = 'id', preCombineField = 'ts')""".stripMargin)
        spark.sql(s"insert into $syncDb.$tableName values (1, 'a', 1)")

        // Current database is `default`; break the unrelated default.refresh_t so that ANY resolution
        // of it fails (mimics the inaccessible wrong-bucket table from the issue).
        spark.sql("use default")
        FileUtils.deleteDirectory(new File(pathDefault))

        // (1) Refreshing for the sync db must target `$syncDb.refresh_t` (intact), never the broken
        //     `default.refresh_t`. A buggy (unqualified) refresh would resolve `default.refresh_t`
        //     and throw here.
        HoodieSparkSqlWriterInternal.refreshSparkCatalogTableCache(spark, syncDb, Seq(tableName))

        // (2) Best-effort: even when the intended table's storage is also inaccessible, the refresh
        //     must swallow the error and not fail the (already committed + synced) write.
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

  test("HUDI-18139: refresh invalidates the cached relation so newly committed data becomes visible") {
    withTempDir { tmp =>
      val syncDb = "hudi_18139_fresh_db"
      val tableName = "fresh_t"
      val path = new File(tmp, "fresh_t").getCanonicalPath
      try {
        spark.sql(s"create database if not exists $syncDb")
        spark.sql(
          s"""create table $syncDb.$tableName (id int, name string, ts long) using hudi
             | location '$path'
             | tblproperties (primaryKey = 'id', preCombineField = 'ts')""".stripMargin)
        spark.sql(s"insert into $syncDb.$tableName values (1, 'a', 1)")

        // Read once to cache the catalog relation (file listing).
        assertResult(1)(spark.table(s"$syncDb.$tableName").count())

        // Append a row directly to the table's storage, bypassing the catalog so the cached relation
        // is now stale - this mimics the just-completed write whose data Hudi must make visible.
        spark.sql("select 2 as id, 'b' as name, cast(2 as bigint) as ts")
          .write.format("hudi")
          .option("hoodie.datasource.write.recordkey.field", "id")
          .option("hoodie.datasource.write.precombine.field", "ts")
          .option("hoodie.datasource.write.partitionpath.field", "")
          .option("hoodie.table.name", tableName)
          .mode(SaveMode.Append)
          .save(path)

        // Without refresh the cached relation still reports the stale row count, proving the refresh
        // below is doing real work (not a no-op).
        assertResult(1)(spark.table(s"$syncDb.$tableName").count())

        HoodieSparkSqlWriterInternal.refreshSparkCatalogTableCache(spark, syncDb, Seq(tableName))

        assertResult(2)(spark.table(s"$syncDb.$tableName").count())
      } finally {
        spark.sql("use default")
        spark.sql(s"drop table if exists $syncDb.$tableName")
        spark.sql(s"drop database if exists $syncDb cascade")
      }
    }
  }
}
