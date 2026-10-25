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

package org.apache.hudi.functional

import org.apache.hudi.HoodieFileIndex
import org.apache.hudi.cdc.HoodieCDCFileIndex
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.log.InstantRange.RangeType
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals}
import org.junit.jupiter.api.Test

/**
 * CACHE TABLE must recache a Hudi table in place on write, not strand the previous dataset.
 *
 * Spark's CacheManager matches entries by plan. If a rebuilt HoodieFileIndex does not compare
 * equal to the one already cached, recacheByPlan cannot find the existing entry, so it is left
 * materialised and unreachable while a fresh one is added alongside it.
 */
class TestHoodieFileIndexCaching extends HoodieSparkClientTestBase {

  /** CacheManager.cachedData is private; read its size reflectively. */
  private def cacheEntries(): Int = {
    val cm = sparkSession.sharedState.cacheManager
    val m = cm.getClass.getDeclaredMethods.find(_.getName == "cachedData").get
    m.setAccessible(true)
    m.invoke(cm).asInstanceOf[scala.collection.Seq[_]].size
  }

  /**
   * Repeated cache/write cycles must not accumulate CacheManager entries.
   *
   * The discriminating input is that every insert targets the SAME partition that is already
   * cached: partition pruning therefore cannot mask the behaviour, and each write is guaranteed
   * to invalidate. A row count would not separate "recached" from "stranded" - both return the
   * right answer - so the assertion is on the entry count, and on UNCACHE reclaiming it.
   */
  @Test
  def testCacheTableRecachesInPlaceOnWrite(): Unit = {
    val table = "file_index_cache_tbl"
    sparkSession.sql(
      s"""
         |create table $table (id int, name string, price double, part string)
         | using hudi partitioned by (part)
         | tblproperties (primaryKey = 'id', type = 'cow')
         | location '$basePath/file_index_cache'
       """.stripMargin)
    sparkSession.sql(s"insert into $table values (1,'a',cast(10.0 as double),'p1')")
    assertEquals(0, cacheEntries(), "no cache entries before the first CACHE TABLE")

    for (i <- 1 to 5) {
      sparkSession.sql(s"cache table $table")
      sparkSession.sql(s"select count(*) from $table").collect()
      sparkSession.sql(s"insert into $table values (${100 + i},'x',cast(1.0 as double),'p1')")
      assertEquals(1, cacheEntries(),
        s"cycle $i: the table should hold exactly one CacheManager entry, not one per write")
    }

    sparkSession.sql(s"uncache table $table")
    assertEquals(0, cacheEntries(), "UNCACHE TABLE must reclaim the entry")
  }

  /**
   * The same cycle on plain Parquet, pinning the expected behaviour rather than asserting a
   * number chosen by hand. If this ever fails, the expectation above is wrong, not Hudi.
   */
  @Test
  def testParquetBaselineRecachesInPlaceOnWrite(): Unit = {
    val table = "file_index_cache_pq"
    sparkSession.sql(
      s"""
         |create table $table (id int, name string, price double, part string)
         | using parquet partitioned by (part)
         | location '$basePath/file_index_cache_pq'
       """.stripMargin)
    sparkSession.sql(s"insert into $table values (1,'a',cast(10.0 as double),'p1')")

    for (i <- 1 to 5) {
      sparkSession.sql(s"cache table $table")
      sparkSession.sql(s"select count(*) from $table").collect()
      sparkSession.sql(s"insert into $table values (${100 + i},'x',cast(1.0 as double),'p1')")
      assertEquals(1, cacheEntries(), s"cycle $i: parquet holds exactly one entry")
    }

    sparkSession.sql(s"uncache table $table")
    assertEquals(0, cacheEntries(), "UNCACHE TABLE must reclaim the entry")
  }

  /**
   * A subclass index must not compare equal to a plain snapshot index over the same table.
   *
   * Discriminating input: a HoodieCDCFileIndex and a HoodieFileIndex constructed with IDENTICAL
   * arguments. Every compared field matches, so only the exact-class check separates them. A
   * canEqual-based equals (what a case class generates) would report these equal, which would put
   * a CDC read and a snapshot read under the same CacheManager key.
   */
  @Test
  def testSubclassIndexDoesNotEqualSnapshotIndex(): Unit = {
    val table = "file_index_subclass_tbl"
    val path = s"$basePath/file_index_subclass"
    sparkSession.sql(
      s"""
         |create table $table (id int, name string, price double, part string)
         | using hudi partitioned by (part)
         | tblproperties (primaryKey = 'id', type = 'cow', hoodie.table.cdc.enabled = 'true')
         | location '$path'
       """.stripMargin)
    sparkSession.sql(s"insert into $table values (1,'a',cast(10.0 as double),'p1')")

    val metaClient = HoodieTableMetaClient.builder()
      .setConf(HoodieTestUtils.getDefaultStorageConf)
      .setBasePath(path).build()
    // Identical options for both, including the one CDC requires, so that CLASS is the only
    // thing that differs. In real use a CDC or incremental index always carries options a
    // snapshot index does not, which is why this conflation is latent rather than live - but
    // equality should not depend on that accident.
    val firstCommit = metaClient.getActiveTimeline.filterCompletedInstants().firstInstant().get().requestedTime()
    val opts = Map("path" -> path, "hoodie.datasource.read.begin.instanttime" -> firstCommit)

    val snapshot = HoodieFileIndex(sparkSession, metaClient, None, opts,
      includeLogFiles = true, shouldEmbedFileSlices = true)
    val cdc = new HoodieCDCFileIndex(sparkSession, metaClient, None, opts,
      includeLogFiles = true, rangeType = RangeType.CLOSED_CLOSED)

    assertNotEquals(snapshot, cdc,
      "a CDC index must not equal a snapshot index built from identical arguments")
    assertNotEquals(cdc, snapshot, "equality must be symmetric")

    val snapshot2 = HoodieFileIndex(sparkSession, metaClient, None, opts,
      includeLogFiles = true, shouldEmbedFileSlices = true)
    assertEquals(snapshot, snapshot2,
      "two snapshot indexes over the same table must still be equal - that is what the cache fix needs")
  }
}
