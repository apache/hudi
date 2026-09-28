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

package org.apache.hudi.functional

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

/**
 * A write must invalidate the Spark session cache entries built from the table it wrote to,
 * otherwise a cached DataFrame keeps serving the pre-write snapshot.
 *
 * Neither table here is registered in the catalog and meta sync stays off, which is what makes
 * the Hudi case discriminating: the only cache invalidation Hudi performed before this test was
 * a catalog refreshTable guarded by hoodie.meta.sync.enable, so it could not reach these entries.
 */
class TestHoodieWriteCacheInvalidation extends HoodieSparkClientTestBase {

  var spark: SparkSession = _

  private val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "2",
    "hoodie.upsert.shuffle.parallelism" -> "2",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "part",
    HoodieTableConfig.ORDERING_FIELDS.key -> "ts",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_cache_invalidation_test"
  )

  @BeforeEach override def setUp(): Unit = {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initHoodieStorage()
  }

  @AfterEach override def tearDown(): Unit = {
    spark.catalog.clearCache()
    cleanupSparkContexts()
    cleanupFileSystem()
  }

  private def rows(ids: Seq[Int]): DataFrame = {
    val ss = spark
    import ss.implicits._
    ids.map(i => (i.toString, "p1", i.toLong, s"v$i")).toDF("id", "part", "ts", "value")
  }

  /**
   * MOR is covered as well as COW because the two produce different relations on the read path,
   * and the cache entry is only reachable by path if the relation exposes a file index rooted at
   * the table base path. A fix that worked for one and silently did nothing for the other would
   * still leave this test green if only COW were exercised.
   */
  @ParameterizedTest
  @ValueSource(strings = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testCachedDataFrameSeesHudiWrite(tableType: String): Unit = {
    val opts = commonOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType)

    rows(1 to 3).write.format("hudi")
      .options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val cached = spark.read.format("hudi").load(basePath)
    cached.cache()
    assertEquals(3, cached.count(), "the cache should be populated from the first three rows")

    rows(Seq(4)).write.format("hudi")
      .options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    assertEquals(4, cached.count(),
      "the cached DataFrame served the pre-write snapshot: the write did not invalidate it")
  }

  /**
   * An overwrite may leave the cached plan unusable rather than merely stale -- replacing a
   * partitioned table with a non-partitioned one leaves the cached plan holding the old partition
   * schema, and rebuilding it fails on the new layout. The write has already committed by then,
   * so it must still succeed, and the stale entry must be gone rather than left serving the old
   * snapshot.
   */
  @Test
  def testLayoutChangingOverwriteDoesNotFailTheWrite(): Unit = {
    rows(1 to 3).write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val cached = spark.read.format("hudi").load(basePath)
    cached.cache()
    assertEquals(3, cached.count(), "the cache should be populated from the partitioned table")

    // The same path, overwritten as a non-partitioned table: the cached plan above cannot be
    // rebuilt against this layout.
    rows(1 to 5).write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key,
        classOf[org.apache.hudi.keygen.NonpartitionedKeyGenerator].getName)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertEquals(5, spark.read.format("hudi").load(basePath).count(),
      "the overwrite should have committed and be readable")
  }

  /**
   * The same cycle on Parquet, where Spark invalidates the cache itself from
   * InsertIntoHadoopFsRelationCommand. It is here so a failure of the Hudi case above can be
   * attributed to Hudi rather than to the harness or to Spark's caching.
   */
  @Test
  def testCachedDataFrameSeesParquetWrite(): Unit = {
    val parquetPath = tempDir.resolve("parquet_control").toAbsolutePath.toString
    rows(1 to 3).write.mode(SaveMode.Overwrite).parquet(parquetPath)

    val cached = spark.read.parquet(parquetPath)
    cached.cache()
    assertEquals(3, cached.count(), "the cache should be populated from the first three rows")

    rows(Seq(4)).write.mode(SaveMode.Append).parquet(parquetPath)

    assertEquals(4, cached.count(),
      "the cached DataFrame served the pre-write snapshot: the write did not invalidate it")
  }
}
