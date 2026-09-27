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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.UnsafeProjectionPool
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, LongType, StringType, StructType}
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.Path

/**
 * Queries whose operators consume the rows of a scan without copying them first. With the vectorized reader off,
 * Spark does not copy the rows of a parquet scan into rows of its own, so reused projections hand their rows straight
 * to union, joins, the cache, sorts and limits. One executor thread (local[1]) runs every task, so reuse crosses files,
 * tasks and queries. The results must be the ones of generating a projection for every file.
 */
class TestRowProjectionReuseQueries {

  private var spark: SparkSession = _

  @TempDir
  var tempDir: Path = _

  @BeforeEach
  def setUp(): Unit = {
    val conf = new SparkConf()
      .set("spark.app.name", getClass.getName)
      .set("spark.master", "local[1]")
      .set("spark.default.parallelism", "2")
      .set("spark.sql.shuffle.partitions", "2")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .set("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .set("spark.sql.parquet.enableVectorizedReader", "false")
      // Small files, so that every scan reads several of them per task
      .set("spark.sql.files.maxPartitionBytes", "1048576")
    spark = SparkSession.builder.config(conf).getOrCreate()
  }

  @AfterEach
  def tearDown(): Unit = {
    UnsafeProjectionPool.poolingEnabled = true
    if (spark != null) {
      spark.stop()
      spark = null
    }
  }

  private val nestedType = new StructType()
    .add("a", IntegerType)
    .add("arr", ArrayType(new StructType().add("x", LongType).add("y", StringType)))

  /** A partitioned table whose base files hold `v` as int or as long, with updates (log files on MOR). */
  private def writeTable(tableType: HoodieTableType, basePath: String): Unit = {
    val opts = Map(
      HoodieWriteConfig.TBL_NAME.key -> "row_projection_queries",
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name,
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "p",
      HoodieTableConfig.ORDERING_FIELDS.key -> "ts",
      HoodieCompactionConfig.INLINE_COMPACT.key -> "false",
      "hoodie.parquet.small.file.limit" -> "0",
      "hoodie.insert.shuffle.parallelism" -> "2",
      "hoodie.upsert.shuffle.parallelism" -> "2")
    def write(vType: DataType, ids: Range, ts: Long, operation: String): Unit = {
      val schema = new StructType().add("id", StringType).add("p", StringType).add("ts", LongType).add("v", vType)
        .add("s", nestedType)
      val rows = ids.map { id =>
        val v: Any = if (vType == IntegerType) Int.box(id * 7 + ts.toInt) else Long.box(id * 7L + ts)
        Row(s"id$id", s"p${id % 3}", ts, v, Row(id, Seq(Row(id.toLong, s"y$id"), Row(ts, null))))
      }
      spark.createDataFrame(spark.sparkContext.parallelize(rows, 3), schema).write.format("hudi")
        .options(opts)
        .option(DataSourceWriteOptions.OPERATION.key, operation)
        .mode(if (ts == 1L) SaveMode.Overwrite else SaveMode.Append)
        .save(basePath)
    }
    write(IntegerType, 0 until 30, 1L, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
    write(LongType, 30 until 60, 2L, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
    write(LongType, 0 until 60 by 5, 3L, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
  }

  /** Every query of the test, by name, each result in a deterministic order. */
  private def runQueries(basePath: String): Seq[(String, Seq[String])] = {
    def table: DataFrame = spark.read.format("hudi").load(basePath).select("id", "p", "ts", "v", "s")
    def sorted(df: DataFrame): Seq[String] = df.collect().map(_.toString).sorted.toSeq
    def join(): Seq[String] = {
      val a = table.alias("a")
      val b = table.alias("b")
      sorted(a.join(b, col("a.id") === col("b.id")).select(col("a.id"), col("a.v"), col("b.v"), col("b.s")))
    }
    Seq(
      "union" -> sorted(table.union(table)),
      "broadcast join" -> join(),
      "sort merge join" -> {
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        try join() finally spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
      },
      "cache" -> {
        val cached = table.cache()
        try {
          assertEquals(60L, cached.count())
          sorted(cached)
        } finally {
          cached.unpersist(blocking = true)
        }
      },
      "order by" -> table.orderBy(col("v").desc, col("id")).collect().map(_.toString).toSeq,
      "order by and limit" -> table.orderBy(col("id")).limit(7).collect().map(_.toString).toSeq,
      "limit" -> Seq(table.limit(9).count().toString),
      "filter" -> sorted(table.where(col("v") > 100 && col("s.a") % 2 === 0)))
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testQueriesReturnTheRowsOfFreshProjections(tableType: HoodieTableType): Unit = {
    val basePath = tempDir.resolve("table").toString
    writeTable(tableType, basePath)
    val keyClasses = Seq(classOf[RowProjectionKey], classOf[ByNameProjectionKey], classOf[NestedPruningProjectionKey])
    def generations(): Seq[Long] = keyClasses.map(UnsafeProjectionPool.generations)

    UnsafeProjectionPool.poolingEnabled = false
    val start = generations()
    val expected = runQueries(basePath)
    val withoutPool = generations().zip(start).map(p => p._1 - p._2)

    UnsafeProjectionPool.poolingEnabled = true
    val firstStart = generations()
    val first = runQueries(basePath)
    val secondStart = generations()
    val second = runQueries(basePath)
    val secondRun = generations().zip(secondStart).map(p => p._1 - p._2)
    val firstRun = secondStart.zip(firstStart).map(p => p._1 - p._2)

    TestRowProjectionReuseQueries.LOG.warn(s"Projections generated by key class $keyClasses on $tableType: without the pool " +
      s"$withoutPool, first pooled run $firstRun, second pooled run $secondRun")
    assertEquals(60, expected.find(_._1 == "cache").get._2.size)
    expected.zip(first).zip(second).foreach { case ((e, f), s) =>
      assertEquals(e, f, s"First run of ${e._1}")
      assertEquals(e, s, s"Second run of ${e._1}")
    }
    // The queries read their base files row-based, and the pooled runs generate far fewer projections.
    assertTrue(withoutPool.head > 0, s"No row projection generated: $withoutPool")
    assertTrue(firstRun.sum < withoutPool.sum, s"Pooled $firstRun, not pooled $withoutPool")
    assertTrue(secondRun.sum < firstRun.sum, s"Second pooled run $secondRun, first $firstRun")
  }
}

object TestRowProjectionReuseQueries {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[TestRowProjectionReuseQueries])
}
