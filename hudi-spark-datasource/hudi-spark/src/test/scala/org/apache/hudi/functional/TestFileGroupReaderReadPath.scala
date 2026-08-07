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

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieFileIndex, ScalaAssertionSupport}
import org.apache.hudi.HoodieConversionUtils.toJavaOption
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.{DataSourceTestUtils, HoodieSparkClientTestBase}
import org.apache.hudi.util.JFunction

import org.apache.spark.sql.{SaveMode, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.execution.datasources.PartitionDirectory
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType, StringType}
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

import java.util.function.Consumer

/**
 * Functional coverage for the default (file-group reader) Spark read path.
 *
 * These tests keep [[org.apache.hudi.common.config.HoodieReaderConfig.FILE_GROUP_READER_ENABLED]]
 * at its default (enabled) and drive read-time schema evolution, MOR base+log merge, partition
 * pruning and typed partition-value projection through public DataFrame reads. They intentionally
 * target branches left uncovered by the legacy-reader suite (PR #19133) and the existing
 * COW/MOR/CDC functional suites: schema-on-read filter rebuilding, add-column / type-promotion
 * evolution branches, HoodieFileIndex/SparkHoodieTableFileIndex partition pruning decisions, and
 * the per-version HoodiePartitionValues getters used when partition columns are reconstructed.
 */
class TestFileGroupReaderReadPath extends HoodieSparkClientTestBase with ScalaAssertionSupport {

  var spark: SparkSession = _

  private val baseOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "2",
    "hoodie.upsert.shuffle.parallelism" -> "2",
    "hoodie.bulkinsert.shuffle.parallelism" -> "2",
    "hoodie.delete.shuffle.parallelism" -> "1",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
    HoodieTableConfig.ORDERING_FIELDS.key -> "ts",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_fg_reader_test",
    // keep log files around on MOR so snapshot reads exercise base+log merge
    "hoodie.compact.inline" -> "false"
  )

  override def getSparkSessionExtensionsInjector: org.apache.hudi.common.util.Option[Consumer[SparkSessionExtensions]] =
    toJavaOption(
      Some(
        JFunction.toJavaConsumer((receiver: SparkSessionExtensions) =>
          new HoodieSparkSessionExtension().apply(receiver))))

  @BeforeEach
  override def setUp(): Unit = {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initHoodieStorage()
  }

  @AfterEach
  override def tearDown(): Unit = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  /**
   * Add-column schema evolution read: a column is introduced by a later commit while
   * schema-on-read is enabled. The snapshot read must expose the new column (null for the
   * older, un-touched rows), and pushed-down filters over both the pre-existing and the newly
   * added columns must produce correct results. On MOR the update batch lands in log files, so
   * the snapshot read additionally exercises the base+log merge iteration.
   */
  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testAddColumnEvolutionSnapshotAndIncrementalRead(tableType: HoodieTableType): Unit = {
    val _spark = spark
    import _spark.implicits._

    val writeOpts = baseOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name,
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "part",
      "hoodie.schema.on.read.enable" -> "true"
    )

    // V1: (id, name, age, ts, part) with ages 10..17 across two partitions
    val v1 = (0 until 8).map(i => (s"id$i", s"n$i", 10 + i, 1L, if (i % 2 == 0) "p1" else "p2"))
      .toDF("id", "name", "age", "ts", "part")
    v1.write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val firstCompletion = DataSourceTestUtils.latestCommitCompletionTime(storage, basePath)

    // V2: introduce column `bonus`; update id2/id5 and insert id8/id9. The new column is
    // nullable (Option[Double]) so that add-column auto-evolution passes the backwards
    // compatibility check (a non-nullable added column has no default for the older rows).
    val v2 = Seq[(String, String, Int, Long, String, Option[Double])](
      ("id2", "n2u", 12, 2L, "p1", Some(100.0d)),
      ("id5", "n5u", 15, 2L, "p2", Some(200.0d)),
      ("id8", "n8", 20, 2L, "p1", Some(300.0d)),
      ("id9", "n9", 21, 2L, "p2", Some(400.0d))
    ).toDF("id", "name", "age", "ts", "part", "bonus")
    v2.write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val snapshot = spark.read.format("hudi")
      .option("hoodie.schema.on.read.enable", "true")
      .load(basePath)

    // new column is surfaced with the expected type
    assertEquals(DoubleType, snapshot.schema("bonus").dataType)
    assertEquals(10, snapshot.count())
    // bonus is only populated for the rows written by V2
    assertEquals(4, snapshot.filter("bonus is not null").count())
    // filter pushdown over a pre-existing column across the evolved schema
    assertEquals(8, snapshot.filter("age >= 12").count())
    // combined filter that rebuilds over both the old and the added column
    assertEquals(4, snapshot.filter("age >= 12 AND bonus is not null").count())

    val byId = snapshot.select("id", "name", "age", "bonus").collect().map(r => r.getString(0) -> r).toMap
    // updated row reflects the V2 value
    assertEquals("n2u", byId("id2").getString(1))
    assertEquals(100.0d, byId("id2").getDouble(3))
    // old, untouched row keeps its value and has a null bonus
    assertEquals(10, byId("id0").getInt(2))
    assertTrue(byId("id0").isNullAt(3))

    // Incremental read of everything written after V1 must return exactly the V2 records. The
    // incremental range is start-exclusive (OPEN_CLOSED), so starting from V1's completion time
    // pulls only the commits that landed after it (i.e. V2).
    val incremental = spark.read.format("hudi")
      .option("hoodie.schema.on.read.enable", "true")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, firstCompletion)
      .load(basePath)
    val incIds = incremental.select("id").collect().map(_.getString(0)).toSet
    assertEquals(Set("id2", "id5", "id8", "id9"), incIds)
    assertTrue(incremental.schema.fieldNames.contains("bonus"))
  }

  /**
   * Type-promotion schema evolution read: with schema-on-read enabled, a later commit widens an
   * integer column to long. The snapshot read must return the promoted type and correctly read
   * the older files (written as int) through the promotion path, including pushed-down filters
   * over the promoted column.
   */
  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testTypePromotionEvolutionRead(tableType: HoodieTableType): Unit = {
    val _spark = spark
    import _spark.implicits._

    val writeOpts = baseOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name,
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "part",
      "hoodie.schema.on.read.enable" -> "true"
    )

    // V1: age is int
    val v1 = (0 until 6).map(i => (s"id$i", 10 + i, 1L, if (i % 2 == 0) "p1" else "p2"))
      .toDF("id", "age", "ts", "part")
    v1.write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    // V2: same column, now long -> promotes age int => long via schema-on-read
    val v2 = Seq(
      ("id2", 12L, 2L, "p1"),
      ("id3", 9999999999L, 2L, "p2"),
      ("id6", 42L, 2L, "p1")
    ).toDF("id", "age", "ts", "part")
    v2.write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val snapshot = spark.read.format("hudi")
      .option("hoodie.schema.on.read.enable", "true")
      .load(basePath)

    // promoted column type is now long
    assertEquals(LongType, snapshot.schema("age").dataType)
    assertEquals(7, snapshot.count())

    val byId = snapshot.select("id", "age").collect().map(r => r.getString(0) -> r.getLong(1)).toMap
    // value that only fits in a long is read back correctly (id3)
    assertEquals(9999999999L, byId("id3"))
    // an older (int-written) row is read through the promotion path
    assertEquals(10L, byId("id0"))
    // an updated row carries its promoted value (id6 == 42)
    assertEquals(42L, byId("id6"))

    // filter pushdown over the promoted column rebuilds correctly across file schemas:
    // ages after evolution are {10, 11, 12, 9999999999, 14, 15, 42} => two rows exceed 40 (id3, id6)
    assertEquals(2, snapshot.filter("age > 40").count())
    // and a single row exceeds the int range
    assertEquals(1, snapshot.filter("age > 1000000000").count())
  }

  /**
   * Partition pruning + typed partition-value projection over the file-group reader.
   *
   * A multi-column partition key mixes a string column (`dt`) and an integer column (`region`),
   * so the reader reconstructs partition values from the partition path through the per-version
   * HoodiePartitionValues getters (string + int). We assert both the pruned partition/file list
   * produced by HoodieFileIndex (which parses typed values from the path) and the rows / typed
   * values returned by the corresponding DataFrame read.
   */
  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testPartitionPruningAndTypedPartitionValues(tableType: HoodieTableType): Unit = {
    val _spark = spark
    import _spark.implicits._

    val writeOpts = baseOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name,
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "dt,region",
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.ComplexKeyGenerator",
      DataSourceWriteOptions.URL_ENCODE_PARTITIONING.key -> "false",
      DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key -> "false"
    )

    // 4 partitions: dt in {2024-01-01, 2024-01-02} x region in {1, 2}, 3 rows each
    val rows = for {
      dt <- Seq("2024-01-01", "2024-01-02")
      region <- Seq(1, 2)
      i <- 0 until 3
    } yield (s"$dt-$region-$i", s"name$i", 100 + i, 1L, dt, region)
    val df = rows.toDF("id", "name", "value", "ts", "dt", "region")
    df.write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val metaClient = createMetaClient(spark, basePath)
    val readOpts = Map(
      DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL,
      "path" -> basePath
    )
    val fileIndex = HoodieFileIndex(spark, metaClient, None, readOpts)

    // Prune to a single (dt, region) partition; the int predicate exercises typed value parsing.
    // `region` is an integer partition column, so the pruning predicate is bound to the typed
    // (Integer) partition value reconstructed from the path and compares against an int literal.
    val singlePartitionFilter = And(
      EqualTo(attr("dt", StringType), lit("2024-01-01")),
      EqualTo(attr("region", IntegerType), lit(1))
    )
    val prunedSingle = fileIndex.listFiles(Seq(singlePartitionFilter), Seq.empty)
    assertEquals(1, prunedSingle.size)
    val PartitionDirectory(values, files) = prunedSingle.head
    assertTrue(files.nonEmpty)
    assertEquals("2024-01-01,1", values.toSeq(Seq(StringType, IntegerType)).mkString(","))

    // Prune on the string column alone -> both region partitions under that date survive.
    val dateOnlyFilter = EqualTo(attr("dt", StringType), lit("2024-01-02"))
    val prunedDate = fileIndex.listFiles(Seq(dateOnlyFilter), Seq.empty)
    assertEquals(2, prunedDate.size)
    assertTrue(prunedDate.forall(_.files.nonEmpty))

    // No filter -> all four partitions are listed.
    assertEquals(4, fileIndex.listFiles(Seq.empty, Seq.empty).size)

    // DataFrame read with the same predicate reconstructs the typed partition columns.
    val readDf = spark.read.format("hudi").load(basePath)
    assertEquals(12, readDf.count())
    assertEquals(IntegerType, readDf.schema("region").dataType)

    val onePartition = readDf.filter("dt = '2024-01-01' AND region = 1")
    assertEquals(3, onePartition.count())
    // region is served as a typed integer partition value
    val regions = onePartition.select("region").collect().map(_.getInt(0)).toSet
    assertEquals(Set(1), regions)
    val dates = onePartition.select("dt").collect().map(_.getString(0)).toSet
    assertEquals(Set("2024-01-01"), dates)
    assertFalse(onePartition.select("value").collect().isEmpty)
  }

  private def attr(name: String, dataType: DataType): AttributeReference =
    AttributeReference(name, dataType, nullable = true)()

  private def lit(value: Any): Literal = Literal(value)
}
