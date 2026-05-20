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
import org.apache.hudi.DataSourceWriteOptions.{OPERATION, PARTITIONPATH_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Tag}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import java.sql.Timestamp

/**
 * End-to-end tests for apache/hudi#18752: Spark write path used to silently ignore
 * {@code spark.sql.parquet.outputTimestampType}, always emitting TIMESTAMP(MICROS) for
 * {@code TimestampType} columns regardless of what the user requested.
 *
 * Verifies that for both bulk_insert and upsert paths, requesting:
 *   - TIMESTAMP_MICROS  → parquet schema is INT64 TIMESTAMP(MICROS)
 *   - TIMESTAMP_MILLIS  → parquet schema is INT64 TIMESTAMP(MILLIS)
 *   - INT96             → parquet schema is INT96 (bulk_insert only — avro doesn't
 *                         model INT96 so the upsert path downgrades to MICROS)
 * and that the column's value round-trips at the precision the schema implies.
 */
@Tag("functional")
class TestOutputTimestampType extends HoodieSparkClientTestBase {

  var spark: SparkSession = _

  private val schema = StructType(Seq(
    StructField("id",    IntegerType,   nullable = false),
    StructField("ts_tz", TimestampType, nullable = false)
  ))

  // 2026-05-15 12:34:56.123456 UTC.
  private val testTimestampMicros = 1779193296123456L
  private val testRow = Row(1, new Timestamp(testTimestampMicros / 1000L))

  private val baseOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "2",
    "hoodie.upsert.shuffle.parallelism" -> "2",
    "hoodie.bulkinsert.shuffle.parallelism" -> "2",
    RECORDKEY_FIELD.key -> "id",
    HoodieTableConfig.ORDERING_FIELDS.key -> "id",
    HoodieWriteConfig.TBL_NAME.key -> "ts_output_type_test",
    HoodieMetadataConfig.ENABLE.key -> "false",
    "hoodie.datasource.write.partitionpath.field" -> "",
    "hoodie.datasource.write.keygenerator.class" -> "org.apache.hudi.keygen.NonpartitionedKeyGenerator"
  )

  @BeforeEach
  override def setUp() {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    initTestDataGenerator()
    initHoodieStorage()
  }

  @AfterEach
  override def tearDown() = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  /** Inspects the first parquet file written under {@code path} and returns
   * the primitive type and logical-type annotation of the {@code ts_tz} column. */
  private def parquetTimestampType(path: String): (PrimitiveTypeName, String) = {
    val pq = listParquetFiles(path).head
    val reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(pq), spark.sparkContext.hadoopConfiguration))
    try {
      val field = reader.getFooter.getFileMetaData.getSchema.getColumns.asScalaCollection
        .find(_.getPath()(0) == "ts_tz").get
      val annotation = Option(field.getPrimitiveType.getLogicalTypeAnnotation).map(_.toString).getOrElse("(none)")
      (field.getPrimitiveType.getPrimitiveTypeName, annotation)
    } finally {
      reader.close()
    }
  }

  private def listParquetFiles(path: String): Seq[String] = {
    val root = new java.io.File(java.net.URI.create(if (path.startsWith("file:")) path else "file:" + path))
    walk(root).filter(f => f.endsWith(".parquet") && !f.contains("/.hoodie/")).toSeq
  }

  private def walk(f: java.io.File): Iterator[String] = {
    if (f.isDirectory) f.listFiles.iterator.flatMap(walk) else Iterator(f.getAbsolutePath)
  }

  /**
   * BULK_INSERT path — uses HoodieRowParquetWriteSupport, which is the writer my fix
   * directly modifies. All three outputTimestampType values (MICROS, MILLIS, INT96)
   * should be honored.
   */
  @ParameterizedTest
  @CsvSource(Array(
    "TIMESTAMP_MICROS, INT64,  timestamp(MICROS,true)",
    "TIMESTAMP_MILLIS, INT64,  timestamp(MILLIS,true)",
    "INT96,            INT96,  (none)"
  ))
  def testBulkInsertHonorsOutputTimestampType(outputType: String, expectedPrimitive: String, expectedAnnotation: String): Unit = {
    spark.conf.set("spark.sql.parquet.outputTimestampType", outputType)
    spark.createDataFrame(java.util.Arrays.asList(testRow), schema)
      .write.format("org.apache.hudi")
      .options(baseOpts ++ Map(OPERATION.key -> DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL))
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val (primitive, annotation) = parquetTimestampType(basePath)
    assertEquals(PrimitiveTypeName.valueOf(expectedPrimitive.trim), primitive,
      s"bulk_insert with outputTimestampType=$outputType produced wrong parquet primitive type")
    assertEquals(expectedAnnotation.trim, annotation,
      s"bulk_insert with outputTimestampType=$outputType produced wrong parquet logical type annotation")

    // Round-trip check: value should be preserved at the precision the schema implies.
    val readBack = spark.read.format("org.apache.hudi").load(basePath)
      .select("ts_tz").collect()(0).getTimestamp(0)
    val readBackMicros = readBack.getTime * 1000L + readBack.getNanos / 1000L % 1000L
    val expectedMicros = if (outputType == "TIMESTAMP_MILLIS")
      // MILLIS truncates the last 3 digits
      testTimestampMicros / 1000L * 1000L
    else
      testTimestampMicros
    assertEquals(expectedMicros, readBackMicros,
      s"bulk_insert+$outputType: read-back microseconds don't match the precision implied by the schema")
  }

  /**
   * UPSERT path — uses HoodieAvroWriteSupport. Avro doesn't model INT96, so:
   *   - TIMESTAMP_MICROS / TIMESTAMP_MILLIS are honored
   *   - INT96 is downgraded to MICROS (Hudi can't emit INT96 via the avro pipeline)
   * The "downgrade to MICROS for INT96" is the limit of how far the fix can extend
   * without rewriting the entire Spark→Avro conversion to bypass the avro intermediate.
   */
  @ParameterizedTest
  @CsvSource(Array(
    "TIMESTAMP_MICROS, INT64,  timestamp(MICROS,true)",
    "TIMESTAMP_MILLIS, INT64,  timestamp(MILLIS,true)"
  ))
  def testUpsertHonorsOutputTimestampType(outputType: String, expectedPrimitive: String, expectedAnnotation: String): Unit = {
    spark.conf.set("spark.sql.parquet.outputTimestampType", outputType)
    spark.createDataFrame(java.util.Arrays.asList(testRow), schema)
      .write.format("org.apache.hudi")
      .options(baseOpts ++ Map(OPERATION.key -> DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL))
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val (primitive, annotation) = parquetTimestampType(basePath)
    assertEquals(PrimitiveTypeName.valueOf(expectedPrimitive.trim), primitive,
      s"upsert with outputTimestampType=$outputType produced wrong parquet primitive type")
    assertEquals(expectedAnnotation.trim, annotation,
      s"upsert with outputTimestampType=$outputType produced wrong parquet logical type annotation")
  }

  /**
   * If the user explicitly sets the Hudi-specific {@code hoodie.parquet.outputtimestamptype},
   * it overrides spark.sql.parquet.outputTimestampType. Verifies the priority chain of
   * the resolver (documented in HoodieRowParquetWriteSupport.resolveOutputTimestampType).
   */
  @ParameterizedTest
  @CsvSource(Array(
    // spark conf,         hoodie override,    expected
    "TIMESTAMP_MICROS,     TIMESTAMP_MILLIS,   timestamp(MILLIS,true)",
    "TIMESTAMP_MILLIS,     TIMESTAMP_MICROS,   timestamp(MICROS,true)"
  ))
  def testHoodieConfigOverridesSparkSqlConf(sparkConfVal: String, hoodieConfVal: String, expectedAnnotation: String): Unit = {
    spark.conf.set("spark.sql.parquet.outputTimestampType", sparkConfVal)
    spark.createDataFrame(java.util.Arrays.asList(testRow), schema)
      .write.format("org.apache.hudi")
      .options(baseOpts ++ Map(
        OPERATION.key -> DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL,
        "hoodie.parquet.outputtimestamptype" -> hoodieConfVal))
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val (_, annotation) = parquetTimestampType(basePath)
    assertEquals(expectedAnnotation.trim, annotation,
      s"hoodie config=$hoodieConfVal should override spark conf=$sparkConfVal")
  }

  // Tiny extension to iterate a Java Collection from Scala without scala.jdk dependency
  // (so the test compiles across Scala 2.12 and 2.13 without an import).
  private implicit class JCollectionOps[T](c: java.util.Collection[T]) {
    def asScalaCollection: Iterable[T] = {
      val out = scala.collection.mutable.ArrayBuffer.empty[T]
      val it = c.iterator
      while (it.hasNext) out += it.next()
      out
    }
  }
}
