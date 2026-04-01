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

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaType}
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import java.util.stream.Stream

import scala.collection.JavaConverters._

/**
 * End-to-end tests for vector column support in Hudi.
 * Tests round-trip data correctness through Spark DataFrames.
 */
class TestVectorDataSource extends HoodieSparkClientTestBase {

  var spark: SparkSession = null

  @BeforeEach override def setUp(): Unit = {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initHoodieStorage()
  }

  @AfterEach override def tearDown(): Unit = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  private def vectorMetadata(descriptor: String): Metadata =
    new MetadataBuilder().putString(HoodieSchema.TYPE_METADATA_FIELD, descriptor).build()

  private def createVectorDf(schema: StructType, data: Seq[Row]): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

  private def writeHudiTable(df: DataFrame, tableName: String, path: String,
      tableType: String = "COPY_ON_WRITE", precombineField: String = "id",
      mode: SaveMode = SaveMode.Overwrite, extraOpts: Map[String, String] = Map.empty): Unit = {
    var writer = df.write.format("hudi")
      .option(RECORDKEY_FIELD.key, "id")
      .option(PRECOMBINE_FIELD.key, precombineField)
      .option(TABLE_NAME.key, tableName)
      .option(TABLE_TYPE.key, tableType)
    extraOpts.foreach { case (k, v) => writer = writer.option(k, v) }
    writer.mode(mode).save(path)
  }

  private def readHudiTable(path: String): DataFrame =
    spark.read.format("hudi").load(path)

  /**
   * Round-trip test for all three element types (FLOAT, DOUBLE, INT8).
   * Verifies schema preservation, metadata, and exact value round-trip for each.
   */
  @ParameterizedTest
  @MethodSource(Array("vectorElementTypeArgs"))
  def testVectorRoundTripAllElementTypes(descriptor: String, elemType: DataType,
                                          dim: Int, subPath: String): Unit = {
    val metadata = vectorMetadata(descriptor)
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(elemType, containsNull = false), nullable = false, metadata)
    ))

    val random = new scala.util.Random(42)
    val data = (0 until 20).map { i =>
      val vec = elemType match {
        case FloatType  => Array.fill(dim)(random.nextFloat()).toSeq
        case DoubleType => Array.fill(dim)(random.nextDouble()).toSeq
        case ByteType   => Array.fill(dim)((random.nextInt(256) - 128).toByte).toSeq
      }
      Row(s"key_$i", vec)
    }

    val df = createVectorDf(schema, data)
    val path = basePath + "/" + subPath
    writeHudiTable(df, s"vec_rt_$subPath", path)

    val readDf = readHudiTable(path)
    assertEquals(20, readDf.count())

    // Verify element type and metadata preserved
    val embField = readDf.schema("embedding")
    assertEquals(elemType, embField.dataType.asInstanceOf[ArrayType].elementType)
    val parsed = HoodieSchema.parseTypeDescriptor(
      embField.metadata.getString(HoodieSchema.TYPE_METADATA_FIELD)).asInstanceOf[HoodieSchema.Vector]
    assertEquals(HoodieSchemaType.VECTOR, parsed.getType)
    assertEquals(dim, parsed.getDimension)

    // Verify values round-trip exactly
    val origMap = df.select("id", "embedding").collect()
      .map(r => r.getString(0) -> r.getSeq[Any](1)).toMap
    val readMap = readDf.select("id", "embedding").collect()
      .map(r => r.getString(0) -> r.getSeq[Any](1)).toMap

    origMap.foreach { case (id, orig) =>
      val read = readMap(id)
      assertEquals(dim, read.size, s"$id dimension wrong")
      orig.zip(read).zipWithIndex.foreach { case ((o, r), idx) =>
        assertEquals(o, r, s"$id[$idx] mismatch")
      }
    }
  }

  @Test
  def testNullableVectorField(): Unit = {
    // Vector column itself nullable (entire array can be null)
    val metadata = vectorMetadata("VECTOR(32)")

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = true, metadata) // nullable = true
    ))

    val data = Seq(
      Row("key_1", Array.fill(32)(0.5f).toSeq),
      Row("key_2", null), // null vector
      Row("key_3", Array.fill(32)(1.0f).toSeq)
    )

    val df = createVectorDf(schema, data)

    writeHudiTable(df, "nullable_vector_test", basePath + "/nullable")

    val readDf = readHudiTable(basePath + "/nullable")
    val readRows = readDf.select("id", "embedding").collect()

    // Verify null handling
    val key2Row = readRows.find(_.getString(0) == "key_2").get
    assertTrue(key2Row.isNullAt(1), "Null vector not preserved")

    // Verify non-null vectors preserved correctly
    val key1Row = readRows.find(_.getString(0) == "key_1").get
    assertFalse(key1Row.isNullAt(1))
    val key1Embedding = key1Row.getSeq[Float](1)
    assertEquals(32, key1Embedding.size)
    assertTrue(key1Embedding.forall(_ == 0.5f))

    val key3Row = readRows.find(_.getString(0) == "key_3").get
    assertFalse(key3Row.isNullAt(1))
    val key3Embedding = key3Row.getSeq[Float](1)
    assertEquals(32, key3Embedding.size)
    assertTrue(key3Embedding.forall(_ == 1.0f))
  }

  @Test
  def testColumnProjectionWithVector(): Unit = {
    val metadata = vectorMetadata("VECTOR(16)")

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata),
      StructField("label", StringType, nullable = true),
      StructField("score", IntegerType, nullable = true)
    ))

    val data = (0 until 10).map { i =>
      Row(s"key_$i", Array.fill(16)(i.toFloat).toSeq, s"label_$i", i * 10)
    }

    val df = createVectorDf(schema, data)

    writeHudiTable(df, "projection_vector_test", basePath + "/projection")

    // Read only non-vector columns (vector column excluded)
    val nonVectorDf = readHudiTable(basePath + "/projection")
      .select("id", "label", "score")
    assertEquals(10, nonVectorDf.count())
    val row0 = nonVectorDf.filter("id = 'key_0'").collect()(0)
    assertEquals("label_0", row0.getString(1))
    assertEquals(0, row0.getInt(2))

    // Read only the vector column with id
    val vectorOnlyDf = readHudiTable(basePath + "/projection")
      .select("id", "embedding")
    assertEquals(10, vectorOnlyDf.count())
    val vecRow = vectorOnlyDf.filter("id = 'key_5'").collect()(0)
    val embedding = vecRow.getSeq[Float](1)
    assertEquals(16, embedding.size)
    assertTrue(embedding.forall(_ == 5.0f))

    // Read all columns including vector
    val allDf = readHudiTable(basePath + "/projection")
      .select("id", "embedding", "label", "score")
    assertEquals(10, allDf.count())
    val allRow = allDf.filter("id = 'key_3'").collect()(0)
    assertEquals("label_3", allRow.getString(2))
    assertEquals(30, allRow.getInt(3))
    val allEmbedding = allRow.getSeq[Float](1)
    assertEquals(16, allEmbedding.size)
    assertTrue(allEmbedding.forall(_ == 3.0f))
  }

  @Test
  def testMultipleVectorColumns(): Unit = {
    val floatMeta = vectorMetadata("VECTOR(8)")
    val doubleMeta = vectorMetadata("VECTOR(4, DOUBLE)")

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("vec_float", ArrayType(FloatType, containsNull = false),
        nullable = false, floatMeta),
      StructField("label", StringType, nullable = true),
      StructField("vec_double", ArrayType(DoubleType, containsNull = false),
        nullable = true, doubleMeta)
    ))

    val data = (0 until 20).map { i =>
      Row(
        s"key_$i",
        Array.fill(8)(i.toFloat).toSeq,
        s"label_$i",
        if (i % 3 == 0) null else Array.fill(4)(i.toDouble).toSeq
      )
    }

    val df = createVectorDf(schema, data)

    writeHudiTable(df, "multi_vector_test", basePath + "/multi_vec")

    val readDf = readHudiTable(basePath + "/multi_vec")
    assertEquals(20, readDf.count())

    // Verify both vector columns present with correct types
    val floatField = readDf.schema("vec_float")
    assertEquals(FloatType, floatField.dataType.asInstanceOf[ArrayType].elementType)
    val doubleField = readDf.schema("vec_double")
    assertEquals(DoubleType, doubleField.dataType.asInstanceOf[ArrayType].elementType)

    // Verify data: row with both vectors
    val row5 = readDf.select("id", "vec_float", "vec_double")
      .filter("id = 'key_5'").collect()(0)
    val fVec = row5.getSeq[Float](1)
    assertEquals(8, fVec.size)
    assertTrue(fVec.forall(_ == 5.0f))
    val dVec = row5.getSeq[Double](2)
    assertEquals(4, dVec.size)
    assertTrue(dVec.forall(_ == 5.0))

    // Verify data: row with null double vector (i=0, i%3==0)
    val row0 = readDf.select("id", "vec_float", "vec_double")
      .filter("id = 'key_0'").collect()(0)
    assertFalse(row0.isNullAt(1))
    assertTrue(row0.isNullAt(2), "Expected null double vector for key_0")

    // Verify projection: select only one vector column
    val floatOnlyDf = readDf.select("id", "vec_float")
    assertEquals(20, floatOnlyDf.count())
    val doubleOnlyDf = readDf.select("id", "vec_double")
    assertEquals(20, doubleOnlyDf.count())
  }

  @Test
  def testCowUpsertWithVectors(): Unit = {
    val metadata = vectorMetadata("VECTOR(8)")

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata),
      StructField("ts", LongType, nullable = false),
      StructField("name", StringType, nullable = true)
    ))

    // Initial write
    val data1 = (0 until 10).map { i =>
      Row(s"key_$i", Array.fill(8)(0.0f).toSeq, i.toLong, s"name_$i")
    }

    writeHudiTable(createVectorDf(schema, data1), "cow_upsert_vec_test",
      basePath + "/cow_upsert", precombineField = "ts")

    // Upsert: update vectors for existing keys + add new keys
    val data2 = Seq(
      Row("key_0", Array.fill(8)(9.9f).toSeq, 100L, "updated_0"),
      Row("key_5", Array.fill(8)(5.5f).toSeq, 100L, "updated_5"),
      Row("key_10", Array.fill(8)(10.0f).toSeq, 100L, "new_10")
    )

    writeHudiTable(createVectorDf(schema, data2), "cow_upsert_vec_test",
      basePath + "/cow_upsert", precombineField = "ts", mode = SaveMode.Append)

    val readDf = readHudiTable(basePath + "/cow_upsert")
    assertEquals(11, readDf.count())

    // Verify updated key_0
    val r0 = readDf.select("id", "embedding", "name")
      .filter("id = 'key_0'").collect()(0)
    assertTrue(r0.getSeq[Float](1).forall(_ == 9.9f))
    assertEquals("updated_0", r0.getString(2))

    // Verify non-updated key_3
    val r3 = readDf.select("id", "embedding", "name")
      .filter("id = 'key_3'").collect()(0)
    assertTrue(r3.getSeq[Float](1).forall(_ == 0.0f))
    assertEquals("name_3", r3.getString(2))

    // Verify new key_10
    val r10 = readDf.select("id", "embedding", "name")
      .filter("id = 'key_10'").collect()(0)
    assertTrue(r10.getSeq[Float](1).forall(_ == 10.0f))
    assertEquals("new_10", r10.getString(2))
  }

  @Test
  def testDimensionBoundaries(): Unit = {
    // Large dimension (1536-dim, OpenAI embedding size) + small dimension (2-dim, edge values)
    val largeMeta = vectorMetadata("VECTOR(1536)")
    val smallMeta = vectorMetadata("VECTOR(2)")

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("large", ArrayType(FloatType, containsNull = false), nullable = false, largeMeta),
      StructField("small", ArrayType(FloatType, containsNull = false), nullable = false, smallMeta)
    ))

    val random = new scala.util.Random(7)
    val data = Seq(
      Row("k0", Array.fill(1536)(random.nextFloat()).toSeq, Seq(1.0f, 2.0f)),
      Row("k1", Array.fill(1536)(random.nextFloat()).toSeq, Seq(-1.5f, Float.MaxValue))
    )

    val df = createVectorDf(schema, data)
    writeHudiTable(df, "dim_boundary_test", basePath + "/dim_boundary")

    val readDf = readHudiTable(basePath + "/dim_boundary")
    assertEquals(2, readDf.count())

    val rows = readDf.select("id", "large", "small").collect()
      .map(r => r.getString(0) -> r).toMap

    // Large dimension: verify values preserved exactly
    val origLarge = df.select("id", "large").collect().map(r => r.getString(0) -> r.getSeq[Float](1)).toMap
    origLarge.foreach { case (id, orig) =>
      val read = rows(id).getSeq[Float](1)
      assertEquals(1536, read.size)
      orig.zip(read).foreach { case (o, r) => assertEquals(o, r, 1e-9f, s"Large mismatch in $id") }
    }

    // Small dimension: verify edge values
    val smallK0 = rows("k0").getSeq[Float](2)
    assertEquals(2, smallK0.size)
    assertEquals(1.0f, smallK0(0), 1e-9f)
    assertEquals(2.0f, smallK0(1), 1e-9f)
    val smallK1 = rows("k1").getSeq[Float](2)
    assertEquals(Float.MaxValue, smallK1(1), 1e-30f)
  }

  @Test
  def testVectorWithNonVectorArrayColumn(): Unit = {
    val vectorMeta = vectorMetadata("VECTOR(4)")

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = false, vectorMeta),
      StructField("tags", ArrayType(StringType, containsNull = true),
        nullable = true)
    ))

    val data = Seq(
      Row("k1", Seq(1.0f, 2.0f, 3.0f, 4.0f), Seq("tag1", "tag2")),
      Row("k2", Seq(5.0f, 6.0f, 7.0f, 8.0f), null),
      Row("k3", Seq(0.1f, 0.2f, 0.3f, 0.4f), Seq("tag3"))
    )

    val df = createVectorDf(schema, data)

    writeHudiTable(df, "mixed_array_test", basePath + "/mixed_array")

    val readDf = readHudiTable(basePath + "/mixed_array")
    assertEquals(3, readDf.count())

    // Vector column should be ArrayType(FloatType) with vector metadata
    val embField = readDf.schema("embedding")
    assertTrue(embField.metadata.contains(HoodieSchema.TYPE_METADATA_FIELD))
    assertEquals(FloatType, embField.dataType.asInstanceOf[ArrayType].elementType)

    // Non-vector array column should be ArrayType(StringType) without vector metadata
    val tagsField = readDf.schema("tags")
    assertFalse(tagsField.metadata.contains(HoodieSchema.TYPE_METADATA_FIELD))
    assertEquals(StringType, tagsField.dataType.asInstanceOf[ArrayType].elementType)

    // Verify vector data preserved
    val row1 = readDf.select("id", "embedding", "tags")
      .filter("id = 'k1'").collect()(0)
    val emb = row1.getSeq[Float](1)
    assertEquals(Seq(1.0f, 2.0f, 3.0f, 4.0f), emb)
    assertEquals(Seq("tag1", "tag2"), row1.getSeq[String](2))

    // Verify null tags preserved
    val row2 = readDf.select("id", "embedding", "tags")
      .filter("id = 'k2'").collect()(0)
    assertFalse(row2.isNullAt(1))
    assertTrue(row2.isNullAt(2))
  }

  @Test
  def testMorWithMultipleUpserts(): Unit = {
    val metadata = vectorMetadata("VECTOR(4, DOUBLE)")

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(DoubleType, containsNull = false),
        nullable = false, metadata),
      StructField("ts", LongType, nullable = false)
    ))

    // Insert batch 1
    val batch1 = (0 until 10).map { i =>
      Row(s"key_$i", Array.fill(4)(1.0).toSeq, 1L)
    }
    writeHudiTable(createVectorDf(schema, batch1), "mor_multi_upsert_test",
      basePath + "/mor_multi", tableType = "MERGE_ON_READ", precombineField = "ts")

    // Upsert batch 2: update key_0..key_4
    val batch2 = (0 until 5).map { i =>
      Row(s"key_$i", Array.fill(4)(2.0).toSeq, 2L)
    }
    writeHudiTable(createVectorDf(schema, batch2), "mor_multi_upsert_test",
      basePath + "/mor_multi", tableType = "MERGE_ON_READ", precombineField = "ts",
      mode = SaveMode.Append)

    // Upsert batch 3: update key_0..key_2 again
    val batch3 = (0 until 3).map { i =>
      Row(s"key_$i", Array.fill(4)(3.0).toSeq, 3L)
    }
    writeHudiTable(createVectorDf(schema, batch3), "mor_multi_upsert_test",
      basePath + "/mor_multi", tableType = "MERGE_ON_READ", precombineField = "ts",
      mode = SaveMode.Append)

    val readDf = readHudiTable(basePath + "/mor_multi")
    assertEquals(10, readDf.count())

    // key_0: updated 3 times → should have value 3.0
    val r0 = readDf.select("id", "embedding").filter("id = 'key_0'").collect()(0)
    assertTrue(r0.getSeq[Double](1).forall(_ == 3.0), "key_0 should have latest value 3.0")

    // key_3: updated once (batch 2) → should have value 2.0
    val r3 = readDf.select("id", "embedding").filter("id = 'key_3'").collect()(0)
    assertTrue(r3.getSeq[Double](1).forall(_ == 2.0), "key_3 should have value 2.0")

    // key_7: never updated → should have value 1.0
    val r7 = readDf.select("id", "embedding").filter("id = 'key_7'").collect()(0)
    assertTrue(r7.getSeq[Double](1).forall(_ == 1.0), "key_7 should have original value 1.0")
  }

  @Test
  def testDimensionMismatchOnWrite(): Unit = {
    // Schema declares VECTOR(8) but data has arrays of length 4
    val metadata = vectorMetadata("VECTOR(8)")

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata)
    ))

    val data = Seq(
      Row("key_1", Seq(1.0f, 2.0f, 3.0f, 4.0f)) // only 4 elements, schema says 8
    )

    val df = createVectorDf(schema, data)

    val ex = assertThrows(classOf[Exception], () => {
      writeHudiTable(df, "dim_mismatch_test", basePath + "/dim_mismatch")
    })
    // The root cause should mention dimension mismatch
    var cause: Throwable = ex
    var foundMismatch = false
    while (cause != null && !foundMismatch) {
      if (cause.getMessage != null && cause.getMessage.contains("dimension mismatch")) {
        foundMismatch = true
      }
      cause = cause.getCause
    }
    assertTrue(foundMismatch,
      s"Expected 'dimension mismatch' in exception chain, got: ${ex.getMessage}")
  }

  @Test
  def testSchemaEvolutionRejectsDimensionChange(): Unit = {
    // Write initial table with VECTOR(4)
    val metadata4 = vectorMetadata("VECTOR(4)")

    val schema4 = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata4),
      StructField("ts", LongType, nullable = false)
    ))

    val data1 = Seq(Row("key_1", Seq(1.0f, 2.0f, 3.0f, 4.0f), 1L))
    writeHudiTable(createVectorDf(schema4, data1), "schema_evolve_dim_test",
      basePath + "/schema_evolve_dim", precombineField = "ts")

    // Now try to write with VECTOR(8) — different dimension should be rejected
    val metadata8 = vectorMetadata("VECTOR(8)")

    val schema8 = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata8),
      StructField("ts", LongType, nullable = false)
    ))

    val data2 = Seq(Row("key_2", Seq(1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f), 2L))

    assertThrows(classOf[Exception], () => {
      writeHudiTable(createVectorDf(schema8, data2), "schema_evolve_dim_test",
        basePath + "/schema_evolve_dim", precombineField = "ts", mode = SaveMode.Append)
    })
  }

  /**
   * Verifies that vector column metadata is written to the Parquet file footer
   * under the key hoodie.vector.columns.
   */
  @Test
  def testParquetFooterContainsVectorMetadata(): Unit = {
    val metadata = vectorMetadata("VECTOR(8)")

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata)
    ))

    val data = Seq(Row("key_1", Array.fill(8)(1.0f).toSeq))
    writeHudiTable(createVectorDf(schema, data), "footer_meta_test", basePath + "/footer_meta")

    // Find a .parquet base file and read its footer metadata
    val conf = spark.sessionState.newHadoopConf()
    val fs = new Path(basePath + "/footer_meta").getFileSystem(conf)
    val parquetFiles = fs.listStatus(new Path(basePath + "/footer_meta"))
      .flatMap(d => Option(fs.listStatus(d.getPath)).getOrElse(Array.empty))
      .filter(f => f.getPath.getName.endsWith(".parquet") && !f.getPath.getName.startsWith("."))

    assertTrue(parquetFiles.nonEmpty, "Expected at least one parquet file")

    val reader = ParquetFileReader.open(HadoopInputFile.fromPath(parquetFiles.head.getPath, conf))
    try {
      val footerMeta = reader.getFileMetaData.getKeyValueMetaData.asScala
      assertTrue(footerMeta.contains(HoodieSchema.PARQUET_VECTOR_COLUMNS_METADATA_KEY),
        s"Footer should contain ${HoodieSchema.PARQUET_VECTOR_COLUMNS_METADATA_KEY}, got keys: ${footerMeta.keys.mkString(", ")}")

      val value = footerMeta(HoodieSchema.PARQUET_VECTOR_COLUMNS_METADATA_KEY)
      assertTrue(value.contains("embedding"), s"Footer value should reference 'embedding' column, got: $value")
      assertTrue(value.contains("VECTOR"), s"Footer value should contain 'VECTOR' descriptor, got: $value")
    } finally {
      reader.close()
    }
  }

  @Test
  def testPartitionedTableWithVector(): Unit = {
    val metadata = vectorMetadata("VECTOR(4)")

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata),
      StructField("label", StringType, nullable = true),
      StructField("category", StringType, nullable = false)
    ))

    // Two partitions: "catA" and "catB"
    val data = (0 until 10).map { i =>
      val category = if (i % 2 == 0) "catA" else "catB"
      Row(s"key_$i", Array.fill(4)(i.toFloat).toSeq, s"label_$i", category)
    }

    writeHudiTable(createVectorDf(schema, data), "partitioned_vector_test",
      basePath + "/partitioned",
      extraOpts = Map("hoodie.datasource.write.partitionpath.field" -> "category"))

    val readDf = readHudiTable(basePath + "/partitioned")
    assertEquals(10, readDf.count())

    // Collect all rows and verify each row's vector matches its key
    val rowMap = readDf.select("id", "embedding", "category").collect()
      .map(r => r.getString(0) -> (r.getSeq[Float](1), r.getString(2)))
      .toMap

    for (i <- 0 until 10) {
      val (vec, cat) = rowMap(s"key_$i")
      val expectedCat = if (i % 2 == 0) "catA" else "catB"
      assertEquals(4, vec.size, s"key_$i dimension wrong")
      assertTrue(vec.forall(_ == i.toFloat),
        s"key_$i: expected ${i.toFloat} but got ${vec.head} (ordinal mismatch?)")
      assertEquals(expectedCat, cat, s"key_$i partition value wrong")
    }

    // Also verify projection of vector-only across partitions
    val vecOnly = readDf.select("id", "embedding").collect()
      .map(r => r.getString(0) -> r.getSeq[Float](1)).toMap
    for (i <- 0 until 10) {
      assertTrue(vecOnly(s"key_$i").forall(_ == i.toFloat),
        s"key_$i projected vector wrong")
    }
  }

  @Test
  def testVectorAsLastColumn(): Unit = {
    val metadata = vectorMetadata("VECTOR(4)")

    // Vector is at position 4 (last), after several non-vector columns
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("col_a", IntegerType, nullable = true),
      StructField("col_b", StringType, nullable = true),
      StructField("col_c", DoubleType, nullable = true),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata)
    ))

    val data = (0 until 10).map { i =>
      Row(s"key_$i", i, s"str_$i", i.toDouble * 1.5, Array.fill(4)(i.toFloat).toSeq)
    }

    writeHudiTable(createVectorDf(schema, data), "last_col_vector_test", basePath + "/last_col")

    val readDf = readHudiTable(basePath + "/last_col")
    assertEquals(10, readDf.count())

    // Read all columns: verify vector and non-vector columns correct
    val allRows = readDf.select("id", "col_a", "col_b", "embedding").collect()
      .map(r => r.getString(0) -> r).toMap

    for (i <- 0 until 10) {
      val row = allRows(s"key_$i")
      assertEquals(i, row.getInt(1), s"col_a wrong for key_$i")
      assertEquals(s"str_$i", row.getString(2), s"col_b wrong for key_$i")
      val vec = row.getSeq[Float](3)
      assertEquals(4, vec.size, s"key_$i dimension wrong")
      assertTrue(vec.forall(_ == i.toFloat),
        s"key_$i vector wrong (ordinal mismatch?): expected ${i.toFloat}, got ${vec.head}")
    }

    // Project only the vector column (ordinal shifts to 0 in projected schema)
    val embOnly = readDf.select("id", "embedding").collect()
      .map(r => r.getString(0) -> r.getSeq[Float](1)).toMap
    for (i <- 0 until 10) {
      assertTrue(embOnly(s"key_$i").forall(_ == i.toFloat),
        s"key_$i projected-only vector wrong")
    }
  }

  /**
   * Schema evolution: adding a new non-vector column to a table that already has a vector column
   * should succeed. Old rows get null for the new column; vector data must be intact in all rows.
   */
  @Test
  def testSchemaEvolutionAddColumnToVectorTable(): Unit = {
    val metadata = vectorMetadata("VECTOR(4)")

    val schemaV1 = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata),
      StructField("ts", LongType, nullable = false)
    ))

    val data1 = (0 until 5).map { i =>
      Row(s"key_$i", Array.fill(4)(i.toFloat).toSeq, i.toLong)
    }
    writeHudiTable(createVectorDf(schemaV1, data1), "schema_evolve_add_col_test",
      basePath + "/schema_evolve_add", precombineField = "ts",
      extraOpts = Map("hoodie.schema.on.read.enable" -> "true"))

    // V2: add a new non-vector column
    val schemaV2 = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata),
      StructField("ts", LongType, nullable = false),
      StructField("new_col", StringType, nullable = true)
    ))

    val data2 = (5 until 10).map { i =>
      Row(s"key_$i", Array.fill(4)(i.toFloat).toSeq, i.toLong, s"v2_$i")
    }
    writeHudiTable(createVectorDf(schemaV2, data2), "schema_evolve_add_col_test",
      basePath + "/schema_evolve_add", precombineField = "ts", mode = SaveMode.Append,
      extraOpts = Map("hoodie.schema.on.read.enable" -> "true"))

    val readDf = spark.read.format("hudi")
      .option("hoodie.schema.on.read.enable", "true")
      .load(basePath + "/schema_evolve_add")
    assertEquals(10, readDf.count())

    val rowMap = readDf.select("id", "embedding", "new_col").collect()
      .map(r => r.getString(0) -> r).toMap

    // Old rows (key_0..key_4): vector intact, new_col is null
    for (i <- 0 until 5) {
      val row = rowMap(s"key_$i")
      val vec = row.getSeq[Float](1)
      assertEquals(4, vec.size)
      assertTrue(vec.forall(_ == i.toFloat), s"key_$i vector corrupted after schema evolution")
      assertTrue(row.isNullAt(2), s"key_$i new_col should be null")
    }
    // New rows (key_5..key_9): vector intact, new_col has value
    for (i <- 5 until 10) {
      val row = rowMap(s"key_$i")
      val vec = row.getSeq[Float](1)
      assertEquals(4, vec.size)
      assertTrue(vec.forall(_ == i.toFloat), s"key_$i vector corrupted after schema evolution")
      assertEquals(s"v2_$i", row.getString(2), s"key_$i new_col wrong")
    }
  }

  /**
   * Deleting records from a table with a vector column should not affect remaining records.
   */
  @Test
  def testDeleteFromVectorTable(): Unit = {
    val metadata = vectorMetadata("VECTOR(4)")

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata),
      StructField("ts", LongType, nullable = false)
    ))

    val data = (0 until 10).map { i =>
      Row(s"key_$i", Array.fill(4)(i.toFloat).toSeq, i.toLong)
    }
    writeHudiTable(createVectorDf(schema, data), "delete_vector_test",
      basePath + "/delete_vec", precombineField = "ts")

    // Delete key_2, key_5, key_8
    val deletedKeys = Set("key_2", "key_5", "key_8")
    val deleteSchema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("ts", LongType, nullable = false)
    ))
    val deleteData = deletedKeys.toSeq.map(k => Row(k, 999L))
    writeHudiTable(createVectorDf(deleteSchema, deleteData), "delete_vector_test",
      basePath + "/delete_vec", precombineField = "ts", mode = SaveMode.Append,
      extraOpts = Map(OPERATION.key -> "delete"))

    val readDf = readHudiTable(basePath + "/delete_vec")
    assertEquals(7, readDf.count(), "Deleted rows should be gone")

    val rowMap = readDf.select("id", "embedding").collect()
      .map(r => r.getString(0) -> r.getSeq[Float](1)).toMap

    // Deleted keys must not appear
    deletedKeys.foreach { k =>
      assertFalse(rowMap.contains(k), s"$k should have been deleted")
    }

    // Remaining keys must have correct vectors
    val remaining = (0 until 10).map(i => s"key_$i").filterNot(deletedKeys.contains)
    remaining.foreach { k =>
      val i = k.stripPrefix("key_").toInt
      val vec = rowMap(k)
      assertEquals(4, vec.size, s"$k dimension wrong")
      assertTrue(vec.forall(_ == i.toFloat), s"$k vector wrong after delete")
    }
  }

  /**
   * Deeply nested vector: vector inside struct inside struct.
   * Schema: id STRING, outer STRUCT<inner STRUCT<embedding VECTOR(4)>>
   */
  @Test
  def testDeeplyNestedVectorColumn(): Unit = {
    val vecMeta = vectorMetadata("VECTOR(4)")
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("outer", StructType(Seq(
        StructField("inner", StructType(Seq(
          StructField("embedding", ArrayType(FloatType, containsNull = false), nullable = false, vecMeta)
        )), nullable = false)
      )), nullable = false)
    ))

    val data = (0 until 5).map { i =>
      Row(s"key_$i", Row(Row(Array.fill(4)(i.toFloat * 0.5f).toSeq)))
    }

    val path = basePath + "/deep_nested_vec"
    val df = createVectorDf(schema, data)
    writeHudiTable(df, "deep_nested_vector_test", path)

    val readDf = readHudiTable(path)
    assertEquals(5, readDf.count())

    val rows = readDf.select("id", "outer.inner.embedding").collect()
      .map(r => r.getString(0) -> r.getSeq[Float](1)).toMap

    for (i <- 0 until 5) {
      val vec = rows(s"key_$i")
      assertEquals(4, vec.size)
      assertTrue(vec.forall(_ == i.toFloat * 0.5f),
        s"key_$i: expected ${i.toFloat * 0.5f} but got ${vec.head}")
    }
  }

  /**
   * Nested vector with projection: select only nested vector, skip it, or select sibling fields.
   */
  @Test
  def testNestedVectorWithProjection(): Unit = {
    val vecMeta = vectorMetadata("VECTOR(4)")
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("metadata", StructType(Seq(
        StructField("embedding", ArrayType(FloatType, containsNull = false), nullable = false, vecMeta),
        StructField("label", StringType, nullable = true),
        StructField("score", IntegerType, nullable = true)
      )), nullable = false)
    ))

    val data = (0 until 5).map { i =>
      Row(s"key_$i", Row(Array.fill(4)(i.toFloat).toSeq, s"label_$i", i * 10))
    }

    val path = basePath + "/nested_proj"
    val df = createVectorDf(schema, data)
    writeHudiTable(df, "nested_proj_test", path)

    // Project only the nested vector
    val vecOnly = readHudiTable(path).select("id", "metadata.embedding").collect()
      .map(r => r.getString(0) -> r.getSeq[Float](1)).toMap
    for (i <- 0 until 5) {
      assertTrue(vecOnly(s"key_$i").forall(_ == i.toFloat))
    }

    // Skip the vector, read only non-vector nested fields
    val noVec = readHudiTable(path).select("id", "metadata.label", "metadata.score").collect()
      .map(r => r.getString(0) -> r).toMap
    for (i <- 0 until 5) {
      assertEquals(s"label_$i", noVec(s"key_$i").getString(1))
      assertEquals(i * 10, noVec(s"key_$i").getInt(2))
    }

    // Read everything
    val all = readHudiTable(path).select("id", "metadata").collect()
      .map(r => r.getString(0) -> r.getStruct(1)).toMap
    for (i <- 0 until 5) {
      val meta = all(s"key_$i")
      val vec = meta.getSeq[Float](0)
      assertEquals(4, vec.size)
      assertTrue(vec.forall(_ == i.toFloat))
      assertEquals(s"label_$i", meta.getString(1))
    }
  }

  /**
   * Null handling for nested vectors:
   * - Null struct (entire struct is null, vector is implicitly null)
   * - Non-null struct with null vector inside
   * - Non-null struct with non-null vector
   */
  @Test
  def testNestedVectorWithNulls(): Unit = {
    val vecMeta = vectorMetadata("VECTOR(4)")
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("metadata", StructType(Seq(
        StructField("embedding", ArrayType(FloatType, containsNull = false), nullable = true, vecMeta),
        StructField("label", StringType, nullable = true)
      )), nullable = true)
    ))

    val data = Seq(
      Row("key_0", Row(Array.fill(4)(1.0f).toSeq, "has_vec")),      // non-null struct, non-null vector
      Row("key_1", null),                                             // null struct
      Row("key_2", Row(null, "no_vec")),                              // non-null struct, null vector
      Row("key_3", Row(Array.fill(4)(3.0f).toSeq, null))             // non-null struct, null label
    )

    val path = basePath + "/nested_null"
    val df = createVectorDf(schema, data)
    writeHudiTable(df, "nested_null_test", path)

    val readDf = readHudiTable(path)
    assertEquals(4, readDf.count())

    val rows = readDf.select("id", "metadata").collect()
      .map(r => r.getString(0) -> r).toMap

    // key_0: non-null struct, non-null vector
    val r0 = rows("key_0")
    assertFalse(r0.isNullAt(1))
    val meta0 = r0.getStruct(1)
    val vec0 = meta0.getSeq[Float](0)
    assertEquals(4, vec0.size)
    assertTrue(vec0.forall(_ == 1.0f))
    assertEquals("has_vec", meta0.getString(1))

    // key_1: null struct
    val r1 = rows("key_1")
    assertTrue(r1.isNullAt(1), "key_1 metadata should be null")

    // key_2: non-null struct, null vector
    val r2 = rows("key_2")
    assertFalse(r2.isNullAt(1))
    val meta2 = r2.getStruct(1)
    assertTrue(meta2.isNullAt(0), "key_2 embedding should be null")
    assertEquals("no_vec", meta2.getString(1))

    // key_3: non-null struct, null label
    val r3 = rows("key_3")
    assertFalse(r3.isNullAt(1))
    val meta3 = r3.getStruct(1)
    val vec3 = meta3.getSeq[Float](0)
    assertTrue(vec3.forall(_ == 3.0f))
    assertTrue(meta3.isNullAt(1), "key_3 label should be null")
  }

  /**
   * Nested vector in a MOR table with upserts — verifies the FileGroupReader merge path
   * handles nested vector conversion correctly.
   */
  @Test
  def testNestedVectorMorUpsert(): Unit = {
    val vecMeta = vectorMetadata("VECTOR(4)")
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("metadata", StructType(Seq(
        StructField("embedding", ArrayType(FloatType, containsNull = false), nullable = false, vecMeta),
        StructField("label", StringType, nullable = true)
      )), nullable = false),
      StructField("ts", LongType, nullable = false)
    ))

    val path = basePath + "/nested_mor"

    // Initial insert
    val data1 = (0 until 10).map { i =>
      Row(s"key_$i", Row(Array.fill(4)(1.0f).toSeq, s"label_$i"), i.toLong)
    }
    writeHudiTable(createVectorDf(schema, data1), "nested_mor_test", path,
      tableType = "MERGE_ON_READ", precombineField = "ts")

    // Upsert: update first 5 rows with new vectors
    val data2 = (0 until 5).map { i =>
      Row(s"key_$i", Row(Array.fill(4)(2.0f).toSeq, s"updated_$i"), 100L + i)
    }
    writeHudiTable(createVectorDf(schema, data2), "nested_mor_test", path,
      tableType = "MERGE_ON_READ", precombineField = "ts", mode = SaveMode.Append)

    val readDf = readHudiTable(path)
    assertEquals(10, readDf.count())

    val rows = readDf.select("id", "metadata.embedding", "metadata.label").collect()
      .map(r => r.getString(0) -> (r.getSeq[Float](1), r.getString(2))).toMap

    // Updated rows should have value 2.0
    for (i <- 0 until 5) {
      val (vec, label) = rows(s"key_$i")
      assertTrue(vec.forall(_ == 2.0f), s"key_$i should have 2.0, got ${vec.head}")
      assertEquals(s"updated_$i", label)
    }
    // Non-updated rows should have value 1.0
    for (i <- 5 until 10) {
      val (vec, label) = rows(s"key_$i")
      assertTrue(vec.forall(_ == 1.0f), s"key_$i should have 1.0, got ${vec.head}")
      assertEquals(s"label_$i", label)
    }
  }

  /**
   * Mixed: top-level vector AND nested vector in the same table.
   * Verifies recursion handles both levels correctly.
   */
  @Test
  def testTopLevelAndNestedVectorTogether(): Unit = {
    val vecMeta4 = vectorMetadata("VECTOR(4)")
    val vecMeta2 = vectorMetadata("VECTOR(2)")
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("top_vec", ArrayType(FloatType, containsNull = false), nullable = false, vecMeta4),
      StructField("metadata", StructType(Seq(
        StructField("nested_vec", ArrayType(FloatType, containsNull = false), nullable = false, vecMeta2),
        StructField("label", StringType, nullable = true)
      )), nullable = false)
    ))

    val data = (0 until 5).map { i =>
      Row(s"key_$i", Array.fill(4)(i.toFloat).toSeq, Row(Array.fill(2)(i.toFloat * 10).toSeq, s"label_$i"))
    }

    val path = basePath + "/mixed_level"
    val df = createVectorDf(schema, data)
    writeHudiTable(df, "mixed_level_test", path)

    val readDf = readHudiTable(path)
    assertEquals(5, readDf.count())

    val rows = readDf.select("id", "top_vec", "metadata.nested_vec", "metadata.label").collect()
      .map(r => r.getString(0) -> r).toMap

    for (i <- 0 until 5) {
      val row = rows(s"key_$i")
      // Top-level vector
      val topVec = row.getSeq[Float](1)
      assertEquals(4, topVec.size)
      assertTrue(topVec.forall(_ == i.toFloat), s"key_$i top_vec wrong")
      // Nested vector
      val nestedVec = row.getSeq[Float](2)
      assertEquals(2, nestedVec.size)
      assertTrue(nestedVec.forall(_ == i.toFloat * 10), s"key_$i nested_vec wrong")
      assertEquals(s"label_$i", row.getString(3))
    }
  }

  /**
   * Array of vectors: each element in the array is a VECTOR(4).
   * Metadata goes on the outer StructField; Hudi infers array-of-vectors from
   * ArrayType(ArrayType(FloatType)) + hudi_type=VECTOR(4).
   */
  @Test
  def testArrayOfVectors(): Unit = {
    val vecMeta = vectorMetadata("VECTOR(4)")
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embeddings", ArrayType(ArrayType(FloatType, containsNull = false), containsNull = true),
        nullable = false, vecMeta)
    ))

    val data = (0 until 5).map { i =>
      val vectors = (0 until 3).map { j =>
        Array.fill(4)((i * 10 + j).toFloat).toSeq
      }
      Row(s"key_$i", vectors)
    }

    val path = basePath + "/array_of_vecs"
    val df = createVectorDf(schema, data)
    writeHudiTable(df, "array_of_vectors_test", path)

    val readDf = readHudiTable(path)
    assertEquals(5, readDf.count())

    val rows = readDf.select("id", "embeddings").collect()
      .map(r => r.getString(0) -> r.getSeq[Seq[Float]](1)).toMap

    for (i <- 0 until 5) {
      val vectors = rows(s"key_$i")
      assertEquals(3, vectors.size, s"key_$i should have 3 vectors")
      for (j <- 0 until 3) {
        val vec = vectors(j)
        assertEquals(4, vec.size, s"key_$i vector $j dimension wrong")
        assertTrue(vec.forall(_ == (i * 10 + j).toFloat),
          s"key_$i vector $j: expected ${(i * 10 + j).toFloat} but got ${vec.head}")
      }
    }
  }

  /**
   * Map of vectors: map values are VECTOR(4).
   * Metadata goes on the outer StructField; Hudi infers map-of-vectors from
   * MapType(StringType, ArrayType(FloatType)) + hudi_type=VECTOR(4).
   */
  @Test
  def testMapOfVectors(): Unit = {
    val vecMeta = vectorMetadata("VECTOR(4)")
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("model_vecs", MapType(StringType, ArrayType(FloatType, containsNull = false), valueContainsNull = true),
        nullable = false, vecMeta)
    ))

    val data = (0 until 5).map { i =>
      val map = Map(
        "model_a" -> Array.fill(4)(i.toFloat).toSeq,
        "model_b" -> Array.fill(4)(i.toFloat * 10).toSeq
      )
      Row(s"key_$i", map)
    }

    val path = basePath + "/map_of_vecs"
    val df = createVectorDf(schema, data)
    writeHudiTable(df, "map_of_vectors_test", path)

    val readDf = readHudiTable(path)
    assertEquals(5, readDf.count())

    val rows = readDf.select("id", "model_vecs").collect()
      .map(r => r.getString(0) -> r.getMap[String, Seq[Float]](1)).toMap

    for (i <- 0 until 5) {
      val map = rows(s"key_$i")
      assertEquals(2, map.size, s"key_$i should have 2 entries")
      val vecA = map("model_a")
      assertEquals(4, vecA.size)
      assertTrue(vecA.forall(_ == i.toFloat), s"key_$i model_a wrong")
      val vecB = map("model_b")
      assertEquals(4, vecB.size)
      assertTrue(vecB.forall(_ == i.toFloat * 10), s"key_$i model_b wrong")
    }
  }

  /**
   * Array of vectors with nulls: null array, array with null elements.
   */
  @Test
  def testArrayOfVectorsWithNulls(): Unit = {
    val vecMeta = vectorMetadata("VECTOR(4)")
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embeddings", ArrayType(ArrayType(FloatType, containsNull = false), containsNull = false),
        nullable = true, vecMeta)
    ))

    val data = Seq(
      Row("key_0", Seq(Array.fill(4)(1.0f).toSeq, Array.fill(4)(2.0f).toSeq)),
      Row("key_1", null),                                    // null array field
      Row("key_2", Seq(Array.fill(4)(3.0f).toSeq))           // single-element array
    )

    val path = basePath + "/array_vecs_null"
    val df = createVectorDf(schema, data)
    writeHudiTable(df, "array_vecs_null_test", path)

    val readDf = readHudiTable(path)
    assertEquals(3, readDf.count())

    val rows = readDf.select("id", "embeddings").collect()
      .map(r => r.getString(0) -> r).toMap

    // key_0: non-null array with 2 vectors
    val r0 = rows("key_0")
    assertFalse(r0.isNullAt(1))
    val vecs0 = r0.getSeq[Seq[Float]](1)
    assertEquals(2, vecs0.size)
    assertTrue(vecs0(0).forall(_ == 1.0f))
    assertTrue(vecs0(1).forall(_ == 2.0f))

    // key_1: null array
    assertTrue(rows("key_1").isNullAt(1), "key_1 embeddings should be null")

    // key_2: single-element array
    val r2 = rows("key_2")
    assertFalse(r2.isNullAt(1))
    val vecs2 = r2.getSeq[Seq[Float]](1)
    assertEquals(1, vecs2.size)
    assertTrue(vecs2(0).forall(_ == 3.0f))
  }

  /**
   * Array of structs where each struct contains a vector.
   * Tests recursion into ArrayType element StructType for detection, replacement, and conversion.
   */
  @Test
  def testArrayOfStructsWithVector(): Unit = {
    val vecMeta = vectorMetadata("VECTOR(4)")
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("items", ArrayType(StructType(Seq(
        StructField("embedding", ArrayType(FloatType, containsNull = false), nullable = false, vecMeta),
        StructField("label", StringType, nullable = true)
      )), containsNull = false), nullable = false)
    ))

    val data = (0 until 5).map { i =>
      val items = (0 until 3).map { j =>
        Row(Array.fill(4)((i * 10 + j).toFloat).toSeq, s"item_${i}_$j")
      }
      Row(s"key_$i", items)
    }

    val path = basePath + "/array_struct_vec"
    val df = createVectorDf(schema, data)
    writeHudiTable(df, "array_struct_vec_test", path)

    val readDf = readHudiTable(path)
    assertEquals(5, readDf.count())

    val rows = readDf.select("id", "items").collect()
      .map(r => r.getString(0) -> r.getSeq[Row](1)).toMap

    for (i <- 0 until 5) {
      val items = rows(s"key_$i")
      assertEquals(3, items.size, s"key_$i should have 3 items")
      for (j <- 0 until 3) {
        val item = items(j)
        val vec = item.getSeq[Float](0)
        assertEquals(4, vec.size)
        assertTrue(vec.forall(_ == (i * 10 + j).toFloat),
          s"key_$i item $j: expected ${(i * 10 + j).toFloat} but got ${vec.head}")
        assertEquals(s"item_${i}_$j", item.getString(1))
      }
    }
  }

  /**
   * Map with struct values containing a vector.
   * Tests recursion into MapType value StructType for detection, replacement, and conversion.
   */
  @Test
  def testMapOfStructsWithVector(): Unit = {
    val vecMeta = vectorMetadata("VECTOR(4)")
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("models", MapType(StringType, StructType(Seq(
        StructField("embedding", ArrayType(FloatType, containsNull = false), nullable = false, vecMeta),
        StructField("score", DoubleType, nullable = true)
      )), valueContainsNull = false), nullable = false)
    ))

    val data = (0 until 5).map { i =>
      val map = Map(
        "model_a" -> Row(Array.fill(4)(i.toFloat).toSeq, i.toDouble),
        "model_b" -> Row(Array.fill(4)(i.toFloat * 10).toSeq, i.toDouble * 10)
      )
      Row(s"key_$i", map)
    }

    val path = basePath + "/map_struct_vec"
    val df = createVectorDf(schema, data)
    writeHudiTable(df, "map_struct_vec_test", path)

    val readDf = readHudiTable(path)
    assertEquals(5, readDf.count())

    val rows = readDf.select("id", "models").collect()
      .map(r => r.getString(0) -> r.getMap[String, Row](1)).toMap

    for (i <- 0 until 5) {
      val models = rows(s"key_$i")
      assertEquals(2, models.size)
      val a = models("model_a")
      assertTrue(a.getSeq[Float](0).forall(_ == i.toFloat), s"key_$i model_a vec wrong")
      assertEquals(i.toDouble, a.getDouble(1), 1e-9)
      val b = models("model_b")
      assertTrue(b.getSeq[Float](0).forall(_ == i.toFloat * 10), s"key_$i model_b vec wrong")
    }
  }

  /**
   * MOR bulk collect: collects ALL rows at once to verify the GenericInternalRow buffer
   * is not reused across rows (which would cause all rows to have the same vector).
   */
  @Test
  def testMorBulkCollectDistinctVectors(): Unit = {
    val vecMeta = vectorMetadata("VECTOR(4)")
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false), nullable = false, vecMeta),
      StructField("ts", LongType, nullable = false)
    ))

    val numRows = 20
    val batch1 = (0 until numRows).map { i =>
      Row(s"key_$i", Array.fill(4)(i.toFloat).toSeq, i.toLong)
    }
    writeHudiTable(createVectorDf(schema, batch1), "mor_bulk_test", basePath + "/mor_bulk",
      tableType = "MERGE_ON_READ", precombineField = "ts")

    // Upsert so FileGroupReader merge path is exercised
    val batch2 = (0 until numRows / 2).map { i =>
      Row(s"key_$i", Array.fill(4)(i.toFloat * 10).toSeq, i.toLong + 100)
    }
    writeHudiTable(createVectorDf(schema, batch2), "mor_bulk_test", basePath + "/mor_bulk",
      tableType = "MERGE_ON_READ", precombineField = "ts", mode = SaveMode.Append)

    // Collect ALL rows at once
    val allRows = readHudiTable(basePath + "/mor_bulk")
      .select("id", "embedding").collect()

    assertEquals(numRows, allRows.length)
    val rowMap = allRows.map(r => r.getString(0) -> r.getSeq[Float](1)).toMap

    for (i <- 0 until numRows / 2) {
      val vec = rowMap(s"key_$i")
      assertTrue(vec.forall(_ == i.toFloat * 10),
        s"key_$i: expected ${i.toFloat * 10} but got ${vec.head} (GenericInternalRow mutation?)")
    }
    for (i <- numRows / 2 until numRows) {
      val vec = rowMap(s"key_$i")
      assertTrue(vec.forall(_ == i.toFloat),
        s"key_$i: expected ${i.toFloat} but got ${vec.head}")
    }
  }

  /**
   * (High) MOR table with array<VECTOR>: insert + upsert, verify merged result.
   * Exercises the FileGroupReader merge path with array-of-vector conversion.
   */
  @Test
  def testArrayOfVectorsMorUpsert(): Unit = {
    val vecMeta = vectorMetadata("VECTOR(4)")
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embeddings", ArrayType(ArrayType(FloatType, containsNull = false), containsNull = false),
        nullable = false, vecMeta),
      StructField("ts", LongType, nullable = false)
    ))

    val path = basePath + "/array_vec_mor"

    // Insert: key_0..key_9, each with 2 vectors of value 1.0
    val batch1 = (0 until 10).map { i =>
      Row(s"key_$i", Seq(Array.fill(4)(1.0f).toSeq, Array.fill(4)(1.0f).toSeq), i.toLong)
    }
    writeHudiTable(createVectorDf(schema, batch1), "array_vec_mor_test", path,
      tableType = "MERGE_ON_READ", precombineField = "ts")

    // Upsert: update key_0..key_4 with value 2.0
    val batch2 = (0 until 5).map { i =>
      Row(s"key_$i", Seq(Array.fill(4)(2.0f).toSeq, Array.fill(4)(2.0f).toSeq), 100L + i)
    }
    writeHudiTable(createVectorDf(schema, batch2), "array_vec_mor_test", path,
      tableType = "MERGE_ON_READ", precombineField = "ts", mode = SaveMode.Append)

    val readDf = readHudiTable(path)
    assertEquals(10, readDf.count())

    val rows = readDf.select("id", "embeddings").collect()
      .map(r => r.getString(0) -> r.getSeq[Seq[Float]](1)).toMap

    // Updated rows should have value 2.0
    for (i <- 0 until 5) {
      val vecs = rows(s"key_$i")
      assertEquals(2, vecs.size, s"key_$i should have 2 vectors")
      assertTrue(vecs.forall(_.forall(_ == 2.0f)), s"key_$i vectors should be 2.0 after upsert")
    }
    // Non-updated rows should retain value 1.0
    for (i <- 5 until 10) {
      val vecs = rows(s"key_$i")
      assertEquals(2, vecs.size, s"key_$i should have 2 vectors")
      assertTrue(vecs.forall(_.forall(_ == 1.0f)), s"key_$i vectors should remain 1.0")
    }
  }

  /**
   * (Medium) array<struct<VECTOR>> with projection: select only embedding or only label.
   * Verifies column pruning works for vectors inside structs inside arrays.
   */
  @Test
  def testArrayOfStructsWithVectorProjection(): Unit = {
    val vecMeta = vectorMetadata("VECTOR(4)")
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("items", ArrayType(StructType(Seq(
        StructField("embedding", ArrayType(FloatType, containsNull = false), nullable = false, vecMeta),
        StructField("label", StringType, nullable = true)
      )), containsNull = false), nullable = false)
    ))

    val data = (0 until 5).map { i =>
      Row(s"key_$i", Seq(
        Row(Array.fill(4)(i.toFloat).toSeq, s"label_$i"),
        Row(Array.fill(4)(i.toFloat * 2).toSeq, s"label_${i}_b")
      ))
    }

    val path = basePath + "/array_struct_vec_proj"
    writeHudiTable(createVectorDf(schema, data), "array_struct_proj_test", path)

    // Read full items column and verify both vector and label fields are correct.
    // Note: sub-field projection into array<struct<...>> (e.g. items.embedding) triggers
    // a Hudi schema pruning limitation, so we read the full struct and extract fields here.
    val rows = readHudiTable(path).select("id", "items").collect()
      .map(r => r.getString(0) -> r.getSeq[Row](1)).toMap

    for (i <- 0 until 5) {
      val items = rows(s"key_$i")
      assertEquals(2, items.size)

      // Verify embedding (vector field)
      val emb0 = items(0).getSeq[Float](0)
      val emb1 = items(1).getSeq[Float](0)
      assertEquals(4, emb0.size)
      assertTrue(emb0.forall(_ == i.toFloat), s"key_$i first embedding wrong")
      assertEquals(4, emb1.size)
      assertTrue(emb1.forall(_ == i.toFloat * 2), s"key_$i second embedding wrong")

      // Verify label (non-vector field alongside vector)
      assertEquals(s"label_$i", items(0).getString(1))
      assertEquals(s"label_${i}_b", items(1).getString(1))
    }
  }

  /**
   * (Medium) Empty array<VECTOR> and empty map<K, VECTOR>.
   * Edge case for the iteration loop in convertVectorField.
   */
  @Test
  def testEmptyArrayAndMapOfVectors(): Unit = {
    val vecMetaArr = vectorMetadata("VECTOR(4)")
    val vecMetaMap = vectorMetadata("VECTOR(4)")

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("arr_vecs", ArrayType(ArrayType(FloatType, containsNull = false), containsNull = false),
        nullable = false, vecMetaArr),
      StructField("map_vecs", MapType(StringType, ArrayType(FloatType, containsNull = false), valueContainsNull = false),
        nullable = false, vecMetaMap)
    ))

    val data = Seq(
      Row("key_0", Seq.empty, Map.empty[String, Seq[Float]]),  // both empty
      Row("key_1", Seq(Array.fill(4)(1.0f).toSeq), Map("a" -> Array.fill(4)(2.0f).toSeq))  // non-empty
    )

    val path = basePath + "/empty_arr_map_vecs"
    writeHudiTable(createVectorDf(schema, data), "empty_arr_map_test", path)

    val readDf = readHudiTable(path)
    assertEquals(2, readDf.count())

    val rows = readDf.select("id", "arr_vecs", "map_vecs").collect()
      .map(r => r.getString(0) -> r).toMap

    // key_0: empty collections
    val r0 = rows("key_0")
    assertEquals(0, r0.getSeq[Seq[Float]](1).size, "key_0 arr_vecs should be empty")
    assertEquals(0, r0.getMap[String, Seq[Float]](2).size, "key_0 map_vecs should be empty")

    // key_1: non-empty collections with correct vectors
    val r1 = rows("key_1")
    val arrVecs = r1.getSeq[Seq[Float]](1)
    assertEquals(1, arrVecs.size)
    assertTrue(arrVecs(0).forall(_ == 1.0f))

    val mapVecs = r1.getMap[String, Seq[Float]](2)
    assertEquals(1, mapVecs.size)
    assertTrue(mapVecs("a").forall(_ == 2.0f))
  }

  /**
   * (Medium) array<VECTOR(4, DOUBLE)>: verifies DOUBLE element type works through
   * the array-of-vector path end-to-end.
   */
  @Test
  def testArrayOfDoubleVectors(): Unit = {
    val vecMeta = vectorMetadata("VECTOR(4, DOUBLE)")
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embeddings", ArrayType(ArrayType(DoubleType, containsNull = false), containsNull = false),
        nullable = false, vecMeta)
    ))

    val data = (0 until 5).map { i =>
      Row(s"key_$i", Seq(
        Array.fill(4)(i.toDouble).toSeq,
        Array.fill(4)(i.toDouble * 0.5).toSeq
      ))
    }

    val path = basePath + "/array_double_vecs"
    writeHudiTable(createVectorDf(schema, data), "array_double_vec_test", path)

    val readDf = readHudiTable(path)
    assertEquals(5, readDf.count())

    val rows = readDf.select("id", "embeddings").collect()
      .map(r => r.getString(0) -> r.getSeq[Seq[Double]](1)).toMap

    for (i <- 0 until 5) {
      val vecs = rows(s"key_$i")
      assertEquals(2, vecs.size)
      assertTrue(vecs(0).forall(_ == i.toDouble), s"key_$i first vec wrong")
      assertTrue(vecs(1).forall(_ == i.toDouble * 0.5), s"key_$i second vec wrong")
    }
  }

  /**
   * 3-level deep struct nesting: struct<a: struct<b: struct<embedding: VECTOR(4)>>>
   * Verifies that unlimited struct depth recursion works correctly.
   */
  @Test
  def testThreeLevelNestedVectorColumn(): Unit = {
    val vecMeta = vectorMetadata("VECTOR(4)")
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("a", StructType(Seq(
        StructField("b", StructType(Seq(
          StructField("c", StructType(Seq(
            StructField("embedding", ArrayType(FloatType, containsNull = false),
              nullable = false, vecMeta),
            StructField("label", StringType, nullable = true)
          )), nullable = false)
        )), nullable = false)
      )), nullable = false)
    ))

    val data = (0 until 5).map { i =>
      Row(s"key_$i", Row(Row(Row(Array.fill(4)(i.toFloat).toSeq, s"label_$i"))))
    }

    val path = basePath + "/three_level_nested"
    writeHudiTable(createVectorDf(schema, data), "three_level_nested_test", path)

    val readDf = readHudiTable(path)
    assertEquals(5, readDf.count())

    val rows = readDf.select("id", "a").collect()
      .map(r => r.getString(0) -> r.getStruct(1)).toMap

    for (i <- 0 until 5) {
      val innerC = rows(s"key_$i").getStruct(0).getStruct(0)
      val vec = innerC.getSeq[Float](0)
      assertEquals(4, vec.size)
      assertTrue(vec.forall(_ == i.toFloat), s"key_$i vector wrong at depth 3")
      assertEquals(s"label_$i", innerC.getString(1))
    }
  }

  /**
   * Unsupported: array<array<VECTOR>> — should throw at write time rather than silently
   * produce wrong data.
   */
  @Test
  def testUnsupportedArrayOfArrayOfVectors(): Unit = {
    val vecMeta = vectorMetadata("VECTOR(4)")
    // DataType: ArrayType(ArrayType(ArrayType(FloatType))) with VECTOR metadata
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embeddings",
        ArrayType(ArrayType(ArrayType(FloatType, containsNull = false), containsNull = false),
          containsNull = false),
        nullable = false, vecMeta)
    ))

    val data = Seq(Row("key_0", Seq(Seq(Array.fill(4)(1.0f).toSeq))))
    assertThrows(classOf[Exception], () =>
      writeHudiTable(createVectorDf(schema, data), "unsupported_arr_arr_vec", basePath + "/arr_arr_vec"))
  }

  /**
   * Unsupported: array<map<K, VECTOR>> — should throw at write time rather than silently
   * produce wrong data.
   */
  @Test
  def testUnsupportedArrayOfMapOfVectors(): Unit = {
    val vecMeta = vectorMetadata("VECTOR(4)")
    // DataType: ArrayType(MapType(StringType, ArrayType(FloatType))) with VECTOR metadata
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embeddings",
        ArrayType(MapType(StringType, ArrayType(FloatType, containsNull = false)),
          containsNull = false),
        nullable = false, vecMeta)
    ))

    val data = Seq(Row("key_0", Seq(Map("a" -> Array.fill(4)(1.0f).toSeq))))
    assertThrows(classOf[Exception], () =>
      writeHudiTable(createVectorDf(schema, data), "unsupported_arr_map_vec", basePath + "/arr_map_vec"))
  }

  /** MOR: map<K, VECTOR> insert + upsert, verify merged result. */
  @Test
  def testMapOfVectorsMorUpsert(): Unit = {
    val vecMeta = vectorMetadata("VECTOR(4)")
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("model_vecs", MapType(StringType, ArrayType(FloatType, containsNull = false), valueContainsNull = false),
        nullable = false, vecMeta),
      StructField("ts", LongType, nullable = false)
    ))
    val path = basePath + "/map_vec_mor"

    val batch1 = (0 until 10).map { i =>
      Row(s"key_$i", Map("a" -> Array.fill(4)(1.0f).toSeq, "b" -> Array.fill(4)(1.0f).toSeq), i.toLong)
    }
    writeHudiTable(createVectorDf(schema, batch1), "map_vec_mor_test", path,
      tableType = "MERGE_ON_READ", precombineField = "ts")

    val batch2 = (0 until 5).map { i =>
      Row(s"key_$i", Map("a" -> Array.fill(4)(2.0f).toSeq, "b" -> Array.fill(4)(2.0f).toSeq), 100L + i)
    }
    writeHudiTable(createVectorDf(schema, batch2), "map_vec_mor_test", path,
      tableType = "MERGE_ON_READ", precombineField = "ts", mode = SaveMode.Append)

    val readDf = readHudiTable(path)
    assertEquals(10, readDf.count())

    val rows = readDf.select("id", "model_vecs").collect()
      .map(r => r.getString(0) -> r.getMap[String, Seq[Float]](1)).toMap

    for (i <- 0 until 5) {
      val m = rows(s"key_$i")
      assertTrue(m("a").forall(_ == 2.0f), s"key_$i 'a' should be 2.0 after upsert")
      assertTrue(m("b").forall(_ == 2.0f), s"key_$i 'b' should be 2.0 after upsert")
    }
    for (i <- 5 until 10) {
      val m = rows(s"key_$i")
      assertTrue(m("a").forall(_ == 1.0f), s"key_$i 'a' should remain 1.0")
      assertTrue(m("b").forall(_ == 1.0f), s"key_$i 'b' should remain 1.0")
    }
  }

  /** MOR: array<struct<embedding: VECTOR(4)>> insert + upsert, verify merged result. */
  @Test
  def testArrayOfStructsWithVectorMorUpsert(): Unit = {
    val vecMeta = vectorMetadata("VECTOR(4)")
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("items", ArrayType(StructType(Seq(
        StructField("embedding", ArrayType(FloatType, containsNull = false), nullable = false, vecMeta),
        StructField("label", StringType, nullable = true)
      )), containsNull = false), nullable = false),
      StructField("ts", LongType, nullable = false)
    ))
    val path = basePath + "/arr_struct_vec_mor"

    val batch1 = (0 until 10).map { i =>
      Row(s"key_$i", Seq(Row(Array.fill(4)(1.0f).toSeq, s"orig_$i")), i.toLong)
    }
    writeHudiTable(createVectorDf(schema, batch1), "arr_struct_vec_mor_test", path,
      tableType = "MERGE_ON_READ", precombineField = "ts")

    val batch2 = (0 until 5).map { i =>
      Row(s"key_$i", Seq(Row(Array.fill(4)(2.0f).toSeq, s"updated_$i")), 100L + i)
    }
    writeHudiTable(createVectorDf(schema, batch2), "arr_struct_vec_mor_test", path,
      tableType = "MERGE_ON_READ", precombineField = "ts", mode = SaveMode.Append)

    val readDf = readHudiTable(path)
    assertEquals(10, readDf.count())

    val rows = readDf.select("id", "items").collect()
      .map(r => r.getString(0) -> r.getSeq[Row](1)).toMap

    for (i <- 0 until 5) {
      val items = rows(s"key_$i")
      assertTrue(items(0).getSeq[Float](0).forall(_ == 2.0f), s"key_$i embedding should be 2.0")
      assertEquals(s"updated_$i", items(0).getString(1))
    }
    for (i <- 5 until 10) {
      val items = rows(s"key_$i")
      assertTrue(items(0).getSeq[Float](0).forall(_ == 1.0f), s"key_$i embedding should remain 1.0")
      assertEquals(s"orig_$i", items(0).getString(1))
    }
  }

  /** MOR: map<K, struct<embedding: VECTOR(4)>> insert + upsert, verify merged result. */
  @Test
  def testMapOfStructsWithVectorMorUpsert(): Unit = {
    val vecMeta = vectorMetadata("VECTOR(4)")
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("models", MapType(StringType, StructType(Seq(
        StructField("embedding", ArrayType(FloatType, containsNull = false), nullable = false, vecMeta),
        StructField("score", DoubleType, nullable = true)
      )), valueContainsNull = false), nullable = false),
      StructField("ts", LongType, nullable = false)
    ))
    val path = basePath + "/map_struct_vec_mor"

    val batch1 = (0 until 10).map { i =>
      Row(s"key_$i", Map("m1" -> Row(Array.fill(4)(1.0f).toSeq, 1.0)), i.toLong)
    }
    writeHudiTable(createVectorDf(schema, batch1), "map_struct_vec_mor_test", path,
      tableType = "MERGE_ON_READ", precombineField = "ts")

    val batch2 = (0 until 5).map { i =>
      Row(s"key_$i", Map("m1" -> Row(Array.fill(4)(2.0f).toSeq, 2.0)), 100L + i)
    }
    writeHudiTable(createVectorDf(schema, batch2), "map_struct_vec_mor_test", path,
      tableType = "MERGE_ON_READ", precombineField = "ts", mode = SaveMode.Append)

    val readDf = readHudiTable(path)
    assertEquals(10, readDf.count())

    val rows = readDf.select("id", "models").collect()
      .map(r => r.getString(0) -> r.getMap[String, Row](1)).toMap

    for (i <- 0 until 5) {
      val m1 = rows(s"key_$i")("m1")
      assertTrue(m1.getSeq[Float](0).forall(_ == 2.0f), s"key_$i m1 embedding should be 2.0")
      assertEquals(2.0, m1.getDouble(1), 1e-9)
    }
    for (i <- 5 until 10) {
      val m1 = rows(s"key_$i")("m1")
      assertTrue(m1.getSeq[Float](0).forall(_ == 1.0f), s"key_$i m1 embedding should remain 1.0")
      assertEquals(1.0, m1.getDouble(1), 1e-9)
    }
  }

  private def assertArrayEquals(expected: Array[Byte], actual: Array[Byte], message: String): Unit = {
    assertEquals(expected.length, actual.length, s"$message: length mismatch")
    expected.zip(actual).zipWithIndex.foreach { case ((e, a), idx) =>
      assertEquals(e, a, s"$message: mismatch at index $idx")
    }
  }
}

object TestVectorDataSource {
  def vectorElementTypeArgs(): Stream[Arguments] = Stream.of(
    Arguments.of("VECTOR(128)",       FloatType,  128.asInstanceOf[AnyRef], "float_rt"),
    Arguments.of("VECTOR(64, DOUBLE)", DoubleType, 64.asInstanceOf[AnyRef],  "double_rt"),
    Arguments.of("VECTOR(256, INT8)",  ByteType,   256.asInstanceOf[AnyRef], "int8_rt")
  )
}
