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
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.testutils.{DataSourceTestUtils, HoodieSparkClientTestBase}

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._

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

  @Test
  def testVectorRoundTrip(): Unit = {
    // 1. Create schema with vector metadata
    val metadata = vectorMetadata("VECTOR(128)")

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata),
      StructField("label", StringType, nullable = true)
    ))

    // 2. Generate test data (128-dim float vectors)
    val random = new scala.util.Random(42)
    val data = (0 until 100).map { i =>
      val embedding = Array.fill(128)(random.nextFloat())
      Row(s"key_$i", embedding.toSeq, s"label_$i")
    }

    val df = createVectorDf(schema, data)

    // 3. Write as COW Hudi table
    writeHudiTable(df, "vector_test_table", basePath)

    // 4. Read back
    val readDf = readHudiTable(basePath)

    // 5. Verify row count
    assertEquals(100, readDf.count())

    // 6. Verify schema preserved
    val embeddingField = readDf.schema("embedding")
    assertTrue(embeddingField.dataType.isInstanceOf[ArrayType])
    val arrayType = embeddingField.dataType.asInstanceOf[ArrayType]
    assertEquals(FloatType, arrayType.elementType)
    assertFalse(arrayType.containsNull)

    // 7. Verify vector metadata preserved
    val readMetadata = embeddingField.metadata
    assertTrue(readMetadata.contains(HoodieSchema.TYPE_METADATA_FIELD))
    val parsedSchema = HoodieSchema.parseTypeDescriptor(
      readMetadata.getString(HoodieSchema.TYPE_METADATA_FIELD))
    assertEquals(HoodieSchemaType.VECTOR, parsedSchema.getType)
    val vectorSchema = parsedSchema.asInstanceOf[HoodieSchema.Vector]
    assertEquals(128, vectorSchema.getDimension)

    // 8. Verify float values match exactly
    val originalRows = df.select("id", "embedding").collect()
      .map(r => (r.getString(0), r.getSeq[Float](1)))
      .toMap

    val readRows = readDf.select("id", "embedding").collect()
      .map(r => (r.getString(0), r.getSeq[Float](1)))
      .toMap

    originalRows.foreach { case (id, origEmbedding) =>
      val readEmbedding = readRows(id)
      assertEquals(128, readEmbedding.size, s"Vector size mismatch for $id")

      origEmbedding.zip(readEmbedding).zipWithIndex.foreach {
        case ((orig, read), idx) =>
          assertEquals(orig, read, 1e-9f,
            s"Vector mismatch at $id index $idx: orig=$orig read=$read")
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
  def testDoubleVectorRoundTrip(): Unit = {
    val metadata = vectorMetadata("VECTOR(64, DOUBLE)")

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(DoubleType, containsNull = false),
        nullable = false, metadata),
      StructField("label", StringType, nullable = true)
    ))

    val random = new scala.util.Random(123)
    val data = (0 until 50).map { i =>
      val embedding = Array.fill(64)(random.nextDouble())
      Row(s"key_$i", embedding.toSeq, s"label_$i")
    }

    val df = createVectorDf(schema, data)

    writeHudiTable(df, "double_vector_test", basePath + "/double_vec")

    val readDf = readHudiTable(basePath + "/double_vec")
    assertEquals(50, readDf.count())

    // Verify schema: ArrayType(DoubleType)
    val embField = readDf.schema("embedding")
    val arrType = embField.dataType.asInstanceOf[ArrayType]
    assertEquals(DoubleType, arrType.elementType)

    // Verify metadata preserved with DOUBLE element type
    val readMeta = embField.metadata
    assertTrue(readMeta.contains(HoodieSchema.TYPE_METADATA_FIELD))
    val parsed = HoodieSchema.parseTypeDescriptor(
      readMeta.getString(HoodieSchema.TYPE_METADATA_FIELD))
    assertEquals(HoodieSchemaType.VECTOR, parsed.getType)
    val vecSchema = parsed.asInstanceOf[HoodieSchema.Vector]
    assertEquals(64, vecSchema.getDimension)
    assertEquals(HoodieSchema.Vector.VectorElementType.DOUBLE, vecSchema.getVectorElementType)

    // Verify actual values
    val origMap = df.select("id", "embedding").collect()
      .map(r => (r.getString(0), r.getSeq[Double](1))).toMap
    val readMap = readDf.select("id", "embedding").collect()
      .map(r => (r.getString(0), r.getSeq[Double](1))).toMap

    origMap.foreach { case (id, orig) =>
      val read = readMap(id)
      assertEquals(64, read.size, s"Dimension mismatch for $id")
      orig.zip(read).zipWithIndex.foreach { case ((o, r), idx) =>
        assertEquals(o, r, 1e-15, s"Double mismatch at $id[$idx]")
      }
    }
  }

  @Test
  def testInt8VectorRoundTrip(): Unit = {
    val metadata = vectorMetadata("VECTOR(256, INT8)")

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(ByteType, containsNull = false),
        nullable = false, metadata)
    ))

    val random = new scala.util.Random(99)
    val data = (0 until 30).map { i =>
      val embedding = Array.fill(256)((random.nextInt(256) - 128).toByte)
      Row(s"key_$i", embedding.toSeq)
    }

    val df = createVectorDf(schema, data)

    writeHudiTable(df, "int8_vector_test", basePath + "/int8_vec")

    val readDf = readHudiTable(basePath + "/int8_vec")
    assertEquals(30, readDf.count())

    // Verify schema: ArrayType(ByteType)
    val embField = readDf.schema("embedding")
    val arrType = embField.dataType.asInstanceOf[ArrayType]
    assertEquals(ByteType, arrType.elementType)

    // Verify metadata
    val readMeta = embField.metadata
    val parsed = HoodieSchema.parseTypeDescriptor(
      readMeta.getString(HoodieSchema.TYPE_METADATA_FIELD))
    assertEquals(HoodieSchemaType.VECTOR, parsed.getType)
    val vecSchema = parsed.asInstanceOf[HoodieSchema.Vector]
    assertEquals(256, vecSchema.getDimension)
    assertEquals(HoodieSchema.Vector.VectorElementType.INT8, vecSchema.getVectorElementType)

    // Verify byte values
    val origMap = df.select("id", "embedding").collect()
      .map(r => (r.getString(0), r.getSeq[Byte](1))).toMap
    val readMap = readDf.select("id", "embedding").collect()
      .map(r => (r.getString(0), r.getSeq[Byte](1))).toMap

    origMap.foreach { case (id, orig) =>
      val read = readMap(id)
      assertEquals(256, read.size, s"Dimension mismatch for $id")
      assertArrayEquals(orig.toArray, read.toArray, s"INT8 vector mismatch for $id")
    }
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
  def testMorTableWithVectors(): Unit = {
    val metadata = vectorMetadata("VECTOR(16)")

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata),
      StructField("ts", LongType, nullable = false)
    ))

    // Initial insert
    val data1 = (0 until 20).map { i =>
      Row(s"key_$i", Array.fill(16)(1.0f).toSeq, i.toLong)
    }

    val df1 = createVectorDf(schema, data1)

    writeHudiTable(df1, "mor_vector_test", basePath + "/mor_vec",
      tableType = "MERGE_ON_READ", precombineField = "ts")

    // Upsert: update some vectors with new values
    val data2 = (0 until 10).map { i =>
      Row(s"key_$i", Array.fill(16)(2.0f).toSeq, 100L + i)
    }

    val df2 = createVectorDf(schema, data2)

    writeHudiTable(df2, "mor_vector_test", basePath + "/mor_vec",
      tableType = "MERGE_ON_READ", precombineField = "ts", mode = SaveMode.Append)

    // Read the merged view
    val readDf = readHudiTable(basePath + "/mor_vec")
    assertEquals(20, readDf.count())

    // Updated rows (key_0 through key_9) should have new vectors
    val updatedRow = readDf.select("id", "embedding")
      .filter("id = 'key_5'").collect()(0)
    val updatedVec = updatedRow.getSeq[Float](1)
    assertEquals(16, updatedVec.size)
    assertTrue(updatedVec.forall(_ == 2.0f), "Updated vector should have value 2.0")

    // Non-updated rows (key_10 through key_19) should keep original vectors
    val origRow = readDf.select("id", "embedding")
      .filter("id = 'key_15'").collect()(0)
    val origVec = origRow.getSeq[Float](1)
    assertEquals(16, origVec.size)
    assertTrue(origVec.forall(_ == 1.0f), "Non-updated vector should have value 1.0")
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
  def testLargeDimensionVector(): Unit = {
    val metadata = vectorMetadata("VECTOR(1536)")

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata)
    ))

    val random = new scala.util.Random(7)
    val data = (0 until 5).map { i =>
      Row(s"key_$i", Array.fill(1536)(random.nextFloat()).toSeq)
    }

    val df = createVectorDf(schema, data)

    writeHudiTable(df, "large_dim_vec_test", basePath + "/large_dim")

    val readDf = readHudiTable(basePath + "/large_dim")
    assertEquals(5, readDf.count())

    // Verify dimension preserved
    val readMeta = readDf.schema("embedding").metadata
    val vecSchema = HoodieSchema.parseTypeDescriptor(
      readMeta.getString(HoodieSchema.TYPE_METADATA_FIELD)).asInstanceOf[HoodieSchema.Vector]
    assertEquals(1536, vecSchema.getDimension)

    // Verify values
    val origMap = df.select("id", "embedding").collect()
      .map(r => (r.getString(0), r.getSeq[Float](1))).toMap
    val readMap = readDf.select("id", "embedding").collect()
      .map(r => (r.getString(0), r.getSeq[Float](1))).toMap

    origMap.foreach { case (id, orig) =>
      val read = readMap(id)
      assertEquals(1536, read.size)
      orig.zip(read).foreach { case (o, r) =>
        assertEquals(o, r, 1e-9f, s"Mismatch in $id")
      }
    }
  }

  @Test
  def testSmallDimensionVector(): Unit = {
    val metadata = vectorMetadata("VECTOR(2)")

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("coords", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata)
    ))

    val data = Seq(
      Row("a", Seq(1.0f, 2.0f)),
      Row("b", Seq(-1.5f, 3.14f)),
      Row("c", Seq(0.0f, Float.MaxValue))
    )

    val df = createVectorDf(schema, data)

    writeHudiTable(df, "small_dim_test", basePath + "/small_dim")

    val readDf = readHudiTable(basePath + "/small_dim")
    assertEquals(3, readDf.count())

    val rowA = readDf.select("id", "coords").filter("id = 'a'").collect()(0)
    val coordsA = rowA.getSeq[Float](1)
    assertEquals(2, coordsA.size)
    assertEquals(1.0f, coordsA(0), 1e-9f)
    assertEquals(2.0f, coordsA(1), 1e-9f)

    val rowC = readDf.select("id", "coords").filter("id = 'c'").collect()(0)
    val coordsC = rowC.getSeq[Float](1)
    assertEquals(Float.MaxValue, coordsC(1), 1e-30f)
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
  def testMorLogOnlyCompactionPreservesVectorMetadata(): Unit = {
    val path = basePath + "/mor_log_only_vec"
    val tableName = "mor_log_only_vec_test"
    try {
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  embedding VECTOR(3),
           |  ts long
           |) using hudi
           | location '$path'
           | tblproperties (
           |  primaryKey = 'id',
           |  type = 'mor',
           |  preCombineField = 'ts',
           |  hoodie.index.type = 'INMEMORY',
           |  hoodie.compact.inline = 'true',
           |  hoodie.compact.inline.max.delta.commits = '5',
           |  hoodie.clean.commits.retained = '1'
           | )
       """.stripMargin)

      def readOrdered(): Seq[Row] =
        spark.sql(s"select id, embedding, ts from $tableName order by id").collect().toSeq

      def embeddingOf(id: Int, rows: Seq[Row]): Seq[Float] =
        rows.find(_.getInt(0) == id)
          .getOrElse(fail(s"No row with id=$id"))
          .getSeq[Float](1)

      spark.sql(
        s"insert into $tableName values " +
          "(1, array(cast(0.1 as float), cast(0.2 as float), cast(0.3 as float)), 1000)")
      spark.sql(
        s"insert into $tableName values " +
          "(2, array(cast(0.4 as float), cast(0.5 as float), cast(0.6 as float)), 1000)")
      spark.sql(
        s"insert into $tableName values " +
          "(3, array(cast(0.7 as float), cast(0.8 as float), cast(0.9 as float)), 1000)")
      // 3 commits will not trigger compaction, so it should be log only.
      assertTrue(DataSourceTestUtils.isLogFileOnly(path))
      val afterInserts = readOrdered()
      assertEquals(3, afterInserts.size)
      assertEquals(Seq(0.1f, 0.2f, 0.3f), embeddingOf(1, afterInserts))
      assertEquals(Seq(0.4f, 0.5f, 0.6f), embeddingOf(2, afterInserts))
      assertEquals(Seq(0.7f, 0.8f, 0.9f), embeddingOf(3, afterInserts))

      spark.sql(
        s"""
           |merge into $tableName h0
           |using (
           |  select 1 as id,
           |         array(cast(0.11 as float), cast(0.22 as float), cast(0.33 as float)) as embedding,
           |         1001L as ts
           |) s0
           | on h0.id = s0.id
           | when matched then update set *
           |""".stripMargin)
      // 4 commits will not trigger compaction, so it should be log only.
      assertTrue(DataSourceTestUtils.isLogFileOnly(path))
      val afterUpdate = readOrdered()
      assertEquals(Seq(0.11f, 0.22f, 0.33f), embeddingOf(1, afterUpdate))
      assertEquals(Seq(0.4f, 0.5f, 0.6f), embeddingOf(2, afterUpdate))
      assertEquals(Seq(0.7f, 0.8f, 0.9f), embeddingOf(3, afterUpdate))

      spark.sql(
        s"""
           |merge into $tableName h0
           |using (
           |  select 4 as id,
           |         array(cast(0.44 as float), cast(0.55 as float), cast(0.66 as float)) as embedding,
           |         1000L as ts
           |) s0
           | on h0.id = s0.id
           | when not matched then insert *
           |""".stripMargin)

      // 5 commits will trigger compaction.
      assertFalse(DataSourceTestUtils.isLogFileOnly(path))
      val afterCompaction = readOrdered()
      assertEquals(4, afterCompaction.size)
      assertEquals(Seq(0.11f, 0.22f, 0.33f), embeddingOf(1, afterCompaction))
      assertEquals(Seq(0.4f, 0.5f, 0.6f), embeddingOf(2, afterCompaction))
      assertEquals(Seq(0.7f, 0.8f, 0.9f), embeddingOf(3, afterCompaction))
      assertEquals(Seq(0.44f, 0.55f, 0.66f), embeddingOf(4, afterCompaction))

      // VECTOR custom-type descriptor must survive the compacted base-file read path.
      val embeddingField = spark.table(tableName).schema.find(_.name == "embedding").get
      assertTrue(embeddingField.metadata.contains(HoodieSchema.TYPE_METADATA_FIELD),
        s"Expected VECTOR type metadata on embedding field after compaction, " +
          s"got: ${embeddingField.metadata}")

      // 6th commit drives an auto-clean that retires the now-superseded log-only slice.
      // Inline compaction on commit 5 ran AFTER its own postCommit clean, so the prior
      // slice was not yet superseded when that clean fired and no .clean instant was
      // written. This deltacommit's postCommit clean sees the post-compaction base
      // file and writes the .clean instant.
      spark.sql(
        s"""
           |merge into $tableName h0
           |using (
           |  select 2 as id,
           |         array(cast(0.222 as float), cast(0.555 as float), cast(0.888 as float)) as embedding,
           |         1002L as ts
           |) s0
           | on h0.id = s0.id
           | when matched then update set *
           |""".stripMargin)
      val afterCleanup = readOrdered()
      assertEquals(Seq(0.11f, 0.22f, 0.33f), embeddingOf(1, afterCleanup))
      assertEquals(Seq(0.222f, 0.555f, 0.888f), embeddingOf(2, afterCleanup))
      assertEquals(Seq(0.7f, 0.8f, 0.9f), embeddingOf(3, afterCleanup))
      assertEquals(Seq(0.44f, 0.55f, 0.66f), embeddingOf(4, afterCleanup))

      val metaClient = HoodieTableMetaClient.builder()
        .setBasePath(path).setConf(storageConf).build()
      metaClient.reloadActiveTimeline()
      assertTrue(metaClient.getActiveTimeline.getCleanerTimeline.countInstants() > 0,
        "Expected at least one .clean instant on the timeline after compaction")
    } finally {
      spark.sql(s"drop table if exists $tableName")
    }
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
      assertTrue(footerMeta.contains(HoodieSchema.VECTOR_COLUMNS_METADATA_KEY),
        s"Footer should contain ${HoodieSchema.VECTOR_COLUMNS_METADATA_KEY}, got keys: ${footerMeta.keys.mkString(", ")}")

      val value = footerMeta(HoodieSchema.VECTOR_COLUMNS_METADATA_KEY)
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

  @Test
  def testNestedVectorWriteThrows(): Unit = {
    // A VECTOR nested inside a struct field must be rejected at write time.
    val meta = vectorMetadata("VECTOR(4)")
    val nestedStruct = StructType(Seq(
      StructField("embedding", ArrayType(FloatType, containsNull = false), nullable = false, metadata = meta)
    ))
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("data", nestedStruct, nullable = false)
    ))
    val data = Seq(Row("key_1", Row(Seq(1.0f, 2.0f, 3.0f, 4.0f))))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    val ex = assertThrows(classOf[Exception], () => {
      writeHudiTable(df, "nested_vector_test", basePath + "/nested_vector")
    })
    assertTrue(nestedVectorMessageInCauseChain(ex),
      s"Expected nested VECTOR guard to fire, but got: ${ex.getMessage}")
  }

  private def assertArrayEquals(expected: Array[Byte], actual: Array[Byte], message: String): Unit = {
    assertEquals(expected.length, actual.length, s"$message: length mismatch")
    expected.zip(actual).zipWithIndex.foreach { case ((e, a), idx) =>
      assertEquals(e, a, s"$message: mismatch at index $idx")
    }
  }

  private def nestedVectorMessageInCauseChain(ex: Throwable): Boolean =
    ex != null && (Option(ex.getMessage).exists(_.contains(
      "VECTOR column 'embedding' must be a top-level field. Nested VECTOR columns (inside STRUCT, ARRAY, or MAP) are not supported."))
      || nestedVectorMessageInCauseChain(ex.getCause))
}
