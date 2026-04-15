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
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._

/**
 * End-to-end tests for the hudi_vector_search table-valued function.
 * Tests both single-query and batch-query modes with Spark SQL and DataFrame API.
 */
class TestHoodieVectorSearchFunction extends HoodieSparkClientTestBase {

  var spark: SparkSession = null
  private val corpusPath = "corpus"
  private val corpusViewName = "corpus_view"

  // Test corpus: 5 unit-ish vectors in 3D for easy manual verification
  // doc_1: [1, 0, 0] - x-axis
  // doc_2: [0, 1, 0] - y-axis
  // doc_3: [0, 0, 1] - z-axis
  // doc_4: [0.707, 0.707, 0] - 45 degrees in xy-plane (normalized)
  // doc_5: [0.577, 0.577, 0.577] - equal in all 3 dims (normalized)
  private val corpusData = Seq(
    ("doc_1", Seq(1.0f, 0.0f, 0.0f), "x-axis"),
    ("doc_2", Seq(0.0f, 1.0f, 0.0f), "y-axis"),
    ("doc_3", Seq(0.0f, 0.0f, 1.0f), "z-axis"),
    ("doc_4", Seq(0.70710678f, 0.70710678f, 0.0f), "xy-diagonal"),
    ("doc_5", Seq(0.57735027f, 0.57735027f, 0.57735027f), "xyz-diagonal")
  )

  @BeforeEach override def setUp(): Unit = {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initHoodieStorage()
    createCorpusTable()
  }

  @AfterEach override def tearDown(): Unit = {
    spark.catalog.dropTempView(corpusViewName)
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  private def createCorpusTable(): Unit = {
    val metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(3)")
      .build()

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata),
      StructField("label", StringType, nullable = true)
    ))

    val rows = corpusData.map { case (id, emb, label) =>
      Row(id, emb, label)
    }

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(rows),
      schema
    )

    df.write.format("hudi")
      .option(RECORDKEY_FIELD.key, "id")
      .option(PRECOMBINE_FIELD.key, "id")
      .option(TABLE_NAME.key, "vector_search_corpus")
      .option(TABLE_TYPE.key, "COPY_ON_WRITE")
      .mode(SaveMode.Overwrite)
      .save(basePath + "/" + corpusPath)

    spark.read.format("hudi").load(basePath + "/" + corpusPath)
      .createOrReplaceTempView(corpusViewName)
  }

  /**
   * Creates an in-memory Float corpus temp view (no Hudi write).
   * Schema: id (String), embedding (Array[Float]).
   */
  private def createFloatInMemoryView(viewName: String, data: Seq[(String, Seq[Float])]): Unit = {
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false), nullable = false)
    ))
    val rows = data.map { case (id, emb) => Row(id, emb) }
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      .createOrReplaceTempView(viewName)
  }

  /**
   * Creates an in-memory Byte corpus temp view (no Hudi write).
   * Schema: id (String), embedding (Array[Byte]).
   */
  private def createByteCorpusView(viewName: String, data: Seq[(String, Seq[Byte])]): Unit = {
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(ByteType, containsNull = false), nullable = false)
    ))
    val rows = data.map { case (id, emb) => Row(id, emb) }
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      .createOrReplaceTempView(viewName)
  }

  /**
   * Creates a Float query temp view with configurable id and vector column names.
   * Used by batch-query tests to avoid repeating StructType + createDataFrame boilerplate.
   */
  private def createFloatQueryView(viewName: String, idCol: String, vecCol: String,
                                    data: Seq[(String, Seq[Float])]): Unit = {
    val schema = StructType(Seq(
      StructField(idCol, StringType, nullable = false),
      StructField(vecCol, ArrayType(FloatType, containsNull = false), nullable = false)
    ))
    val rows = data.map { case (id, vec) => Row(id, vec) }
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      .createOrReplaceTempView(viewName)
  }

  /**
   * Writes rows to a Hudi table and registers the result as a Spark temp view.
   * The supplied schema must include an "id" column used as the record key.
   */
  private def writeHudiAndCreateView(schema: StructType, data: Seq[Row], tableName: String,
                                      subPath: String, viewName: String,
                                      tableType: String = "COPY_ON_WRITE",
                                      precombineField: String = "id"): Unit = {
    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      .write.format("hudi")
      .option(RECORDKEY_FIELD.key, "id")
      .option(PRECOMBINE_FIELD.key, precombineField)
      .option(TABLE_NAME.key, tableName)
      .option(TABLE_TYPE.key, tableType)
      .mode(SaveMode.Overwrite)
      .save(basePath + "/" + subPath)
    spark.read.format("hudi").load(basePath + "/" + subPath)
      .createOrReplaceTempView(viewName)
  }

  @Test
  def testSingleQueryDistanceMetrics(): Unit = {
    // Verify all three distance metrics with query [1,0,0] against the shared corpus.

    // --- Cosine ---
    val cosine = spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search('$corpusViewName', 'embedding', ARRAY(1.0, 0.0, 0.0), 3, 'cosine')
         |ORDER BY _hudi_distance
         |""".stripMargin
    ).collect()
    assertEquals(3, cosine.length)
    assertEquals("doc_1", cosine(0).getAs[String]("id"))
    assertEquals(0.0, cosine(0).getAs[Double]("_hudi_distance"), 1e-5)
    assertEquals("doc_4", cosine(1).getAs[String]("id"))
    assertEquals(1.0 - 0.70710678, cosine(1).getAs[Double]("_hudi_distance"), 1e-4)
    assertEquals("doc_5", cosine(2).getAs[String]("id"))
    assertEquals(1.0 - 0.57735027, cosine(2).getAs[Double]("_hudi_distance"), 1e-4)

    // --- L2 ---
    val l2 = spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search('$corpusViewName', 'embedding', ARRAY(1.0, 0.0, 0.0), 3, 'l2')
         |ORDER BY _hudi_distance
         |""".stripMargin
    ).collect()
    assertEquals(3, l2.length)
    assertEquals("doc_1", l2(0).getAs[String]("id"))
    assertEquals(0.0, l2(0).getAs[Double]("_hudi_distance"), 1e-5)
    assertEquals("doc_4", l2(1).getAs[String]("id"))
    val expectedL2Doc4 = math.sqrt(math.pow(1.0 - 0.70710678, 2) + math.pow(0.70710678, 2))
    assertEquals(expectedL2Doc4, l2(1).getAs[Double]("_hudi_distance"), 1e-4)

    // --- Dot product ---
    val dot = spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search('$corpusViewName', 'embedding', ARRAY(1.0, 0.0, 0.0), 3, 'dot_product')
         |ORDER BY _hudi_distance
         |""".stripMargin
    ).collect()
    assertEquals(3, dot.length)
    assertEquals("doc_1", dot(0).getAs[String]("id"))
    assertEquals(-1.0, dot(0).getAs[Double]("_hudi_distance"), 1e-5)
    assertEquals("doc_4", dot(1).getAs[String]("id"))
    assertEquals(-0.70710678, dot(1).getAs[Double]("_hudi_distance"), 1e-4)
  }

  @Test
  def testSingleQueryDefaultMetric(): Unit = {
    // Omit metric arg, should default to cosine
    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  3
         |)
         |ORDER BY _hudi_distance
         |""".stripMargin
    ).collect()

    assertEquals(3, result.length)
    // Should match cosine: doc_1 first with distance ~0
    assertEquals("doc_1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_hudi_distance"), 1e-5)
  }

  @Test
  def testSingleQueryReturnsAllCorpusColumns(): Unit = {
    val result = spark.sql(
      s"""
         |SELECT *
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  2
         |)
         |""".stripMargin
    )

    // Should have the _hudi_distance column plus original corpus columns (embedding is dropped)
    assertTrue(result.columns.contains("_hudi_distance"))
    assertTrue(result.columns.contains("id"))
    assertTrue(result.columns.contains("label"))
    assertFalse(result.columns.contains("embedding"))
    assertEquals(2, result.count())
  }

  @Test
  def testKGreaterThanCorpus(): Unit = {
    // k=100, corpus has 5 rows -> should return all 5
    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  100
         |)
         |""".stripMargin
    ).collect()

    assertEquals(5, result.length)
  }

  @Test
  def testVectorSearchWithWhereClause(): Unit = {
    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  5,
         |  'cosine'
         |)
         |WHERE _hudi_distance < 0.5
         |ORDER BY _hudi_distance
         |""".stripMargin
    ).collect()

    // doc_1 (distance ~0), doc_4 (distance ~0.29), doc_5 (~0.42) should pass
    // doc_2 and doc_3 have distance = 1.0 and should be filtered out
    assertEquals(3, result.length)
    assertTrue(result.forall(_.getAs[Double]("_hudi_distance") < 0.5))
  }

  @Test
  def testVectorSearchAsSubquery(): Unit = {
    val result = spark.sql(
      s"""
         |SELECT sub.id, sub.label, sub._hudi_distance
         |FROM (
         |  SELECT *
         |  FROM hudi_vector_search(
         |    '$corpusViewName',
         |    'embedding',
         |    ARRAY(0.0, 1.0, 0.0),
         |    3
         |  )
         |) sub
         |WHERE sub.label != 'y-axis'
         |ORDER BY sub._hudi_distance
         |""".stripMargin
    ).collect()

    // doc_2 (y-axis) is filtered out
    assertTrue(result.forall(_.getAs[String]("id") != "doc_2"))
  }

  @Test
  def testBatchQueryResultsPerQuery(): Unit = {
    createFloatQueryView("batch_queries", "qid", "qvec", Seq(
      ("q1", Seq(1.0f, 0.0f, 0.0f)),
      ("q2", Seq(0.0f, 0.0f, 1.0f))
    ))

    val resultDf = spark.sql(
      s"""
         |SELECT *
         |FROM hudi_vector_search_batch(
         |  '$corpusViewName',
         |  'embedding',
         |  'batch_queries',
         |  'qvec',
         |  2,
         |  'cosine'
         |)
         |""".stripMargin
    )

    // Verify output columns
    val columns = resultDf.columns
    assertTrue(columns.contains("_hudi_distance"))
    assertTrue(columns.contains("_hudi_query_index"))

    // Each query should get exactly 2 results
    val resultsByQuery = resultDf.groupBy("_hudi_query_index").count().collect()
    assertEquals(2, resultsByQuery.length)
    resultsByQuery.foreach { row =>
      assertEquals(2, row.getLong(1))
    }

    // Validate that _hudi_query_index has two distinct values
    val queryIndexValues = resultDf.select("_hudi_query_index").distinct().collect()
      .map(_.getLong(0)).sorted
    assertEquals(2, queryIndexValues.length)
    assertTrue(queryIndexValues(0) != queryIndexValues(1))

    // Verify DataFrame operations work on the result (merged from testBatchQueryViaDataFrameApi)
    val topResults = resultDf.filter("_hudi_distance < 0.5").select("id", "_hudi_distance", "_hudi_query_index")
    assertTrue(topResults.count() > 0)

    spark.catalog.dropTempView("batch_queries")
  }

  @Test
  def testBatchQuerySameEmbeddingColumnName(): Unit = {
    // Both corpus and query use the column name "embedding" — previously caused ambiguity error
    createFloatQueryView("same_col_queries", "query_name", "embedding", Seq(
      ("q_x", Seq(1.0f, 0.0f, 0.0f)),
      ("q_y", Seq(0.0f, 1.0f, 0.0f))
    ))

    val result = spark.sql(
      s"""
         |SELECT *
         |FROM hudi_vector_search_batch(
         |  '$corpusViewName',
         |  'embedding',
         |  'same_col_queries',
         |  'embedding',
         |  2,
         |  'cosine'
         |)
         |""".stripMargin
    ).collect()

    // 2 queries x 2 results each = 4 rows; should not throw AnalysisException
    assertEquals(4, result.length)
    assertTrue(result.head.schema.fieldNames.contains("_hudi_distance"))

    spark.catalog.dropTempView("same_col_queries")
  }

  @Test
  def testTableByPath(): Unit = {
    val tablePath = basePath + "/" + corpusPath
    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search(
         |  '$tablePath',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  2
         |)
         |ORDER BY _hudi_distance
         |""".stripMargin
    ).collect()

    assertEquals(2, result.length)
    assertEquals("doc_1", result(0).getAs[String]("id"))
  }

  @Test
  def testDoubleVectorEmbeddings(): Unit = {
    val metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(3, DOUBLE)")
      .build()

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(DoubleType, containsNull = false),
        nullable = false, metadata)
    ))

    writeHudiAndCreateView(schema, Seq(
      Row("d1", Seq(1.0, 0.0, 0.0)),
      Row("d2", Seq(0.0, 1.0, 0.0)),
      Row("d3", Seq(0.0, 0.0, 1.0))
    ), "double_vec_search", "double_search", "double_corpus")

    val result = spark.sql(
      """
        |SELECT id, _hudi_distance
        |FROM hudi_vector_search(
        |  'double_corpus',
        |  'embedding',
        |  ARRAY(1.0, 0.0, 0.0),
        |  2
        |)
        |ORDER BY _hudi_distance
        |""".stripMargin
    ).collect()

    assertEquals(2, result.length)
    assertEquals("d1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_hudi_distance"), 1e-10)

    spark.catalog.dropTempView("double_corpus")
  }

  @Test
  def testInvalidEmbeddingColumn(): Unit = {
    val ex = assertThrows(classOf[Exception], () => {
      spark.sql(
        s"""
           |SELECT *
           |FROM hudi_vector_search(
           |  '$corpusViewName',
           |  'nonexistent_col',
           |  ARRAY(1.0, 0.0, 0.0),
           |  3
           |)
           |""".stripMargin
      ).collect()
    })
    assertTrue(ex.getMessage.contains("nonexistent_col") ||
      ex.getCause.getMessage.contains("nonexistent_col"))
  }

  @Test
  def testInvalidDistanceMetric(): Unit = {
    val ex = assertThrows(classOf[Exception], () => {
      spark.sql(
        s"""
           |SELECT *
           |FROM hudi_vector_search(
           |  '$corpusViewName',
           |  'embedding',
           |  ARRAY(1.0, 0.0, 0.0),
           |  3,
           |  'invalid_metric'
           |)
           |""".stripMargin
      ).collect()
    })
    assertTrue(ex.getMessage.contains("Unsupported distance metric") ||
      ex.getCause.getMessage.contains("Unsupported distance metric"))
  }

  @Test
  def testArgumentCountValidation(): Unit = {
    // Too few arguments
    val exFew = assertThrows(classOf[Exception], () => {
      spark.sql(
        s"""SELECT * FROM hudi_vector_search('$corpusViewName', 'embedding')""".stripMargin
      ).collect()
    })
    assertTrue(exFew.getMessage.contains("expects 4-6 arguments") ||
      exFew.getCause.getMessage.contains("expects 4-6 arguments"))

    // Too many arguments
    val exMany = assertThrows(classOf[Exception], () => {
      spark.sql(
        s"""SELECT * FROM hudi_vector_search(
           |  '$corpusViewName', 'embedding', ARRAY(1.0, 0.0, 0.0), 3, 'cosine', 'brute_force', 'extra_arg'
           |)""".stripMargin
      ).collect()
    })
    val msg = if (exMany.getCause != null) exMany.getCause.getMessage else exMany.getMessage
    assertTrue(msg.contains("4-6 arguments"), s"Expected arg-count error, got: $msg")
  }

  @Test
  def testDistanceExactValues(): Unit = {
    // --- Cosine: query [0,1,0] against all 5 corpus docs ---
    val cosine = spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search('$corpusViewName', 'embedding', ARRAY(0.0, 1.0, 0.0), 5, 'cosine')
         |ORDER BY _hudi_distance
         |""".stripMargin
    ).collect()
    assertEquals(5, cosine.length)
    val cosMap = cosine.map(r => r.getAs[String]("id") -> r.getAs[Double]("_hudi_distance")).toMap
    assertEquals(0.0, cosMap("doc_2"), 1e-5)
    assertEquals(1.0, cosMap("doc_1"), 1e-5)
    assertEquals(1.0, cosMap("doc_3"), 1e-5)
    assertEquals(1.0 - 0.70710678, cosMap("doc_4"), 1e-4)
    assertEquals(1.0 - 0.57735027, cosMap("doc_5"), 1e-4)

    // --- L2: query [1,0,0] against all 5 corpus docs ---
    val l2 = spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search('$corpusViewName', 'embedding', ARRAY(1.0, 0.0, 0.0), 5, 'l2')
         |ORDER BY _hudi_distance
         |""".stripMargin
    ).collect()
    assertEquals(5, l2.length)
    assertEquals("doc_1", l2(0).getAs[String]("id"))
    assertEquals(0.0, l2(0).getAs[Double]("_hudi_distance"), 1e-4)
    assertEquals("doc_4", l2(1).getAs[String]("id"))
    assertEquals(math.sqrt(math.pow(1.0 - 0.70710678, 2) + math.pow(0.70710678, 2)),
      l2(1).getAs[Double]("_hudi_distance"), 1e-4)
    assertEquals(math.sqrt(2.0), l2(3).getAs[Double]("_hudi_distance"), 1e-4)
    assertEquals(math.sqrt(2.0), l2(4).getAs[Double]("_hudi_distance"), 1e-4)
    // Verify ascending order
    for (i <- 0 until l2.length - 1) {
      assertTrue(l2(i).getAs[Double]("_hudi_distance") <= l2(i + 1).getAs[Double]("_hudi_distance"))
    }
  }

  @Test
  def testNullEmbeddingsAreFiltered(): Unit = {
    val metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(3)")
      .build()

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = true, metadata),
      StructField("label", StringType, nullable = true)
    ))

    writeHudiAndCreateView(schema, Seq(
      Row("n1", Seq(1.0f, 0.0f, 0.0f), "has-vector"),
      Row("n2", null, "null-vector"),
      Row("n3", Seq(0.0f, 1.0f, 0.0f), "has-vector")
    ), "null_vec_search", "null_search", "null_corpus")

    // Should not throw NPE — null rows are filtered out
    val result = spark.sql(
      """
        |SELECT id, _hudi_distance
        |FROM hudi_vector_search(
        |  'null_corpus',
        |  'embedding',
        |  ARRAY(1.0, 0.0, 0.0),
        |  5,
        |  'cosine'
        |)
        |ORDER BY _hudi_distance
        |""".stripMargin
    ).collect()

    // Only non-null rows returned
    assertEquals(2, result.length)
    assertEquals("n1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_hudi_distance"), 1e-5)

    spark.catalog.dropTempView("null_corpus")
  }

  @Test
  def testEmptyCorpus(): Unit = {
    val metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(3)")
      .build()

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata)
    ))

    // Create an empty DataFrame and write it — we need an actual Hudi table,
    // so write one row then filter it out in the view
    val data = Seq(Row("temp", Seq(1.0f, 0.0f, 0.0f)))
    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      .write.format("hudi")
      .option(RECORDKEY_FIELD.key, "id")
      .option(PRECOMBINE_FIELD.key, "id")
      .option(TABLE_NAME.key, "empty_vec_search")
      .option(TABLE_TYPE.key, "COPY_ON_WRITE")
      .mode(SaveMode.Overwrite)
      .save(basePath + "/empty_search")

    spark.read.format("hudi").load(basePath + "/empty_search")
      .filter("id = 'nonexistent'")
      .createOrReplaceTempView("empty_corpus")

    val result = spark.sql(
      """
        |SELECT id, _hudi_distance
        |FROM hudi_vector_search(
        |  'empty_corpus',
        |  'embedding',
        |  ARRAY(1.0, 0.0, 0.0),
        |  3
        |)
        |""".stripMargin
    ).collect()

    assertEquals(0, result.length)

    spark.catalog.dropTempView("empty_corpus")
  }

  @Test
  def testDimensionMismatchErrors(): Unit = {
    // With VECTOR(3) metadata — caught at analysis time
    val ex = assertThrows(classOf[Exception], () => {
      spark.sql(
        s"""
           |SELECT *
           |FROM hudi_vector_search(
           |  '$corpusViewName',
           |  'embedding',
           |  ARRAY(1.0, 0.0, 0.0, 0.0, 0.0),
           |  3
           |)
           |""".stripMargin
      ).collect()
    })
    assertTrue(ex.getMessage.contains("dimension") ||
      (ex.getCause != null && ex.getCause.getMessage.contains("dimension")))

    // Without VECTOR metadata — caught at UDF runtime
    createFloatInMemoryView("no_meta_corpus", Seq(
      ("n1", Seq(1.0f, 0.0f, 0.0f)),
      ("n2", Seq(0.0f, 1.0f, 0.0f))
    ))
    val ex2 = assertThrows(classOf[Exception], () => {
      spark.sql(
        """
          |SELECT *
          |FROM hudi_vector_search('no_meta_corpus', 'embedding', ARRAY(1.0, 0.0, 0.0, 0.0, 0.0), 2, 'cosine')
          |""".stripMargin
      ).collect()
    })
    def rootMessage(e: Throwable): String = if (e.getCause != null) rootMessage(e.getCause) else e.getMessage
    val msg = rootMessage(ex2)
    assertTrue(msg.contains("dimension mismatch") || msg.contains("mismatch"),
      s"Expected dimension mismatch error, got: $msg")
    spark.catalog.dropTempView("no_meta_corpus")
  }

  @Test
  def testInvalidK(): Unit = {
    // k=0
    val exZero = assertThrows(classOf[Exception], () => {
      spark.sql(
        s"""
           |SELECT *
           |FROM hudi_vector_search(
           |  '$corpusViewName',
           |  'embedding',
           |  ARRAY(1.0, 0.0, 0.0),
           |  0
           |)
           |""".stripMargin
      ).collect()
    })
    assertTrue(exZero.getMessage.contains("positive integer") ||
      (exZero.getCause != null && exZero.getCause.getMessage.contains("positive integer")))

    // k=-5
    val exNeg = assertThrows(classOf[Exception], () => {
      spark.sql(
        s"""
           |SELECT *
           |FROM hudi_vector_search(
           |  '$corpusViewName',
           |  'embedding',
           |  ARRAY(1.0, 0.0, 0.0),
           |  -5
           |)
           |""".stripMargin
      ).collect()
    })
    assertTrue(exNeg.getMessage.contains("positive integer") ||
      (exNeg.getCause != null && exNeg.getCause.getMessage.contains("positive integer")))
  }

  @Test
  def testByteVectorDistanceMetrics(): Unit = {
    // --- Cosine ---
    createByteCorpusView("byte_corpus", Seq(
      ("b1", Seq(127.toByte, 0.toByte, 0.toByte)),
      ("b2", Seq(0.toByte, 127.toByte, 0.toByte)),
      ("b3", Seq(0.toByte, 0.toByte, 127.toByte))
    ))
    val cosine = spark.sql(
      """SELECT id, _hudi_distance
        |FROM hudi_vector_search('byte_corpus', 'embedding', ARRAY(127.0, 0.0, 0.0), 2, 'cosine')
        |ORDER BY _hudi_distance""".stripMargin
    ).collect()
    assertEquals(2, cosine.length)
    assertEquals("b1", cosine(0).getAs[String]("id"))
    assertEquals(0.0, cosine(0).getAs[Double]("_hudi_distance"), 1e-5)
    spark.catalog.dropTempView("byte_corpus")

    // --- L2 ---
    createByteCorpusView("byte_l2_corpus", Seq(
      ("b1", Seq(10.toByte, 0.toByte, 0.toByte)),
      ("b2", Seq(0.toByte, 10.toByte, 0.toByte))
    ))
    val l2 = spark.sql(
      """SELECT id, _hudi_distance
        |FROM hudi_vector_search('byte_l2_corpus', 'embedding', ARRAY(10.0, 0.0, 0.0), 2, 'l2')
        |ORDER BY _hudi_distance""".stripMargin
    ).collect()
    assertEquals(2, l2.length)
    assertEquals("b1", l2(0).getAs[String]("id"))
    assertEquals(0.0, l2(0).getAs[Double]("_hudi_distance"), 1e-5)
    assertEquals(math.sqrt(200.0), l2(1).getAs[Double]("_hudi_distance"), 1e-4)
    spark.catalog.dropTempView("byte_l2_corpus")

    // --- Dot product ---
    createByteCorpusView("byte_dot_corpus", Seq(
      ("b1", Seq(10.toByte, 5.toByte, 0.toByte)),
      ("b2", Seq(0.toByte, 5.toByte, 0.toByte))
    ))
    val dot = spark.sql(
      """SELECT id, _hudi_distance
        |FROM hudi_vector_search('byte_dot_corpus', 'embedding', ARRAY(10.0, 0.0, 0.0), 2, 'dot_product')
        |ORDER BY _hudi_distance""".stripMargin
    ).collect()
    assertEquals(2, dot.length)
    assertEquals("b1", dot(0).getAs[String]("id"))
    assertEquals(-100.0, dot(0).getAs[Double]("_hudi_distance"), 1e-5)
    assertEquals("b2", dot(1).getAs[String]("id"))
    assertEquals(0.0, dot(1).getAs[Double]("_hudi_distance"), 1e-5)
    spark.catalog.dropTempView("byte_dot_corpus")
  }

  @Test
  def testBatchQueryCorrectnessVerification(): Unit = {
    createFloatQueryView("correctness_queries", "query_name", "query_vec", Seq(
      ("q_x", Seq(1.0f, 0.0f, 0.0f)),
      ("q_y", Seq(0.0f, 1.0f, 0.0f))
    ))

    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance, query_name
         |FROM hudi_vector_search_batch(
         |  '$corpusViewName',
         |  'embedding',
         |  'correctness_queries',
         |  'query_vec',
         |  1,
         |  'cosine'
         |)
         |""".stripMargin
    ).collect()

    assertEquals(2, result.length)

    // Verify which corpus doc is nearest to each query
    val resultMap = result.map(r =>
      r.getAs[String]("query_name") -> r.getAs[String]("id")).toMap

    assertEquals("doc_1", resultMap("q_x"))
    assertEquals("doc_2", resultMap("q_y"))

    spark.catalog.dropTempView("correctness_queries")
  }

  @Test
  def testBatchQueryMismatchedElementTypes(): Unit = {
    // Query table uses array<double>, corpus uses array<float>
    val querySchema = StructType(Seq(
      StructField("qid", StringType, nullable = false),
      StructField("qvec", ArrayType(DoubleType, containsNull = false), nullable = false)
    ))

    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("q1", Seq(1.0, 0.0, 0.0)))),
      querySchema
    ).createOrReplaceTempView("mismatched_type_queries")

    val ex = assertThrows(classOf[Exception], () => {
      spark.sql(
        s"""
           |SELECT *
           |FROM hudi_vector_search_batch(
           |  '$corpusViewName',
           |  'embedding',
           |  'mismatched_type_queries',
           |  'qvec',
           |  2,
           |  'cosine'
           |)
           |""".stripMargin
      ).collect()
    })
    assertTrue(ex.getMessage.contains("element type") ||
      (ex.getCause != null && ex.getCause.getMessage.contains("element type")))

    spark.catalog.dropTempView("mismatched_type_queries")
  }

  @Test
  def testBatchQueryDimensionMismatch(): Unit = {
    val metadata5 = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(5)")
      .build()

    val querySchema = StructType(Seq(
      StructField("qid", StringType, nullable = false),
      StructField("qvec", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata5)
    ))

    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("q1", Seq(1.0f, 0.0f, 0.0f, 0.0f, 0.0f)))),
      querySchema
    ).createOrReplaceTempView("dim_mismatch_queries")

    val ex = assertThrows(classOf[Exception], () => {
      spark.sql(
        s"""
           |SELECT *
           |FROM hudi_vector_search_batch(
           |  '$corpusViewName',
           |  'embedding',
           |  'dim_mismatch_queries',
           |  'qvec',
           |  2,
           |  'cosine'
           |)
           |""".stripMargin
      ).collect()
    })
    assertTrue(ex.getMessage.contains("dimension") ||
      (ex.getCause != null && ex.getCause.getMessage.contains("dimension")))

    spark.catalog.dropTempView("dim_mismatch_queries")
  }

  @Test
  def testBatchQueryOverlappingNonEmbeddingColumns(): Unit = {
    // Query table also has "id" and "label" columns, same as corpus
    val querySchema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("label", StringType, nullable = true),
      StructField("query_vec", ArrayType(FloatType, containsNull = false), nullable = false)
    ))

    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("q1", "query_label_1", Seq(1.0f, 0.0f, 0.0f)),
        Row("q2", "query_label_2", Seq(0.0f, 1.0f, 0.0f))
      )), querySchema
    ).createOrReplaceTempView("overlapping_queries")

    val result = spark.sql(
      s"""
         |SELECT *
         |FROM hudi_vector_search_batch(
         |  '$corpusViewName',
         |  'embedding',
         |  'overlapping_queries',
         |  'query_vec',
         |  1,
         |  'cosine'
         |)
         |""".stripMargin
    )

    // Query columns that clash with corpus columns get the _hudi_query_ prefix.
    // "id" -> "_hudi_query_id", "label" -> "_hudi_query_label"
    val columns = result.columns
    assertTrue(columns.contains("id"))                // corpus id (unchanged)
    assertTrue(columns.contains("label"))             // corpus label (unchanged)
    assertTrue(columns.contains("_hudi_query_id"))    // query id renamed
    assertTrue(columns.contains("_hudi_query_label")) // query label renamed
    assertTrue(columns.contains("_hudi_distance"))

    val rows = result.collect()
    assertEquals(2, rows.length)

    spark.catalog.dropTempView("overlapping_queries")
  }

  @Test
  def testNonFoldableQueryVectorError(): Unit = {
    // A non-constant expression (column reference) as the query vector must fail.
    // We use a subquery that returns an array — this is foldable=false.
    assertThrows(classOf[Exception], () => {
      spark.sql(
        s"""
           |SELECT *
           |FROM hudi_vector_search(
           |  '$corpusViewName',
           |  'embedding',
           |  embedding,
           |  3
           |)
           |""".stripMargin
      ).collect()
    })
  }

  @Test
  def testIntegerQueryVector(): Unit = {
    // ARRAY(1, 0, 0) is inferred as decimal by Spark, should still work
    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1, 0, 0),
         |  2
         |)
         |ORDER BY _hudi_distance
         |""".stripMargin
    ).collect()

    assertEquals(2, result.length)
    assertEquals("doc_1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_hudi_distance"), 1e-5)
  }

  @Test
  def testNonHudiTableAsCorpus(): Unit = {
    val metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(3)")
      .build()

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata)
    ))

    // Create a plain DataFrame (no Hudi write) and register as temp view
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("plain_1", Seq(1.0f, 0.0f, 0.0f)),
        Row("plain_2", Seq(0.0f, 1.0f, 0.0f))
      )), schema
    ).createOrReplaceTempView("plain_corpus")

    val result = spark.sql(
      """
        |SELECT id, _hudi_distance
        |FROM hudi_vector_search(
        |  'plain_corpus',
        |  'embedding',
        |  ARRAY(1.0, 0.0, 0.0),
        |  2,
        |  'cosine'
        |)
        |ORDER BY _hudi_distance
        |""".stripMargin
    ).collect()

    assertEquals(2, result.length)
    assertEquals("plain_1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_hudi_distance"), 1e-5)

    spark.catalog.dropTempView("plain_corpus")
  }

  @Test
  def testZeroVectorInCorpus(): Unit = {
    createFloatInMemoryView("zero_corpus", Seq(
      ("normal", Seq(1.0f, 0.0f, 0.0f)),
      ("zero", Seq(0.0f, 0.0f, 0.0f))
    ))

    val result = spark.sql(
      """
        |SELECT id, _hudi_distance
        |FROM hudi_vector_search(
        |  'zero_corpus',
        |  'embedding',
        |  ARRAY(1.0, 0.0, 0.0),
        |  2,
        |  'cosine'
        |)
        |ORDER BY _hudi_distance
        |""".stripMargin
    ).collect()

    assertEquals(2, result.length)
    // Normal vector should be closest (distance = 0)
    assertEquals("normal", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_hudi_distance"), 1e-5)
    // Zero vector should have maximal cosine distance (1.0)
    assertEquals("zero", result(1).getAs[String]("id"))
    assertEquals(1.0, result(1).getAs[Double]("_hudi_distance"), 1e-5)

    spark.catalog.dropTempView("zero_corpus")
  }

  @Test
  def testAllIdenticalVectors(): Unit = {
    createFloatInMemoryView("same_corpus", Seq(
      ("same_1", Seq(1.0f, 0.0f, 0.0f)),
      ("same_2", Seq(1.0f, 0.0f, 0.0f)),
      ("same_3", Seq(1.0f, 0.0f, 0.0f))
    ))

    val result = spark.sql(
      """
        |SELECT id, _hudi_distance
        |FROM hudi_vector_search(
        |  'same_corpus',
        |  'embedding',
        |  ARRAY(1.0, 0.0, 0.0),
        |  3,
        |  'cosine'
        |)
        |""".stripMargin
    ).collect()

    assertEquals(3, result.length)
    // All distances should be 0.0
    result.foreach { row =>
      assertEquals(0.0, row.getAs[Double]("_hudi_distance"), 1e-5)
    }

    spark.catalog.dropTempView("same_corpus")
  }

  @Test
  def testBatchQueryLargeK(): Unit = {
    createFloatQueryView("large_k_queries", "qid", "qvec", Seq(
      ("q1", Seq(1.0f, 0.0f, 0.0f)),
      ("q2", Seq(0.0f, 1.0f, 0.0f))
    ))

    val result = spark.sql(
      s"""
         |SELECT *
         |FROM hudi_vector_search_batch(
         |  '$corpusViewName',
         |  'embedding',
         |  'large_k_queries',
         |  'qvec',
         |  100,
         |  'cosine'
         |)
         |""".stripMargin
    )

    // k=100 but corpus has 5 rows, so each query gets 5 results = 10 total
    val resultsByQuery = result.groupBy("_hudi_query_index").count().collect()
    assertEquals(2, resultsByQuery.length)
    resultsByQuery.foreach { row =>
      assertEquals(5, row.getLong(1))
    }

    spark.catalog.dropTempView("large_k_queries")
  }

  @Test
  def testUnsupportedAlgorithmError(): Unit = {
    val ex = assertThrows(classOf[Exception], () => {
      spark.sql(
        s"""
           |SELECT *
           |FROM hudi_vector_search(
           |  '$corpusViewName',
           |  'embedding',
           |  ARRAY(1.0, 0.0, 0.0),
           |  3,
           |  'cosine',
           |  'hnsw'
           |)
           |""".stripMargin
      ).collect()
    })
    assertTrue(ex.getMessage.contains("Unsupported search algorithm") ||
      (ex.getCause != null && ex.getCause.getMessage.contains("Unsupported search algorithm")))
  }

  @Test
  def testExplicitBruteForceAlgorithm(): Unit = {
    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  2,
         |  'cosine',
         |  'brute_force'
         |)
         |ORDER BY _hudi_distance
         |""".stripMargin
    ).collect()

    assertEquals(2, result.length)
    assertEquals("doc_1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_hudi_distance"), 1e-5)
  }

  @Test
  def testCosineDistanceAntiparallelVectors(): Unit = {
    // [1,0,0] vs [-1,0,0] are exactly antiparallel; cosine distance should be 2.0.
    // FP rounding can push dot/denom slightly below -1.0, making 1 - x > 2.0.
    // Verify the upper clamp keeps the result at exactly 2.0.
    createFloatInMemoryView("antiparallel_corpus", Seq(("anti", Seq(-1.0f, 0.0f, 0.0f))))

    val result = spark.sql(
      """
        |SELECT id, _hudi_distance
        |FROM hudi_vector_search('antiparallel_corpus', 'embedding', ARRAY(1.0, 0.0, 0.0), 1, 'cosine')
        |""".stripMargin
    ).collect()

    assertEquals(1, result.length)
    val dist = result(0).getAs[Double]("_hudi_distance")
    assertTrue(dist <= 2.0, s"Cosine distance should be <= 2.0 but was $dist")
    assertEquals(2.0, dist, 1e-5)

    spark.catalog.dropTempView("antiparallel_corpus")
  }

  @Test
  def testByteVectorQueryValidation(): Unit = {
    // Out-of-range value (200 exceeds byte range)
    createByteCorpusView("byte_range_corpus", Seq(("b1", Seq(10.toByte, 0.toByte, 0.toByte))))
    val exRange = assertThrows(classOf[Exception], () => {
      spark.sql(
        """SELECT * FROM hudi_vector_search('byte_range_corpus', 'embedding', ARRAY(200.0, 0.0, 0.0), 1, 'cosine')"""
      ).collect()
    })
    val rangeMsg = if (exRange.getCause != null) exRange.getCause.getMessage else exRange.getMessage
    assertTrue(rangeMsg.contains("Query vector value 200.0 is out of range for byte corpus"),
      s"Expected out-of-range error, got: $rangeMsg")
    spark.catalog.dropTempView("byte_range_corpus")

    // Fractional value (10.5 is not a whole number)
    createByteCorpusView("byte_frac_corpus", Seq(("b1", Seq(10.toByte, 0.toByte, 0.toByte))))
    val exFrac = assertThrows(classOf[Exception], () => {
      spark.sql(
        """SELECT * FROM hudi_vector_search('byte_frac_corpus', 'embedding', ARRAY(10.5, 0.0, 0.0), 1, 'cosine')"""
      ).collect()
    })
    val fracMsg = if (exFrac.getCause != null) exFrac.getCause.getMessage else exFrac.getMessage
    assertTrue(fracMsg.contains("not a whole number"),
      s"Expected integrality error, got: $fracMsg")
    spark.catalog.dropTempView("byte_frac_corpus")
  }

  @Test
  def testNullQueryVector(): Unit = {
    // ARRAY(1.0, null, 0.0) — the null element should produce a useful error, not an NPE
    val ex = assertThrows(classOf[Exception], () => {
      spark.sql(
        s"""
           |SELECT *
           |FROM hudi_vector_search('$corpusViewName', 'embedding', ARRAY(1.0, null, 0.0), 3)
           |""".stripMargin
      ).collect()
    })
    val rootCause = Option(ex.getCause).getOrElse(ex)
    assertFalse(rootCause.isInstanceOf[NullPointerException],
      s"Expected a meaningful error, not NPE: $rootCause")
  }

  @Test
  def testMorTableVectorSearch(): Unit = {
    val metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(3)")
      .build()

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata),
      StructField("ts", LongType, nullable = false)
    ))

    writeHudiAndCreateView(schema, Seq(
      Row("m1", Seq(1.0f, 0.0f, 0.0f), 1L),
      Row("m2", Seq(0.0f, 1.0f, 0.0f), 1L),
      Row("m3", Seq(0.0f, 0.0f, 1.0f), 1L)
    ), "mor_search_test", "mor_search", "mor_corpus", "MERGE_ON_READ", "ts")

    val result = spark.sql(
      """
        |SELECT id, _hudi_distance
        |FROM hudi_vector_search(
        |  'mor_corpus',
        |  'embedding',
        |  ARRAY(1.0, 0.0, 0.0),
        |  2,
        |  'cosine'
        |)
        |ORDER BY _hudi_distance
        |""".stripMargin
    ).collect()

    assertEquals(2, result.length)
    assertEquals("m1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_hudi_distance"), 1e-5)

    spark.catalog.dropTempView("mor_corpus")
  }

  @Test
  def testBatchQueryExactDistanceValues(): Unit = {
    createFloatQueryView("exact_dist_queries", "query_name", "query_vec", Seq(
      ("q_x", Seq(1.0f, 0.0f, 0.0f)),
      ("q_z", Seq(0.0f, 0.0f, 1.0f))
    ))

    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance, query_name
         |FROM hudi_vector_search_batch(
         |  '$corpusViewName',
         |  'embedding',
         |  'exact_dist_queries',
         |  'query_vec',
         |  5,
         |  'cosine'
         |)
         |""".stripMargin
    ).collect()

    // 2 queries x 5 results each = 10 rows
    assertEquals(10, result.length)

    // Verify exact distances for q_x query [1,0,0]
    val qxResults = result.filter(_.getAs[String]("query_name") == "q_x")
      .map(r => r.getAs[String]("id") -> r.getAs[Double]("_hudi_distance")).toMap

    // doc_1 [1,0,0]: cosine distance = 0.0
    assertEquals(0.0, qxResults("doc_1"), 1e-5)
    // doc_2 [0,1,0]: cosine distance = 1.0
    assertEquals(1.0, qxResults("doc_2"), 1e-5)
    // doc_3 [0,0,1]: cosine distance = 1.0
    assertEquals(1.0, qxResults("doc_3"), 1e-5)
    // doc_4 [0.707,0.707,0]: cosine distance = 1 - 0.707 ~= 0.293
    assertEquals(1.0 - 0.70710678, qxResults("doc_4"), 1e-4)

    // Verify exact distances for q_z query [0,0,1]
    val qzResults = result.filter(_.getAs[String]("query_name") == "q_z")
      .map(r => r.getAs[String]("id") -> r.getAs[Double]("_hudi_distance")).toMap

    // doc_3 [0,0,1]: cosine distance = 0.0
    assertEquals(0.0, qzResults("doc_3"), 1e-5)
    // doc_1 [1,0,0]: cosine distance = 1.0
    assertEquals(1.0, qzResults("doc_1"), 1e-5)

    spark.catalog.dropTempView("exact_dist_queries")
  }

  @Test
  def testNaNInCorpusVectors(): Unit = {
    // Corpus with a NaN element — should not crash, NaN distances sort last
    createFloatInMemoryView("nan_corpus", Seq(
      ("normal", Seq(1.0f, 0.0f, 0.0f)),
      ("has_nan", Seq(Float.NaN, 0.0f, 0.0f))
    ))

    val result = spark.sql(
      """
        |SELECT id, _hudi_distance
        |FROM hudi_vector_search(
        |  'nan_corpus',
        |  'embedding',
        |  ARRAY(1.0, 0.0, 0.0),
        |  2,
        |  'cosine'
        |)
        |ORDER BY _hudi_distance
        |""".stripMargin
    ).collect()

    assertEquals(2, result.length)
    // Normal vector should be first (distance = 0)
    assertEquals("normal", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_hudi_distance"), 1e-5)
    // NaN vector sorts last in Spark's orderBy (NaN > all other values)
    assertEquals("has_nan", result(1).getAs[String]("id"))
    assertTrue(result(1).getAs[Double]("_hudi_distance").isNaN)

    spark.catalog.dropTempView("nan_corpus")
  }

  @Test
  def testZeroQueryVectorCosineDistance(): Unit = {
    // Zero query vector with cosine distance: all corpus vectors should have distance 1.0
    createFloatInMemoryView("zero_q_corpus", Seq(
      ("a", Seq(1.0f, 0.0f, 0.0f)),
      ("b", Seq(0.0f, 1.0f, 0.0f)),
      ("c", Seq(0.0f, 0.0f, 1.0f))
    ))

    val result = spark.sql(
      """
        |SELECT id, _hudi_distance
        |FROM hudi_vector_search(
        |  'zero_q_corpus',
        |  'embedding',
        |  ARRAY(0.0, 0.0, 0.0),
        |  3,
        |  'cosine'
        |)
        |""".stripMargin
    ).collect()

    assertEquals(3, result.length)
    // All distances should be 1.0 (convention for zero query vector)
    result.foreach { row =>
      assertEquals(1.0, row.getAs[Double]("_hudi_distance"), 1e-5)
    }

    spark.catalog.dropTempView("zero_q_corpus")
  }

}
