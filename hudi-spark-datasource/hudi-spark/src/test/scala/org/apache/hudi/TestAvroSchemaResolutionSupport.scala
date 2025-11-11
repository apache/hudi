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
 *
 */
package org.apache.hudi

import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.SchemaCompatibilityException
import org.apache.hudi.testutils.HoodieClientTestBase

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, ValueSource}

import scala.language.postfixOps

/**
 * Test cases to validate Hudi's support for writing and reading when evolving schema implicitly via Avro's Schema Resolution
 * Note: Test will explicitly write into different partitions to ensure that a Hudi table will have multiple filegroups with different schemas.
 */
class TestAvroSchemaResolutionSupport extends HoodieClientTestBase with ScalaAssertionSupport {

  var spark: SparkSession = _
  val commonOpts: Map[String, String] = Map(
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_avro_schema_resolution_support",
    "hoodie.insert.shuffle.parallelism" -> "1",
    "hoodie.upsert.shuffle.parallelism" -> "1",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "id",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "name",
    DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.SimpleKeyGenerator",
    HoodieMetadataConfig.ENABLE.key -> "false"
  )

  /**
   * Setup method running before each test.
   */
  @BeforeEach override def setUp(): Unit = {
    setTableName("hoodie_avro_schema_resolution_support")
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
  }

  @AfterEach override def tearDown(): Unit = {
    cleanupSparkContexts()
  }

  def castColToX(x: Int, colToCast: String, df: DataFrame): DataFrame = x match {
    case 0 => df.withColumn(colToCast, df.col(colToCast).cast("long"))
    case 1 => df.withColumn(colToCast, df.col(colToCast).cast("float"))
    case 2 => df.withColumn(colToCast, df.col(colToCast).cast("double"))
    case 3 => df.withColumn(colToCast, df.col(colToCast).cast("binary"))
    case 4 => df.withColumn(colToCast, df.col(colToCast).cast("string"))
  }

  def initialiseTable(df: DataFrame, saveDir: String, isCow: Boolean = true): Unit = {
    val opts = if (isCow) {
      commonOpts ++ Map(DataSourceWriteOptions.TABLE_TYPE.key -> "COPY_ON_WRITE")
    } else {
      commonOpts ++ Map(DataSourceWriteOptions.TABLE_TYPE.key -> "MERGE_ON_READ")
    }

    df.write.format("hudi")
      .options(opts)
      .mode("overwrite")
      .save(saveDir)
  }

  def upsertData(df: DataFrame, saveDir: String, isCow: Boolean = true, shouldAllowDroppedColumns: Boolean = false,
                 enableSchemaValidation: Boolean = HoodieWriteConfig.AVRO_SCHEMA_VALIDATE_ENABLE.defaultValue().toBoolean): Unit = {
    var opts = if (isCow) {
      commonOpts ++ Map(DataSourceWriteOptions.TABLE_TYPE.key -> "COPY_ON_WRITE")
    } else {
      commonOpts ++ Map(DataSourceWriteOptions.TABLE_TYPE.key -> "MERGE_ON_READ")
    }
    opts = opts ++ Map(HoodieWriteConfig.SCHEMA_ALLOW_AUTO_EVOLUTION_COLUMN_DROP.key -> shouldAllowDroppedColumns.toString)

    df.write.format("hudi")
      .options(opts)
      .mode("append")
      .save(saveDir)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testDataTypePromotions(isCow: Boolean): Unit = {
    // test to read tables with columns that are promoted via avro schema resolution
    val tempRecordPath = basePath + "/record_tbl/"
    val _spark = spark
    import _spark.implicits._

    val colToCast = "userId"
    val df1 = Seq((1, 100, "aaa")).toDF("id", "userid", "name")
    val df2 = Seq((2, 200L, "bbb")).toDF("id", "userid", "name")

    def prepDataFrame(df: DataFrame, colInitType: String): DataFrame = {
      // convert int to string first before conversion to binary
      // after which, initialise df with initType
      if (colInitType == "binary") {
        val castDf = df.withColumn(colToCast, df.col(colToCast).cast("string"))
        castDf.withColumn(colToCast, castDf.col(colToCast).cast(colInitType))
      } else {
        df.withColumn(colToCast, df.col(colToCast).cast(colInitType))
      }
    }

    def doTest(colInitType: String, start: Int, end: Int): Unit = {
      for (a <- Range(start, end)) {
        try {
          Console.println(s"Performing test: $a with initialColType of: $colInitType")

          // convert int to string first before conversion to binary
          val initDF = prepDataFrame(df1, colInitType)
          initDF.printSchema()
          initDF.show(false)

          // recreate table
          initialiseTable(initDF, tempRecordPath, isCow)

          // perform avro supported casting
          var upsertDf = prepDataFrame(df2, colInitType)
          upsertDf = castColToX(a, colToCast, upsertDf)
          upsertDf.printSchema()
          upsertDf.show(false)

          // upsert
          upsertData(upsertDf, tempRecordPath, isCow)

          // read out the table
          val readDf = spark.read.format("hudi").load(tempRecordPath)
          readDf.printSchema()
          readDf.show(false)
          readDf.foreach(_ => {})

          assert(true)
        } catch {
          case e: Exception => {
            // e.printStackTrace()
            // Console.println(s"Test $a failed with error: ${e.getMessage}")
            assert(false, e)
          }
        }
      }
    }

    // INT -> [Long, Float, Double, String]
    doTest("int", 0, 3)
    // Long -> [Float, Double, String]
    doTest("long", 1, 3)
    // Float -> [Double, String]
    doTest("float", 2, 3)
    // String -> [Bytes]
    doTest("string", 3, 4)
    // Bytes -> [String]
    doTest("binary", 4, 5)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testAddNewColumn(isCow: Boolean): Unit = {
    // test to add a column
    val tempRecordPath = basePath + "/record_tbl/"
    val _spark = spark
    import _spark.implicits._

    val df1 = Seq((1, 100, "aaa")).toDF("id", "userid", "name")
    val df2 = Seq((2, 200, "newCol", "bbb")).toDF("id", "userid", "newcol", "name")

    // convert int to string first before conversion to binary
    val initDF = df1
    initDF.printSchema()
    initDF.show(false)

    // recreate table
    initialiseTable(initDF, tempRecordPath, isCow)

    // perform avro supported operation of adding a new column at the end of the table
    val upsertDf = df2
    upsertDf.printSchema()
    upsertDf.show(false)

    // upsert
    upsertData(upsertDf, tempRecordPath, isCow)

    // read out the table
    val readDf = spark.read.format("hudi").load(tempRecordPath)
    readDf.printSchema()
    readDf.show(false)
    readDf.foreach(_ => {})
  }

  @ParameterizedTest
  @CsvSource(value = Array(
    "COPY_ON_WRITE,true",
    "COPY_ON_WRITE,false",
    "MERGE_ON_READ,true",
    "MERGE_ON_READ,false"
  ))
  def testDeleteColumn(tableType: String, schemaValidationEnabled : Boolean): Unit = {
    // test to delete a column
    val tempRecordPath = basePath + "/record_tbl/"
    val _spark = spark
    import _spark.implicits._
    val isCow = tableType.equals(HoodieTableType.COPY_ON_WRITE.name())

    val df1 = Seq((1, 100, "aaa")).toDF("id", "userid", "name")
    val df2 = Seq((2, "bbb")).toDF("id", "name")

    // convert int to string first before conversion to binary
    val initDF = df1
    initDF.printSchema()
    initDF.show(false)

    // recreate table
    initialiseTable(initDF, tempRecordPath, isCow)

    // perform avro supported operation of deleting a column
    val upsertDf = df2
    upsertDf.printSchema()
    upsertDf.show(false)

    // upsert
    assertThrows(classOf[SchemaCompatibilityException]) {
      upsertData(upsertDf, tempRecordPath, isCow, enableSchemaValidation = schemaValidationEnabled)
    }

    upsertData(upsertDf, tempRecordPath, isCow, shouldAllowDroppedColumns = true, enableSchemaValidation = schemaValidationEnabled)

    // read out the table
    val readDf = spark.read.format("hudi").load(tempRecordPath)
    readDf.printSchema()
    readDf.show(false)
    readDf.foreach(_ => {})
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testColumnPositionChange(isCow: Boolean): Unit = {
    // test to change column positions
    val tempRecordPath = basePath + "/record_tbl/"
    val _spark = spark
    import _spark.implicits._

    val df1 = Seq((1, 100, "col1", "aaa")).toDF("id", "userid", "newcol", "name")
    val df2 = Seq((2, "col2", 200, "bbb")).toDF("id", "newcol", "userid", "name")

    // convert int to string first before conversion to binary
    val initDF = df1
    initDF.printSchema()
    initDF.show(false)

    // recreate table
    initialiseTable(initDF, tempRecordPath, isCow)

    // perform avro supported operation of deleting a column
    val upsertDf = df2
    upsertDf.printSchema()
    upsertDf.show(false)

    // upsert
    upsertData(upsertDf, tempRecordPath, isCow)

    // read out the table
    val readDf = spark.read.format("hudi").load(tempRecordPath)
    readDf.printSchema()
    readDf.show(false)
    readDf.foreach(_ => {})
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testArrayOfStructsAddNewColumn(isCow: Boolean): Unit = {
    // test to add a field to a STRUCT in a column of ARRAY< STRUCT<..> > type

    // there is a bug on Spark3 that will prevent Array[Map/Struct] schema evolved tables form being read
    // bug fix: https://github.com/apache/spark/commit/32a393395ee43b573ae75afba591b587ca51879b
    // bug fix is only available Spark >= v3.1.3
    if (HoodieSparkUtils.isSpark2 || (HoodieSparkUtils.isSpark3 && HoodieSparkUtils.gteqSpark3_1_3)) {
      val tempRecordPath = basePath + "/record_tbl/"
      val arrayStructData = Seq(
        Row(1, 100, List(Row("Java", "XX", 120), Row("Scala", "XA", 300)), "aaa")
      )
      val arrayStructSchema = new StructType()
        .add("id", IntegerType)
        .add("userid", IntegerType)
        .add("language", ArrayType(new StructType()
          .add("name", StringType)
          .add("author", StringType)
          .add("pages", IntegerType)))
        .add("name", StringType)
      val df1 = spark.createDataFrame(spark.sparkContext.parallelize(arrayStructData), arrayStructSchema)
      df1.printSchema()
      df1.show(false)

      // recreate table
      initialiseTable(df1, tempRecordPath, isCow)

      // add a column to array of struct
      val newArrayStructData = Seq(
        Row(2, 200, List(Row("JavaV2", "XXX", 130, 20), Row("ScalaV2", "XXA", 310, 40)), "bbb")
      )
      val newArrayStructSchema = new StructType()
        .add("id", IntegerType)
        .add("userid", IntegerType)
        .add("language", ArrayType(new StructType()
          .add("name", StringType)
          .add("author", StringType)
          .add("pages", IntegerType)
          .add("progress", IntegerType)
        ))
        .add("name", StringType)
      val df2 = spark.createDataFrame(spark.sparkContext.parallelize(newArrayStructData), newArrayStructSchema)
      df2.printSchema()
      df2.show(false)
      // upsert
      upsertData(df2, tempRecordPath, isCow)

      // read out the table
      val readDf = spark.read.format("hudi").load(tempRecordPath)
      readDf.printSchema()
      readDf.show(false)
      readDf.foreach(_ => {})
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testArrayOfStructsChangeColumnType(isCow: Boolean): Unit = {
    // test to change the type of a field from a STRUCT in a column of ARRAY< STRUCT<..> > type
    val tempRecordPath = basePath + "/record_tbl/"
    val arrayStructData = Seq(
      Row(1, 100, List(Row("Java", "XX", 120), Row("Scala", "XA", 300)), "aaa")
    )
    val arrayStructSchema = new StructType()
      .add("id", IntegerType)
      .add("userid", IntegerType)
      .add("language", ArrayType(new StructType()
        .add("name", StringType)
        .add("author", StringType)
        .add("pages", IntegerType)))
      .add("name", StringType)
    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(arrayStructData), arrayStructSchema)
    df1.printSchema()
    df1.show(false)

    // recreate table
    initialiseTable(df1, tempRecordPath, isCow)

    // add a column to array of struct
    val newArrayStructData = Seq(
      Row(2, 200, List(Row("XXX", "JavaV2", 130L), Row("XXA", "ScalaV2", 310L)), "bbb")
    )
    val newArrayStructSchema = new StructType()
      .add("id", IntegerType)
      .add("userid", IntegerType)
      .add("language", ArrayType(new StructType()
        .add("author", StringType)
        .add("name", StringType)
        .add("pages", LongType)))
      .add("name", StringType)
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(newArrayStructData), newArrayStructSchema)
    df2.printSchema()
    df2.show(false)
    // upsert
    upsertData(df2, tempRecordPath, isCow)

    withSQLConf("spark.sql.parquet.enableNestedColumnVectorizedReader" -> "false") {
      // read out the table
      val readDf = spark.read.format("hudi").load(tempRecordPath)
      readDf.printSchema()
      readDf.show(false)
      readDf.foreach(_ => {})
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testArrayOfStructsChangeColumnPosition(isCow: Boolean): Unit = {
    // test to change the position of a field from a STRUCT in a column of ARRAY< STRUCT<..> > type
    val tempRecordPath = basePath + "/record_tbl/"
    val arrayStructData = Seq(
      Row(1, 100, List(Row("Java", "XX", 120), Row("Scala", "XA", 300)), "aaa")
    )
    val arrayStructSchema = new StructType()
      .add("id", IntegerType)
      .add("userid", IntegerType)
      .add("language", ArrayType(new StructType()
        .add("name", StringType)
        .add("author", StringType)
        .add("pages", IntegerType)))
      .add("name", StringType)
    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(arrayStructData), arrayStructSchema)
    df1.printSchema()
    df1.show(false)

    // recreate table
    initialiseTable(df1, tempRecordPath, isCow)

    // add a column to array of struct
    val newArrayStructData = Seq(
      Row(2, 200, List(Row(130, "JavaV2", "XXX"), Row(310, "ScalaV2", "XXA")), "bbb")
    )
    val newArrayStructSchema = new StructType()
      .add("id", IntegerType)
      .add("userid", IntegerType)
      .add("language", ArrayType(new StructType()
        .add("pages", IntegerType)
        .add("name", StringType)
        .add("author", StringType)))
      .add("name", StringType)
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(newArrayStructData), newArrayStructSchema)
    df2.printSchema()
    df2.show(false)
    // upsert
    upsertData(df2, tempRecordPath, isCow)

    // read out the table
    val readDf = spark.read.format("hudi").load(tempRecordPath)
    readDf.printSchema()
    readDf.show(false)
    readDf.foreach(_ => {})
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testArrayOfMapsChangeValueType(isCow: Boolean): Unit = {
    // test to change the value type of a MAP in a column of ARRAY< MAP<k,v> > type
    val tempRecordPath = basePath + "/record_tbl/"
    val arrayMapData = Seq(
      Row(1, 100, List(Map("2022-12-01" -> 120), Map("2022-12-02" -> 130)), "aaa")
    )
    val arrayMapSchema = new StructType()
      .add("id", IntegerType)
      .add("userid", IntegerType)
      .add("salesMap", ArrayType(
        new MapType(StringType, IntegerType, true)))
      .add("name", StringType)
    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(arrayMapData), arrayMapSchema)
    df1.printSchema()
    df1.show(false)

    // recreate table
    initialiseTable(df1, tempRecordPath, isCow)

    // change value type from integer to long
    val newArrayMapData = Seq(
      Row(2, 200, List(Map("2022-12-01" -> 220L), Map("2022-12-02" -> 230L)), "bbb")
    )
    val newArrayMapSchema = new StructType()
      .add("id", IntegerType)
      .add("userid", IntegerType)
      .add("salesMap", ArrayType(
        new MapType(StringType, LongType, true)))
      .add("name", StringType)
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(newArrayMapData), newArrayMapSchema)
    df2.printSchema()
    df2.show(false)
    // upsert
    upsertData(df2, tempRecordPath, isCow)

    withSQLConf("spark.sql.parquet.enableNestedColumnVectorizedReader" -> "false") {
      // read out the table
      val readDf = spark.read.format("hudi").load(tempRecordPath)
      readDf.printSchema()
      readDf.show(false)
      readDf.foreach(_ => {})
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testArrayOfMapsStructChangeFieldType(isCow: Boolean): Unit = {
    // test to change a field type of a STRUCT in a column of ARRAY< MAP< k,STRUCT<..> > > type
    val tempRecordPath = basePath + "/record_tbl/"
    val arrayMapData = Seq(
      Row(1, 100,
        List(
          Map("2022-12-01" -> Row("a1", "b1", 20)),
          Map("2022-12-02" -> Row("a2", "b2", 30))
        ),
        "aaa")
    )
    val innerStructSchema = new StructType()
      .add("col1", StringType)
      .add("col2", StringType)
      .add("col3", IntegerType)
    val arrayMapSchema = new StructType()
      .add("id", IntegerType)
      .add("userid", IntegerType)
      .add("structcol", ArrayType(
        new MapType(StringType, innerStructSchema, true)))
      .add("name", StringType)
    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(arrayMapData), arrayMapSchema)
    df1.printSchema()
    df1.show(false)

    // recreate table
    initialiseTable(df1, tempRecordPath, isCow)

    // change inner struct's type from integer to long
    val newArrayMapData = Seq(
      Row(2, 200,
        List(
          Map("2022-12-03" -> Row("a3", "b3", 40L)),
          Map("2022-12-04" -> Row("a4", "b4", 50L))
        ),
        "bbb")
    )
    val newInnerStructSchema = new StructType()
      .add("col1", StringType)
      .add("col2", StringType)
      .add("col3", LongType)
    val newArrayMapSchema = new StructType()
      .add("id", IntegerType)
      .add("userid", IntegerType)
      .add("structcol", ArrayType(
        new MapType(StringType, newInnerStructSchema, true)))
      .add("name", StringType)
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(newArrayMapData), newArrayMapSchema)
    df2.printSchema()
    df2.show(false)
    // upsert
    upsertData(df2, tempRecordPath, isCow)

    withSQLConf("spark.sql.parquet.enableNestedColumnVectorizedReader" -> "false") {
      // read out the table
      val readDf = spark.read.format("hudi").load(tempRecordPath)
      readDf.printSchema()
      readDf.show(false)
      readDf.foreach(_ => {})
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testArrayOfMapsStructAddField(isCow: Boolean): Unit = {
    // test to add a field to a STRUCT in a column of ARRAY< MAP< k,STRUCT<..> > > type

    // there is a bug on Spark3 that will prevent Array[Map/Struct] schema evolved tables form being read
    // bug fix: https://github.com/apache/spark/commit/32a393395ee43b573ae75afba591b587ca51879b
    // bug fix is only available Spark >= v3.1.3
    if (HoodieSparkUtils.isSpark2 || (HoodieSparkUtils.isSpark3 && HoodieSparkUtils.gteqSpark3_1_3)) {
      val tempRecordPath = basePath + "/record_tbl/"
      val arrayMapData = Seq(
        Row(1, 100,
          List(
            Map("2022-12-01" -> Row("a1", "b1", 20)),
            Map("2022-12-02" -> Row("a2", "b2", 30))
          ),
          "aaa")
      )
      val innerStructSchema = new StructType()
        .add("col1", StringType)
        .add("col2", StringType)
        .add("col3", IntegerType)
      val arrayMapSchema = new StructType()
        .add("id", IntegerType)
        .add("userid", IntegerType)
        .add("structcol", ArrayType(
          new MapType(StringType, innerStructSchema, true)))
        .add("name", StringType)
      val df1 = spark.createDataFrame(spark.sparkContext.parallelize(arrayMapData), arrayMapSchema)
      df1.printSchema()
      df1.show(false)

      // recreate table
      initialiseTable(df1, tempRecordPath, isCow)

      // add a new column
      val newArrayMapData = Seq(
        Row(2, 200,
          List(
            Map("2022-12-01" -> Row("a3", "b3", 20, 40)),
            Map("2022-12-02" -> Row("a4", "b4", 30, 40))
          ),
          "bbb")
      )
      val newInnerStructSchema = new StructType()
        .add("col1", StringType)
        .add("col2", StringType)
        .add("col3", IntegerType)
        .add("col4", IntegerType)
      val newArrayMapSchema = new StructType()
        .add("id", IntegerType)
        .add("userid", IntegerType)
        .add("structcol", ArrayType(
          new MapType(StringType, newInnerStructSchema, true)))
        .add("name", StringType)
      val df2 = spark.createDataFrame(spark.sparkContext.parallelize(newArrayMapData), newArrayMapSchema)
      df2.printSchema()
      df2.show(false)
      // upsert
      upsertData(df2, tempRecordPath, isCow)

      // read out the table
      val readDf = spark.read.format("hudi").load(tempRecordPath)
      readDf.printSchema()
      readDf.show(false)
      readDf.foreach(_ => {})
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testArrayOfMapsStructChangeFieldPosition(isCow: Boolean): Unit = {
    // test to change the position of fields of a STRUCT in a column of ARRAY< MAP< k,STRUCT<..> > > type
    val tempRecordPath = basePath + "/record_tbl/"
    val arrayMapData = Seq(
      Row(1, 100,
        List(
          Map("2022-12-01" -> Row("a1", "b1", 20)),
          Map("2022-12-02" -> Row("a2", "b2", 30))
        ),
        "aaa")
    )
    val innerStructSchema = new StructType()
      .add("col1", StringType)
      .add("col2", StringType)
      .add("col3", IntegerType)
    val arrayMapSchema = new StructType()
      .add("id", IntegerType)
      .add("userid", IntegerType)
      .add("structcol", ArrayType(
        new MapType(StringType, innerStructSchema, true)))
      .add("name", StringType)
    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(arrayMapData), arrayMapSchema)
    df1.printSchema()
    df1.show(false)

    // recreate table
    initialiseTable(df1, tempRecordPath, isCow)

    // change column position
    val newArrayMapData = Seq(
      Row(2, 200,
        List(
          Map("2022-12-01" -> Row("a3", 40, "b3")),
          Map("2022-12-02" -> Row("a4", 50, "b4"))
        ),
        "bbb")
    )
    val newInnerStructSchema = new StructType()
      .add("col1", StringType)
      .add("col3", IntegerType)
      .add("col2", StringType)
    val newArrayMapSchema = new StructType()
      .add("id", IntegerType)
      .add("userid", IntegerType)
      .add("structcol", ArrayType(
        new MapType(StringType, newInnerStructSchema, true)))
      .add("name", StringType)
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(newArrayMapData), newArrayMapSchema)
    df2.printSchema()
    df2.show(false)
    // upsert
    upsertData(df2, tempRecordPath, isCow)

    // read out the table
    val readDf = spark.read.format("hudi").load(tempRecordPath)
    readDf.printSchema()
    readDf.show(false)
    readDf.foreach(_ => {})
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testArrayOfMapsStructDeleteField(isCow: Boolean): Unit = {
    // test to delete a field of a STRUCT in a column of ARRAY< MAP< k,STRUCT<..> > > type

    val tempRecordPath = basePath + "/record_tbl/"
    val arrayMapData = Seq(
      Row(1, 100,
        List(
          Map("2022-12-01" -> Row("a1", "b1", 20)),
          Map("2022-12-02" -> Row("a2", "b2", 30))
        ),
        "aaa")
    )
    val innerStructSchema = new StructType()
      .add("col1", StringType)
      .add("col2", StringType)
      .add("col3", IntegerType)
    val arrayMapSchema = new StructType()
      .add("id", IntegerType)
      .add("userid", IntegerType)
      .add("structcol", ArrayType(
        new MapType(StringType, innerStructSchema, true)))
      .add("name", StringType)
    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(arrayMapData), arrayMapSchema)
    df1.printSchema()
    df1.show(false)

    // recreate table
    initialiseTable(df1, tempRecordPath, isCow)

    // change column position
    val newArrayMapData = Seq(
      Row(2, 200,
        List(
          Map("2022-12-01" -> Row("a3", 40)),
          Map("2022-12-02" -> Row("a4", 50))
        ),
        "bbb")
    )
    val newInnerStructSchema = new StructType()
      .add("col1", StringType)
      .add("col3", IntegerType)
    val newArrayMapSchema = new StructType()
      .add("id", IntegerType)
      .add("userid", IntegerType)
      .add("structcol", ArrayType(
        new MapType(StringType, newInnerStructSchema, true)))
      .add("name", StringType)
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(newArrayMapData), newArrayMapSchema)
    df2.printSchema()
    df2.show(false)
    // upsert
    upsertData(df2, tempRecordPath, isCow, true)

    // read out the table
    val readDf = spark.read.format("hudi").load(tempRecordPath)
    readDf.printSchema()
    readDf.show(false)
    readDf.foreach(_ => {})
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testComplexOperationsOnTable(isCow: Boolean): Unit = {
    // test a series of changes on a Hudi table

    var defaultPartitionIdx = 0

    def newPartition: String = {
      defaultPartitionIdx = defaultPartitionIdx + 1
      "aaa" + defaultPartitionIdx
    }

    val tempRecordPath = basePath + "/record_tbl/"
    val _spark = spark
    import _spark.implicits._

    // 1. Initialise table
    val df1 = Seq((1, 100, newPartition)).toDF("id", "userid", "name")
    df1.printSchema()
    df1.show(false)
    initialiseTable(df1, tempRecordPath, isCow)

    // 2. Promote INT type to LONG into a different partition
    val df2 = Seq((2, 200L, newPartition)).toDF("id", "userid", "name")
    df2.printSchema()
    df2.show(false)
    upsertData(df2, tempRecordPath, isCow)

    // 3. Promote LONG to FLOAT
    var df3 = Seq((3, 300, newPartition)).toDF("id", "userid", "name")
    df3 = df3.withColumn("userid", df3.col("userid").cast("float"))
    df3.printSchema()
    df3.show(false)
    upsertData(df3, tempRecordPath)

    // 4. Promote FLOAT to DOUBLE
    var df4 = Seq((4, 400, newPartition)).toDF("id", "userid", "name")
    df4 = df4.withColumn("userid", df4.col("userid").cast("float"))
    df4.printSchema()
    df4.show(false)
    upsertData(df4, tempRecordPath)

    // 5. Add two new column
    var df5 = Seq((5, 500, "newcol1", "newcol2", newPartition)).toDF("id", "userid", "newcol1", "newcol2", "name")
    df5 = df5.withColumn("userid", df5.col("userid").cast("float"))
    df5.printSchema()
    df5.show(false)
    upsertData(df5, tempRecordPath)

    // 6. Delete a column
    var df6 = Seq((6, 600, "newcol1", newPartition)).toDF("id", "userid", "newcol1", "name")
    df6 = df6.withColumn("userid", df6.col("userid").cast("float"))
    df6.printSchema()
    df6.show(false)
    assertThrows(classOf[SchemaCompatibilityException]) {
      upsertData(df6, tempRecordPath)
    }
    upsertData(df6, tempRecordPath, shouldAllowDroppedColumns = true)

    // 7. Rearrange column position
    var df7 = Seq((7, "newcol1", 700, newPartition)).toDF("id", "newcol1", "userid", "name")
    df7 = df7.withColumn("userid", df7.col("userid").cast("float"))
    df7.printSchema()
    df7.show(false)
    upsertData(df7, tempRecordPath)

    // read out the table
    val readDf = spark.read.format("hudi").load(tempRecordPath)
    readDf.printSchema()
    readDf.show(false)
    readDf.foreach(_ => {})
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testNestedTypeVectorizedReadWithTypeChange(isCow: Boolean): Unit = {
    // test to change the value type of a MAP in a column of ARRAY< MAP<k,v> > type
    val tempRecordPath = basePath + "/record_tbl/"
    val arrayMapData = Seq(
      Row(1, 100, List(Map("2022-12-01" -> 120), Map("2022-12-02" -> 130)), "aaa")
    )
    val arrayMapSchema = new StructType()
      .add("id", IntegerType)
      .add("userid", IntegerType)
      .add("salesMap", ArrayType(
        new MapType(StringType, IntegerType, true)))
      .add("name", StringType)
    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(arrayMapData), arrayMapSchema)
    df1.printSchema()
    df1.show(false)

    // recreate table
    initialiseTable(df1, tempRecordPath, isCow)

    // read out the table, will not throw any exception
    readTable(tempRecordPath)

    // change value type from integer to long
    val newArrayMapData = Seq(
      Row(2, 200, List(Map("2022-12-01" -> 220L), Map("2022-12-02" -> 230L)), "bbb")
    )
    val newArrayMapSchema = new StructType()
      .add("id", IntegerType)
      .add("userid", IntegerType)
      .add("salesMap", ArrayType(
        new MapType(StringType, LongType, true)))
      .add("name", StringType)
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(newArrayMapData), newArrayMapSchema)
    df2.printSchema()
    df2.show(false)
    // upsert
    upsertData(df2, tempRecordPath, isCow)

    // after implicit type change, read the table with vectorized read enabled
    if (HoodieSparkUtils.gteqSpark3_3) {
      assertThrows(classOf[SparkException]){
        withSQLConf("spark.sql.parquet.enableNestedColumnVectorizedReader" -> "true") {
          readTable(tempRecordPath)
        }
      }
    }

    withSQLConf("spark.sql.parquet.enableNestedColumnVectorizedReader" -> "false") {
      readTable(tempRecordPath)
    }
  }


  private def readTable(path: String): Unit = {
    // read out the table
    val readDf = spark.read.format("hudi").load(path)
    readDf.printSchema()
    readDf.show(false)
    readDf.foreach(_ => {})
  }

  protected def withSQLConf[T](pairs: (String, String)*)(f: => T): T = {
    val conf = spark.sessionState.conf
    val currentValues = pairs.unzip._1.map { k =>
      if (conf.contains(k)) {
        Some(conf.getConfString(k))
      } else None
    }
    pairs.foreach { case (k, v) => conf.setConfString(k, v) }
    try f finally {
      pairs.unzip._1.zip(currentValues).foreach {
        case (key, Some(value)) => conf.setConfString(key, value)
        case (key, None) => conf.unsetConf(key)
      }
    }
  }
}
