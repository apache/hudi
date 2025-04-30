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

package org.apache.hudi.common.table.read

import org.apache.hudi.{DataSourceWriteOptions, SparkAdapterSupport, SparkFileFormatInternalRowReaderContext}
import org.apache.hudi.DataSourceWriteOptions.{OPERATION, PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_TYPE}
import org.apache.hudi.common.config.{HoodieReaderConfig, RecordMergeMode, TypedProperties}
import org.apache.hudi.common.engine.HoodieReaderContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieRecord, WriteOperationType}
import org.apache.hudi.common.model.DefaultHoodieRecordPayload.{DELETE_KEY, DELETE_MARKER}
import org.apache.hudi.common.model.HoodieRecord.DEFAULT_ORDERING_VALUE
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.table.read.TestHoodieFileGroupReaderOnSpark.getFileCount
import org.apache.hudi.common.testutils.{HoodieTestUtils, RawTripTestPayload}
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.storage.{StorageConfiguration, StoragePath}
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{HoodieSparkKryoRegistrar, SparkConf}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.parquet.SparkParquetReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}
import org.mockito.Mockito
import org.mockito.Mockito.when

import java.util

import scala.collection.JavaConverters._

/**
 * Tests {@link HoodieFileGroupReader} with {@link SparkFileFormatInternalRowReaderContext}
 * on Spark
 */
class TestHoodieFileGroupReaderOnSpark extends TestHoodieFileGroupReaderBase[InternalRow] with SparkAdapterSupport {
  var spark: SparkSession = _

  @BeforeEach
  def setup() {
    val sparkConf = new SparkConf
    sparkConf.set("spark.app.name", getClass.getName)
    sparkConf.set("spark.master", "local[8]")
    sparkConf.set("spark.default.parallelism", "4")
    sparkConf.set("spark.sql.shuffle.partitions", "4")
    sparkConf.set("spark.driver.maxResultSize", "2g")
    sparkConf.set("spark.hadoop.mapred.output.compress", "true")
    sparkConf.set("spark.hadoop.mapred.output.compression.codec", "true")
    sparkConf.set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
    sparkConf.set("spark.hadoop.mapred.output.compression.type", "BLOCK")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
    sparkConf.set("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    sparkConf.set("spark.sql.parquet.enableVectorizedReader", "false")
    HoodieSparkKryoRegistrar.register(sparkConf)
    spark = SparkSession.builder.config(sparkConf).getOrCreate
  }

  @AfterEach
  def teardown() {
    if (spark != null) {
      spark.stop()
    }
  }

  override def getStorageConf: StorageConfiguration[_] = {
    HoodieTestUtils.getDefaultStorageConf.getInline
  }

  override def getBasePath: String = {
    tempDir.toAbsolutePath.toUri.toString
  }

  override def getHoodieReaderContext(tablePath: String, avroSchema: Schema, storageConf: StorageConfiguration[_], metaClient: HoodieTableMetaClient): HoodieReaderContext[InternalRow] = {
    val reader = sparkAdapter.createParquetFileReader(vectorized = false, spark.sessionState.conf, Map.empty, storageConf.unwrapAs(classOf[Configuration]))
    new SparkFileFormatInternalRowReaderContext(reader, Seq.empty, Seq.empty, getStorageConf, metaClient.getTableConfig)
  }

  override def commitToTable(recordList: util.List[HoodieRecord[_]], operation: String, options: util.Map[String, String]): Unit = {
    val recs = RawTripTestPayload.recordsToStrings(recordList)
    val inputDF: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(recs.asScala.toList, 2))

    inputDF.write.format("hudi")
      .options(options)
      .option("hoodie.compact.inline", "false") // else fails due to compaction & deltacommit instant times being same
      .option("hoodie.datasource.write.operation", operation)
      .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
      .mode(if (operation.equalsIgnoreCase(WriteOperationType.INSERT.value())) SaveMode.Overwrite
      else SaveMode.Append)
      .save(getBasePath)
  }

  override def getCustomPayload: String = classOf[CustomPayloadForTesting].getName

  override def assertRecordsEqual(schema: Schema, expected: InternalRow, actual: InternalRow): Unit = {
    assertEquals(expected.numFields, actual.numFields)
    val expectedStruct = sparkAdapter.getAvroSchemaConverters.toSqlType(schema)._1.asInstanceOf[StructType]
    expected.toSeq(expectedStruct).zip(actual.toSeq(expectedStruct)).foreach( converted => {
      assertEquals(converted._1, converted._2)
    })
  }

  @Test
  def testGetOrderingValue(): Unit = {
    val reader = Mockito.mock(classOf[SparkParquetReader])
    val tableConfig = Mockito.mock(classOf[HoodieTableConfig])
    Mockito.when(tableConfig.populateMetaFields()).thenReturn(true)
    val sparkReaderContext = new SparkFileFormatInternalRowReaderContext(reader, Seq.empty, Seq.empty, getStorageConf, tableConfig)
    val orderingFieldName = "col2"
    val avroSchema = new Schema.Parser().parse(
      "{\"type\": \"record\",\"name\": \"test\",\"namespace\": \"org.apache.hudi\",\"fields\": ["
        + "{\"name\": \"col1\", \"type\": \"string\" },"
        + "{\"name\": \"col2\", \"type\": \"long\" },"
        + "{ \"name\": \"col3\", \"type\": [\"null\", \"string\"], \"default\": null}]}")
    val row = InternalRow("item", 1000L, "blue")
    testGetOrderingValue(sparkReaderContext, row, avroSchema, orderingFieldName, 1000L)
    testGetOrderingValue(
      sparkReaderContext, row, avroSchema, "col3", UTF8String.fromString("blue"))
    testGetOrderingValue(
      sparkReaderContext, row, avroSchema, "non_existent_col", DEFAULT_ORDERING_VALUE)
  }

  val expectedEventTimeBased: Seq[(Int, String, String, String, Double, String)] = Seq(
    (10, "5", "rider-E", "driver-E", 17.85, "i"),
    (10, "3", "rider-C", "driver-C", 33.9, "i"),
    (10, "2", "rider-B", "driver-B", 27.7, "i"),
    (20, "1", "rider-Z", "driver-Z", 27.7, "i"))
  val expectedCommitTimeBased: Seq[(Int, String, String, String, Double, String)] = Seq(
    (10, "5", "rider-E", "driver-E", 17.85, "i"),
    (10, "3", "rider-C", "driver-C", 33.9, "i"),
    (20, "1", "rider-Z", "driver-Z", 27.7, "i"))

  @ParameterizedTest
  @MethodSource(Array("customDeleteTestParams"))
  def testCustomDelete(useFgReader: String,
                       tableType: String,
                       positionUsed: String,
                       mergeMode: String): Unit = {
    val payloadClass = "org.apache.hudi.common.table.read.CustomPayloadForTesting"
    val fgReaderOpts: Map[String, String] = Map(
      HoodieWriteConfig.MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key -> "0",
      HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key -> useFgReader,
      HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS.key -> positionUsed,
      HoodieWriteConfig.RECORD_MERGE_MODE.key -> mergeMode
    )
    val deleteOpts: Map[String, String] = Map(
      DELETE_KEY -> "op", DELETE_MARKER -> "d")
    val readOpts = if (mergeMode.equals("CUSTOM")) {
      fgReaderOpts ++ deleteOpts ++ Map(
        HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key -> payloadClass)
    } else {
      fgReaderOpts ++ deleteOpts
    }
    val opts = readOpts
    val columns = Seq("ts", "key", "rider", "driver", "fare", "op")

    val data = Seq(
      (10, "1", "rider-A", "driver-A", 19.10, "i"),
      (10, "2", "rider-B", "driver-B", 27.70, "i"),
      (10, "3", "rider-C", "driver-C", 33.90, "i"),
      (10, "4", "rider-D", "driver-D", 34.15, "i"),
      (10, "5", "rider-E", "driver-E", 17.85, "i"))
    val inserts = spark.createDataFrame(data).toDF(columns: _*)
    inserts.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(PRECOMBINE_FIELD.key(), "ts").
      option(TABLE_TYPE.key(), tableType).
      option(DataSourceWriteOptions.TABLE_NAME.key(), "test_table").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      options(opts).
      mode(SaveMode.Overwrite).
      save(getBasePath)
    val metaClient = HoodieTableMetaClient
      .builder().setConf(getStorageConf).setBasePath(getBasePath).build
    assertEquals((1, 0), getFileCount(metaClient, getBasePath))

    // Delete using delete markers.
    val updateData = Seq(
      (11, "1", "rider-X", "driver-X", 19.10, "d"),
      (9, "2", "rider-Y", "driver-Y", 27.70, "d"))
    val updates = spark.createDataFrame(updateData).toDF(columns: _*)
    updates.write.format("hudi").
      option(OPERATION.key(), "upsert").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      options(opts).
      mode(SaveMode.Append).
      save(getBasePath)
    assertEquals((1, 1), getFileCount(metaClient, getBasePath))

    // Delete from operation.
    val deletesData = Seq((-5, "4", "rider-D", "driver-D", 34.15, 6))
    val deletes = spark.createDataFrame(deletesData).toDF(columns: _*)
    deletes.write.format("hudi").
      option(OPERATION.key(), "DELETE").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      options(opts).
      mode(SaveMode.Append).
      save(getBasePath)
    assertEquals((1, 2), getFileCount(metaClient, getBasePath))

    // Add a record back to test ensure event time ordering work.
    val updateDataSecond = Seq(
      (20, "1", "rider-Z", "driver-Z", 27.70, "i"))
    val updatesSecond = spark.createDataFrame(updateDataSecond).toDF(columns: _*)
    updatesSecond.write.format("hudi").
      option(OPERATION.key(), "upsert").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      options(opts).
      mode(SaveMode.Append).
      save(getBasePath)
    // Validate data file number.
    assertEquals((1, 3), getFileCount(metaClient, getBasePath))

    // Validate in the end.
    val columnsToCompare = Set("ts", "key", "rider", "driver", "fare", "op")
    val df = spark.read.options(readOpts).format("hudi").load(getBasePath)
    val finalDf = df.select("ts", "key", "rider", "driver", "fare", "op").sort("key")
    val expected = if (mergeMode != RecordMergeMode.COMMIT_TIME_ORDERING.name()) {
      expectedEventTimeBased
    } else {
      expectedCommitTimeBased
    }
    val expectedDf = spark.createDataFrame(expected).toDF(columns: _*).sort("key")
    assertTrue(
      SparkClientFunctionalTestHarness.areDataframesEqual(expectedDf, finalDf, columnsToCompare.asJava))
  }

  private def testGetOrderingValue(sparkReaderContext: HoodieReaderContext[InternalRow],
                                   row: InternalRow,
                                   avroSchema: Schema,
                                   orderingColumn: String,
                                   expectedOrderingValue: Comparable[_]): Unit = {
    assertEquals(expectedOrderingValue, sparkReaderContext.getOrderingValue(
      row, avroSchema, HOption.of(orderingColumn)))
  }

  @Test
  def getRecordKeyFromMetadataFields(): Unit = {
    val reader = Mockito.mock(classOf[SparkParquetReader])
    val tableConfig = Mockito.mock(classOf[HoodieTableConfig])
    val storageConf = Mockito.mock(classOf[StorageConfiguration[_]])
    when(tableConfig.populateMetaFields()).thenReturn(true)
    val sparkReaderContext = new SparkFileFormatInternalRowReaderContext(reader, Seq.empty, Seq.empty, storageConf, tableConfig)
    val schema = SchemaBuilder.builder()
      .record("test")
      .fields()
      .requiredString(HoodieRecord.RECORD_KEY_METADATA_FIELD)
      .optionalString("field2")
      .endRecord()
    val key = "my_key"
    val row = InternalRow.fromSeq(Seq(UTF8String.fromString(key), UTF8String.fromString("value2")))
    assertEquals(key, sparkReaderContext.getRecordKey(row, schema))
  }

  @Test
  def getRecordKeySingleKey(): Unit = {
    val reader = Mockito.mock(classOf[SparkParquetReader])
    val tableConfig = Mockito.mock(classOf[HoodieTableConfig])
    when(tableConfig.populateMetaFields()).thenReturn(false)
    when(tableConfig.getRecordKeyFields).thenReturn(HOption.of(Array("field1")))
    val storageConf = Mockito.mock(classOf[StorageConfiguration[_]])
    val props = new TypedProperties
    props.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "field1,field2")
    when(tableConfig.getProps).thenReturn(props)
    val sparkReaderContext = new SparkFileFormatInternalRowReaderContext(reader, Seq.empty, Seq.empty, storageConf, tableConfig)
    val schema = SchemaBuilder.builder()
      .record("test")
      .fields()
      .requiredString("field1")
      .optionalString("field2")
      .endRecord()
    val key = "key"
    val row = InternalRow.fromSeq(Seq(UTF8String.fromString(key), UTF8String.fromString("other")))
    assertEquals(key, sparkReaderContext.getRecordKey(row, schema))
  }

  @Test
  def getRecordKeyWithMultipleKeys(): Unit = {
    val reader = Mockito.mock(classOf[SparkParquetReader])
    val tableConfig = Mockito.mock(classOf[HoodieTableConfig])
    when(tableConfig.populateMetaFields()).thenReturn(false)
    when(tableConfig.getRecordKeyFields).thenReturn(HOption.of(Array("outer1.field1", "outer1.field2", "outer1.field3")))
    val storageConf = Mockito.mock(classOf[StorageConfiguration[_]])
    val sparkReaderContext = new SparkFileFormatInternalRowReaderContext(reader, Seq.empty, Seq.empty, storageConf, tableConfig)
    val schema: _root_.org.apache.avro.Schema = buildMultiLevelSchema
    val key = "outer1.field1:compound,outer1.field2:__empty__,outer1.field3:__null__"
    val innerRow = InternalRow.fromSeq(Seq(UTF8String.fromString("compound"), UTF8String.fromString(""), null))
    val row = InternalRow.fromSeq(Seq(innerRow, UTF8String.fromString("value2")))
    assertEquals(key, sparkReaderContext.getRecordKey(row, schema))
  }

  @Test
  def getNestedValue(): Unit = {
    val reader = Mockito.mock(classOf[SparkParquetReader])
    val tableConfig = Mockito.mock(classOf[HoodieTableConfig])
    when(tableConfig.populateMetaFields()).thenReturn(true)
    val storageConf = Mockito.mock(classOf[StorageConfiguration[_]])
    val sparkReaderContext = new SparkFileFormatInternalRowReaderContext(reader, Seq.empty, Seq.empty, storageConf, tableConfig)
    val schema: Schema = buildMultiLevelSchema
    val innerRow = InternalRow.fromSeq(Seq(UTF8String.fromString("nested_value"), UTF8String.fromString(""), null))
    val row = InternalRow.fromSeq(Seq(innerRow, UTF8String.fromString("value2")))
    assertEquals("nested_value", sparkReaderContext.getValue(row, schema, "outer1.field1").toString)
  }

  private def buildMultiLevelSchema = {
    val innerSchema = SchemaBuilder.builder()
      .record("inner")
      .fields()
      .requiredString("field1")
      .optionalString("field2")
      .optionalString("field3")
      .endRecord()
    val schema = Schema.createRecord("outer", null, null, false);
    schema.setFields(util.Arrays.asList(
      new Schema.Field("outer1", innerSchema, null, null),
      new Schema.Field("outer2", Schema.create(Schema.Type.STRING), null, null)
    ))
    schema
  }
}

object TestHoodieFileGroupReaderOnSpark {
  def customDeleteTestParams(): java.util.List[Arguments] = {
    java.util.Arrays.asList(
      Arguments.of("true", "MERGE_ON_READ", "false", "EVENT_TIME_ORDERING"),
      Arguments.of("true", "MERGE_ON_READ", "true", "EVENT_TIME_ORDERING"),
      Arguments.of("true", "MERGE_ON_READ", "false", "COMMIT_TIME_ORDERING"),
      Arguments.of("true", "MERGE_ON_READ", "true", "COMMIT_TIME_ORDERING"),
      Arguments.of("true", "MERGE_ON_READ", "false", "CUSTOM"),
      Arguments.of("true", "MERGE_ON_READ", "true", "CUSTOM"))
  }

  def getFileCount(metaClient: HoodieTableMetaClient, basePath: String): (Long, Long) = {
    val newMetaClient = HoodieTableMetaClient.reload(metaClient)
    val files = newMetaClient.getStorage.listFiles(new StoragePath(basePath))
    (files.stream().filter(f =>
      f.getPath.getParent.equals(new StoragePath(basePath))
        && FSUtils.isBaseFile(f.getPath)).count(),
      files.stream().filter(f =>
        f.getPath.getParent.equals(new StoragePath(basePath))
          && FSUtils.isLogFile(f.getPath)).count())
  }
}
