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

import org.apache.hudi.{AvroConversionUtils, DataSourceUtils, DataSourceWriteOptions, HoodieWriterUtils, SparkAdapterSupport, SparkFileFormatInternalRowReaderContext}
import org.apache.hudi.DataSourceWriteOptions.{OPERATION, PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_TYPE}
import org.apache.hudi.avro.AvroSchemaUtils.getAvroRecordQualifiedName
import org.apache.hudi.client.utils.SparkInternalSchemaConverter
import org.apache.hudi.common.config.{HoodieReaderConfig, RecordMergeMode, TypedProperties}
import org.apache.hudi.common.engine.HoodieReaderContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieCommitMetadata, HoodieFailedWritesCleaningPolicy, HoodieRecord, WriteOperationType}
import org.apache.hudi.common.model.DefaultHoodieRecordPayload.{DELETE_KEY, DELETE_MARKER}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.table.read.TestHoodieFileGroupReaderOnSpark.getFileCount
import org.apache.hudi.common.table.timeline.HoodieInstant.State
import org.apache.hudi.common.testutils.{HoodieTestUtils, SchemaOnReadEvolutionTestUtils}
import org.apache.hudi.common.testutils.SchemaOnWriteEvolutionTestUtils.SchemaOnWriteConfigs
import org.apache.hudi.common.util.{CollectionUtils, CommitUtils, Option => HOption, OrderingValues}
import org.apache.hudi.config.{HoodieArchivalConfig, HoodieCleanConfig, HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter
import org.apache.hudi.internal.schema.io.FileBasedInternalSchemaStorageManager
import org.apache.hudi.internal.schema.utils.SerDeHelper
import org.apache.hudi.storage.{StorageConfiguration, StoragePath}
import org.apache.hudi.table.HoodieSparkTable
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{HoodieSparkKryoRegistrar, SparkConf}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{Dataset, HoodieInternalRowUtils, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.execution.datasources.SparkColumnarFileReader
import org.apache.spark.sql.hudi.MultipleColumnarFileFormatReader
import org.apache.spark.sql.internal.SQLConf.LEGACY_RESPECT_NULLABILITY_IN_TEXT_DATASET_CONVERSION
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}
import org.mockito.Mockito
import org.mockito.Mockito.when

import java.util
import java.util.Collections

import scala.collection.JavaConverters._

/**
 * Tests {@link HoodieFileGroupReader} with {@link SparkFileFormatInternalRowReaderContext}
 * on Spark
 */
class TestHoodieFileGroupReaderOnSpark extends TestHoodieFileGroupReaderBase[InternalRow] with SparkAdapterSupport {
  var spark: SparkSession = _

  private var internalStorageConf: StorageConfiguration[Configuration] = _

  @BeforeEach
  def setup() {
    internalStorageConf = HoodieTestUtils.getDefaultStorageConf.getInline
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
    sparkConf.set("spark.sql.orc.enableVectorizedReader", "false")
    sparkConf.set(LEGACY_RESPECT_NULLABILITY_IN_TEXT_DATASET_CONVERSION.key, "true")
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
    internalStorageConf
  }

  override def getBasePath: String = {
    tempDir.toAbsolutePath.toUri.toString
  }

  override def getHoodieReaderContext(tablePath: String, avroSchema: Schema, storageConf: StorageConfiguration[_], metaClient: HoodieTableMetaClient): HoodieReaderContext[InternalRow] = {
    val parquetReader = sparkAdapter.createParquetFileReader(vectorized = false, spark.sessionState.conf, Map.empty, storageConf.unwrapAs(classOf[Configuration]))
    val dataSchema = AvroConversionUtils.convertAvroSchemaToStructType(avroSchema);
    val orcReader = sparkAdapter.createOrcFileReader(vectorized = false, spark.sessionState.conf, Map.empty, storageConf.unwrapAs(classOf[Configuration]), dataSchema)
    val multiFormatReader = new MultipleColumnarFileFormatReader(parquetReader, orcReader)
    new SparkFileFormatInternalRowReaderContext(multiFormatReader, Seq.empty, Seq.empty, getStorageConf, metaClient.getTableConfig)
  }

  override def commitToTable(recordList: util.List[HoodieRecord[_]],
                             operation: String,
                             firstCommit: Boolean,
                             options: util.Map[String, String],
                             schemaStr: String): Unit = {
    val schema = new Schema.Parser().parse(schemaStr)
    val genericRecords = spark.sparkContext.parallelize(recordList.asScala.map(_.toIndexedRecord(schema, CollectionUtils.emptyProps))
      .filter(r => r.isPresent).map(r => r.get.getData.asInstanceOf[GenericRecord]).toSeq, 2)
    val inputDF: Dataset[Row] = AvroConversionUtils.createDataFrame(genericRecords, schemaStr, spark);

    inputDF.write.format("hudi")
      .options(options)
      .option("hoodie.compact.inline", "false") // else fails due to compaction & deltacommit instant times being same
      .option("hoodie.datasource.write.operation", operation)
      .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
      .mode(if (firstCommit) SaveMode.Overwrite else SaveMode.Append)
      .save(getBasePath)
  }

  override def getCustomPayload: String = classOf[CustomPayloadForTesting].getName

  override def assertRecordsEqual(schema: Schema, expected: InternalRow, actual: InternalRow): Unit = {
    assertEquals(expected.numFields, actual.numFields)
    val expectedStruct = sparkAdapter.getAvroSchemaConverters.toSqlType(schema)._1.asInstanceOf[StructType]

    expected.toSeq(expectedStruct).zip(actual.toSeq(expectedStruct)).zipWithIndex.foreach {
      case ((v1, v2), i) =>
        val fieldType = expectedStruct(i).dataType

        (v1, v2, fieldType) match {
          case (a1: Array[Byte], a2: Array[Byte], _) =>
            assert(java.util.Arrays.equals(a1, a2), s"Mismatch at field $i: expected ${a1.mkString(",")} but got ${a2.mkString(",")}")

          case (m1: MapData, m2: MapData, MapType(keyType, valueType, _)) =>
            val map1 = mapDataToScalaMap(m1, keyType, valueType)
            val map2 = mapDataToScalaMap(m2, keyType, valueType)
            assertEquals(map1, map2, s"Mismatch at field $i: maps not equal")

          case _ =>
            assertEquals(v1, v2, s"Mismatch at field $i")
        }
    }
  }

  def mapDataToScalaMap(mapData: MapData, keyType: DataType, valueType: DataType): Map[Any, Any] = {
    val keys = mapData.keyArray()
    val values = mapData.valueArray()
    (0 until mapData.numElements()).map { i =>
      val k = extractValue(keys, i, keyType)
      val v = extractValue(values, i, valueType)
      k -> v
    }.toMap
  }

  def extractValue(array: ArrayData, index: Int, dt: DataType): Any = dt match {
    case IntegerType => array.getInt(index)
    case LongType    => array.getLong(index)
    case StringType  => array.getUTF8String(index).toString
    case DoubleType  => array.getDouble(index)
    case FloatType   => array.getFloat(index)
    case BooleanType => array.getBoolean(index)
    case BinaryType  => array.getBinary(index)
    // Extend this to support StructType, ArrayType, etc. if needed
    case other       => throw new UnsupportedOperationException(s"Unsupported type: $other")
  }

  @Test
  def testGetOrderingValue(): Unit = {
    val reader = Mockito.mock(classOf[SparkColumnarFileReader])
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
      sparkReaderContext, row, avroSchema, "non_existent_col", OrderingValues.getDefault)
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
    assertEquals(expectedOrderingValue, sparkReaderContext.getRecordContext.getOrderingValue(row, avroSchema, Collections.singletonList(orderingColumn)))
  }

  @Test
  def getRecordKeyFromMetadataFields(): Unit = {
    val reader = Mockito.mock(classOf[SparkColumnarFileReader])
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
    assertEquals(key, sparkReaderContext.getRecordContext().getRecordKey(row, schema))
  }

  @Test
  def getRecordKeySingleKey(): Unit = {
    val reader = Mockito.mock(classOf[SparkColumnarFileReader])
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
    assertEquals(key, sparkReaderContext.getRecordContext().getRecordKey(row, schema))
  }

  @Test
  def getRecordKeyWithMultipleKeys(): Unit = {
    val reader = Mockito.mock(classOf[SparkColumnarFileReader])
    val tableConfig = Mockito.mock(classOf[HoodieTableConfig])
    when(tableConfig.populateMetaFields()).thenReturn(false)
    when(tableConfig.getRecordKeyFields).thenReturn(HOption.of(Array("outer1.field1", "outer1.field2", "outer1.field3")))
    val storageConf = Mockito.mock(classOf[StorageConfiguration[_]])
    val sparkReaderContext = new SparkFileFormatInternalRowReaderContext(reader, Seq.empty, Seq.empty, storageConf, tableConfig)
    val schema: _root_.org.apache.avro.Schema = buildMultiLevelSchema
    val key = "outer1.field1:compound,outer1.field2:__empty__,outer1.field3:__null__"
    val innerRow = InternalRow.fromSeq(Seq(UTF8String.fromString("compound"), UTF8String.fromString(""), null))
    val row = InternalRow.fromSeq(Seq(innerRow, UTF8String.fromString("value2")))
    assertEquals(key, sparkReaderContext.getRecordContext.getRecordKey(row, schema))
  }

  @Test
  def getNestedValue(): Unit = {
    val reader = Mockito.mock(classOf[SparkColumnarFileReader])
    val tableConfig = Mockito.mock(classOf[HoodieTableConfig])
    when(tableConfig.populateMetaFields()).thenReturn(true)
    val storageConf = Mockito.mock(classOf[StorageConfiguration[_]])
    val sparkReaderContext = new SparkFileFormatInternalRowReaderContext(reader, Seq.empty, Seq.empty, storageConf, tableConfig)
    val schema: Schema = buildMultiLevelSchema
    val innerRow = InternalRow.fromSeq(Seq(UTF8String.fromString("nested_value"), UTF8String.fromString(""), null))
    val row = InternalRow.fromSeq(Seq(innerRow, UTF8String.fromString("value2")))
    assertEquals("nested_value", sparkReaderContext.getRecordContext().getValue(row, schema, "outer1.field1").toString)
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

  override def assertRecordMatchesSchema(schema: Schema, record: InternalRow): Unit = {
    val structType = HoodieInternalRowUtils.getCachedSchema(schema)
    assertRecordMatchesSchema(structType, record)
  }

  private def assertRecordMatchesSchema(structType: StructType, record: InternalRow): Unit = {
    val values = record.toSeq(structType)
    structType.zip(values).foreach { r =>
      r._1.dataType match {
        case struct: StructType => assertRecordMatchesSchema(struct, r._2.asInstanceOf[InternalRow])
        case array: ArrayType => assertArrayMatchesSchema(array.elementType, r._2.asInstanceOf[ArrayData])
        case map: MapType => asserMapMatchesSchema(map, r._2.asInstanceOf[MapData])
        case _ =>
      }
    }
  }

  private def assertArrayMatchesSchema(schema: DataType, array: ArrayData): Unit = {
    val arrayValues = array.toSeq[Any](schema)
    schema match {
      case structType: StructType =>
        arrayValues.foreach(v => assertRecordMatchesSchema(structType, v.asInstanceOf[InternalRow]))
      case arrayType: ArrayType =>
        arrayValues.foreach(v => assertArrayMatchesSchema(arrayType.elementType, v.asInstanceOf[ArrayData]))
      case mapType: MapType =>
        arrayValues.foreach(v => asserMapMatchesSchema(mapType, v.asInstanceOf[MapData]))
      case _ =>
    }
  }

  private def asserMapMatchesSchema(schema: MapType, map: MapData): Unit = {
    assertArrayMatchesSchema(schema.keyType, map.keyArray())
    assertArrayMatchesSchema(schema.valueType, map.valueArray())
  }

  override def commitSchemaToTable(schema: InternalSchema, writeConfigs: util.Map[String, String], historySchemaStr: String): Unit = {
    val tableName = writeConfigs.get(HoodieTableConfig.HOODIE_TABLE_NAME_KEY)
    val avroSchema = AvroInternalSchemaConverter.convert(schema, getAvroRecordQualifiedName(tableName))
    val jsc = new JavaSparkContext(spark.sparkContext)
    val client = DataSourceUtils.createHoodieClient(
      jsc,
      avroSchema.toString,
      getBasePath,
      tableName,
      HoodieWriterUtils.parametersWithWriteDefaults(
        writeConfigs.asScala.toMap ++ Map(
          HoodieCleanConfig.AUTO_CLEAN.key -> "false",
          HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key -> HoodieFailedWritesCleaningPolicy.NEVER.name,
          HoodieArchivalConfig.AUTO_ARCHIVE.key -> "false"
        )).asJava)

    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(getBasePath)
      .setConf(getStorageConf)
      .setTimeGeneratorConfig(client.getConfig.getTimeGeneratorConfig)
      .build()

    val commitActionType = CommitUtils.getCommitActionType(WriteOperationType.ALTER_SCHEMA, metaClient.getTableType)
    val instantTime = client.startCommit(commitActionType)
    client.setOperationType(WriteOperationType.ALTER_SCHEMA)

    val hoodieTable = HoodieSparkTable.create(client.getConfig, client.getEngineContext)
    val timeLine = hoodieTable.getActiveTimeline
    val instantGenerator = metaClient.getTimelineLayout.getInstantGenerator
    val requested = instantGenerator.createNewInstant(State.REQUESTED, commitActionType, instantTime)
    val metadata = new HoodieCommitMetadata
    metadata.setOperationType(WriteOperationType.ALTER_SCHEMA)
    timeLine.transitionRequestedToInflight(requested, HOption.of(metadata))
    val extraMeta = new util.HashMap[String, String]()
    extraMeta.put(SerDeHelper.LATEST_SCHEMA, SerDeHelper.toJson(schema.setSchemaId(instantTime.toLong)))
    val schemaManager = new FileBasedInternalSchemaStorageManager(metaClient)
    schemaManager.persistHistorySchemaStr(instantTime, SerDeHelper.inheritSchemas(schema, historySchemaStr))
    client.commit(instantTime, jsc.emptyRDD, HOption.of(extraMeta))
  }

  override def getSchemaOnWriteConfigs: SchemaOnWriteConfigs = {
    new SchemaOnWriteConfigs()
  }

  override def getSchemaOnReadConfigs: SchemaOnReadEvolutionTestUtils.SchemaOnReadConfigs = {
    val configs = new SchemaOnReadEvolutionTestUtils.SchemaOnReadConfigs()
    configs.intToDecimalBytesSupport = false
    configs.intToDecimalFixedSupport = false
    configs.longToDecimalBytesSupport = false
    configs.longToDecimalFixedSupport = false
    configs.floatToDecimalBytesSupport = false
    configs.floatToDecimalFixedSupport = false
    configs.doubleToDecimalBytesSupport = false
    configs.doubleToDecimalFixedSupport = false
    configs
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
