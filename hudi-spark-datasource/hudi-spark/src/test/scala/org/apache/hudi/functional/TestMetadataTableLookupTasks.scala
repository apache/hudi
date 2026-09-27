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

import org.apache.hudi.{ColumnStatsIndexSupport, DataSourceReadOptions, DataSourceWriteOptions, HoodieFileIndex, HoodieSchemaConversionUtils}
import org.apache.hudi.client.SparkRDDReadClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.data.{HoodieData, HoodieListData, HoodiePairData}
import org.apache.hudi.common.engine.HoodieEngineContext
import org.apache.hudi.common.metrics.Registry
import org.apache.hudi.common.model.{EmptyHoodieRecordPayload, HoodieAvroRecord, HoodieKey, HoodieRecord, HoodieRecordGlobalLocation}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.common.util.collection.Pair
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.data.{HoodieJavaPairRDD, HoodieJavaRDD}
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.index.{PartitionedRecordIndexFileGroupLookupFunction, SparkHoodieIndexFactory, SparkMetadataTableGlobalRecordLevelIndex}
import org.apache.hudi.index.HoodieIndex.IndexType
import org.apache.hudi.index.bloom.HoodieMetadataBloomFilterProbingFunction
import org.apache.hudi.metadata.{ColumnStatsIndexPrefixRawKey, HoodieBackedTableMetadata, HoodieTableMetadata, HoodieTableMetadataUtil, MetadataPartitionReader, MetadataPartitionType, RecordIndexRawKey}
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.hudi.table.{HoodieSparkTable, HoodieTable}
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness.getSparkSqlConf
import org.apache.hudi.testutils.SparkClientFunctionalTestHarnessScala

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, GreaterThan, Literal}
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.junit.jupiter.api.{BeforeEach, Tag, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

/**
 * Verifies that metadata table lookups run in Spark tasks (column stats, record level and secondary index
 * lookups on the query path, record level and bloom index tagging on the write path) return correct results
 * without shipping the table metadata to every task and without loading the table timeline or table config
 * inside a task.
 */
@Tag("functional")
class TestMetadataTableLookupTasks extends SparkClientFunctionalTestHarnessScala {

  private val numPartitions = 4
  private val recordsPerBatch = 40

  override def conf: SparkConf = conf(getSparkSqlConf)

  @BeforeEach
  override def runBeforeEach(): Unit = {
    super.runBeforeEach()
    spark.sql("set hoodie.write.lock.provider = org.apache.hudi.client.transaction.lock.InProcessLockProvider")
  }

  @Test
  def testQueryIndexLookupsDoNotLoadTimelineInTasks(): Unit = {
    val tablePath = basePath()
    writeTable(tablePath, globalRecordIndexOpts)
    createSecondaryIndex(tablePath, "name")

    Seq(
      "price > 70.0" -> GreaterThan(attribute("price", DoubleType), Literal(70.0)),
      "id = 'key0007'" -> EqualTo(attribute("id"), Literal("key0007")),
      "name = 'name0011'" -> EqualTo(attribute("name"), Literal("name0011"))
    ).foreach { case (condition, filter) =>
      assertFilePruningInTasks(tablePath, globalRecordIndexOpts, filter)
      assertQueryResult(tablePath, globalRecordIndexOpts, condition)
    }
  }

  @Test
  def testPartitionedRecordIndexLookupDoesNotLoadTimelineInTasks(): Unit = {
    val tablePath = basePath()
    writeTable(tablePath, partitionedRecordIndexOpts)
    assertFilePruningInTasks(tablePath, partitionedRecordIndexOpts, EqualTo(attribute("id"), Literal("key0007")))
    assertQueryResult(tablePath, partitionedRecordIndexOpts, "id = 'key0007'")
  }

  @Test
  def testRecordIndexTagLocationDoesNotLoadTableMetadataInTasks(): Unit = {
    val tablePath = basePath()
    writeTable(tablePath, globalRecordIndexOpts)

    val keys = (0 until 2 * recordsPerBatch by 7).map(i => f"key$i%04d") ++ Seq("missing0", "missing1")
    val restore = TaskMetaIOCountingFileSystem.install(jsc().hadoopConfiguration())
    val tagged = try {
      tagLocation(tablePath, IndexType.GLOBAL_RECORD_LEVEL_INDEX, keys)
    } finally {
      restore()
    }

    assertTrue(TaskMetaIOCountingFileSystem.taskMetadataTableFileReads.nonEmpty,
      "the record index lookup should read the metadata table inside tasks")
    assertNoTableMetadataLoadsInTasks()
    keys.foreach { key =>
      assertEquals(!key.startsWith("missing"), tagged(key), s"unexpected tagging for $key")
    }
  }

  @Test
  def testDistributedLookupsDoNotShipTableMetadata(): Unit = {
    val tablePath = basePath()
    writeTable(tablePath, globalRecordIndexOpts)
    createSecondaryIndex(tablePath, "name")
    val metaClient = newMetaClient(tablePath)
    val metadataConfig = HoodieMetadataConfig.newBuilder().fromProperties(toProps(globalRecordIndexOpts)).build()
    val metadata = new HoodieBackedTableMetadata(context(), metaClient.getStorage, metadataConfig, tablePath)
    try {
      val columnStats = metadata.getRecordsByKeyPrefixes(
        HoodieListData.eager(Seq(new ColumnStatsIndexPrefixRawKey("price")).asJava),
        HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS, false)
      assertShipsNoTableMetadata(HoodieJavaRDD.getJavaRDD(columnStats).rdd)

      val recordIndex = metadata.readRecordIndexLocationsWithKeys(HoodieListData.eager(Seq("key0001", "key0002", "key0042").asJava))
      assertShipsNoTableMetadata(HoodieJavaRDD.getJavaRDD(recordIndex.values()).rdd)

      val secondaryIndex = metadata.readSecondaryIndexDataTableRecordKeysV2(
        HoodieListData.eager(Seq("name0001", "name0002").asJava), "secondary_index_idx_name")
      assertShipsNoTableMetadata(HoodieJavaRDD.getJavaRDD(secondaryIndex).rdd)

      assertEquals(3, recordIndex.collectAsList().size())
      assertEquals(Set("key0001", "key0002"), secondaryIndex.collectAsList().asScala.toSet)
    } finally {
      metadata.close()
    }
  }

  @Test
  def testColumnStatsCandidateFileFilterIsNotShippedPerTask(): Unit = {
    val tablePath = basePath()
    writeTable(tablePath, globalRecordIndexOpts)
    val metaClient = newMetaClient(tablePath)
    val metadataConfig = HoodieMetadataConfig.newBuilder().fromProperties(toProps(globalRecordIndexOpts)).build()
    val tableSchema = new TableSchemaResolver(metaClient).getTableSchema
    val indexSupport = new ColumnStatsIndexSupport(spark,
      HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(tableSchema), tableSchema, metadataConfig, metaClient)
    // The table's files plus enough other names that shipping them would dominate the task payload.
    val tableFileNames = spark.read.format("hudi").load(tablePath).select("_hoodie_file_name").distinct()
      .collect().map(_.getString(0)).toSet
    val candidateFileNames = tableFileNames ++ (0 until 20000).map(i => f"$i%036d_0-0-0_20250101000000000.parquet")
    val (indexedFiles, serializedSizes) = indexSupport.loadTransposed(Seq("price"), shouldReadInMemory = false,
      prunedFileNamesOpt = Some(candidateFileNames)) { df =>
      val rdd = df.queryExecution.toRdd
      (df.select("fileName").collect().map(_.getString(0)).toSet, lineage(rdd).map(r => serializeRecordingClasses(r)._1))
    }
    assertEquals(tableFileNames, indexedFiles)
    assertTrue(serializedSizes.size > 2, s"the column stats read should run in tasks: $serializedSizes")
    assertTrue(serializedSizes.max < 200 * 1024, s"a column stats read stage ships ${serializedSizes.max} bytes per task")
  }

  @Test
  def testIndexLookupFunctionsShipOnlyBroadcastState(): Unit = {
    val tablePath = basePath()
    writeTable(tablePath, globalRecordIndexOpts)
    val writeConfig = recordIndexWriteConfig(tablePath, IndexType.GLOBAL_RECORD_LEVEL_INDEX)
    val table = HoodieSparkTable.create(writeConfig, context())
    val records = HoodieJavaRDD.of(jsc().parallelize(Seq("key0001", "key0042", "missing0").map { key =>
      new HoodieAvroRecord(new HoodieKey(key, "p0"), new EmptyHoodieRecordPayload()).asInstanceOf[HoodieRecord[EmptyHoodieRecordPayload]]
    }.asJava, 2))

    // The global record index lookup stage, including its shuffle partitioner.
    val index = new LookupExposingRecordLevelIndex(writeConfig)
    val locations = index.lookup(records, context(), table)
    lineage(HoodieJavaPairRDD.getJavaPairRDD(locations).rdd).foreach(rdd => assertShipsOnlyBroadcastState(rdd))
    assertEquals(Set("key0001", "key0042"), locations.collectAsList().asScala.map(_.getKey).toSet)

    val metadata = table.getTableMetadata
    assertShipsOnlyBroadcastState(PartitionedRecordIndexFileGroupLookupFunction.create(context(), metadata, HOption.empty[Registry]()))
    val baseFileView = jsc().broadcast(new HoodieTableFileSystemView(metadata, table.getMetaClient, table.getMetaClient.getActiveTimeline))
    val bloomFilterReader = context().broadcast(metadata.getPartitionReader(MetadataPartitionType.BLOOM_FILTERS.getPartitionPath))
    assertShipsOnlyBroadcastState(new HoodieMetadataBloomFilterProbingFunction(baseFileView, bloomFilterReader))
  }

  @Test
  def testBloomIndexWithMetadataTagsLikeRecordIndex(): Unit = {
    val tablePath = basePath()
    writeTable(tablePath, globalRecordIndexOpts)
    val keys = (0 until 2 * recordsPerBatch by 3).map(i => f"key$i%04d") ++ Seq("missing0", "missing1")
    val bloomOpts = Map(
      HoodieIndexConfig.BLOOM_INDEX_USE_METADATA.key -> "true",
      HoodieIndexConfig.BLOOM_INDEX_PRUNE_BY_RANGES.key -> "false")
    assertEquals(tagLocation(tablePath, IndexType.GLOBAL_RECORD_LEVEL_INDEX, keys),
      tagLocation(tablePath, IndexType.GLOBAL_BLOOM, keys, bloomOpts))
  }

  @Test
  def testReusedReadClientSeesLaterCommits(): Unit = {
    val tablePath = basePath()
    writeTable(tablePath, globalRecordIndexOpts)
    val readClient = new SparkRDDReadClient[EmptyHoodieRecordPayload](context(),
      recordIndexWriteConfig(tablePath, IndexType.GLOBAL_RECORD_LEVEL_INDEX))
    def newRecords(keys: String*): JavaRDD[HoodieRecord[EmptyHoodieRecordPayload]] = jsc().parallelize(keys.map { key =>
      new HoodieAvroRecord(new HoodieKey(key, "p0"), new EmptyHoodieRecordPayload()).asInstanceOf[HoodieRecord[EmptyHoodieRecordPayload]]
    }.asJava, 2)
    def notYetWritten(keys: String*): Set[String] = readClient.filterExists(newRecords(keys: _*)).collect().asScala.map(_.getRecordKey).toSet

    val laterKey = f"key${10 * recordsPerBatch}%04d"
    assertEquals(Set(laterKey), notYetWritten("key0001", laterKey))
    batchDf(10 * recordsPerBatch, 3).limit(1).write.format("hudi")
      .options(globalRecordIndexOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(tablePath)
    assertEquals(Set.empty, notYetWritten("key0001", laterKey))
  }

  @Test
  def testPartitionReaderRoundTripsThroughSparkSerializers(): Unit = {
    val tablePath = basePath()
    writeTable(tablePath, globalRecordIndexOpts)
    val metaClient = newMetaClient(tablePath)
    val metadataConfig = HoodieMetadataConfig.newBuilder().fromProperties(toProps(globalRecordIndexOpts)).build()
    val metadata = new HoodieBackedTableMetadata(context(), metaClient.getStorage, metadataConfig, tablePath)
    try {
      val keys = (0 until 2 * recordsPerBatch).map(i => f"key$i%04d") :+ "missing0"
      val recordIndexReader = metadata.getPartitionReader(MetadataPartitionType.RECORD_INDEX.getPartitionPath)
      val bloomFilterReader = metadata.getPartitionReader(MetadataPartitionType.BLOOM_FILTERS.getPartitionPath)
      val baseFiles = spark.read.format("hudi").load(tablePath).select("part", "_hoodie_file_name").distinct()
        .collect().map(row => Pair.of(row.getString(0), row.getString(1))).toSeq.asJava

      def locations(reader: MetadataPartitionReader): Map[String, String] = reader.lookupRecords(keys.map(new RecordIndexRawKey(_)).asJava).asScala
        .map(r => r.getRecordKey -> r.getData.getRecordGlobalLocation.toString).toMap
      def bloomFilters(reader: MetadataPartitionReader): Map[Pair[String, String], String] = reader.getBloomFilters(baseFiles).asScala
        .map { case (file, filter) => file -> filter.serializeToString() }.toMap
      val expectedLocations = locations(recordIndexReader)
      val expectedBloomFilters = bloomFilters(bloomFilterReader)
      assertEquals(2 * recordsPerBatch, expectedLocations.size)
      assertEquals(baseFiles.size(), expectedBloomFilters.size)

      Seq(new KryoSerializer(spark.sparkContext.getConf), new JavaSerializer(spark.sparkContext.getConf)).foreach { serializer =>
        val instance = serializer.newInstance()
        def roundTrip(reader: MetadataPartitionReader): MetadataPartitionReader =
          instance.deserialize[MetadataPartitionReader](instance.serialize(reader))
        assertEquals(expectedLocations, locations(roundTrip(recordIndexReader)))
        assertEquals(expectedBloomFilters, bloomFilters(roundTrip(bloomFilterReader)))
      }
    } finally {
      metadata.close()
    }
  }

  private def assertFilePruningInTasks(tablePath: String, opts: Map[String, String], filter: Expression): Unit = {
    val readOpts = opts ++ queryIndexOpts + ("path" -> tablePath)
    val restore = TaskMetaIOCountingFileSystem.install(jsc().hadoopConfiguration())
    val (prunedFiles, taskMetadataReads) = try {
      val fileIndex = HoodieFileIndex(spark, newMetaClient(tablePath), None, readOpts, includeLogFiles = true)
      try {
        withSQLConf("hoodie.fileIndex.dataSkippingFailureMode" -> "strict") {
          (fileIndex.listFiles(Seq.empty, Seq(filter)).flatMap(_.files).size,
            TaskMetaIOCountingFileSystem.taskMetadataTableFileReads.size)
        }
      } finally {
        fileIndex.close()
      }
    } finally {
      restore()
    }
    val allFilesIndex = HoodieFileIndex(spark, newMetaClient(tablePath),
      None, readOpts + (DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "false"), includeLogFiles = true)
    val allFiles = try allFilesIndex.listFiles(Seq.empty, Seq(filter)).flatMap(_.files).size finally allFilesIndex.close()

    assertTrue(prunedFiles > 0 && prunedFiles < allFiles, s"$filter should prune files: $prunedFiles of $allFiles")
    assertTrue(taskMetadataReads > 0, s"$filter should read the metadata table inside tasks")
    assertNoTableMetadataLoadsInTasks()
  }

  private def assertNoTableMetadataLoadsInTasks(): Unit = {
    assertEquals(Seq.empty, TaskMetaIOCountingFileSystem.taskTimelineListings, "timeline listings inside tasks")
    assertEquals(Seq.empty, TaskMetaIOCountingFileSystem.taskTablePropertiesReads, "table config reads inside tasks")
  }

  private def assertQueryResult(tablePath: String, opts: Map[String, String], condition: String): Unit = {
    def read(dataSkipping: Boolean): Set[String] = spark.read.format("hudi")
      .options(opts ++ queryIndexOpts + (DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> dataSkipping.toString))
      .load(tablePath)
      .where(condition)
      .select("id", "name", "price", "part")
      .collect().map(_.mkString(",")).toSet

    val expected = read(dataSkipping = false)
    assertFalse(expected.isEmpty)
    withSQLConf("hoodie.fileIndex.dataSkippingFailureMode" -> "strict") {
      assertEquals(expected, read(dataSkipping = true))
    }
  }

  /** Returns, per record key, whether the index found an existing location. */
  private def tagLocation(tablePath: String, indexType: IndexType, keys: Seq[String],
                          extraOpts: Map[String, String] = Map.empty): Map[String, Boolean] = {
    val writeConfig = recordIndexWriteConfig(tablePath, indexType, extraOpts)
    // A new engine context picks up the current Hadoop conf of the Spark context.
    val engineContext = new HoodieSparkEngineContext(jsc())
    val table = HoodieSparkTable.create(writeConfig, engineContext)
    val records = jsc().parallelize(keys.map { key =>
      new HoodieAvroRecord(new HoodieKey(key, "p0"), new EmptyHoodieRecordPayload()).asInstanceOf[HoodieRecord[EmptyHoodieRecordPayload]]
    }.asJava, 2)
    val index = SparkHoodieIndexFactory.createIndex(writeConfig)
    index.tagLocation(HoodieJavaRDD.of(records), engineContext, table).collectAsList().asScala
      .map(r => r.getRecordKey -> r.isCurrentLocationKnown).toMap
  }

  private def recordIndexWriteConfig(tablePath: String, indexType: IndexType,
                                     extraOpts: Map[String, String] = Map.empty): HoodieWriteConfig =
    HoodieWriteConfig.newBuilder()
      .withPath(tablePath)
      .withProps(toProps(globalRecordIndexOpts ++ extraOpts))
      .withSchema(new TableSchemaResolver(newMetaClient(tablePath)).getTableSchema(false).toString)
      .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType).build())
      .build()

  private def assertShipsNoTableMetadata(rdd: RDD[_]): Unit = lineage(rdd).foreach { stageRdd =>
    val (_, classes) = serializeRecordingClasses(stageRdd)
    assertFalse(classes.contains(classOf[HoodieBackedTableMetadata].getName), s"task ships the table metadata: $classes")
    assertFalse(classes.keys.exists(_.endsWith("FileSystemView")), s"task ships a file system view: $classes")
    assertTrue(classes.getOrElse(classOf[HoodieTableMetaClient].getName, 0) <= 1, s"task ships more than one meta client: $classes")
    assertTrue(classes.getOrElse(classOf[HadoopStorageConfiguration].getName, 0) <= 1, s"task ships more than one Hadoop conf: $classes")
  }

  /** Asserts the object carries no table metadata, meta client, Hadoop conf, write config or table; broadcasts are fine. */
  private def assertShipsOnlyBroadcastState(obj: AnyRef): Unit = {
    val (_, classes) = serializeRecordingClasses(obj)
    val heavyClasses = Seq(classOf[HoodieTableMetadata], classOf[HoodieTableMetaClient], classOf[HadoopStorageConfiguration],
      classOf[HoodieWriteConfig], classOf[HoodieTable[_, _, _, _]])
    val shipped = classes.keys.filter { name =>
      Try(Class.forName(name, false, getClass.getClassLoader)).toOption.exists(c => heavyClasses.exists(_.isAssignableFrom(c)))
    }
    assertEquals(Seq.empty, shipped.toSeq, s"${obj.getClass.getSimpleName} ships table state per task")
  }

  /** The RDD and its ancestors, across shuffles: serializing each one covers what every stage ships per task. */
  private def lineage(rdd: RDD[_]): Seq[RDD[_]] = rdd +: rdd.dependencies.flatMap(dep => lineage(dep.rdd))

  /** Java-serializes the object as a Spark task binary would, returning its size and the count of each class written. */
  private def serializeRecordingClasses(obj: AnyRef): (Int, Map[String, Int]) = {
    val classes = mutable.Map[String, Int]().withDefaultValue(0)
    val bytes = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bytes) {
      enableReplaceObject(true)

      override def replaceObject(o: AnyRef): AnyRef = {
        classes(o.getClass.getName) += 1
        o
      }
    }
    out.writeObject(obj)
    out.close()
    (bytes.size(), classes.toMap)
  }

  private def writeTable(tablePath: String, opts: Map[String, String]): Unit = {
    (0 until 3).foreach { batch =>
      val operation = if (batch == 2) DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL else DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL
      // The upsert rewrites the first batch so the metadata table has updates in its log files.
      val start = if (batch == 2) 0 else batch * recordsPerBatch
      batchDf(start, batch).write.format("hudi")
        .options(opts)
        .option(DataSourceWriteOptions.OPERATION.key, operation)
        .mode(if (batch == 0) SaveMode.Overwrite else SaveMode.Append)
        .save(tablePath)
    }
  }

  private def batchDf(start: Int, batch: Int): DataFrame = {
    val rows = (start until start + recordsPerBatch).map { i =>
      (f"key$i%04d", f"name$i%04d", i.toDouble + batch, s"p${i % numPartitions}", batch.toLong)
    }
    spark.createDataFrame(rows).toDF("id", "name", "price", "part", "ts")
  }

  private def createSecondaryIndex(tablePath: String, column: String): Unit = {
    val tableName = s"lookup_tasks_${System.nanoTime()}"
    spark.sql(s"create table $tableName using hudi location '$tablePath'")
    spark.sql(s"create index idx_$column on $tableName ($column)")
    spark.sql(s"drop table $tableName")
  }

  private def newMetaClient(tablePath: String): HoodieTableMetaClient = HoodieTableMetaClient.builder()
    .setBasePath(tablePath)
    .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc().hadoopConfiguration()))
    .build()

  private def attribute(name: String, dataType: org.apache.spark.sql.types.DataType = StringType): AttributeReference =
    AttributeReference(name, dataType, nullable = true)()

  private def toProps(opts: Map[String, String]): java.util.Properties = {
    val props = new java.util.Properties()
    opts.foreach { case (k, v) => props.setProperty(k, v) }
    props
  }

  private val commonOpts: Map[String, String] = Map(
    HoodieWriteConfig.TBL_NAME.key -> "lookup_tasks",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "part",
    HoodieTableConfig.ORDERING_FIELDS.key -> "ts",
    "hoodie.insert.shuffle.parallelism" -> "2",
    "hoodie.upsert.shuffle.parallelism" -> "2",
    "hoodie.parquet.small.file.limit" -> "0",
    HoodieMetadataConfig.ENABLE.key -> "true",
    HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
    HoodieMetadataConfig.METADATA_INDEX_COLUMN_STATS_FILE_GROUP_COUNT.key -> "2",
    HoodieMetadataConfig.ENABLE_METADATA_INDEX_BLOOM_FILTER.key -> "true",
    HoodieMetadataConfig.METADATA_INDEX_BLOOM_FILTER_FILE_GROUP_COUNT.key -> "2")

  private val globalRecordIndexOpts: Map[String, String] = commonOpts ++ Map(
    HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "true",
    HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key -> "2",
    HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP.key -> "2")

  private val partitionedRecordIndexOpts: Map[String, String] = commonOpts ++ Map(
    HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "false",
    HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "true",
    HoodieMetadataConfig.RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key -> "2",
    HoodieMetadataConfig.RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP.key -> "2")

  // Column stats are read by Spark tasks rather than on the driver.
  private val queryIndexOpts: Map[String, String] = Map(
    DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true",
    HoodieMetadataConfig.COLUMN_STATS_INDEX_PROCESSING_MODE_OVERRIDE.key -> HoodieMetadataConfig.COLUMN_STATS_INDEX_PROCESSING_MODE_ENGINE)
}

/** Exposes the record index lookup stage of the global record level index. */
class LookupExposingRecordLevelIndex(config: HoodieWriteConfig) extends SparkMetadataTableGlobalRecordLevelIndex(config) {
  def lookup(records: HoodieData[HoodieRecord[EmptyHoodieRecordPayload]], context: HoodieEngineContext,
             table: HoodieTable[_, _, _, _]): HoodiePairData[String, HoodieRecordGlobalLocation] =
    lookupRecords(records, context, table, fetchFileGroupSize(table))
}
