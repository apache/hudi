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

package org.apache.hudi.dataframe

import org.apache.hudi.{HoodieDatasetBulkInsertHelper, HoodieSchemaConversionUtils, HoodieSparkUtils}
import org.apache.hudi.AvroConversionUtils.getAvroRecordNameAndNamespace
import org.apache.hudi.client.{SparkRDDWriteClient, WriteStatus}
import org.apache.hudi.common.config.{HoodieReaderConfig, TypedProperties}
import org.apache.hudi.common.data.HoodieData.HoodieDataCacheKey
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieRecord, HoodieTableType, WriteOperationType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.util.{CommitUtils, ConfigUtils, Option => HOption, StringUtils}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.data.HoodieJavaRDD
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.execution.bulkinsert.NonSortPartitionerWithRows
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.keygen.{ComplexKeyGenerator, NonpartitionedKeyGenerator, SimpleKeyGenerator}
import org.apache.hudi.table.HoodieTable

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions.{col, concat, hash, lit, lpad, pmod, udf, when}
import org.apache.spark.sql.internal.SQLConf

import scala.collection.JavaConverters._

/**
 * DataFrame-native write path for Hudi on Spark (PoC).
 *
 * <p>Takes the input [[DataFrame]] end to end as Dataset/InternalRow operations: key generation
 * and meta-column stamping as a projection, index tagging as a join against the latest base-file
 * meta columns, routing as a repartition on a routing-key column, and the record write as
 * per-file-group tasks driving the row-native write handles ({@code HoodieRowCreateHandle} for
 * new files, {@code FileGroupReaderBasedMergeHandle} for merges). The commit goes through the
 * regular {@code SparkRDDWriteClient#commit} so timeline, metadata table, and table services
 * behave exactly as on the RDD path.
 *
 * <p>Scope: insert and upsert on COW tables with the simple index tagging strategy.
 */
object HoodieDataFrameWriter extends Logging {

  val DATAFRAME_WRITE_PATH_ENABLE = "hoodie.datasource.write.dataframe.path.enable"
  val INSERT_BUCKETS_PER_PARTITION = "hoodie.datasource.write.dataframe.insert.buckets"
  val WRITE_TASKS = "hoodie.datasource.write.dataframe.write.tasks"

  val OPERATION_KEY = "hoodie.datasource.write.operation"
  val RECORD_KEY_FIELD = "hoodie.datasource.write.recordkey.field"
  val PARTITION_PATH_FIELD = "hoodie.datasource.write.partitionpath.field"

  private val TAGGED_FILE_ID_COL = "_hoodie_tagged_file_id"
  private val ROUTING_BUCKET_COL = "_hoodie_routing_bucket"
  private val UPDATE_BUCKET_PREFIX = "u:"
  private val INSERT_BUCKET_PREFIX = "i:"

  def write(sqlContext: SQLContext,
            mode: SaveMode,
            params: Map[String, String],
            df: DataFrame): Boolean = {
    val spark = sqlContext.sparkSession
    val basePath = params.getOrElse("path",
      throw new HoodieException("'path' must be set for the DataFrame write path"))
    val tableName = params.getOrElse(HoodieWriteConfig.TBL_NAME.key(),
      throw new HoodieException(s"'${HoodieWriteConfig.TBL_NAME.key()}' must be set"))
    val operation = WriteOperationType.fromValue(
      params.getOrElse(OPERATION_KEY, WriteOperationType.UPSERT.value()))
    if (operation != WriteOperationType.INSERT && operation != WriteOperationType.UPSERT) {
      throw new HoodieException(s"Operation $operation is not supported by the DataFrame write path yet")
    }

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val storageConf = HadoopFSUtils.getStorageConfWithCopy(hadoopConf)
    initTableIfNeeded(basePath, tableName, params, storageConf)

    val writeConfig = buildWriteConfig(basePath, tableName, operation, params, df)
    val engineContext = new org.apache.hudi.client.common.HoodieSparkEngineContext(
      new JavaSparkContext(spark.sparkContext))
    val client = new SparkRDDWriteClient[Any](engineContext, writeConfig)
    try {
      val instantTime = client.startCommit()
      val table = client.initTable(operation, HOption.of(instantTime))
      if (operation == WriteOperationType.UPSERT) {
        table.validateUpsertSchema()
      } else {
        table.validateInsertSchema()
      }
      client.preWrite(instantTime, operation, table.getMetaClient)

      // Stage 1 + 2: key generation, meta columns, and upfront precombine as Dataset operations.
      val prepared = HoodieDatasetBulkInsertHelper.prepareForBulkInsert(
        df, writeConfig, table.getMetaClient.getTableConfig, new NonSortPartitionerWithRows(), instantTime)

      // Stage 3: index tagging as a join against the latest base-file meta columns.
      val tagged = tagRecords(spark, prepared, operation, table)

      // Stage 4: routing on a bucket column; updates pin to their file group, inserts bucket
      // per partition path.
      val numWriteTasks = params.get(WRITE_TASKS).map(_.toInt)
        .getOrElse(spark.sparkContext.defaultParallelism)
      val routed = routeRecords(tagged, params, numWriteTasks)

      val actionType = CommitUtils.getCommitActionType(operation, HoodieTableType.COPY_ON_WRITE)
      table.getActiveTimeline.transitionRequestedToInflight(
        table.getMetaClient.createNewInstant(HoodieInstant.State.REQUESTED, actionType, instantTime),
        HOption.empty())

      // Stage 5: per-bucket write tasks driving the row-native handles. The reader context
      // factory is created on the driver (the table's engine context is not usable on executors)
      // and shipped with the task.
      val writeSchemaLength = prepared.schema.length
      val sparkTable = table.asInstanceOf[HoodieTable[InternalRow, _, _, _]]
      val readerContextFactory = sparkTable.getReaderContextFactoryForWrite
      val task = new DataFrameWriteTask(
        writeConfig, sparkTable, readerContextFactory, instantTime, routed.schema, writeSchemaLength,
        routed.schema.fieldIndex(ROUTING_BUCKET_COL), UPDATE_BUCKET_PREFIX)
      val statusRdd = HoodieSparkUtils.injectSQLConf(
        routed.queryExecution.toRdd.mapPartitions(iter => task.execute(iter)), SQLConf.get)

      val javaRdd = new org.apache.spark.api.java.JavaRDD[WriteStatus](statusRdd)
      HoodieJavaRDD.of(javaRdd).persist(
        writeConfig.getString(HoodieWriteConfig.WRITE_STATUS_STORAGE_LEVEL_VALUE),
        engineContext, HoodieDataCacheKey.of(basePath, instantTime))

      client.commit(instantTime, javaRdd, HOption.empty(), actionType,
        java.util.Collections.emptyMap[String, java.util.List[String]](), HOption.empty())
    } finally {
      client.close()
    }
    true
  }

  private def initTableIfNeeded(basePath: String,
                                tableName: String,
                                params: Map[String, String],
                                storageConf: org.apache.hudi.storage.StorageConfiguration[_]): Unit = {
    val path = new org.apache.hadoop.fs.Path(basePath)
    val fs = path.getFileSystem(storageConf.unwrapAs(classOf[org.apache.hadoop.conf.Configuration]))
    val tableExists = fs.exists(new org.apache.hadoop.fs.Path(path, HoodieTableMetaClient.METAFOLDER_NAME))
    if (!tableExists) {
      HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName(tableName)
        .setRecordKeyFields(params.getOrElse(RECORD_KEY_FIELD, null))
        .setPartitionFields(params.getOrElse(PARTITION_PATH_FIELD, null))
        .setOrderingFields(ConfigUtils.getOrderingFieldsStrDuringWrite(params.asJava))
        .setKeyGeneratorClassProp(resolveKeyGeneratorClass(params))
        .initTable(storageConf, basePath)
    }
  }

  private def resolveKeyGeneratorClass(params: Map[String, String]): String = {
    params.getOrElse(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), {
      val recordKeyFields = params.getOrElse(RECORD_KEY_FIELD, "")
      val partitionFields = params.getOrElse(PARTITION_PATH_FIELD, "")
      if (StringUtils.isNullOrEmpty(partitionFields)) {
        classOf[NonpartitionedKeyGenerator].getName
      } else if (recordKeyFields.contains(",") || partitionFields.contains(",")) {
        classOf[ComplexKeyGenerator].getName
      } else {
        classOf[SimpleKeyGenerator].getName
      }
    })
  }

  private def buildWriteConfig(basePath: String,
                               tableName: String,
                               operation: WriteOperationType,
                               params: Map[String, String],
                               df: DataFrame): HoodieWriteConfig = {
    val (name, namespace) = getAvroRecordNameAndNamespace(tableName)
    val schemaStr = HoodieSchemaConversionUtils
      .convertStructTypeToHoodieSchema(df.schema, name, namespace).toString
    val props = new TypedProperties()
    params.foreach { case (k, v) => props.setProperty(k, v) }
    props.setProperty(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), resolveKeyGeneratorClass(params))
    // The engine-native record merger keeps the merge path on InternalRow end to end.
    props.setProperty(HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY,
      "org.apache.hudi.DefaultSparkRecordMerger")
    // The prepare stage runs the upfront precombine when combine-before-insert is set; map the
    // upsert-side combine flag onto it so upsert batches dedupe by the ordering field.
    val orderingFields = ConfigUtils.getOrderingFieldsStrDuringWrite(params.asJava)
    if (operation == WriteOperationType.UPSERT && !StringUtils.isNullOrEmpty(orderingFields)
      && props.getString(HoodieWriteConfig.COMBINE_BEFORE_UPSERT.key(),
        HoodieWriteConfig.COMBINE_BEFORE_UPSERT.defaultValue()).toBoolean) {
      props.setProperty(HoodieWriteConfig.COMBINE_BEFORE_INSERT.key(), "true")
    }
    HoodieWriteConfig.newBuilder()
      .withPath(basePath)
      .forTable(tableName)
      .withSchema(schemaStr)
      .withProps(props)
      .build()
  }

  /**
   * Simple-index-style tagging: read only the meta columns of the latest base files and left
   * join the input on (record key, partition path), producing the target file id column (null
   * for inserts).
   */
  private def tagRecords(spark: org.apache.spark.sql.SparkSession,
                         prepared: Dataset[Row],
                         operation: WriteOperationType,
                         table: org.apache.hudi.table.HoodieTable[_, _, _, _]): Dataset[Row] = {
    val latestBaseFiles = if (operation == WriteOperationType.UPSERT) {
      // The view only serves partitions already loaded into it; load them all on the same view
      // instance before asking for the latest base files across the table.
      val fsView = table.getHoodieView
      fsView.loadAllPartitions()
      fsView.getLatestBaseFiles.iterator().asScala.map(_.getPath).toSeq
    } else {
      Seq.empty
    }
    if (latestBaseFiles.isEmpty) {
      log.info("Tagging skipped: no latest base files in the table")
      prepared.withColumn(TAGGED_FILE_ID_COL, lit(null).cast("string"))
    } else {
      log.info(s"Tagging incoming records against ${latestBaseFiles.size} latest base files")
      val fileIdFromName = udf((fileName: String) => FSUtils.getFileId(fileName))
      val existing = spark.read.parquet(latestBaseFiles: _*)
        .select(
          col(HoodieRecord.RECORD_KEY_METADATA_FIELD).as("_hoodie_existing_key"),
          col(HoodieRecord.PARTITION_PATH_METADATA_FIELD).as("_hoodie_existing_partition"),
          fileIdFromName(col(HoodieRecord.FILENAME_METADATA_FIELD)).as(TAGGED_FILE_ID_COL))
      prepared.join(existing,
          prepared(HoodieRecord.RECORD_KEY_METADATA_FIELD) === existing("_hoodie_existing_key")
            && prepared(HoodieRecord.PARTITION_PATH_METADATA_FIELD) === existing("_hoodie_existing_partition"),
          "left_outer")
        .drop("_hoodie_existing_key", "_hoodie_existing_partition")
    }
  }

  /**
   * Adds the routing-bucket column (updates keyed by their file group, inserts by partition path
   * and a coarse key-hash bucket) and co-locates each bucket through one shuffle, sorted within
   * partitions so a write task sees each bucket as one contiguous run.
   */
  private def routeRecords(tagged: Dataset[Row],
                           params: Map[String, String],
                           numWriteTasks: Int): Dataset[Row] = {
    val insertBuckets = params.get(INSERT_BUCKETS_PER_PARTITION).map(_.toInt).getOrElse(1)
    val bucketCol = when(col(TAGGED_FILE_ID_COL).isNotNull,
        concat(lit(UPDATE_BUCKET_PREFIX), col(TAGGED_FILE_ID_COL)))
      .otherwise(concat(
        lit(INSERT_BUCKET_PREFIX),
        col(HoodieRecord.PARTITION_PATH_METADATA_FIELD), lit(":"),
        lpad(pmod(hash(col(HoodieRecord.RECORD_KEY_METADATA_FIELD)), lit(insertBuckets))
          .cast("string"), 10, "0")))
    tagged.withColumn(ROUTING_BUCKET_COL, bucketCol)
      .repartition(numWriteTasks, col(ROUTING_BUCKET_COL))
      .sortWithinPartitions(col(ROUTING_BUCKET_COL))
  }
}
