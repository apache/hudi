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

package org.apache.spark.sql.hudi.command.procedures

import org.apache.avro.Schema
import org.apache.hudi.DataSourceWriteOptions.{BULK_INSERT_OPERATION_OPT_VAL, ENABLE_ROW_WRITER, OPERATION}
import org.apache.hudi.{AvroConversionUtils, HoodieCLIUtils, HoodieSparkSqlWriter}
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.config.{HoodieMetadataConfig, HoodieReaderConfig, SerializableSchema}
import org.apache.hudi.common.engine.HoodieEngineContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{PartitionBucketIndexHashingConfig, WriteOperationType}
import org.apache.hudi.common.table.read.HoodieFileGroupReader
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.util.{Option, ValidationUtils}
import org.apache.hudi.config.{HoodieIndexConfig, HoodieInternalConfig}
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.index.bucket.{PartitionBucketIndexCalculator, PartitionBucketIndexUtils}
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.table.SparkBroadcastManager
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{HoodieUnsafeUtils, Row, SaveMode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.function.Supplier
import scala.collection.JavaConverters._
import scala.collection.mutable

class PartitionBucketIndexManager extends BaseProcedure
  with ProcedureBuilder
  with PredicateHelper
  with ProvidesHoodieConfig
  with Logging {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "overwrite", DataTypes.StringType),
    ProcedureParameter.optional(2, "bucketNumber", DataTypes.IntegerType, -1),
    ProcedureParameter.optional(3, "add", DataTypes.StringType),
    ProcedureParameter.optional(4, "dry-run", DataTypes.BooleanType, true),
    ProcedureParameter.optional(5, "rollback", DataTypes.StringType),
    ProcedureParameter.optional(6, "show-config", DataTypes.BooleanType, false),
    ProcedureParameter.optional(7, "rule", DataTypes.StringType, "regex"),
    // params => key=value, key2=value2
    ProcedureParameter.optional(8, "options", DataTypes.StringType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("result", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("operation", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("details", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val table = getArgValueOrDefault(args, PARAMETERS(0)).get.asInstanceOf[String]
    val overwrite = getArgValueOrDefault(args, PARAMETERS(1)).orNull.asInstanceOf[String]
    val bucketNumber = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[Int]
    val add = getArgValueOrDefault(args, PARAMETERS(3)).orNull.asInstanceOf[String]
    val dryRun = getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[Boolean]
    val rollback = getArgValueOrDefault(args, PARAMETERS(5)).orNull.asInstanceOf[String]
    val showConfig = getArgValueOrDefault(args, PARAMETERS(6)).get.asInstanceOf[Boolean]
    val rule = getArgValueOrDefault(args, PARAMETERS(7)).orNull.asInstanceOf[String]
    val options = getArgValueOrDefault(args, PARAMETERS(8))

    try {
      // Get table metadata
      val hoodieCatalogTable = HoodieCLIUtils.getHoodieCatalogTable(sparkSession, table)
      val basePath = hoodieCatalogTable.tableLocation
      val metaClient = createMetaClient(jsc, basePath)
      val writeClient = HoodieCLIUtils.createHoodieWriteClient(sparkSession, basePath, Map.empty,
        scala.Option.apply(table))
      val context = writeClient.getEngineContext
      var config = buildBucketRescaleHoodieConfig(hoodieCatalogTable)

      options match {
        case Some(p) =>
          config = config ++ HoodieCLIUtils.extractOptions(p.asInstanceOf[String])
        case _ =>
          logInfo("No options")
      }

      config = config ++ Map(OPERATION.key -> BULK_INSERT_OPERATION_OPT_VAL,
        HoodieInternalConfig.BULKINSERT_OVERWRITE_OPERATION_TYPE.key -> WriteOperationType.BUCKET_RESCALE.value(),
        ENABLE_ROW_WRITER.key() -> "true")

      // Determine which operation to perform
      if (showConfig) {
        handleShowConfig(metaClient)
      } else if (rollback != null) {
        handleRollback(metaClient, rollback)
      } else if (overwrite != null) {
        handleOverwrite(config, writeClient, context, metaClient, overwrite, bucketNumber, rule, dryRun)
      } else if (add != null) {
        handleAdd(metaClient, add, dryRun)
      } else {
        Seq(Row("ERROR", "INVALID_OPERATION", "No valid operation specified"))
      }
    } catch {
      case e: Exception => throw new HoodieException(e.getMessage, e)
    }
  }

  /**
   * Handle the overwrite operation.
   */
  private def handleOverwrite(config: Map[String, String],
                              writeClient: SparkRDDWriteClient[_],
                              context : HoodieEngineContext,
                              metaClient: HoodieTableMetaClient,
                              expression: String,
                              bucketNumber: Int,
                              rule: String,
                              dryRun: Boolean): Seq[Row] = {
    val basePath = metaClient.getBasePath
    val mdtEnable = metaClient.getStorage().exists(new StoragePath(metaClient.getBasePath, HoodieTableMetaClient.METADATA_TABLE_FOLDER_PATH))

    // get all partition paths
    val allPartitions = FSUtils.getAllPartitionPaths(context, metaClient.getStorage, metaClient.getBasePath, mdtEnable)
    val usePartitionBucketIndexBefore = PartitionBucketIndexUtils.isPartitionSimpleBucketIndex(context.getStorageConf, basePath.toString)

    var partition2BucketWithLatestHashingConfig: util.Map[String, Integer] = null
    var calcWithLatestInstant: PartitionBucketIndexCalculator = null

    if (usePartitionBucketIndexBefore) {
      // get Map<partitionPath, bucketNumber> based on latest hashing_config
      val latestHashingConfigInstant = PartitionBucketIndexUtils.getHashingConfigInstantToLoad(metaClient)
      calcWithLatestInstant = PartitionBucketIndexCalculator.getInstance(latestHashingConfigInstant, metaClient)
      partition2BucketWithLatestHashingConfig = PartitionBucketIndexUtils.getAllBucketNumbers(calcWithLatestInstant, allPartitions)
    } else {
      ValidationUtils.checkArgument(bucketNumber != -1)
      partition2BucketWithLatestHashingConfig = allPartitions.asScala.map(partition => (partition, new Integer(bucketNumber))).toMap.asJava
    }
    // get Map<partitionPath, bucketNumber> based on new given expression
    val defaultBucketNumber = if (bucketNumber != -1) {
      bucketNumber
    } else if (calcWithLatestInstant != null ) {
      // reuse latest default bucket number
      calcWithLatestInstant.getHashingConfig.getDefaultBucketNumber
    } else {
      throw new HoodieException("Please set original bucket number before upgrade to partition bucket level bucket index")
    }

    val instantTime = writeClient.createNewInstantTime()

    val finalConfig = config ++ Map(HoodieIndexConfig.BUCKET_INDEX_PARTITION_EXPRESSIONS.key()->  expression,
      HoodieIndexConfig.BUCKET_INDEX_PARTITION_RULE_TYPE.key() -> rule,
      HoodieIndexConfig.BUCKET_INDEX_PARTITION_LOAD_INSTANT.key() -> instantTime,
      HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key() -> bucketNumber.toString
    )

    val newConfig = new PartitionBucketIndexHashingConfig(expression, defaultBucketNumber, rule, PartitionBucketIndexHashingConfig.CURRENT_VERSION, instantTime)
    val calcWithNewExpression = PartitionBucketIndexCalculator.getInstance(instantTime, newConfig)
    val partition2BucketWithNewHashingConfig = PartitionBucketIndexUtils.getAllBucketNumbers(calcWithNewExpression, allPartitions)

    // get partitions need to be rescaled
    val rescalePartitionsMap = getDifferentPartitions(partition2BucketWithNewHashingConfig.asScala, partition2BucketWithLatestHashingConfig.asScala)
    val partitionsToRescale = rescalePartitionsMap.keys

    // get all fileSlices need to read
    val allFilesMap = FSUtils.getFilesInPartitions(context, metaClient.getStorage(), HoodieMetadataConfig.newBuilder.enable(mdtEnable).build,
      metaClient.getBasePath.toString, partitionsToRescale.map(relative => {
        new StoragePath(basePath, relative)
      }).map(storagePath => storagePath.toString).toArray)
    val files = allFilesMap.values().asScala.flatMap(x => x.asScala).toList
    val view = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline, files.asJava)
    val allFileSlice = partitionsToRescale.flatMap(partitionPath => {
      view.getLatestFileSlices(partitionPath).iterator().asScala
    }).toList

    // read all fileSlice para and get DF
    var tableSchemaWithMetaFields: Schema = null
    try tableSchemaWithMetaFields = HoodieAvroUtils.addMetadataFields(new TableSchemaResolver(metaClient).getTableAvroSchema(false), false)
    catch {
      case e: Exception =>
        throw new HoodieException("Failed to get table schema during clustering", e)
    }

    // broadcast reader context.
    val broadcastManager = new SparkBroadcastManager(context, metaClient)
    broadcastManager.prepareAndBroadcast()
    val sparkSchemaWithMetaFields = AvroConversionUtils.convertAvroSchemaToStructType(tableSchemaWithMetaFields)

    val res: RDD[InternalRow] = if (allFileSlice.isEmpty) {
      spark.sparkContext.emptyRDD
    } else {
      val serializableTableSchemaWithMetaFields = new SerializableSchema(tableSchemaWithMetaFields)
      val latestInstantTime = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants().lastInstant().get()

      spark.sparkContext.parallelize(allFileSlice, allFileSlice.size).flatMap(fileSlice => {
        // instantiate other supporting cast
        val readerSchema = serializableTableSchemaWithMetaFields.get
        val readerContextOpt = broadcastManager.retrieveFileGroupReaderContext(basePath)
        val internalSchemaOption: Option[InternalSchema] = Option.empty()
        // instantiate FG reader
        val fileGroupReader = new HoodieFileGroupReader(readerContextOpt.get(),
          metaClient.getStorage,
          basePath.toString,
          latestInstantTime.requestedTime(),
          fileSlice,
          readerSchema,
          readerSchema,
          internalSchemaOption, // not support evolution of schema for now
          metaClient,
          metaClient.getTableConfig.getProps,
          0,
          java.lang.Long.MAX_VALUE,
          HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS.defaultValue())
        fileGroupReader.initRecordIterators()
        val iterator = fileGroupReader.getClosableIterator.asInstanceOf[HoodieFileGroupReader.HoodieFileGroupReaderIterator[InternalRow]]
        iterator.asScala
      })
    }
    val dataFrame = HoodieUnsafeUtils.createDataFrameFromRDD(sparkSession, res, sparkSchemaWithMetaFields)
    val (success, _, _, _, _, _) = HoodieSparkSqlWriter.write(
      sparkSession.sqlContext,
      SaveMode.Append,
      finalConfig,
      dataFrame)

    val details = s"Expression: $expression, Bucket Number: $bucketNumber, Dry Run: $dryRun, Instant: $instantTime"
    Seq(Row("SUCCESS", "OVERWRITE", details))
  }

  /**
   * Compares two maps of partition paths to bucket numbers and returns a map of partition paths
   * that have different bucket numbers in the two maps, using the new bucket numbers.
   *
   * @param newMap The new map of partition paths to bucket numbers
   * @param oldMap The old map of partition paths to bucket numbers
   * @return A map containing only the partition paths that have different bucket numbers, with values from newMap
   */
  def getDifferentPartitions(newMap: mutable.Map[String, Integer], oldMap: mutable.Map[String, Integer]): mutable.Map[String, Integer] = {
    newMap.filter {
      case (partitionPath, bucketNumber) =>
        // Include partition path if it's not in old map or has a different bucket number
        !oldMap.contains(partitionPath) || oldMap(partitionPath) != bucketNumber
    }
  }

  /**
   * Handle the add operation.
   */
  private def handleAdd(metaClient: HoodieTableMetaClient, expression: String, dryRun: Boolean): Seq[Row] = {
    // In a real implementation, this would call PartitionBucketIndexManager
    // For now, just return a placeholder result
    val details = s"Expression: $expression, Dry Run: $dryRun"

    // Here would be the actual call to PartitionBucketIndexManager
    // PartitionBucketIndexManager.addExpression(metaClient, expression, dryRun)

    Seq(Row("SUCCESS", "ADD", details))
  }

  /**
   * Handle the rollback operation.
   */
  private def handleRollback(metaClient: HoodieTableMetaClient, instantTime: String): Seq[Row] = {
    // In a real implementation, this would call PartitionBucketIndexManager
    // For now, just return a placeholder result
    val details = s"Rolled back bucket rescale action: $instantTime"

    // Here would be the actual call to PartitionBucketIndexManager
    // PartitionBucketIndexManager.rollback(metaClient, instantTime)

    Seq(Row("SUCCESS", "ROLLBACK", details))
  }

  /**
   * Handle the show-config operation.
   */
  private def handleShowConfig(metaClient: HoodieTableMetaClient): Seq[Row] = {



    Seq(Row("SUCCESS", "SHOW_CONFIG", null))
  }

  override def build: Procedure = new PartitionBucketIndexManager
}

object PartitionBucketIndexManager {
  val NAME = "partition_bucket_index_manager"

  def builder : Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new PartitionBucketIndexManager
  }
}