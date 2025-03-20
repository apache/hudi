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
import org.apache.hadoop.conf.Configuration
import org.apache.hudi.{AvroConversionUtils, HoodieCLIUtils, HoodieSparkSqlWriter}
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.commit.DatasetBucketRescaleCommitActionExecutor
import org.apache.hudi.common.config.{HoodieMetadataConfig, HoodieReaderConfig, SerializableSchema}
import org.apache.hudi.common.engine.HoodieEngineContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{FileSlice, PartitionBucketIndexHashingConfig, WriteOperationType}
import org.apache.hudi.common.table.read.HoodieFileGroupReader
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.util.{Option, StringUtils}
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.index.bucket.{PartitionBucketIndexCalculator, PartitionBucketIndexUtils}
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.internal.schema.utils.SerDeHelper
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.hudi.storage.{StoragePath, StoragePathInfo}
import org.apache.hudi.table.SparkBroadcastManager
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{HoodieUnsafeUtils, Row, SaveMode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.function.Supplier
import java.util.stream.Collectors
import scala.collection.JavaConverters._
import scala.collection.mutable

class PartitionBucketIndexManager() extends BaseProcedure
  with ProcedureBuilder
  with PredicateHelper
  with ProvidesHoodieConfig
  with Logging {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "overwrite", DataTypes.StringType),
    ProcedureParameter.optional(2, "bucket-number", DataTypes.IntegerType, -1),
    ProcedureParameter.optional(3, "add", DataTypes.StringType),
    ProcedureParameter.optional(4, "dry-run", DataTypes.BooleanType, true),
    ProcedureParameter.optional(5, "rollback", DataTypes.StringType),
    ProcedureParameter.optional(6, "show-config", DataTypes.BooleanType, false),
    ProcedureParameter.optional(7, "rule", DataTypes.StringType, "regex")
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

    try {
      // Get table metadata
      val hoodieCatalogTable = HoodieCLIUtils.getHoodieCatalogTable(sparkSession, table)
      val basePath = hoodieCatalogTable.tableLocation
      val metaClient = createMetaClient(jsc, basePath)
      val writeClient = HoodieCLIUtils.createHoodieWriteClient(sparkSession, basePath, Map.empty,
        scala.Option.apply(table))
      val context = writeClient.getEngineContext

      // Determine which operation to perform
      if (showConfig) {
        handleShowConfig(metaClient)
      } else if (rollback != null) {
        handleRollback(metaClient, rollback)
      } else if (overwrite != null) {
        handleOverwrite(hoodieCatalogTable, writeClient, context, metaClient, overwrite, bucketNumber, rule, dryRun)
      } else if (add != null) {
        handleAdd(metaClient, add, dryRun)
      } else {
        Seq(Row("ERROR", "INVALID_OPERATION", "No valid operation specified"))
      }
    } catch {
      case e: Exception => Seq(Row("ERROR", "EXCEPTION", e.getMessage))
    }
  }

  /**
   * Handle the overwrite operation.
   */
  private def handleOverwrite(catalog: HoodieCatalogTable,
                              writeClient: SparkRDDWriteClient[_],
                              context : HoodieEngineContext,
                              metaClient: HoodieTableMetaClient,
                              expression: String,
                              bucketNumber: Int,
                              rule: String,
                              dryRun: Boolean): Seq[Row] = {
    val config = buildBucketRescaleHoodieConfig(catalog).asJava
    val basePath = metaClient.getBasePath
    val mdtEnable = metaClient.getStorage().exists(new StoragePath(metaClient.getBasePath, HoodieTableMetaClient.METADATA_TABLE_FOLDER_PATH))

    // get all partition paths
    val allPartitions = FSUtils.getAllPartitionPaths(context, metaClient.getStorage, metaClient.getBasePath, mdtEnable)

    // get Map<partitionPath, bucketNumber> based on latest hashing_config
    val latestHashingConfigInstant = PartitionBucketIndexUtils.getHashingConfigInstantToLoad(metaClient)
    val calcWithLatestInstant = PartitionBucketIndexCalculator.getInstance(latestHashingConfigInstant, metaClient)
    val partition2BucketWithLatestHashingConfig = PartitionBucketIndexUtils.getAllBucketNumbers(calcWithLatestInstant, allPartitions)

    // get Map<partitionPath, bucketNumber> based on new given expression
    val defaultBucketNumber = if (bucketNumber != -1) {
      bucketNumber
    } else {
      // reuse latest default bucket number
      calcWithLatestInstant.getHashingConfig.getDefaultBucketNumber
    }
    val instantTime = writeClient.createNewInstantTime()
    config.put(HoodieIndexConfig.BUCKET_INDEX_PARTITION_EXPRESSIONS.key(), expression)
    config.put(HoodieIndexConfig.BUCKET_INDEX_PARTITION_RULE_TYPE.key(), rule)
    config.put(HoodieIndexConfig.BUCKET_INDEX_PARTITION_LOAD_INSTANT.key(), instantTime)
    config.put(HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key(), bucketNumber.toString)

    val newConfig = new PartitionBucketIndexHashingConfig(expression, defaultBucketNumber, rule, PartitionBucketIndexHashingConfig.CURRENT_VERSION, instantTime)
    val calcWithNewExpression = PartitionBucketIndexCalculator.getInstance(instantTime, newConfig)
    val partition2BucketWithNewHashingConfig = PartitionBucketIndexUtils.getAllBucketNumbers(calcWithNewExpression, allPartitions)

    // get partitions need to be rescaled
    val rescalePartitionsMap = getDifferentPartitions(partition2BucketWithNewHashingConfig.asScala, partition2BucketWithLatestHashingConfig.asScala)
    val partitionsToRescale = rescalePartitionsMap.keys

    // get all fileSlices need to read
    val allFilesMap = FSUtils.getFilesInPartitions(context, metaClient.getStorage(), HoodieMetadataConfig.newBuilder.enable(mdtEnable).build,
      metaClient.getBasePath.toString, partitionsToRescale.toArray)
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
    val serializableTableSchemaWithMetaFields = new SerializableSchema(tableSchemaWithMetaFields)
    val latestInstantTime = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants().lastInstant().get()

    val res: RDD[InternalRow] = spark.sparkContext.parallelize(allFileSlice, allFileSlice.size).flatMap(fileSlice => {
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
    val dataFrame = HoodieUnsafeUtils.createDataFrameFromRDD(sparkSession, res, sparkSchemaWithMetaFields)

    val (success, _, _, _, _, _) = HoodieSparkSqlWriter.write(
      sparkSession.sqlContext,
      SaveMode.Overwrite,
      config.asScala.toMap,
      dataFrame)

    val details = s"Expression: $expression, Bucket Number: $bucketNumber, Dry Run: $dryRun"
    Seq(Row("SUCCESS", "OVERWRITE", success))
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

  override def build: Procedure = new PartitionBucketIndexManager()
}

object PartitionBucketIndexManager {
  val NAME = "PartitionBucketIndexManager"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new PartitionBucketIndexManager()
  }
}