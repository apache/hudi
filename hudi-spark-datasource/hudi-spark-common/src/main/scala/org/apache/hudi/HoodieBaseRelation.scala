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

package org.apache.hudi

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hbase.io.hfile.CacheConfig
import org.apache.hadoop.mapred.JobConf
import org.apache.hudi.HoodieBaseRelation.getPartitionPath
import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.common.config.{HoodieMetadataConfig, SerializableConfiguration}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieFileFormat, HoodieRecord}
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter
import org.apache.hudi.io.storage.HoodieHFileReader
import org.apache.spark.TaskContext
import org.apache.spark.execution.datasources.HoodieInMemoryFileIndex
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, SubqueryExpression}
import org.apache.spark.sql.execution.FileRelation
import org.apache.spark.sql.execution.datasources.{FileStatusCache, PartitionedFile, PartitioningUtils}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.unsafe.types.UTF8String

import java.io.Closeable
import java.net.URI
import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

trait HoodieFileSplit {}

case class HoodieTableSchema(structTypeSchema: StructType, avroSchemaStr: String, internalSchema: InternalSchema = InternalSchema.getEmptyInternalSchema)

case class HoodieTableState(tablePath: String,
                            latestCommitTimestamp: String,
                            recordKeyField: String,
                            preCombineFieldOpt: Option[String],
                            usesVirtualKeys: Boolean,
                            recordPayloadClassName: String,
                            metadataConfig: HoodieMetadataConfig)

/**
 * Hoodie BaseRelation which extends [[PrunedFilteredScan]].
 */
abstract class HoodieBaseRelation(val sqlContext: SQLContext,
                                  val metaClient: HoodieTableMetaClient,
                                  val optParams: Map[String, String],
                                  userSchema: Option[StructType])
  extends BaseRelation
    with FileRelation
    with PrunedFilteredScan
    with SparkAdapterSupport
    with Logging {

  type FileSplit <: HoodieFileSplit

  imbueConfigs(sqlContext)

  protected val sparkSession: SparkSession = sqlContext.sparkSession

  protected lazy val conf: Configuration = new Configuration(sqlContext.sparkContext.hadoopConfiguration)
  protected lazy val jobConf = new JobConf(conf)

  protected lazy val tableConfig: HoodieTableConfig = metaClient.getTableConfig

  protected lazy val basePath: String = metaClient.getBasePath

  // NOTE: Record key-field is assumed singular here due to the either of
  //          - In case Hudi's meta fields are enabled: record key will be pre-materialized (stored) as part
  //          of the record's payload (as part of the Hudi's metadata)
  //          - In case Hudi's meta fields are disabled (virtual keys): in that case record has to bear _single field_
  //          identified as its (unique) primary key w/in its payload (this is a limitation of [[SimpleKeyGenerator]],
  //          which is the only [[KeyGenerator]] permitted for virtual-keys payloads)
  protected lazy val recordKeyField: String =
    if (tableConfig.populateMetaFields()) {
      HoodieRecord.RECORD_KEY_METADATA_FIELD
    } else {
      val keyFields = tableConfig.getRecordKeyFields.get()
      checkState(keyFields.length == 1)
      keyFields.head
    }

  protected lazy val preCombineFieldOpt: Option[String] =
    Option(tableConfig.getPreCombineField)
      .orElse(optParams.get(DataSourceWriteOptions.PRECOMBINE_FIELD.key)) match {
      // NOTE: This is required to compensate for cases when empty string is used to stub
      //       property value to avoid it being set with the default value
      // TODO(HUDI-3456) cleanup
      case Some(f) if !StringUtils.isNullOrEmpty(f) => Some(f)
      case _ => None
    }

  protected lazy val specifiedQueryTimestamp: Option[String] =
    optParams.get(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key)
      .map(HoodieSqlCommonUtils.formatQueryInstant)

  protected lazy val (tableAvroSchema: Schema, internalSchema: InternalSchema) = {
    val schemaUtil = new TableSchemaResolver(metaClient)
    val avroSchema = Try(schemaUtil.getTableAvroSchema).getOrElse(
      // If there is no commit in the table, we can't get the schema
      // t/h [[TableSchemaResolver]], fallback to the provided [[userSchema]] instead.
      userSchema match {
        case Some(s) => sparkAdapter.getAvroSchemaConverters.toAvroType(s, nullable = false, "record")
        case _ => throw new IllegalArgumentException("User-provided schema is required in case the table is empty")
      }
    )
    // try to find internalSchema
    val internalSchemaFromMeta = try {
      schemaUtil.getTableInternalSchemaFromCommitMetadata.orElse(InternalSchema.getEmptyInternalSchema)
    } catch {
      case _: Exception => InternalSchema.getEmptyInternalSchema
    }
    (avroSchema, internalSchemaFromMeta)
  }

  protected val tableStructSchema: StructType = AvroConversionUtils.convertAvroSchemaToStructType(tableAvroSchema)

  protected val partitionColumns: Array[String] = tableConfig.getPartitionFields.orElse(Array.empty)

  /**
   * if true, need to deal with schema for creating file reader.
   */
  protected val dropPartitionColumnsWhenWrite: Boolean =
    metaClient.getTableConfig.isDropPartitionColumns && partitionColumns.nonEmpty

  /**
   * NOTE: PLEASE READ THIS CAREFULLY
   *
   * Even though [[HoodieFileIndex]] initializes eagerly listing all of the files w/in the given Hudi table,
   * this variable itself is _lazy_ (and have to stay that way) which guarantees that it's not initialized, until
   * it's actually accessed
   */
  protected lazy val fileIndex: HoodieFileIndex =
    HoodieFileIndex(sparkSession, metaClient, Some(tableStructSchema), optParams,
      FileStatusCache.getOrCreate(sparkSession))

  /**
   * Columns that relation has to read from the storage to properly execute on its semantic: for ex,
   * for Merge-on-Read tables key fields as well and pre-combine field comprise mandatory set of columns,
   * meaning that regardless of whether this columns are being requested by the query they will be fetched
   * regardless so that relation is able to combine records properly (if necessary)
   *
   * @VisibleInTests
   */
  val mandatoryColumns: Seq[String]

  protected def timeline: HoodieTimeline =
  // NOTE: We're including compaction here since it's not considering a "commit" operation
    metaClient.getCommitsAndCompactionTimeline.filterCompletedInstants

  protected val validCommits = timeline.getInstants.toArray().map(_.asInstanceOf[HoodieInstant].getFileName).mkString(",")

  protected def latestInstant: Option[HoodieInstant] =
    toScalaOption(timeline.lastInstant())

  protected def queryTimestamp: Option[String] =
    specifiedQueryTimestamp.orElse(toScalaOption(timeline.lastInstant()).map(_.getTimestamp))

  override def schema: StructType = tableStructSchema

  /**
   * This method controls whether relation will be producing
   * <ul>
   * <li>[[Row]], when it's being equal to true</li>
   * <li>[[InternalRow]], when it's being equal to false</li>
   * </ul>
   *
   * Returning [[InternalRow]] directly enables us to save on needless ser/de loop from [[InternalRow]] (being
   * produced by file-reader) to [[Row]] and back
   */
  override final def needConversion: Boolean = false

  override def inputFiles: Array[String] = fileIndex.allFiles.map(_.getPath.toUri.toString).toArray

  /**
   * NOTE: DO NOT OVERRIDE THIS METHOD
   */
  override final def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // NOTE: In case list of requested columns doesn't contain the Primary Key one, we
    //       have to add it explicitly so that
    //          - Merging could be performed correctly
    //          - In case 0 columns are to be fetched (for ex, when doing {@code count()} on Spark's [[Dataset]],
    //          Spark still fetches all the rows to execute the query correctly
    //
    //       It's okay to return columns that have not been requested by the caller, as those nevertheless will be
    //       filtered out upstream
    val fetchedColumns: Array[String] = appendMandatoryColumns(requiredColumns)

    val (requiredAvroSchema, requiredStructSchema, requiredInternalSchema) =
      HoodieSparkUtils.getRequiredSchema(tableAvroSchema, fetchedColumns, internalSchema)

    val filterExpressions = convertToExpressions(filters)
    val (partitionFilters, dataFilters) = filterExpressions.partition(isPartitionPredicate)

    val fileSplits = collectFileSplits(partitionFilters, dataFilters)

    val partitionSchema = if (dropPartitionColumnsWhenWrite) {
      // when hoodie.datasource.write.drop.partition.columns is true, partition columns can't be persisted in
      // data files.
      StructType(partitionColumns.map(StructField(_, StringType)))
    } else {
      StructType(Nil)
    }

    val tableSchema = HoodieTableSchema(tableStructSchema, if (internalSchema.isEmptySchema) tableAvroSchema.toString else AvroInternalSchemaConverter.convert(internalSchema, tableAvroSchema.getName).toString, internalSchema)
    val dataSchema = if (dropPartitionColumnsWhenWrite) {
      val dataStructType = StructType(tableStructSchema.filterNot(f => partitionColumns.contains(f.name)))
      HoodieTableSchema(
        dataStructType,
        sparkAdapter.getAvroSchemaConverters.toAvroType(dataStructType, nullable = false, "record").toString()
      )
    } else {
      tableSchema
    }
    val requiredSchema = if (dropPartitionColumnsWhenWrite) {
      val requiredStructType = StructType(requiredStructSchema.filterNot(f => partitionColumns.contains(f.name)))
      HoodieTableSchema(
        requiredStructType,
        sparkAdapter.getAvroSchemaConverters.toAvroType(requiredStructType, nullable = false, "record").toString()
      )
    } else {
      HoodieTableSchema(requiredStructSchema, requiredAvroSchema.toString, requiredInternalSchema)
    }
    // Here we rely on a type erasure, to workaround inherited API restriction and pass [[RDD[InternalRow]]] back as [[RDD[Row]]]
    // Please check [[needConversion]] scala-doc for more details
    if (fileSplits.nonEmpty)
      composeRDD(fileSplits, partitionSchema, dataSchema, requiredSchema, filters).asInstanceOf[RDD[Row]]
    else
      sparkSession.sparkContext.emptyRDD
  }



  /**
   * Composes RDD provided file splits to read from, table and partition schemas, data filters to be applied
   *
   * @param fileSplits      file splits to be handled by the RDD
   * @param partitionSchema target table's partition schema
   * @param tableSchema     target table's schema
   * @param requiredSchema  projected schema required by the reader
   * @param filters         data filters to be applied
   * @return instance of RDD (implementing [[HoodieUnsafeRDD]])
   */
  protected def composeRDD(fileSplits: Seq[FileSplit],
                           partitionSchema: StructType,
                           tableSchema: HoodieTableSchema,
                           requiredSchema: HoodieTableSchema,
                           filters: Array[Filter]): HoodieUnsafeRDD

  /**
   * Provided with partition and date filters collects target file splits to read records from, while
   * performing pruning if necessary
   *
   * @param partitionFilters partition filters to be applied
   * @param dataFilters data filters to be applied
   * @return list of [[FileSplit]] to fetch records from
   */
  protected def collectFileSplits(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[FileSplit]

  protected def listLatestBaseFiles(globbedPaths: Seq[Path], partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Map[Path, Seq[FileStatus]] = {
    val partitionDirs = if (globbedPaths.isEmpty) {
      fileIndex.listFiles(partitionFilters, dataFilters)
    } else {
      val inMemoryFileIndex = HoodieInMemoryFileIndex.create(sparkSession, globbedPaths)
      inMemoryFileIndex.listFiles(partitionFilters, dataFilters)
    }

    val fsView = new HoodieTableFileSystemView(metaClient, timeline, partitionDirs.flatMap(_.files).toArray)
    val latestBaseFiles = fsView.getLatestBaseFiles.iterator().asScala.toList.map(_.getFileStatus)

    latestBaseFiles.groupBy(getPartitionPath)
  }

  protected def convertToExpressions(filters: Array[Filter]): Array[Expression] = {
    val catalystExpressions = HoodieSparkUtils.convertToCatalystExpressions(filters, tableStructSchema)

    val failedExprs = catalystExpressions.zipWithIndex.filter { case (opt, _) => opt.isEmpty }
    if (failedExprs.nonEmpty) {
      val failedFilters = failedExprs.map(p => filters(p._2))
      logWarning(s"Failed to convert Filters into Catalyst expressions (${failedFilters.map(_.toString)})")
    }

    catalystExpressions.filter(_.isDefined).map(_.get).toArray
  }

  /**
   * Checks whether given expression only references partition columns
   * (and involves no sub-query)
   */
  protected def isPartitionPredicate(condition: Expression): Boolean = {
    // Validates that the provided names both resolve to the same entity
    val resolvedNameEquals = sparkSession.sessionState.analyzer.resolver

    condition.references.forall { r => partitionColumns.exists(resolvedNameEquals(r.name, _)) } &&
      !SubqueryExpression.hasSubquery(condition)
  }

  protected final def appendMandatoryColumns(requestedColumns: Array[String]): Array[String] = {
    if (dropPartitionColumnsWhenWrite) {
      if (requestedColumns.isEmpty) {
        mandatoryColumns.toArray
      } else {
        requestedColumns
      }
    } else {
      val missing = mandatoryColumns.filter(col => !requestedColumns.contains(col))
      requestedColumns ++ missing
    }
  }

  protected def getTableState: HoodieTableState = {
    // Subset of the state of table's configuration as of at the time of the query
    HoodieTableState(
      tablePath = basePath,
      latestCommitTimestamp = queryTimestamp.get,
      recordKeyField = recordKeyField,
      preCombineFieldOpt = preCombineFieldOpt,
      usesVirtualKeys = !tableConfig.populateMetaFields(),
      recordPayloadClassName = tableConfig.getPayloadClass,
      metadataConfig = fileIndex.metadataConfig
    )
  }

  def imbueConfigs(sqlContext: SQLContext): Unit = {
    sqlContext.sparkSession.sessionState.conf.setConfString("spark.sql.parquet.filterPushdown", "true")
    sqlContext.sparkSession.sessionState.conf.setConfString("spark.sql.parquet.recordLevelFilter.enabled", "true")
    // TODO(HUDI-3639) vectorized reader has to be disabled to make sure MORIncrementalRelation is working properly
    sqlContext.sparkSession.sessionState.conf.setConfString("spark.sql.parquet.enableVectorizedReader", "false")
  }

  /**
   * For enable hoodie.datasource.write.drop.partition.columns, need to create an InternalRow on partition values
   * and pass this reader on parquet file. So that, we can query the partition columns.
   */
  protected def getPartitionColumnsAsInternalRow(file: FileStatus): InternalRow = {
    try {
      val tableConfig = metaClient.getTableConfig
      if (dropPartitionColumnsWhenWrite) {
        val relativePath = new URI(metaClient.getBasePath).relativize(new URI(file.getPath.getParent.toString)).toString
        val hiveStylePartitioningEnabled = tableConfig.getHiveStylePartitioningEnable.toBoolean
        if (hiveStylePartitioningEnabled) {
          val partitionSpec = PartitioningUtils.parsePathFragment(relativePath)
          InternalRow.fromSeq(partitionColumns.map(partitionSpec(_)).map(UTF8String.fromString))
        } else {
          if (partitionColumns.length == 1) {
            InternalRow.fromSeq(Seq(UTF8String.fromString(relativePath)))
          } else {
            val parts = relativePath.split("/")
            assert(parts.size == partitionColumns.length)
            InternalRow.fromSeq(parts.map(UTF8String.fromString))
          }
        }
      } else {
        InternalRow.empty
      }
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to get the right partition InternalRow for file : ${file.toString}")
        InternalRow.empty
    }
  }
}

object HoodieBaseRelation {

  def getPartitionPath(fileStatus: FileStatus): Path =
    fileStatus.getPath.getParent

  /**
   * Returns file-reader routine accepting [[PartitionedFile]] and returning an [[Iterator]]
   * over [[InternalRow]]
   */
  def createBaseFileReader(spark: SparkSession,
                           partitionSchema: StructType,
                           tableSchema: HoodieTableSchema,
                           requiredSchema: HoodieTableSchema,
                           filters: Seq[Filter],
                           options: Map[String, String],
                           hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val hfileReader = createHFileReader(
      spark = spark,
      tableSchema = tableSchema,
      requiredSchema = requiredSchema,
      filters = filters,
      options = options,
      hadoopConf = hadoopConf
    )
    val parquetReader = HoodieDataSourceHelper.buildHoodieParquetReader(
      sparkSession = spark,
      dataSchema = tableSchema.structTypeSchema,
      partitionSchema = partitionSchema,
      requiredSchema = requiredSchema.structTypeSchema,
      filters = filters,
      options = options,
      hadoopConf = hadoopConf
    )

    partitionedFile => {
      val extension = FSUtils.getFileExtension(partitionedFile.filePath)
      if (HoodieFileFormat.PARQUET.getFileExtension.equals(extension)) {
        parquetReader.apply(partitionedFile)
      } else if (HoodieFileFormat.HFILE.getFileExtension.equals(extension)) {
        hfileReader.apply(partitionedFile)
      } else {
        throw new UnsupportedOperationException(s"Base file format not supported by Spark DataSource ($partitionedFile)")
      }
    }
  }

  private def createHFileReader(spark: SparkSession,
                                tableSchema: HoodieTableSchema,
                                requiredSchema: HoodieTableSchema,
                                filters: Seq[Filter],
                                options: Map[String, String],
                                hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val hadoopConfBroadcast =
      spark.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    partitionedFile => {
      val hadoopConf = hadoopConfBroadcast.value.get()
      val reader = new HoodieHFileReader[GenericRecord](hadoopConf, new Path(partitionedFile.filePath),
        new CacheConfig(hadoopConf))

      val requiredRowSchema = requiredSchema.structTypeSchema
      // NOTE: Schema has to be parsed at this point, since Avro's [[Schema]] aren't serializable
      //       to be passed from driver to executor
      val requiredAvroSchema = new Schema.Parser().parse(requiredSchema.avroSchemaStr)
      val avroToRowConverter = AvroConversionUtils.createAvroToInternalRowConverter(requiredAvroSchema, requiredRowSchema)

      reader.getRecordIterator(requiredAvroSchema).asScala
        .map(record => {
          avroToRowConverter.apply(record.asInstanceOf[GenericRecord]).get
        })
    }
  }
}
