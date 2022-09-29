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
import org.apache.hudi.HoodieBaseRelation._
import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.client.utils.SparkInternalSchemaConverter
import org.apache.hudi.common.config.{HoodieMetadataConfig, SerializableConfiguration}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.fs.FSUtils.getRelativePartitionPath
import org.apache.hudi.common.model.{FileSlice, HoodieFileFormat, HoodieRecord}
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.common.util.StringUtils.isNullOrEmpty
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter
import org.apache.hudi.internal.schema.utils.{InternalSchemaUtils, SerDeHelper}
import org.apache.hudi.internal.schema.{HoodieSchemaException, InternalSchema}
import org.apache.hudi.io.storage.HoodieHFileReader
import org.apache.spark.execution.datasources.HoodieInMemoryFileIndex
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.HoodieCatalystExpressionUtils.convertToCatalystExpression
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, SubqueryExpression, UnsafeProjection}
import org.apache.spark.sql.execution.FileRelation
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.{HoodieParquetFileFormat, ParquetFileFormat}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{HoodieCatalystExpressionUtils, Row, SQLContext, SparkSession}
import org.apache.spark.unsafe.types.UTF8String

import java.net.URI
import java.util.Locale
import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

trait HoodieFileSplit {}

case class HoodieTableSchema(structTypeSchema: StructType, avroSchemaStr: String, internalSchema: Option[InternalSchema] = None)

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
                                  schemaSpec: Option[StructType])
  extends BaseRelation
    with FileRelation
    with PrunedFilteredScan
    with Logging
    with SparkAdapterSupport {

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

  /**
   * NOTE: Initialization of teh following members is coupled on purpose to minimize amount of I/O
   *       required to fetch table's Avro and Internal schemas
   */
  protected lazy val (tableAvroSchema: Schema, internalSchemaOpt: Option[InternalSchema]) = {
    val schemaResolver = new TableSchemaResolver(metaClient)
    val internalSchemaOpt = if (!isSchemaEvolutionEnabled) {
      None
    } else {
      Try {
        specifiedQueryTimestamp.map(schemaResolver.getTableInternalSchemaFromCommitMetadata)
          .getOrElse(schemaResolver.getTableInternalSchemaFromCommitMetadata)
      } match {
        case Success(internalSchemaOpt) => toScalaOption(internalSchemaOpt)
        case Failure(e) =>
          logWarning("Failed to fetch internal-schema from the table", e)
          None
      }
    }

    val avroSchema = internalSchemaOpt.map { is =>
      AvroInternalSchemaConverter.convert(is, "schema")
    } orElse {
      specifiedQueryTimestamp.map(schemaResolver.getTableAvroSchema)
    } orElse {
      schemaSpec.map(convertToAvroSchema)
    } getOrElse {
      Try(schemaResolver.getTableAvroSchema) match {
        case Success(schema) => schema
        case Failure(e) =>
          logError("Failed to fetch schema from the table", e)
          throw new HoodieSchemaException("Failed to fetch schema from the table")
      }
    }

    (avroSchema, internalSchemaOpt)
  }

  protected lazy val tableStructSchema: StructType = AvroConversionUtils.convertAvroSchemaToStructType(tableAvroSchema)

  protected val partitionColumns: Array[String] = tableConfig.getPartitionFields.orElse(Array.empty)

  /**
   * Data schema optimized (externally) by Spark's Optimizer.
   *
   * Please check scala-doc for [[updatePrunedDataSchema]] more details
   */
  protected var optimizerPrunedDataSchema: Option[StructType] = None

  /**
   * Controls whether partition values (ie values of partition columns) should be
   * <ol>
   *    <li>Extracted from partition path and appended to individual rows read from the data file (we
   *    delegate this to Spark's [[ParquetFileFormat]])</li>
   *    <li>Read from the data-file as is (by default Hudi persists all columns including partition ones)</li>
   * </ol>
   *
   * This flag is only be relevant in conjunction with the usage of [["hoodie.datasource.write.drop.partition.columns"]]
   * config, when Hudi will NOT be persisting partition columns in the data file, and therefore values for
   * such partition columns (ie "partition values") will have to be parsed from the partition path, and appended
   * to every row only in the fetched dataset.
   *
   * NOTE: Partition values extracted from partition path might be deviating from the values of the original
   *       partition columns: for ex, if originally as partition column was used column [[ts]] bearing epoch
   *       timestamp, which was used by [[TimestampBasedKeyGenerator]] to generate partition path of the format
   *       [["yyyy/mm/dd"]], appended partition value would bear the format verbatim as it was used in the
   *       partition path, meaning that string value of "2022/01/01" will be appended, and not its original
   *       representation
   */
  protected val shouldExtractPartitionValuesFromPartitionPath: Boolean = {
    // Controls whether partition columns (which are the source for the partition path values) should
    // be omitted from persistence in the data files. On the read path it affects whether partition values (values
    // of partition columns) will be read from the data file or extracted from partition path
    val shouldOmitPartitionColumns = metaClient.getTableConfig.shouldDropPartitionColumns && partitionColumns.nonEmpty
    val shouldExtractPartitionValueFromPath =
      optParams.getOrElse(DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.key,
        DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.defaultValue.toString).toBoolean
    shouldOmitPartitionColumns || shouldExtractPartitionValueFromPath
  }

  /**
   * NOTE: This fields are accessed by [[NestedSchemaPruning]] component which is only enabled for
   *       Spark >= 3.1
   */
  lazy val (fileFormat: FileFormat, fileFormatClassName: String) =
    metaClient.getTableConfig.getBaseFileFormat match {
      case HoodieFileFormat.ORC => (new OrcFileFormat, "orc")
      case HoodieFileFormat.PARQUET =>
        // We're delegating to Spark to append partition values to every row only in cases
        // when these corresponding partition-values are not persisted w/in the data file itself
        val parquetFileFormat = sparkAdapter.createHoodieParquetFileFormat(shouldExtractPartitionValuesFromPartitionPath).get
        (parquetFileFormat, HoodieParquetFileFormat.FILE_FORMAT_ID)
    }

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
  val mandatoryFields: Seq[String]

  protected def timeline: HoodieTimeline =
  // NOTE: We're including compaction here since it's not considering a "commit" operation
    metaClient.getCommitsAndCompactionTimeline.filterCompletedInstants

  protected def latestInstant: Option[HoodieInstant] =
    toScalaOption(timeline.lastInstant())

  protected def queryTimestamp: Option[String] =
    specifiedQueryTimestamp.orElse(latestInstant.map(_.getTimestamp))

  /**
   * Returns true in case table supports Schema on Read (Schema Evolution)
   */
  def hasSchemaOnRead: Boolean = internalSchemaOpt.isDefined

  /**
   * Data schema is determined as the actual schema of the Table's Data Files (for ex, parquet/orc/etc);
   *
   * In cases when partition values are not persisted w/in the data files, data-schema is defined as
   * <pre>table's schema - partition columns</pre>
   *
   * Check scala-doc for [[shouldExtractPartitionValuesFromPartitionPath]] for more details
   */
  def dataSchema: StructType = if (shouldExtractPartitionValuesFromPartitionPath) {
    prunePartitionColumns(tableStructSchema)
  } else {
    tableStructSchema
  }

  /**
   * Determines whether relation's schema could be pruned by Spark's Optimizer
   */
  def canPruneRelationSchema: Boolean =
    (fileFormat.isInstanceOf[ParquetFileFormat] || fileFormat.isInstanceOf[OrcFileFormat]) &&
      // NOTE: Some relations might be disabling sophisticated schema pruning techniques (for ex, nested schema pruning)
      // TODO(HUDI-XXX) internal schema doesn't support nested schema pruning currently
      !hasSchemaOnRead

  override def schema: StructType = {
    // NOTE: Optimizer could prune the schema (applying for ex, [[NestedSchemaPruning]] rule) setting new updated
    //       schema in-place (via [[setPrunedDataSchema]] method), therefore we have to make sure that we pick
    //       pruned data schema (if present) over the standard table's one
    optimizerPrunedDataSchema.getOrElse(tableStructSchema)
  }

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
    // NOTE: PLEAS READ CAREFULLY BEFORE MAKING CHANGES
    //
    //       In case list of requested columns doesn't contain the Primary Key one, we
    //       have to add it explicitly so that
    //          - Merging could be performed correctly
    //          - In case 0 columns are to be fetched (for ex, when doing {@code count()} on Spark's [[Dataset]],
    //            Spark still fetches all the rows to execute the query correctly
    //
    //       *Appending* additional columns to the ones requested by the caller is not a problem, as those
    //       will be "projected out" by the caller's projection;
    //
    // (!!!) IT'S CRITICAL TO AVOID REORDERING OF THE REQUESTED COLUMNS AS THIS WILL BREAK THE UPSTREAM
    //       PROJECTION
    val targetColumns: Array[String] = appendMandatoryColumns(requiredColumns)
    // NOTE: We explicitly fallback to default table's Avro schema to make sure we avoid unnecessary Catalyst > Avro
    //       schema conversion, which is lossy in nature (for ex, it doesn't preserve original Avro type-names) and
    //       could have an effect on subsequent de-/serializing records in some exotic scenarios (when Avro unions
    //       w/ more than 2 types are involved)
    val sourceSchema = optimizerPrunedDataSchema.map(convertToAvroSchema).getOrElse(tableAvroSchema)
    val (requiredAvroSchema, requiredStructSchema, requiredInternalSchema) =
      projectSchema(Either.cond(internalSchemaOpt.isDefined, internalSchemaOpt.get, sourceSchema), targetColumns)

    val filterExpressions = convertToExpressions(filters)
    val (partitionFilters, dataFilters) = filterExpressions.partition(isPartitionPredicate)

    val fileSplits = collectFileSplits(partitionFilters, dataFilters)

    val tableAvroSchemaStr = tableAvroSchema.toString

    val tableSchema = HoodieTableSchema(tableStructSchema, tableAvroSchemaStr, internalSchemaOpt)
    val requiredSchema = HoodieTableSchema(requiredStructSchema, requiredAvroSchema.toString, Some(requiredInternalSchema))

    if (fileSplits.isEmpty) {
      sparkSession.sparkContext.emptyRDD
    } else {
      val rdd = composeRDD(fileSplits, tableSchema, requiredSchema, targetColumns, filters)

      // Here we rely on a type erasure, to workaround inherited API restriction and pass [[RDD[InternalRow]]] back as [[RDD[Row]]]
      // Please check [[needConversion]] scala-doc for more details
      rdd.asInstanceOf[RDD[Row]]
    }
  }

  /**
   * Composes RDD provided file splits to read from, table and partition schemas, data filters to be applied
   *
   * @param fileSplits       file splits to be handled by the RDD
   * @param tableSchema      target table's schema
   * @param requiredSchema   projected schema required by the reader
   * @param requestedColumns columns requested by the query
   * @param filters          data filters to be applied
   * @return instance of RDD (holding [[InternalRow]]s)
   */
  protected def composeRDD(fileSplits: Seq[FileSplit],
                           tableSchema: HoodieTableSchema,
                           requiredSchema: HoodieTableSchema,
                           requestedColumns: Array[String],
                           filters: Array[Filter]): RDD[InternalRow]

  /**
   * Provided with partition and date filters collects target file splits to read records from, while
   * performing pruning if necessary
   *
   * @param partitionFilters partition filters to be applied
   * @param dataFilters data filters to be applied
   * @return list of [[FileSplit]] to fetch records from
   */
  protected def collectFileSplits(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[FileSplit]

  /**
   * Get all PartitionDirectories based on globPaths if specified, otherwise use the table path.
   * Will perform pruning if necessary
   */
  private def listPartitionDirectories(globPaths: Seq[Path], partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    if (globPaths.isEmpty) {
      fileIndex.listFiles(partitionFilters, dataFilters)
    } else {
      val inMemoryFileIndex = HoodieInMemoryFileIndex.create(sparkSession, globPaths)
      inMemoryFileIndex.listFiles(partitionFilters, dataFilters)
    }
  }

  /**
   * Get all latest base files with partition paths, if globPaths is empty, will listing files
   * under the table path.
   */
  protected def listLatestBaseFiles(globPaths: Seq[Path], partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Map[Path, Seq[FileStatus]] = {
    val partitionDirs = listPartitionDirectories(globPaths, partitionFilters, dataFilters)
    val fsView = new HoodieTableFileSystemView(metaClient, timeline, partitionDirs.flatMap(_.files).toArray)

    val latestBaseFiles = fsView.getLatestBaseFiles.iterator().asScala.toList.map(_.getFileStatus)

    latestBaseFiles.groupBy(getPartitionPath)
  }

  /**
   * Get all fileSlices(contains base files and log files if exist) from globPaths if not empty,
   * otherwise will use the table path to do the listing.
   */
  protected def listLatestFileSlices(globPaths: Seq[Path], partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[FileSlice] = {
    latestInstant.map { _ =>
      val partitionDirs = listPartitionDirectories(globPaths, partitionFilters, dataFilters)
      val fsView = new HoodieTableFileSystemView(metaClient, timeline, partitionDirs.flatMap(_.files).toArray)

      val queryTimestamp = this.queryTimestamp.get
      fsView.getPartitionPaths.asScala.flatMap { partitionPath =>
        val relativePath = getRelativePartitionPath(new Path(basePath), partitionPath)
        fsView.getLatestMergedFileSlicesBeforeOrOn(relativePath, queryTimestamp).iterator().asScala.toSeq
      }
    }.getOrElse(Seq())
  }

  protected def convertToExpressions(filters: Array[Filter]): Array[Expression] = {
    val catalystExpressions = filters.map(expr => convertToCatalystExpression(expr, tableStructSchema))

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
    // For a nested field in mandatory columns, we should first get the root-level field, and then
    // check for any missing column, as the requestedColumns should only contain root-level fields
    // We should only append root-level field as well
    val missing = mandatoryFields.map(col => HoodieAvroUtils.getRootLevelFieldName(col))
      .filter(rootField => !requestedColumns.contains(rootField))
    requestedColumns ++ missing
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
      if (shouldExtractPartitionValuesFromPartitionPath) {
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
        logWarning(s"Failed to get the right partition InternalRow for file: ${file.toString}", e)
        InternalRow.empty
    }
  }

  protected def getColName(f: StructField): String = {
    if (sparkSession.sessionState.conf.caseSensitiveAnalysis) {
      f.name
    } else {
      f.name.toLowerCase(Locale.ROOT)
    }
  }

  /**
   * Hook for Spark's Optimizer to update expected relation schema after pruning
   *
   * NOTE: Only limited number of optimizations in respect to schema pruning could be performed
   *       internally w/in the relation itself w/o consideration for how the relation output is used.
   *       Therefore more advanced optimizations (like [[NestedSchemaPruning]]) have to be carried out
   *       by Spark's Optimizer holistically evaluating Spark's [[LogicalPlan]]
   */
  def updatePrunedDataSchema(prunedSchema: StructType): this.type = {
    optimizerPrunedDataSchema = Some(prunedSchema)
    this
  }

  /**
   * Returns file-reader routine accepting [[PartitionedFile]] and returning an [[Iterator]]
   * over [[InternalRow]]
   */
  protected def createBaseFileReader(spark: SparkSession,
                                     partitionSchema: StructType,
                                     dataSchema: HoodieTableSchema,
                                     requiredDataSchema: HoodieTableSchema,
                                     filters: Seq[Filter],
                                     options: Map[String, String],
                                     hadoopConf: Configuration): BaseFileReader = {
    val tableBaseFileFormat = tableConfig.getBaseFileFormat

    // NOTE: PLEASE READ CAREFULLY
    //       Lambda returned from this method is going to be invoked on the executor, and therefore
    //       we have to eagerly initialize all of the readers even though only one specific to the type
    //       of the file being read will be used. This is required to avoid serialization of the whole
    //       relation (containing file-index for ex) and passing it to the executor
    val (read: (PartitionedFile => Iterator[InternalRow]), schema: StructType) =
      tableBaseFileFormat match {
        case HoodieFileFormat.PARQUET =>
          val parquetReader = HoodieDataSourceHelper.buildHoodieParquetReader(
            sparkSession = spark,
            dataSchema = dataSchema.structTypeSchema,
            partitionSchema = partitionSchema,
            requiredSchema = requiredDataSchema.structTypeSchema,
            filters = filters,
            options = options,
            hadoopConf = hadoopConf,
            // We're delegating to Spark to append partition values to every row only in cases
            // when these corresponding partition-values are not persisted w/in the data file itself
            appendPartitionValues = shouldExtractPartitionValuesFromPartitionPath
          )
          // Since partition values by default are omitted, and not persisted w/in data-files by Spark,
          // data-file readers (such as [[ParquetFileFormat]]) have to inject partition values while reading
          // the data. As such, actual full schema produced by such reader is composed of
          //    a) Data-file schema (projected or not)
          //    b) Appended partition column values
          val readerSchema = StructType(requiredDataSchema.structTypeSchema.fields ++ partitionSchema.fields)

          (parquetReader, readerSchema)

      case HoodieFileFormat.HFILE =>
        val hfileReader = createHFileReader(
          spark = spark,
          dataSchema = dataSchema,
          requiredDataSchema = requiredDataSchema,
          filters = filters,
          options = options,
          hadoopConf = hadoopConf
        )

        (hfileReader, requiredDataSchema.structTypeSchema)

      case _ => throw new UnsupportedOperationException(s"Base file format is not currently supported ($tableBaseFileFormat)")
    }

    BaseFileReader(
      read = partitionedFile => {
        val extension = FSUtils.getFileExtension(partitionedFile.filePath)
        if (tableBaseFileFormat.getFileExtension.equals(extension)) {
          read(partitionedFile)
        } else {
          throw new UnsupportedOperationException(s"Invalid base-file format ($extension), expected ($tableBaseFileFormat)")
        }
      },
      schema = schema
    )
  }

  protected def embedInternalSchema(conf: Configuration, internalSchemaOpt: Option[InternalSchema]): Configuration = {
    val internalSchema = internalSchemaOpt.getOrElse(InternalSchema.getEmptyInternalSchema)
    val querySchemaString = SerDeHelper.toJson(internalSchema)
    if (!isNullOrEmpty(querySchemaString)) {
      val validCommits = timeline.getInstants.iterator.asScala.map(_.getFileName).mkString(",")

      conf.set(SparkInternalSchemaConverter.HOODIE_QUERY_SCHEMA, SerDeHelper.toJson(internalSchema))
      conf.set(SparkInternalSchemaConverter.HOODIE_TABLE_PATH, metaClient.getBasePath)
      conf.set(SparkInternalSchemaConverter.HOODIE_VALID_COMMITS_LIST, validCommits)
    }
    conf
  }

  protected def tryPrunePartitionColumns(tableSchema: HoodieTableSchema,
                                         requiredSchema: HoodieTableSchema): (StructType, HoodieTableSchema, HoodieTableSchema) = {
    // Since schema requested by the caller might contain partition columns, we might need to
    // prune it, removing all partition columns from it in case these columns are not persisted
    // in the data files
    //
    // NOTE: This partition schema is only relevant to file reader to be able to embed
    //       values of partition columns (hereafter referred to as partition values) encoded into
    //       the partition path, and omitted from the data file, back into fetched rows;
    //       Note that, by default, partition columns are not omitted therefore specifying
    //       partition schema for reader is not required
    if (shouldExtractPartitionValuesFromPartitionPath) {
      val partitionSchema = StructType(partitionColumns.map(StructField(_, StringType)))
      val prunedDataStructSchema = prunePartitionColumns(tableSchema.structTypeSchema)
      val prunedRequiredSchema = prunePartitionColumns(requiredSchema.structTypeSchema)

      (partitionSchema,
        HoodieTableSchema(prunedDataStructSchema, convertToAvroSchema(prunedDataStructSchema).toString),
        HoodieTableSchema(prunedRequiredSchema, convertToAvroSchema(prunedRequiredSchema).toString))
    } else {
      (StructType(Nil), tableSchema, requiredSchema)
    }
  }

  private def prunePartitionColumns(dataStructSchema: StructType): StructType =
    StructType(dataStructSchema.filterNot(f => partitionColumns.contains(f.name)))

  private def isSchemaEvolutionEnabled = {
    // NOTE: Schema evolution could be configured both t/h optional parameters vehicle as well as
    //       t/h Spark Session configuration (for ex, for Spark SQL)
    optParams.getOrElse(DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.key,
      DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.defaultValue.toString).toBoolean ||
      sparkSession.conf.get(DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.key,
        DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.defaultValue.toString).toBoolean
  }
}

object HoodieBaseRelation extends SparkAdapterSupport {

  case class BaseFileReader(read: PartitionedFile => Iterator[InternalRow], val schema: StructType) {
    def apply(file: PartitionedFile): Iterator[InternalRow] = read.apply(file)
  }

  def generateUnsafeProjection(from: StructType, to: StructType): UnsafeProjection =
    HoodieCatalystExpressionUtils.generateUnsafeProjection(from, to)

  def convertToAvroSchema(structSchema: StructType): Schema =
    sparkAdapter.getAvroSchemaConverters.toAvroType(structSchema, nullable = false, "Record")

  def getPartitionPath(fileStatus: FileStatus): Path =
    fileStatus.getPath.getParent

  /**
   * Projects provided file reader's output from its original schema, into a [[requiredSchema]]
   *
   * NOTE: [[requiredSchema]] has to be a proper subset of the file reader's schema
   *
   * @param reader file reader to be projected
   * @param requiredSchema target schema for the output of the provided file reader
   */
  def projectReader(reader: BaseFileReader, requiredSchema: StructType): BaseFileReader = {
    checkState(reader.schema.fields.toSet.intersect(requiredSchema.fields.toSet).size == requiredSchema.size)

    if (reader.schema == requiredSchema) {
      reader
    } else {
      val read = reader.apply(_)
      val projectedRead: PartitionedFile => Iterator[InternalRow] = (file: PartitionedFile) => {
        // NOTE: Projection is not a serializable object, hence it creation should only happen w/in
        //       the executor process
        val unsafeProjection = generateUnsafeProjection(reader.schema, requiredSchema)
        read(file).map(unsafeProjection)
      }

      BaseFileReader(projectedRead, requiredSchema)
    }
  }

  /**
   * Projects provided schema by picking only required (projected) top-level columns from it
   *
   * @param tableSchema schema to project (either of [[InternalSchema]] or Avro's [[Schema]])
   * @param requiredColumns required top-level columns to be projected
   */
  def projectSchema(tableSchema: Either[Schema, InternalSchema], requiredColumns: Array[String]): (Schema, StructType, InternalSchema) = {
    tableSchema match {
      case Right(internalSchema) =>
        checkState(!internalSchema.isEmptySchema)
        val prunedInternalSchema = InternalSchemaUtils.pruneInternalSchema(internalSchema, requiredColumns.toList.asJava)
        val requiredAvroSchema = AvroInternalSchemaConverter.convert(prunedInternalSchema, "schema")
        val requiredStructSchema = AvroConversionUtils.convertAvroSchemaToStructType(requiredAvroSchema)

        (requiredAvroSchema, requiredStructSchema, prunedInternalSchema)

      case Left(avroSchema) =>
        val fieldMap = avroSchema.getFields.asScala.map(f => f.name() -> f).toMap
        val requiredFields = requiredColumns.map { col =>
          val f = fieldMap(col)
          // We have to create a new [[Schema.Field]] since Avro schemas can't share field
          // instances (and will throw "org.apache.avro.AvroRuntimeException: Field already used")
          new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal(), f.order())
        }.toList
        val requiredAvroSchema = Schema.createRecord(avroSchema.getName, avroSchema.getDoc,
          avroSchema.getNamespace, avroSchema.isError, requiredFields.asJava)
        val requiredStructSchema = AvroConversionUtils.convertAvroSchemaToStructType(requiredAvroSchema)

        (requiredAvroSchema, requiredStructSchema, InternalSchema.getEmptyInternalSchema)
    }
  }

  private def createHFileReader(spark: SparkSession,
                                dataSchema: HoodieTableSchema,
                                requiredDataSchema: HoodieTableSchema,
                                filters: Seq[Filter],
                                options: Map[String, String],
                                hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val hadoopConfBroadcast =
      spark.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    partitionedFile => {
      val hadoopConf = hadoopConfBroadcast.value.get()
      val reader = new HoodieHFileReader[GenericRecord](hadoopConf, new Path(partitionedFile.filePath),
        new CacheConfig(hadoopConf))

      val requiredRowSchema = requiredDataSchema.structTypeSchema
      // NOTE: Schema has to be parsed at this point, since Avro's [[Schema]] aren't serializable
      //       to be passed from driver to executor
      val requiredAvroSchema = new Schema.Parser().parse(requiredDataSchema.avroSchemaStr)
      val avroToRowConverter = AvroConversionUtils.createAvroToInternalRowConverter(requiredAvroSchema, requiredRowSchema)

      reader.getRecordIterator(requiredAvroSchema).asScala
        .map(record => {
          avroToRowConverter.apply(record).get
        })
    }
  }
}
