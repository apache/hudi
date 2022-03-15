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
import org.apache.hadoop.fs.{FileStatus, Path, PathFilter}
import org.apache.hadoop.hbase.io.hfile.CacheConfig
import org.apache.hadoop.mapred.JobConf
import org.apache.hudi.HoodieBaseRelation.{getPartitionPath, isMetadataTable}
import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.common.config.SerializableConfiguration
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieFileFormat, HoodieRecord}
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.hadoop.HoodieROTablePathFilter
import org.apache.hudi.io.storage.HoodieHFileReader
import org.apache.hudi.metadata.{HoodieMetadataPayload, HoodieTableMetadata}
import org.apache.spark.execution.datasources.HoodieInMemoryFileIndex
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, SubqueryExpression}
import org.apache.spark.sql.execution.datasources.{FileStatusCache, PartitionDirectory, PartitionedFile}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

import scala.collection.JavaConverters._
import scala.util.Try

trait HoodieFileSplit {}

case class HoodieTableSchema(structTypeSchema: StructType, avroSchemaStr: String)

case class HoodieTableState(recordKeyField: String,
                            preCombineFieldOpt: Option[String])

/**
 * Hoodie BaseRelation which extends [[PrunedFilteredScan]].
 */
abstract class HoodieBaseRelation(val sqlContext: SQLContext,
                                  val metaClient: HoodieTableMetaClient,
                                  val optParams: Map[String, String],
                                  userSchema: Option[StructType])
  extends BaseRelation with PrunedFilteredScan with Logging {

  type FileSplit <: HoodieFileSplit

  imbueConfigs(sqlContext)

  protected val sparkSession: SparkSession = sqlContext.sparkSession

  protected lazy val conf: Configuration = new Configuration(sqlContext.sparkContext.hadoopConfiguration)
  protected lazy val jobConf = new JobConf(conf)

  protected lazy val tableConfig: HoodieTableConfig = metaClient.getTableConfig

  protected lazy val basePath: String = metaClient.getBasePath

  // If meta fields are enabled, always prefer key from the meta field as opposed to user-specified one
  // NOTE: This is historical behavior which is preserved as is
  protected lazy val recordKeyField: String =
    if (tableConfig.populateMetaFields()) HoodieRecord.RECORD_KEY_METADATA_FIELD
    else tableConfig.getRecordKeyFieldProp

  protected lazy val preCombineFieldOpt: Option[String] = getPrecombineFieldProperty

  protected lazy val specifiedQueryTimestamp: Option[String] =
    optParams.get(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key)
      .map(HoodieSqlCommonUtils.formatQueryInstant)

  protected lazy val tableAvroSchema: Schema = {
    val schemaUtil = new TableSchemaResolver(metaClient)
    Try(schemaUtil.getTableAvroSchema).getOrElse(
      // If there is no commit in the table, we can't get the schema
      // t/h [[TableSchemaResolver]], fallback to the provided [[userSchema]] instead.
      userSchema match {
        case Some(s) => SchemaConverters.toAvroType(s)
        case _ => throw new IllegalArgumentException("User-provided schema is required in case the table is empty")
      }
    )
  }

  protected val tableStructSchema: StructType = AvroConversionUtils.convertAvroSchemaToStructType(tableAvroSchema)

  protected val partitionColumns: Array[String] = tableConfig.getPartitionFields.orElse(Array.empty)

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
   * @VisibleInTests
   */
  lazy val mandatoryColumns: Seq[String] = {
    if (isMetadataTable(metaClient)) {
      Seq(HoodieMetadataPayload.KEY_FIELD_NAME, HoodieMetadataPayload.SCHEMA_FIELD_NAME_TYPE)
    } else {
      // TODO this is MOR table requirement, not necessary for COW
      Seq(recordKeyField) ++ preCombineFieldOpt.map(Seq(_)).getOrElse(Seq())
    }
  }

  protected def timeline: HoodieTimeline =
  // NOTE: We're including compaction here since it's not considering a "commit" operation
    metaClient.getCommitsAndCompactionTimeline.filterCompletedInstants

  protected def latestInstant: Option[HoodieInstant] =
    toScalaOption(timeline.lastInstant())

  protected def queryTimestamp: Option[String] = {
    specifiedQueryTimestamp.orElse(toScalaOption(timeline.lastInstant()).map(i => i.getTimestamp))
  }

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

    val (requiredAvroSchema, requiredStructSchema) =
      HoodieSparkUtils.getRequiredSchema(tableAvroSchema, fetchedColumns)

    val filterExpressions = convertToExpressions(filters)
    val (partitionFilters, dataFilters) = filterExpressions.partition(isPartitionPredicate)

    val fileSplits = collectFileSplits(partitionFilters, dataFilters)

    val partitionSchema = StructType(Nil)
    val tableSchema = HoodieTableSchema(tableStructSchema, tableAvroSchema.toString)
    val requiredSchema = HoodieTableSchema(requiredStructSchema, requiredAvroSchema.toString)

    // Here we rely on a type erasure, to workaround inherited API restriction and pass [[RDD[InternalRow]]] back as [[RDD[Row]]]
    // Please check [[needConversion]] scala-doc for more details
    if (fileSplits.nonEmpty)
      composeRDD(fileSplits, partitionSchema, tableSchema, requiredSchema, filters).asInstanceOf[RDD[Row]]
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
    val missing = mandatoryColumns.filter(col => !requestedColumns.contains(col))
    requestedColumns ++ missing
  }

  private def getPrecombineFieldProperty: Option[String] =
    Option(tableConfig.getPreCombineField)
      .orElse(optParams.get(DataSourceWriteOptions.PRECOMBINE_FIELD.key)) match {
      // NOTE: This is required to compensate for cases when empty string is used to stub
      //       property value to avoid it being set with the default value
      // TODO(HUDI-3456) cleanup
      case Some(f) if !StringUtils.isNullOrEmpty(f) => Some(f)
      case _ => None
    }

  private def imbueConfigs(sqlContext: SQLContext): Unit = {
    sqlContext.sparkSession.sessionState.conf.setConfString("spark.sql.parquet.filterPushdown", "true")
    sqlContext.sparkSession.sessionState.conf.setConfString("spark.sql.parquet.recordLevelFilter.enabled", "true")
    // TODO(HUDI-3639) vectorized reader has to be disabled to make sure MORIncrementalRelation is working properly
    sqlContext.sparkSession.sessionState.conf.setConfString("spark.sql.parquet.enableVectorizedReader", "false")
  }
}

object HoodieBaseRelation {

  def getPartitionPath(fileStatus: FileStatus): Path =
    fileStatus.getPath.getParent

  def isMetadataTable(metaClient: HoodieTableMetaClient): Boolean =
    HoodieTableMetadata.isMetadataTable(metaClient.getBasePath)

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
