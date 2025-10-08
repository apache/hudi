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

package org.apache.hudi.incremental

import org.apache.hudi.{AvroConversionUtils, DataSourceOptionsHelper, DataSourceReadOptions, HoodieFileIndex, HoodieSparkUtils, SparkAdapterSupport}
import org.apache.hudi.AvroConversionUtils.getAvroSchemaWithDefaults
import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.HoodieSparkUtils.sparkAdapter
import org.apache.hudi.common.fs.FSUtils.getRelativePartitionPath
import org.apache.hudi.common.model.{FileSlice, HoodieRecord}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.table.timeline.TimelineUtils.validateTimestampAsOf
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToStoragePath
import org.apache.hudi.incremental.HoodieBaseRelation.{convertToAvroSchema, isSchemaEvolutionEnabledOnRead, metaFieldNames}
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter
import org.apache.hudi.storage.StoragePathInfo

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession,SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.hudi.{HoodieSqlCommonUtils, ProvidesHoodieConfig}
import org.apache.spark.sql.sources.PrunedFilteredScan
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

case class HoodieTableSchema(structTypeSchema: StructType, avroSchemaStr: String, internalSchema: Option[InternalSchema] = None)


/**
 * Hoodie BaseRelation which extends [[PrunedFilteredScan]]
 */
abstract class HoodieBaseRelation(val sqlContext: SQLContext,
                                  val metaClient: HoodieTableMetaClient,
                                  val optParams: Map[String, String],
                                  private val schemaSpec: Option[StructType])
  extends Logging {

  protected val sparkSession: SparkSession = sqlContext.sparkSession

  protected lazy val resolver: Resolver = sparkSession.sessionState.analyzer.resolver

  protected def tableName: String = metaClient.getTableConfig.getTableName

  protected lazy val conf: Configuration = new Configuration(sqlContext.sparkContext.hadoopConfiguration)

  protected lazy val tableConfig: HoodieTableConfig = metaClient.getTableConfig

  protected lazy val basePath: Path = new Path(metaClient.getBasePath.toUri)

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

  protected lazy val orderingFields: List[String] = {
    val tableOrderingFields = tableConfig.getOrderingFields
    if (tableOrderingFields.isEmpty) {
      DataSourceOptionsHelper.getPreCombineFields(optParams)
        .orElse(java.util.Collections.emptyList[String])
        .asScala.toList
    } else {
      tableOrderingFields.asScala.toList
    }
  }

  protected lazy val specifiedQueryTimestamp: Option[String] =
    optParams.get(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key)
      .map(HoodieSqlCommonUtils.formatQueryInstant)

  /**
   * NOTE: Initialization of teh following members is coupled on purpose to minimize amount of I/O
   *       required to fetch table's Avro and Internal schemas
   */
  protected lazy val tableAvroSchema: Schema = {
    val schemaResolver = new TableSchemaResolver(metaClient)
    val internalSchemaOpt = if (!isSchemaEvolutionEnabledOnRead(optParams, sparkSession)) {
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

    val (name, namespace) = AvroConversionUtils.getAvroRecordNameAndNamespace(tableName)
    val avroSchema = internalSchemaOpt.map { is =>
      AvroInternalSchemaConverter.convert(is, namespace + "." + name)
    } orElse {
      specifiedQueryTimestamp.map(schemaResolver.getTableAvroSchema)
    } orElse {
      schemaSpec.map(s => convertToAvroSchema(s, tableName))
    } getOrElse {
      Try(schemaResolver.getTableAvroSchema) match {
        case Success(schema) => schema
        case Failure(e) => throw e
      }
    }
    avroSchema
  }

  protected lazy val tableStructSchema: StructType = {
    val converted = AvroConversionUtils.convertAvroSchemaToStructType(tableAvroSchema)
    val metaFieldMetadata = sparkAdapter.createCatalystMetadataForMetaField

    // NOTE: Here we annotate meta-fields with corresponding metadata such that Spark (>= 3.2)
    //       is able to recognize such fields as meta-fields
    StructType(converted.map { field =>
      if (metaFieldNames.exists(metaFieldName => resolver(metaFieldName, field.name))) {
        field.copy(metadata = metaFieldMetadata)
      } else {
        field
      }
    })
  }

  protected lazy val partitionColumns: Array[String] = tableConfig.getPartitionFields.orElse(Array.empty)

  /**
   * NOTE: PLEASE READ THIS CAREFULLY
   *
   * Even though [[HoodieFileIndex]] initializes eagerly listing all of the files w/in the given Hudi table,
   * this variable itself is _lazy_ (and have to stay that way) which guarantees that it's not initialized, until
   * it's actually accessed
   */
  lazy val fileIndex: HoodieFileIndex =
    HoodieFileIndex(sparkSession, metaClient, Some(tableStructSchema), optParams,
      FileStatusCache.getOrCreate(sparkSession), shouldIncludeLogFiles())

  /**
   * Columns that relation has to read from the storage to properly execute on its semantic: for ex,
   * for Merge-on-Read tables key fields as well and precombine field comprise mandatory set of columns,
   * meaning that regardless of whether this columns are being requested by the query they will be fetched
   * regardless so that relation is able to combine records properly (if necessary)
   *
   * @VisibleInTests
   */
  /**
   * NOTE: These are the fields that are required to properly fulfil Merge-on-Read (MOR)
   *       semantic:
   *
   *       <ol>
   *         <li>Primary key is required to make sure we're able to correlate records from the base
   *         file with the updated records from the delta-log file</li>
   *         <li>Pre-combine key is required to properly perform the combining (or merging) of the
   *         existing and updated records</li>
   *       </ol>
   *
   *       However, in cases when merging is NOT performed (for ex, if file-group only contains base
   *       files but no delta-log files, or if the query-type is equal to [["skip_merge"]]) neither
   *       of primary-key or pre-combine-key are required to be fetched from storage (unless requested
   *       by the query), therefore saving on throughput
   */
  private lazy val mandatoryFieldsForMerging: Seq[String] =
    Seq(recordKeyField) ++ orderingFields

  lazy val mandatoryFields: Seq[String] = mandatoryFieldsForMerging

  protected def timeline: HoodieTimeline =
  // NOTE: We're including compaction here since it's not considering a "commit" operation
    metaClient.getCommitsAndCompactionTimeline.filterCompletedInstants

  private def queryTimestamp: Option[String] =
    specifiedQueryTimestamp.orElse(toScalaOption(timeline.lastInstant()).map(_.requestedTime))


  def inputFiles: Array[String] = fileIndex.allBaseFiles.map(_.getPath.toUri.toString).toArray


  protected def listLatestFileSlices(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[FileSlice] = {
    queryTimestamp match {
      case Some(ts) =>
        specifiedQueryTimestamp.foreach(t => validateTimestampAsOf(metaClient, t))

        val partitionDirs = fileIndex.listFiles(partitionFilters, dataFilters)
        val fsView = new HoodieTableFileSystemView(
          metaClient, timeline, sparkAdapter.getSparkPartitionedFileUtils.toFileStatuses(partitionDirs)
            .map(fileStatus => HadoopFSUtils.convertToStoragePathInfo(fileStatus))
            .asJava)

        fsView.getPartitionPaths.asScala.flatMap { partitionPath =>
          val relativePath = getRelativePartitionPath(convertToStoragePath(basePath), partitionPath)
          fsView.getLatestMergedFileSlicesBeforeOrOn(relativePath, ts).iterator().asScala
        }.toSeq

      case _ => Seq()
    }
  }

  protected def getPartitionColumnValuesAsInternalRow(file: StoragePathInfo): InternalRow = {
    val tablePathWithoutScheme = metaClient.getBasePath.getPathWithoutSchemeAndAuthority
    val partitionPathWithoutScheme = file.getPath.getParent.getPathWithoutSchemeAndAuthority
    val relativePath = tablePathWithoutScheme.toUri.relativize(partitionPathWithoutScheme.toUri).toString
    val timeZoneId = conf.get("timeZone", sparkSession.sessionState.conf.sessionLocalTimeZone)
    val rowValues = HoodieSparkUtils.parsePartitionColumnValues(
      partitionColumns,
      relativePath,
      metaClient.getBasePath,
      tableStructSchema,
      tableConfig.propsMap,
      timeZoneId,
      conf.getBoolean("spark.sql.sources.validatePartitionColumns", true))
    if(rowValues.length != partitionColumns.length) {
      throw new HoodieException("Failed to get partition column values from the partition-path:"
        + s"partition column size: ${partitionColumns.length}, parsed partition value size: ${rowValues.length}")
    }
    InternalRow.fromSeq(rowValues)
  }

  /**
   * Determines if fileIndex should consider log files when filtering file slices. Defaults to false.
   * The subclass can have their own implementation based on the table or relation type.
   */
  protected def shouldIncludeLogFiles(): Boolean = {
    false
  }
}

object HoodieBaseRelation extends SparkAdapterSupport {

  private lazy val metaFieldNames = HoodieRecord.HOODIE_META_COLUMNS.asScala.toSet

  def convertToAvroSchema(structSchema: StructType, tableName: String ): Schema = {
    val (recordName, namespace) = AvroConversionUtils.getAvroRecordNameAndNamespace(tableName)
    val avroSchema = sparkAdapter.getAvroSchemaConverters.toAvroType(structSchema, nullable = false, recordName, namespace)
    getAvroSchemaWithDefaults(avroSchema, structSchema)
  }

  def getPartitionPath(fileStatus: FileStatus): Path =
    fileStatus.getPath.getParent


  def isSchemaEvolutionEnabledOnRead(optParams: Map[String, String], sparkSession: SparkSession): Boolean = {
    // NOTE: Schema evolution could be configured both t/h optional parameters vehicle as well as
    //       t/h Spark Session configuration (for ex, for Spark SQL)
    optParams.getOrElse(DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.key,
      DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.defaultValue.toString).toBoolean ||
      ProvidesHoodieConfig.isSchemaEvolutionEnabled(sparkSession)
  }
}
