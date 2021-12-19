/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import org.apache.avro.Schema
import org.apache.hudi.common.model.HoodieLogFile
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.hadoop.utils.HoodieRealtimeInputFormatUtils
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileStatusCache, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.hudi.HoodieSqlUtils
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

case class HoodieMergeOnReadFileSplit(dataFile: Option[PartitionedFile],
                                      logPaths: Option[List[String]],
                                      latestCommit: String,
                                      tablePath: String,
                                      maxCompactionMemoryInBytes: Long,
                                      mergeType: String)

case class HoodieMergeOnReadTableState(tableStructSchema: StructType,
                                       requiredStructSchema: StructType,
                                       tableAvroSchema: String,
                                       requiredAvroSchema: String,
                                       hoodieRealtimeFileSplits: List[HoodieMergeOnReadFileSplit],
                                       preCombineField: Option[String],
                                       recordKeyFieldOpt: Option[String])

class MergeOnReadSnapshotRelation(val sqlContext: SQLContext,
                                  val optParams: Map[String, String],
                                  val userSchema: StructType,
                                  val globPaths: Option[Seq[Path]],
                                  val metaClient: HoodieTableMetaClient)
  extends BaseRelation with PrunedFilteredScan with Logging {

  private val conf = sqlContext.sparkContext.hadoopConfiguration
  private val jobConf = new JobConf(conf)
  // use schema from latest metadata, if not present, read schema from the data file
  private val schemaUtil = new TableSchemaResolver(metaClient)
  private lazy val tableAvroSchema = {
    try {
      schemaUtil.getTableAvroSchema
    } catch {
      case _: Throwable => // If there is no commit in the table, we cann't get the schema
        // with schemaUtil, use the userSchema instead.
        SchemaConverters.toAvroType(userSchema)
    }
  }

  private lazy val tableStructSchema = AvroConversionUtils.convertAvroSchemaToStructType(tableAvroSchema)
  private val mergeType = optParams.getOrElse(
    DataSourceReadOptions.REALTIME_MERGE.key,
    DataSourceReadOptions.REALTIME_MERGE.defaultValue)
  private val maxCompactionMemoryInBytes = getMaxCompactionMemoryInBytes(jobConf)
  private val preCombineField = {
    val preCombineFieldFromTableConfig = metaClient.getTableConfig.getPreCombineField
    if (preCombineFieldFromTableConfig != null) {
      Some(preCombineFieldFromTableConfig)
    } else {
      // get preCombineFiled from the options if this is a old table which have not store
      // the field to hoodie.properties
      optParams.get(DataSourceReadOptions.READ_PRE_COMBINE_FIELD.key)
    }
  }
  private var recordKeyFieldOpt = Option.empty[String]
  if (!metaClient.getTableConfig.populateMetaFields()) {
    recordKeyFieldOpt = Option(metaClient.getTableConfig.getRecordKeyFieldProp)
  }
  override def schema: StructType = tableStructSchema

  override def needConversion: Boolean = false

  private val specifiedQueryInstant = optParams.get(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key)
    .map(HoodieSqlUtils.formatQueryInstant)

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    log.debug(s" buildScan requiredColumns = ${requiredColumns.mkString(",")}")
    log.debug(s" buildScan filters = ${filters.mkString(",")}")

    val (requiredAvroSchema, requiredStructSchema) =
      MergeOnReadSnapshotRelation.getRequiredSchema(tableAvroSchema, requiredColumns)
    val fileIndex = buildFileIndex(filters)
    val hoodieTableState = HoodieMergeOnReadTableState(
      tableStructSchema,
      requiredStructSchema,
      tableAvroSchema.toString,
      requiredAvroSchema.toString,
      fileIndex,
      preCombineField,
      recordKeyFieldOpt
    )
    val fullSchemaParquetReader = new ParquetFileFormat().buildReaderWithPartitionValues(
      sparkSession = sqlContext.sparkSession,
      dataSchema = tableStructSchema,
      partitionSchema = StructType(Nil),
      requiredSchema = tableStructSchema,
      filters = Seq.empty,
      options = optParams,
      hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf()
    )
    val requiredSchemaParquetReader = new ParquetFileFormat().buildReaderWithPartitionValues(
      sparkSession = sqlContext.sparkSession,
      dataSchema = tableStructSchema,
      partitionSchema = StructType(Nil),
      requiredSchema = requiredStructSchema,
      filters = filters,
      options = optParams,
      hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf()
    )

    val rdd = new HoodieMergeOnReadRDD(
      sqlContext.sparkContext,
      jobConf,
      fullSchemaParquetReader,
      requiredSchemaParquetReader,
      hoodieTableState
    )
    rdd.asInstanceOf[RDD[Row]]
  }

  def buildFileIndex(filters: Array[Filter]): List[HoodieMergeOnReadFileSplit] = {
    if (globPaths.isDefined) {
      // Load files from the global paths if it has defined to be compatible with the original mode
      val inMemoryFileIndex = HoodieSparkUtils.createInMemoryFileIndex(sqlContext.sparkSession, globPaths.get)
      val fsView = new HoodieTableFileSystemView(metaClient,
        // file-slice after pending compaction-requested instant-time is also considered valid
        metaClient.getCommitsAndCompactionTimeline.filterCompletedAndCompactionInstants,
        inMemoryFileIndex.allFiles().toArray)
      val partitionPaths = fsView.getLatestBaseFiles.iterator().asScala.toList.map(_.getFileStatus.getPath.getParent)


      if (partitionPaths.isEmpty) { // If this an empty table, return an empty split list.
        List.empty[HoodieMergeOnReadFileSplit]
      } else {
        val lastInstant = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants.lastInstant()
        if (!lastInstant.isPresent) { // Return empty list if the table has no commit
          List.empty
        } else {
          val queryInstant = specifiedQueryInstant.getOrElse(lastInstant.get().getTimestamp)
          val baseAndLogsList = HoodieRealtimeInputFormatUtils.groupLogsByBaseFile(conf, partitionPaths.asJava).asScala
          val fileSplits = baseAndLogsList.map(kv => {
            val baseFile = kv.getLeft
            val logPaths = if (kv.getRight.isEmpty) Option.empty else Option(kv.getRight.asScala.toList)

            val baseDataPath = if (baseFile.isPresent) {
              Some(PartitionedFile(
                InternalRow.empty,
                MergeOnReadSnapshotRelation.getFilePath(baseFile.get.getFileStatus.getPath),
                0, baseFile.get.getFileLen)
              )
            } else {
              None
            }
            HoodieMergeOnReadFileSplit(baseDataPath, logPaths, queryInstant,
              metaClient.getBasePath, maxCompactionMemoryInBytes, mergeType)
          }).toList
          fileSplits
        }
      }
    } else {
      // Load files by the HoodieFileIndex.
      val hoodieFileIndex = HoodieFileIndex(sqlContext.sparkSession, metaClient,
        Some(tableStructSchema), optParams, FileStatusCache.getOrCreate(sqlContext.sparkSession))

      // Get partition filter and convert to catalyst expression
      val partitionColumns = hoodieFileIndex.partitionSchema.fieldNames.toSet
      val partitionFilters = filters.filter(f => f.references.forall(p => partitionColumns.contains(p)))
      val partitionFilterExpression =
        HoodieSparkUtils.convertToCatalystExpressions(partitionFilters, tableStructSchema)

      // If convert success to catalyst expression, use the partition prune
      val fileSlices = if (partitionFilterExpression.isDefined) {
        hoodieFileIndex.listFileSlices(Seq(partitionFilterExpression.get), Seq.empty)
      } else {
        hoodieFileIndex.listFileSlices(Seq.empty, Seq.empty)
      }

      if (fileSlices.isEmpty) {
        // If this an empty table, return an empty split list.
        List.empty[HoodieMergeOnReadFileSplit]
      } else {
        val fileSplits = fileSlices.values.flatten.map(fileSlice => {
          val latestInstant = metaClient.getActiveTimeline.getCommitsTimeline
            .filterCompletedInstants.lastInstant().get().getTimestamp
          val queryInstant = specifiedQueryInstant.getOrElse(latestInstant)

          val partitionedFile = if (fileSlice.getBaseFile.isPresent) {
            val baseFile = fileSlice.getBaseFile.get()
            val baseFilePath = MergeOnReadSnapshotRelation.getFilePath(baseFile.getFileStatus.getPath)
            Option(PartitionedFile(InternalRow.empty, baseFilePath, 0, baseFile.getFileLen))
          } else {
            Option.empty
          }

          val logPaths = fileSlice.getLogFiles.sorted(HoodieLogFile.getLogFileComparator).iterator().asScala
            .map(logFile => MergeOnReadSnapshotRelation.getFilePath(logFile.getPath)).toList
          val logPathsOptional = if (logPaths.isEmpty) Option.empty else Option(logPaths)
          HoodieMergeOnReadFileSplit(partitionedFile, logPathsOptional, queryInstant, metaClient.getBasePath,
            maxCompactionMemoryInBytes, mergeType)
        }).toList
        fileSplits
      }
    }
  }
}

object MergeOnReadSnapshotRelation {

  def getFilePath(path: Path): String = {
    // Here we use the Path#toUri to encode the path string, as there is a decode in
    // ParquetFileFormat#buildReaderWithPartitionValues in the spark project when read the table
    // .So we should encode the file path here. Otherwise, there is a FileNotException throw
    // out.
    // For example, If the "pt" is the partition path field, and "pt" = "2021/02/02", If
    // we enable the URL_ENCODE_PARTITIONING and write data to hudi table.The data path
    // in the table will just like "/basePath/2021%2F02%2F02/xxxx.parquet". When we read
    // data from the table, if there are no encode for the file path,
    // ParquetFileFormat#buildReaderWithPartitionValues will decode it to
    // "/basePath/2021/02/02/xxxx.parquet" witch will result to a FileNotException.
    // See FileSourceScanExec#createBucketedReadRDD in spark project which do the same thing
    // when create PartitionedFile.
    path.toUri.toString
  }

  def getRequiredSchema(tableAvroSchema: Schema, requiredColumns: Array[String]): (Schema, StructType) = {
    // First get the required avro-schema, then convert the avro-schema to spark schema.
    val name2Fields = tableAvroSchema.getFields.asScala.map(f => f.name() -> f).toMap
    val requiredFields = requiredColumns.map(c => name2Fields(c))
      .map(f => new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal(), f.order())).toList
    val requiredAvroSchema = Schema.createRecord(tableAvroSchema.getName, tableAvroSchema.getDoc,
      tableAvroSchema.getNamespace, tableAvroSchema.isError, requiredFields.asJava)
    val requiredStructSchema = AvroConversionUtils.convertAvroSchemaToStructType(requiredAvroSchema)
    (requiredAvroSchema, requiredStructSchema)
  }
}
