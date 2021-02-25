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

import com.google.common.annotations.VisibleForTesting
import org.apache.hudi.common.model.HoodieBaseFile
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.utils.HoodieRealtimeInputFormatUtils
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.hudi.utils.PushDownUtils
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, CatalystScan, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{StructField, StructType}

import java.util.Locale
import scala.collection.JavaConverters._
import scala.collection.mutable

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
                                       preCombineField: Option[String])

class MergeOnReadSnapshotRelation(val sqlContext: SQLContext,
                                  val optParams: Map[String, String],
                                  val userSchema: StructType,
                                  val globPaths: Seq[Path],
                                  val metaClient: HoodieTableMetaClient)(val sparkSession: SparkSession)
  extends BaseRelation with CatalystScan with Logging {

  private val conf = sqlContext.sparkContext.hadoopConfiguration
  private val jobConf = new JobConf(conf)
  // use schema from latest metadata, if not present, read schema from the data file
  private val schemaUtil = new TableSchemaResolver(metaClient)
  private val tableAvroSchema = schemaUtil.getTableAvroSchema
  private val tableStructSchema = AvroConversionUtils.convertAvroSchemaToStructType(tableAvroSchema)
  private val mergeType = optParams.getOrElse(
    DataSourceReadOptions.REALTIME_MERGE_OPT_KEY,
    DataSourceReadOptions.DEFAULT_REALTIME_MERGE_OPT_VAL)
  private val maxCompactionMemoryInBytes = getMaxCompactionMemoryInBytes(jobConf)
  private val inMemoryFileIndex = HoodieSparkUtils.createInMemoryFileIndex(sqlContext.sparkSession, Some(tableStructSchema), optParams, globPaths)
  private var fileIndex: List[HoodieMergeOnReadFileSplit] = _
  private val preCombineField = {
    val preCombineFieldFromTableConfig = metaClient.getTableConfig.getPreCombineField
    if (preCombineFieldFromTableConfig != null) {
      Some(preCombineFieldFromTableConfig)
    } else {
      // get preCombineFiled from the options if this is a old table which have not store
      // the field to hoodie.properties
      optParams.get(DataSourceReadOptions.READ_PRE_COMBINE_FIELD)
    }
  }

  val partitionStructSchema = inMemoryFileIndex.partitionSpec().partitionColumns
  val overlappedPartCols = mutable.Map.empty[String, StructField]
  partitionStructSchema.foreach { partitionField =>
    if (tableStructSchema.exists(getColName(_) == getColName(partitionField))) {
      overlappedPartCols += getColName(partitionField) -> partitionField
    }
  }

  // When data and partition schemas have overlapping columns, the output
  // schema respects the order of the data schema for the overlapping columns, and it
  // respects the data types of the partition schema.
  override def schema: StructType = {
    StructType(tableStructSchema.map(f => overlappedPartCols.getOrElse(getColName(f), f)) ++
      partitionStructSchema.filterNot(f => overlappedPartCols.contains(getColName(f))))
  }

  override def needConversion: Boolean = false

  override def buildScan(requiredColumns: Seq[Attribute], filters: Seq[Expression]): RDD[Row] = {
    fileIndex = buildFileIndex(filters)
    val pushedFilters  = PushDownUtils.transformFilter(this,filters)
    log.debug(s" buildScan requiredColumns = ${requiredColumns.mkString(",")}")
    log.debug(s" buildScan filters = ${pushedFilters.mkString(",")}")
    var requiredStructSchema = StructType(Seq())
    requiredColumns.foreach(col => {
      val field = tableStructSchema.find(_.name == col.name)
      if (field.isDefined) {
        requiredStructSchema = requiredStructSchema.add(field.get)
      }
    })
    val requiredAvroSchema = AvroConversionUtils
      .convertStructTypeToAvroSchema(requiredStructSchema, tableAvroSchema.getName, tableAvroSchema.getNamespace)
    val hoodieTableState = HoodieMergeOnReadTableState(
      tableStructSchema,
      requiredStructSchema,
      tableAvroSchema.toString,
      requiredAvroSchema.toString,
      fileIndex,
      preCombineField
    )
    val fullSchemaParquetReader = new ParquetFileFormat().buildReaderWithPartitionValues(
      sparkSession = sqlContext.sparkSession,
      dataSchema = tableStructSchema,
      partitionSchema = partitionStructSchema,
      requiredSchema = requiredStructSchema,
      filters = pushedFilters,
      options = optParams,
      hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf()
    )
    val requiredSchemaParquetReader = new ParquetFileFormat().buildReaderWithPartitionValues(
      sparkSession = sqlContext.sparkSession,
      dataSchema = tableStructSchema,
      partitionSchema = partitionStructSchema,
      requiredSchema = requiredStructSchema,
      filters = pushedFilters,
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

  def buildFileIndex(filters: Seq[Expression]): List[HoodieMergeOnReadFileSplit] = {
    val selectedPartitions = inMemoryFileIndex.listFiles(filters, filters)
    val selectedPartitionsPathMap = selectedPartitions.flatMap(x=>{
      val files = x.files
      val fileMap = files.map(file=>{(file.getPath.getName,x.values)})
      fileMap
    }).toMap
    val fileStatuses = selectedPartitions.flatMap(_.files)
    if (fileStatuses.isEmpty) {
      throw new HoodieException("No files found for reading in user provided path.")
    }

    val fsView = new HoodieTableFileSystemView(metaClient,
      metaClient.getActiveTimeline.getCommitsTimeline
        .filterCompletedInstants, fileStatuses.toArray)
    val latestFiles: List[HoodieBaseFile] = fsView.getLatestBaseFiles.iterator().asScala.toList
    val latestCommit = fsView.getLastInstant.get().getTimestamp
    val fileGroup = HoodieRealtimeInputFormatUtils.groupLogsByBaseFile(conf, latestFiles.asJava).asScala
    val fileSplits = fileGroup.map(kv => {
      val baseFile = kv._1
      val logPaths = if (kv._2.isEmpty) Option.empty else Option(kv._2.asScala.toList)
      val partitionValues = selectedPartitionsPathMap.get(baseFile.getFileName).get
      val partitionedFile = PartitionedFile(partitionValues, baseFile.getPath, 0, baseFile.getFileLen)
      HoodieMergeOnReadFileSplit(Option(partitionedFile), logPaths, latestCommit,
        metaClient.getBasePath, maxCompactionMemoryInBytes, mergeType)
    }).toList
    fileSplits
  }

  private def getColName(f: StructField): String = {
    if (sparkSession.sessionState.conf.caseSensitiveAnalysis) {
      f.name
    } else {
      f.name.toLowerCase(Locale.ROOT)
    }
  }

  @VisibleForTesting
  def getFileIndexPaths = fileIndex.map(x => x.dataFile.get.filePath)
}
