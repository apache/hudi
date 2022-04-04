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

import org.apache.hadoop.fs.Path
import org.apache.hudi.common.model.HoodieBaseFile
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.exception.HoodieException
import org.apache.spark.execution.datasources.HoodieInMemoryFileIndex
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileStatusCache, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/**
  * This is Spark relation that can be used for querying metadata/fully bootstrapped query hoodie tables, as well as
  * non-bootstrapped tables. It implements PrunedFilteredScan interface in order to support column pruning and filter
  * push-down. For metadata bootstrapped files, if we query columns from both metadata and actual data then it will
  * perform a merge of both to return the result.
  *
  * Caveat: Filter push-down does not work when querying both metadata and actual data columns over metadata
  * bootstrapped files, because then the metadata file and data file can return different number of rows causing errors
  * merging.
  *
  * @param _sqlContext Spark SQL Context
  * @param userSchema User specified schema in the datasource query
  * @param globPaths  The global paths to query. If it not none, read from the globPaths,
  *                   else read data from tablePath using HoodiFileIndex.
  * @param metaClient Hoodie table meta client
  * @param optParams DataSource options passed by the user
  */
class HoodieBootstrapRelation(@transient val _sqlContext: SQLContext,
                              val userSchema: Option[StructType],
                              val globPaths: Seq[Path],
                              val metaClient: HoodieTableMetaClient,
                              val optParams: Map[String, String]) extends BaseRelation
  with PrunedFilteredScan with Logging {

  val skeletonSchema: StructType = HoodieSparkUtils.getMetaSchema
  var dataSchema: StructType = _
  var fullSchema: StructType = _

  val fileIndex: HoodieBootstrapFileIndex = buildFileIndex()

  override def sqlContext: SQLContext = _sqlContext

  override val needConversion: Boolean = false

  override def schema: StructType = inferFullSchema()

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    logInfo("Starting scan..")

    // Compute splits
    val bootstrapSplits = fileIndex.files.map(hoodieBaseFile => {
      var skeletonFile: Option[PartitionedFile] = Option.empty
      var dataFile: PartitionedFile = null

      if (hoodieBaseFile.getBootstrapBaseFile.isPresent) {
        skeletonFile = Option(PartitionedFile(InternalRow.empty, hoodieBaseFile.getPath, 0, hoodieBaseFile.getFileLen))
        dataFile = PartitionedFile(InternalRow.empty, hoodieBaseFile.getBootstrapBaseFile.get().getPath, 0,
          hoodieBaseFile.getBootstrapBaseFile.get().getFileLen)
      } else {
        dataFile = PartitionedFile(InternalRow.empty, hoodieBaseFile.getPath, 0, hoodieBaseFile.getFileLen)
      }
      HoodieBootstrapSplit(dataFile, skeletonFile)
    })
    val tableState = HoodieBootstrapTableState(bootstrapSplits)

    // Get required schemas for column pruning
    var requiredDataSchema = StructType(Seq())
    var requiredSkeletonSchema = StructType(Seq())
    // requiredColsSchema is the schema of requiredColumns, note that requiredColumns is in a random order
    // so requiredColsSchema is not always equal to (requiredSkeletonSchema.fields ++ requiredDataSchema.fields)
    var requiredColsSchema = StructType(Seq())
    requiredColumns.foreach(col => {
      var field = dataSchema.find(_.name == col)
      if (field.isDefined) {
        requiredDataSchema = requiredDataSchema.add(field.get)
      } else {
        field = skeletonSchema.find(_.name == col)
        requiredSkeletonSchema = requiredSkeletonSchema.add(field.get)
      }
      requiredColsSchema = requiredColsSchema.add(field.get)
    })

    // Prepare readers for reading data file and skeleton files
    val dataReadFunction = HoodieDataSourceHelper.buildHoodieParquetReader(
      sparkSession = _sqlContext.sparkSession,
      dataSchema = dataSchema,
      partitionSchema = StructType(Seq.empty),
      requiredSchema = requiredDataSchema,
      filters = if (requiredSkeletonSchema.isEmpty) filters else Seq() ,
      options = optParams,
      hadoopConf = _sqlContext.sparkSession.sessionState.newHadoopConf()
    )

    val skeletonReadFunction = HoodieDataSourceHelper.buildHoodieParquetReader(
      sparkSession = _sqlContext.sparkSession,
      dataSchema = skeletonSchema,
      partitionSchema = StructType(Seq.empty),
      requiredSchema = requiredSkeletonSchema,
      filters = if (requiredDataSchema.isEmpty) filters else Seq(),
      options = optParams,
      hadoopConf = _sqlContext.sparkSession.sessionState.newHadoopConf()
    )

    val regularReadFunction = HoodieDataSourceHelper.buildHoodieParquetReader(
      sparkSession = _sqlContext.sparkSession,
      dataSchema = fullSchema,
      partitionSchema = StructType(Seq.empty),
      requiredSchema = requiredColsSchema,
      filters = filters,
      options = optParams,
      hadoopConf = _sqlContext.sparkSession.sessionState.newHadoopConf()
    )

    val rdd = new HoodieBootstrapRDD(_sqlContext.sparkSession, dataReadFunction, skeletonReadFunction,
      regularReadFunction, requiredDataSchema, requiredSkeletonSchema, requiredColumns, tableState)
    rdd.asInstanceOf[RDD[Row]]
  }

  def inferFullSchema(): StructType = {
    if (fullSchema == null) {
      logInfo("Inferring schema..")
      val schemaResolver = new TableSchemaResolver(metaClient)
      val tableSchema = schemaResolver.getTableAvroSchemaWithoutMetadataFields
      dataSchema = AvroConversionUtils.convertAvroSchemaToStructType(tableSchema)
      fullSchema = StructType(skeletonSchema.fields ++ dataSchema.fields)
    }
    fullSchema
  }

  def buildFileIndex(): HoodieBootstrapFileIndex = {
    logInfo("Building file index..")
    val fileStatuses  = if (globPaths.nonEmpty) {
      // Load files from the global paths if it has defined to be compatible with the original mode
      val inMemoryFileIndex = HoodieInMemoryFileIndex.create(_sqlContext.sparkSession, globPaths)
      inMemoryFileIndex.allFiles()
    } else { // Load files by the HoodieFileIndex.
        HoodieFileIndex(sqlContext.sparkSession, metaClient, Some(schema), optParams,
          FileStatusCache.getOrCreate(sqlContext.sparkSession)).allFiles
    }
    if (fileStatuses.isEmpty) {
      throw new HoodieException("No files found for reading in user provided path.")
    }

    val fsView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline.getCommitsTimeline
      .filterCompletedInstants, fileStatuses.toArray)
    val latestFiles: List[HoodieBaseFile] = fsView.getLatestBaseFiles.iterator().asScala.toList

    if (log.isDebugEnabled) {
      latestFiles.foreach(file => {
        logDebug("Printing indexed files:")
        if (file.getBootstrapBaseFile.isPresent) {
          logDebug("Skeleton File: " + file.getPath + ", Data File: " + file.getBootstrapBaseFile.get().getPath)
        } else {
          logDebug("Regular Hoodie File: " + file.getPath)
        }
      })
    }

    HoodieBootstrapFileIndex(latestFiles)
  }
}

case class HoodieBootstrapFileIndex(files: List[HoodieBaseFile])

case class HoodieBootstrapTableState(files: List[HoodieBootstrapSplit])

case class HoodieBootstrapSplit(dataFile: PartitionedFile, skeletonFile: Option[PartitionedFile])
