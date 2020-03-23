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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hudi.common.model.HoodieBaseFile
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.util.FSUtils
import org.apache.hudi.hadoop.HoodieParquetInputFormat
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConverters._

class HudiBootstrapRelation(@transient val _sqlContext: SQLContext,
                            val userSchema: StructType,
                            val basePath: String,
                            val optParams: Map[String, String]) extends BaseRelation
  with PrunedScan with Logging {

  val fileIndex: HudiBootstrapFileIndex = buildFileIndex()

  val skeletonSchema: StructType = StructType(Seq(
    StructField("_hoodie_commit_time", StringType, nullable = true),
    StructField("_hoodie_commit_seqno", StringType, nullable = true),
    StructField("_hoodie_record_key", StringType, nullable = true),
    StructField("_hoodie_partition_path", StringType, nullable = true),
    StructField("_hoodie_file_name", StringType, nullable = true)
  ))

  var dataSchema: StructType = _

  var completeSchema: StructType = null

  override def sqlContext: SQLContext = _sqlContext

  override def schema: StructType = {
    if (completeSchema == null) {
      logInfo("Inferring schema..")

      // We need to infer schema from the external data files and then merge the skeleton schema which is fixed
      // to get the complete schema
      val conf = new Configuration()
      val fs = FSUtils.getFs(basePath, conf)

      val externalFileStatus = fs.listStatus(
        fileIndex.files.map(file => new Path(file.getExternalDataFile.get())).toArray)

      val inferredDataSchema = new ParquetFileFormat().inferSchema(
        _sqlContext.sparkSession,
        optParams,
        externalFileStatus
      )

      logInfo("Inferred data schema => " + inferredDataSchema.get.toString())

      dataSchema = inferredDataSchema.get
      completeSchema = StructType(dataSchema.fields ++ skeletonSchema.fields)
      logInfo("Complete schema => " + completeSchema.toString())
    }
    completeSchema
  }

  /**
    * Implementing PrunedScan to support column pruning, by reading only the required columns from the parquet files
    * instead by passing them down to the ParquetFileFormat.
    *
    * TODO: To get better performance with Filters we should implement PrunedFilteredScan push filters down to the
    * parquet files. But this is much more tricky to implement because then with filters being pushed down, unequal
    * number od rows may be returned by external data reader, and skeleton file readers. Merging in this scenario
    * will become much more complicated.
    *
    * @param requiredColumns This contains the columns user has passed in select() or filter() operations on the
    *                        dataframe
    * @return
    */
  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {

    // Compute splits
    val bootstrapSplits = fileIndex.files.map(hoodieBaseFile => {
      val skeletonFile = PartitionedFile(InternalRow.empty, hoodieBaseFile.getPath, 0, hoodieBaseFile.getFileLen)
      val dataFile = PartitionedFile(InternalRow.empty, hoodieBaseFile.getExternalDataFile.get(), 0,
        hoodieBaseFile.getFileLen)
      HudiBootstrapSplit(dataFile, skeletonFile)
    })
    val tableState = HudiBootstrapTableState(bootstrapSplits)

    // Get required schemas for column pruning
    val requiredDataSchema = StructType(dataSchema.filter(field => requiredColumns.contains(field.name)))
    val requiredSkeletonSchema = StructType(skeletonSchema.filter(field => requiredColumns.contains(field.name)))

    // Prepare readers for reading data file and skeleton files
    val dataReadFunction = new ParquetFileFormat()
        .buildReaderWithPartitionValues(
          sparkSession = _sqlContext.sparkSession,
          dataSchema = dataSchema,
          partitionSchema = StructType(Seq.empty),
          requiredSchema = requiredDataSchema,
          filters = Nil,
          options = Map.empty,
          hadoopConf = _sqlContext.sparkSession.sessionState.newHadoopConf()
        )

    val skeletonReadFunction = new ParquetFileFormat()
      .buildReaderWithPartitionValues(
        sparkSession = _sqlContext.sparkSession,
        dataSchema = skeletonSchema,
        partitionSchema = StructType(Seq.empty),
        requiredSchema = requiredSkeletonSchema,
        filters = Nil,
        options = Map.empty,
        hadoopConf = _sqlContext.sparkSession.sessionState.newHadoopConf()
      )

    val rdd = new HudiBootstrapRDD(_sqlContext.sparkSession, dataReadFunction, skeletonReadFunction, requiredDataSchema,
      requiredSkeletonSchema, tableState)

    logInfo("RDD partitions size =>" + rdd.partitions.length)

    val requiredSchema = StructType(requiredDataSchema.fields ++ requiredSkeletonSchema.fields)
    val encoder = RowEncoder(requiredSchema).resolveAndBind()

    logInfo("Row encoder using required schema => " + requiredSchema.toString())
    rdd.map(x => encoder.fromRow(x))
  }

  def buildFileIndex() : HudiBootstrapFileIndex = {
    logInfo("Building file index..")

    val conf = new Configuration()
    val fs = FSUtils.getFs(basePath, conf)

    val metaClient = new HoodieTableMetaClient(fs.getConf, basePath)
    val inputFormat = new HoodieParquetInputFormat()
    inputFormat.setConf(conf)

    val jobConf = new JobConf()
    val partitionPaths = FSUtils.getAllPartitionPaths(fs, basePath, false)
    val fullPartitionPaths = partitionPaths.asScala.map(partitionPath => {
      val fullPartitionPath = basePath + "/" + partitionPath
      fullPartitionPath
    })
    logInfo("Partition paths : " + fullPartitionPaths.mkString(","))

    // Listing using input format listing api.
    jobConf.set("mapreduce.input.fileinputformat.inputdir", fullPartitionPaths.mkString(","))
    val fileStatuses = inputFormat.listStatus(jobConf)
    fileStatuses.foreach(f => logInfo(f.getPath.toString))

    val fsView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline.getCommitsTimeline
      .filterCompletedInstants, fileStatuses)
    val latestFiles: List[HoodieBaseFile] = fsView.getLatestBaseFiles.iterator().asScala.toList

    latestFiles.foreach(file => logInfo("Skeleton file path: " + file.getPath))
    latestFiles.foreach(file => logInfo("External data file path: " + file.getExternalDataFile.get()))

    HudiBootstrapFileIndex(latestFiles)
  }
}

case class HudiBootstrapFileIndex(files: List[HoodieBaseFile])

case class HudiBootstrapTableState(files: List[HudiBootstrapSplit])

case class HudiBootstrapSplit(dataFile: PartitionedFile, skeletonFile: PartitionedFile)
