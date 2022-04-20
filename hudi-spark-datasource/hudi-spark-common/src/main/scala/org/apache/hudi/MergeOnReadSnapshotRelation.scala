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
import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.MergeOnReadSnapshotRelation.getFilePath
import org.apache.hudi.common.fs.FSUtils.getRelativePartitionPath
import org.apache.hudi.common.model.{FileSlice, HoodieLogFile}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.spark.execution.datasources.HoodieInMemoryFileIndex
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

case class HoodieMergeOnReadFileSplit(dataFile: Option[PartitionedFile],
                                      logFiles: List[HoodieLogFile]) extends HoodieFileSplit

class MergeOnReadSnapshotRelation(sqlContext: SQLContext,
                                  optParams: Map[String, String],
                                  userSchema: Option[StructType],
                                  globPaths: Seq[Path],
                                  metaClient: HoodieTableMetaClient)
  extends HoodieBaseRelation(sqlContext, metaClient, optParams, userSchema) {

  override type FileSplit = HoodieMergeOnReadFileSplit

  override lazy val mandatoryColumns: Seq[String] =
    Seq(recordKeyField) ++ preCombineFieldOpt.map(Seq(_)).getOrElse(Seq())

  protected val mergeType: String = optParams.getOrElse(DataSourceReadOptions.REALTIME_MERGE.key,
    DataSourceReadOptions.REALTIME_MERGE.defaultValue)

  override def imbueConfigs(sqlContext: SQLContext): Unit = {
    super.imbueConfigs(sqlContext)
    sqlContext.sparkSession.sessionState.conf.setConfString("spark.sql.parquet.enableVectorizedReader", "true")
  }

  protected override def composeRDD(fileSplits: Seq[HoodieMergeOnReadFileSplit],
                                    partitionSchema: StructType,
                                    dataSchema: HoodieTableSchema,
                                    requiredSchema: HoodieTableSchema,
                                    filters: Array[Filter]): HoodieMergeOnReadRDD = {
    val fullSchemaParquetReader = createBaseFileReader(
      spark = sqlContext.sparkSession,
      partitionSchema = partitionSchema,
      dataSchema = dataSchema,
      requiredSchema = dataSchema,
      // This file-reader is used to read base file records, subsequently merging them with the records
      // stored in delta-log files. As such, we have to read _all_ records from the base file, while avoiding
      // applying any filtering _before_ we complete combining them w/ delta-log records (to make sure that
      // we combine them correctly)
      filters = Seq.empty,
      options = optParams,
      // NOTE: We have to fork the Hadoop Config here as Spark will be modifying it
      //       to configure Parquet reader appropriately
      hadoopConf = HoodieDataSourceHelper.getConfigurationWithInternalSchema(new Configuration(conf), internalSchema, metaClient.getBasePath, validCommits)
    )

    val requiredSchemaParquetReader = createBaseFileReader(
      spark = sqlContext.sparkSession,
      partitionSchema = partitionSchema,
      dataSchema = dataSchema,
      requiredSchema = requiredSchema,
      filters = filters,
      options = optParams,
      // NOTE: We have to fork the Hadoop Config here as Spark will be modifying it
      //       to configure Parquet reader appropriately
      hadoopConf = HoodieDataSourceHelper.getConfigurationWithInternalSchema(new Configuration(conf), requiredSchema.internalSchema, metaClient.getBasePath, validCommits)
    )

    val tableState = getTableState
    new HoodieMergeOnReadRDD(sqlContext.sparkContext, jobConf, fullSchemaParquetReader, requiredSchemaParquetReader,
      dataSchema, requiredSchema, tableState, mergeType, fileSplits)
  }

  protected override def collectFileSplits(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): List[HoodieMergeOnReadFileSplit] = {
    val convertedPartitionFilters =
      HoodieFileIndex.convertFilterForTimestampKeyGenerator(metaClient, partitionFilters)

    if (globPaths.isEmpty) {
      val fileSlices = fileIndex.listFileSlices(convertedPartitionFilters)
      buildSplits(fileSlices.values.flatten.toSeq)
    } else {
      // TODO refactor to avoid iterating over listed files multiple times
      val partitions = listLatestBaseFiles(globPaths, convertedPartitionFilters, dataFilters)
      val partitionPaths = partitions.keys.toSeq
      if (partitionPaths.isEmpty || latestInstant.isEmpty) {
        // If this an empty table OR it has no completed commits yet, return
        List.empty[HoodieMergeOnReadFileSplit]
      } else {
        val fileSlices = listFileSlices(partitionPaths)
        buildSplits(fileSlices)
      }
    }
  }

  protected def buildSplits(fileSlices: Seq[FileSlice]): List[HoodieMergeOnReadFileSplit] = {
    fileSlices.map { fileSlice =>
      val baseFile = toScalaOption(fileSlice.getBaseFile)
      val logFiles = fileSlice.getLogFiles.sorted(HoodieLogFile.getLogFileComparator).iterator().asScala.toList

      val partitionedBaseFile = baseFile.map { file =>
        val filePath = getFilePath(file.getFileStatus.getPath)
        PartitionedFile(getPartitionColumnsAsInternalRow(file.getFileStatus), filePath, 0, file.getFileLen)
      }

      HoodieMergeOnReadFileSplit(partitionedBaseFile, logFiles)
    }.toList
  }

  private def listFileSlices(partitionPaths: Seq[Path]): Seq[FileSlice] = {
    // NOTE: It's critical for us to re-use [[InMemoryFileIndex]] to make sure we're leveraging
    //       [[FileStatusCache]] and avoid listing the whole table again
    val inMemoryFileIndex = HoodieInMemoryFileIndex.create(sparkSession, partitionPaths)
    val fsView = new HoodieTableFileSystemView(metaClient, timeline, inMemoryFileIndex.allFiles.toArray)

    val queryTimestamp = this.queryTimestamp.get

    partitionPaths.flatMap { partitionPath =>
      val relativePath = getRelativePartitionPath(new Path(basePath), partitionPath)
      fsView.getLatestMergedFileSlicesBeforeOrOn(relativePath, queryTimestamp).iterator().asScala.toSeq
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

}
