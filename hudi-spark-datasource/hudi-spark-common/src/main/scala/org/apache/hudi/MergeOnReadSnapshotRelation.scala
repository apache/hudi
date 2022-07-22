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
import org.apache.hudi.HoodieBaseRelation.{BaseFileReader, convertToAvroSchema}
import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.MergeOnReadSnapshotRelation.getFilePath
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.common.fs.FSUtils.getRelativePartitionPath
import org.apache.hudi.common.model.{FileSlice, HoodieLogFile}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.spark.execution.datasources.HoodieInMemoryFileIndex
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
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
  protected lazy val mandatoryFieldsForMerging: Seq[String] =
    Seq(recordKeyField) ++ preCombineFieldOpt.map(Seq(_)).getOrElse(Seq())

  override lazy val mandatoryFields: Seq[String] = mandatoryFieldsForMerging

  protected val mergeType: String = optParams.getOrElse(DataSourceReadOptions.REALTIME_MERGE.key,
    DataSourceReadOptions.REALTIME_MERGE.defaultValue)

  override def imbueConfigs(sqlContext: SQLContext): Unit = {
    super.imbueConfigs(sqlContext)
    sqlContext.sparkSession.sessionState.conf.setConfString("spark.sql.parquet.enableVectorizedReader", "true")
  }

  protected override def composeRDD(fileSplits: Seq[HoodieMergeOnReadFileSplit],
                                    tableSchema: HoodieTableSchema,
                                    requiredSchema: HoodieTableSchema,
                                    requestedColumns: Array[String],
                                    filters: Array[Filter]): RDD[InternalRow] = {
    val (partitionSchema, dataSchema, requiredDataSchema) =
      tryPrunePartitionColumns(tableSchema, requiredSchema)

    val requiredFilters = Seq.empty
    val optionalFilters = filters
    val readers = createBaseFileReaders(partitionSchema, dataSchema, requiredDataSchema, requestedColumns, requiredFilters, optionalFilters)

    val tableState = getTableState
    new HoodieMergeOnReadRDD(
      sqlContext.sparkContext,
      config = jobConf,
      fileReaders = readers,
      tableSchema = dataSchema,
      requiredSchema = requiredSchema,
      tableState = tableState,
      mergeType = mergeType,
      fileSplits = fileSplits)
  }

  protected def createBaseFileReaders(partitionSchema: StructType,
                                      dataSchema: HoodieTableSchema,
                                      requiredDataSchema: HoodieTableSchema,
                                      requestedColumns: Array[String],
                                      requiredFilters: Seq[Filter],
                                      optionalFilters: Seq[Filter] = Seq.empty): HoodieMergeOnReadBaseFileReaders = {
    val fullSchemaReader = createBaseFileReader(
      spark = sqlContext.sparkSession,
      partitionSchema = partitionSchema,
      dataSchema = dataSchema,
      requiredDataSchema = dataSchema,
      // This file-reader is used to read base file records, subsequently merging them with the records
      // stored in delta-log files. As such, we have to read _all_ records from the base file, while avoiding
      // applying any filtering _before_ we complete combining them w/ delta-log records (to make sure that
      // we combine them correctly);
      // As such only required filters could be pushed-down to such reader
      filters = requiredFilters,
      options = optParams,
      // NOTE: We have to fork the Hadoop Config here as Spark will be modifying it
      //       to configure Parquet reader appropriately
      hadoopConf = embedInternalSchema(new Configuration(conf), internalSchemaOpt)
    )

    val requiredSchemaReader = createBaseFileReader(
      spark = sqlContext.sparkSession,
      partitionSchema = partitionSchema,
      dataSchema = dataSchema,
      requiredDataSchema = requiredDataSchema,
      // This file-reader is used to read base file records, subsequently merging them with the records
      // stored in delta-log files. As such, we have to read _all_ records from the base file, while avoiding
      // applying any filtering _before_ we complete combining them w/ delta-log records (to make sure that
      // we combine them correctly);
      // As such only required filters could be pushed-down to such reader
      filters = requiredFilters,
      options = optParams,
      // NOTE: We have to fork the Hadoop Config here as Spark will be modifying it
      //       to configure Parquet reader appropriately
      hadoopConf = embedInternalSchema(new Configuration(conf), requiredDataSchema.internalSchema)
    )

    // Check whether fields required for merging were also requested to be fetched
    // by the query:
    //    - In case they were, there's no optimization we could apply here (we will have
    //    to fetch such fields)
    //    - In case they were not, we will provide 2 separate file-readers
    //        a) One which would be applied to file-groups w/ delta-logs (merging)
    //        b) One which would be applied to file-groups w/ no delta-logs or
    //           in case query-mode is skipping merging
    val mandatoryColumns = mandatoryFieldsForMerging.map(HoodieAvroUtils.getRootLevelFieldName)
    if (mandatoryColumns.forall(requestedColumns.contains)) {
      HoodieMergeOnReadBaseFileReaders(
        fullSchemaReader = fullSchemaReader,
        requiredSchemaReader = requiredSchemaReader,
        requiredSchemaReaderSkipMerging = requiredSchemaReader
      )
    } else {
      val prunedRequiredSchema = {
        val superfluousColumnNames = mandatoryColumns.filterNot(requestedColumns.contains)
        val prunedStructSchema =
          StructType(requiredDataSchema.structTypeSchema.fields
            .filterNot(f => superfluousColumnNames.contains(f.name)))

        HoodieTableSchema(prunedStructSchema, convertToAvroSchema(prunedStructSchema).toString)
      }

      val requiredSchemaReaderSkipMerging = createBaseFileReader(
        spark = sqlContext.sparkSession,
        partitionSchema = partitionSchema,
        dataSchema = dataSchema,
        requiredDataSchema = prunedRequiredSchema,
        // This file-reader is only used in cases when no merging is performed, therefore it's safe to push
        // down these filters to the base file readers
        filters = requiredFilters ++ optionalFilters,
        options = optParams,
        // NOTE: We have to fork the Hadoop Config here as Spark will be modifying it
        //       to configure Parquet reader appropriately
        hadoopConf = embedInternalSchema(new Configuration(conf), requiredDataSchema.internalSchema)
      )

      HoodieMergeOnReadBaseFileReaders(
        fullSchemaReader = fullSchemaReader,
        requiredSchemaReader = requiredSchemaReader,
        requiredSchemaReaderSkipMerging = requiredSchemaReaderSkipMerging
      )
    }
  }

  protected override def collectFileSplits(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): List[HoodieMergeOnReadFileSplit] = {
    val convertedPartitionFilters =
      HoodieFileIndex.convertFilterForTimestampKeyGenerator(metaClient, partitionFilters)

    if (globPaths.isEmpty) {
      val fileSlices = fileIndex.listFileSlices(convertedPartitionFilters)
      buildSplits(fileSlices.values.flatten.toSeq)
    } else {
      val fileSlices = listLatestFileSlices(globPaths, partitionFilters, dataFilters)
      buildSplits(fileSlices)
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

  protected def createMergeOnReadBaseFileReaders(partitionSchema: StructType,
                                                 dataSchema: HoodieTableSchema,
                                                 requiredDataSchema: HoodieTableSchema,
                                                 requestedColumns: Array[String],
                                                 filters: Array[Filter]): (BaseFileReader, BaseFileReader) = {
    val requiredSchemaFileReaderMerging = createBaseFileReader(
      spark = sqlContext.sparkSession,
      partitionSchema = partitionSchema,
      dataSchema = dataSchema,
      requiredDataSchema = requiredDataSchema,
      filters = filters,
      options = optParams,
      // NOTE: We have to fork the Hadoop Config here as Spark will be modifying it
      //       to configure Parquet reader appropriately
      hadoopConf = embedInternalSchema(new Configuration(conf), requiredDataSchema.internalSchema)
    )

    // Check whether fields required for merging were also requested to be fetched
    // by the query:
    //    - In case they were, there's no optimization we could apply here (we will have
    //    to fetch such fields)
    //    - In case they were not, we will provide 2 separate file-readers
    //        a) One which would be applied to file-groups w/ delta-logs (merging)
    //        b) One which would be applied to file-groups w/ no delta-logs or
    //           in case query-mode is skipping merging
    val requiredColumns = mandatoryFieldsForMerging.map(HoodieAvroUtils.getRootLevelFieldName)
    if (requiredColumns.forall(requestedColumns.contains)) {
      (requiredSchemaFileReaderMerging, requiredSchemaFileReaderMerging)
    } else {
      val prunedRequiredSchema = {
        val superfluousColumnNames = requiredColumns.filterNot(requestedColumns.contains)
        val prunedStructSchema =
          StructType(requiredDataSchema.structTypeSchema.fields
            .filterNot(f => superfluousColumnNames.contains(f.name)))

        HoodieTableSchema(prunedStructSchema, convertToAvroSchema(prunedStructSchema).toString)
      }

      val requiredSchemaFileReaderNoMerging = createBaseFileReader(
        spark = sqlContext.sparkSession,
        partitionSchema = partitionSchema,
        dataSchema = dataSchema,
        requiredDataSchema = prunedRequiredSchema,
        filters = filters,
        options = optParams,
        // NOTE: We have to fork the Hadoop Config here as Spark will be modifying it
        //       to configure Parquet reader appropriately
        hadoopConf = embedInternalSchema(new Configuration(conf), requiredDataSchema.internalSchema)
      )

      (requiredSchemaFileReaderMerging, requiredSchemaFileReaderNoMerging)
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
