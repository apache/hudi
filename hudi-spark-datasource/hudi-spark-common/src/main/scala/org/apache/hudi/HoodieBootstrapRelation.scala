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

import org.apache.hudi.HoodieBaseRelation.{convertToAvroSchema, projectReader, BaseFileReader}
import org.apache.hudi.HoodieBootstrapRelation.{createPartitionedFile, validate}
import org.apache.hudi.common.model.FileSlice
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.storage.StoragePath

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionedFile}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.isMetaField
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

trait BaseHoodieBootstrapSplit extends HoodieFileSplit {
  val dataFile: PartitionedFile
  val skeletonFile: Option[PartitionedFile]
}

case class HoodieBootstrapSplit(dataFile: PartitionedFile,
                                skeletonFile: Option[PartitionedFile]) extends BaseHoodieBootstrapSplit

case class HoodieBootstrapRelation(override val sqlContext: SQLContext,
                                   private val userSchema: Option[StructType],
                                   override val metaClient: HoodieTableMetaClient,
                                   override val optParams: Map[String, String],
                                   private val prunedDataSchema: Option[StructType] = None)
  extends BaseHoodieBootstrapRelation(sqlContext, userSchema, metaClient, optParams, prunedDataSchema) {

  override type Relation = HoodieBootstrapRelation

  override protected def createFileSplit(fileSlice: FileSlice, dataFile: PartitionedFile, skeletonFile: Option[PartitionedFile]): FileSplit = {
    HoodieBootstrapSplit(dataFile, skeletonFile)
  }

  override def updatePrunedDataSchema(prunedSchema: StructType): Relation =
    this.copy(prunedDataSchema = Some(prunedSchema))

  def toHadoopFsRelation: HadoopFsRelation = {
    HadoopFsRelation(
      location = fileIndex,
      partitionSchema = fileIndex.partitionSchema,
      dataSchema = fileIndex.dataSchema,
      bucketSpec = None,
      fileFormat = fileFormat,
      optParams)(sparkSession)
  }
}
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
 * @param sqlContext Spark SQL Context
 * @param userSchema User specified schema in the datasource query
 * @param globPaths  The global paths to query. If it not none, read from the globPaths,
 *                   else read data from tablePath using HoodieFileIndex.
 * @param metaClient Hoodie table meta client
 * @param optParams  DataSource options passed by the user
 */
abstract class BaseHoodieBootstrapRelation(override val sqlContext: SQLContext,
                                           private val userSchema: Option[StructType],
                                           override val metaClient: HoodieTableMetaClient,
                                           override val optParams: Map[String, String],
                                           private val prunedDataSchema: Option[StructType] = None)
  extends HoodieBaseRelation(sqlContext, metaClient, optParams, userSchema, prunedDataSchema) {

  override type FileSplit = BaseHoodieBootstrapSplit

  private lazy val skeletonSchema = HoodieSparkUtils.getMetaSchema

  override lazy val mandatoryFields: Seq[String] = Seq.empty

  protected def getFileSlices(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[FileSlice] = {
    listLatestFileSlices(partitionFilters, dataFilters)
  }

  protected def createFileSplit(fileSlice: FileSlice, dataFile: PartitionedFile, skeletonFile: Option[PartitionedFile]): FileSplit

  protected override def collectFileSplits(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[FileSplit] = {
    val fileSlices = getFileSlices(partitionFilters, dataFilters)
    val isPartitioned = metaClient.getTableConfig.isTablePartitioned
    fileSlices.map { fileSlice =>
      val baseFile = fileSlice.getBaseFile.get()
      if (baseFile.getBootstrapBaseFile.isPresent) {
        val partitionValues = getPartitionColumnsAsInternalRowInternal(baseFile.getPathInfo,
          metaClient.getBasePath, extractPartitionValuesFromPartitionPath = isPartitioned)
        val dataFile = createPartitionedFile(
          partitionValues, baseFile.getBootstrapBaseFile.get.getPathInfo.getPath,
          0, baseFile.getBootstrapBaseFile.get().getFileLen)
        val skeletonFile = Option(createPartitionedFile(
          InternalRow.empty, baseFile.getStoragePath, 0, baseFile.getFileLen))

        createFileSplit(fileSlice, dataFile, skeletonFile)
      } else {
        val dataFile = createPartitionedFile(
          getPartitionColumnsAsInternalRow(baseFile.getPathInfo), baseFile.getStoragePath, 0, baseFile.getFileLen)
        createFileSplit(fileSlice, dataFile, Option.empty)
      }
    }
  }

  /**
   * get all the file readers required for composeRDD
   */
  protected def getFileReaders(tableSchema: HoodieTableSchema,
                               requiredSchema: HoodieTableSchema,
                               requestedColumns: Array[String],
                               filters: Array[Filter]): (BaseFileReader, BaseFileReader, BaseFileReader) = {
    val requiredSkeletonFileSchema =
      StructType(skeletonSchema.filter(f => requestedColumns.exists(col => resolver(f.name, col))))

    val (bootstrapDataFileReader, bootstrapSkeletonFileReader) =
      createBootstrapFileReaders(tableSchema, requiredSchema, requiredSkeletonFileSchema, filters)

    val regularFileReader = createRegularFileReader(tableSchema, requiredSchema, filters)
    (bootstrapDataFileReader, bootstrapSkeletonFileReader, regularFileReader)
  }

  protected override def composeRDD(fileSplits: Seq[FileSplit],
                                    tableSchema: HoodieTableSchema,
                                    requiredSchema: HoodieTableSchema,
                                    requestedColumns: Array[String],
                                    filters: Array[Filter]): RDD[InternalRow] = {

    val (bootstrapDataFileReader, bootstrapSkeletonFileReader, regularFileReader) = getFileReaders(tableSchema,
      requiredSchema, requestedColumns, filters)
    new HoodieBootstrapRDD(sqlContext.sparkSession, bootstrapDataFileReader, bootstrapSkeletonFileReader, regularFileReader,
      requiredSchema, fileSplits)
  }

  /**
   * Creates skeleton and base file reader
   */
  private def createBootstrapFileReaders(tableSchema: HoodieTableSchema,
                                         requiredSchema: HoodieTableSchema,
                                         requiredSkeletonFileSchema: StructType,
                                         filters: Array[Filter]): (BaseFileReader, BaseFileReader) = {
    // NOTE: "Data" schema in here refers to the whole table's schema that doesn't include only partition
    //       columns, as opposed to data file schema not including any meta-fields columns in case of
    //       Bootstrap relation
    val (partitionSchema, dataSchema, requiredDataSchema) =
      tryPrunePartitionColumnsInternal(tableSchema, requiredSchema, extractPartitionValuesFromPartitionPath = true)

    val bootstrapDataFileSchema = StructType(dataSchema.structTypeSchema.filterNot(sf => isMetaField(sf.name)))
    val requiredBootstrapDataFileSchema = StructType(requiredDataSchema.structTypeSchema.filterNot(sf => isMetaField(sf.name)))

    validate(requiredDataSchema, requiredBootstrapDataFileSchema, requiredSkeletonFileSchema)

    val bootstrapDataFileReader = createBaseFileReader(
      spark = sqlContext.sparkSession,
      dataSchema = new HoodieTableSchema(bootstrapDataFileSchema, convertToAvroSchema(bootstrapDataFileSchema, tableName).toString),
      partitionSchema = partitionSchema,
      requiredDataSchema = new HoodieTableSchema(requiredBootstrapDataFileSchema, convertToAvroSchema(requiredBootstrapDataFileSchema, tableName).toString),
      // NOTE: For bootstrapped files we can't apply any filtering in case we'd need to merge it with
      //       a skeleton-file as we rely on matching ordering of the records across bootstrap- and skeleton-files
      filters = if (requiredSkeletonFileSchema.isEmpty) filters else Seq(),
      options = optParams,
      hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf(),
      // NOTE: Bootstrap relation have to always extract partition values from the partition-path as this is a
      //       default Spark behavior: Spark by default strips partition-columns from the data schema and does
      //       NOT persist them in the data files, instead parsing them from partition-paths (on the fly) whenever
      //       table is queried
      shouldAppendPartitionValuesOverride = Some(true)
    )

    val boostrapSkeletonFileReader = createBaseFileReader(
      spark = sqlContext.sparkSession,
      dataSchema = new HoodieTableSchema(skeletonSchema, convertToAvroSchema(skeletonSchema, tableName).toString),
      // NOTE: Here we specify partition-schema as empty since we don't need Spark to inject partition-values
      //       parsed from the partition-path
      partitionSchema = StructType(Seq.empty),
      requiredDataSchema = new HoodieTableSchema(requiredSkeletonFileSchema, convertToAvroSchema(requiredSkeletonFileSchema, tableName).toString),
      // NOTE: For bootstrapped files we can't apply any filtering in case we'd need to merge it with
      //       a skeleton-file as we rely on matching ordering of the records across bootstrap- and skeleton-files
      filters = if (requiredBootstrapDataFileSchema.isEmpty) filters else Seq(),
      options = optParams,
      hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf(),
      // NOTE: We override Spark to avoid injecting partition values into the records read from
      //       skeleton-file
      shouldAppendPartitionValuesOverride = Some(false)
    )

    (bootstrapDataFileReader, boostrapSkeletonFileReader)
  }

  /**
   * create reader for hudi base files
   */
  private def createRegularFileReader(tableSchema: HoodieTableSchema,
                                     requiredSchema: HoodieTableSchema,
                                     filters: Array[Filter]): BaseFileReader = {
    // NOTE: "Data" schema in here refers to the whole table's schema that doesn't include only partition
    //       columns, as opposed to data file schema not including any meta-fields columns in case of
    //       Bootstrap relation
    val (partitionSchema, dataSchema, requiredDataSchema) =
      tryPrunePartitionColumns(tableSchema, requiredSchema)

    // NOTE: Bootstrapped table allows Hudi created file-slices to be co-located w/ the "bootstrapped"
    //       ones (ie persisted by Spark). Therefore to be able to read the data from Bootstrapped
    //       table we also need to create regular file-reader to read file-slices created by Hudi
    val regularFileReader = createBaseFileReader(
      spark = sqlContext.sparkSession,
      dataSchema = dataSchema,
      partitionSchema = partitionSchema,
      requiredDataSchema = requiredDataSchema,
      filters = filters,
      options = optParams,
      hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf()
    )

    // NOTE: In some case schema of the reader's output (reader's schema) might not match the schema expected by the caller.
    //       This could occur for ex, when requested schema contains partition columns which might not be persisted w/in the
    //       data file, but instead would be parsed from the partition path. In that case output of the file-reader will have
    //       different ordering of the fields than the original required schema (for more details please check out
    //       [[ParquetFileFormat]] impl). In that case we have to project the rows from the file-reader's schema
    //       back into the one expected by the caller
    projectReader(regularFileReader, requiredSchema.structTypeSchema)
  }
}

object HoodieBootstrapRelation extends SparkAdapterSupport {
  def validate(requiredDataSchema: HoodieTableSchema, requiredDataFileSchema: StructType, requiredSkeletonFileSchema: StructType): Unit = {
    val requiredDataColumns: Seq[String] = requiredDataSchema.structTypeSchema.fieldNames.toSeq
    val combinedColumns = (requiredSkeletonFileSchema.fieldNames ++ requiredDataFileSchema.fieldNames).toSeq

    // NOTE: Here we validate that all required data columns are covered by the combination of the columns
    //       from both skeleton file and the corresponding data file
    checkState(combinedColumns.sorted == requiredDataColumns.sorted)
  }

  def createPartitionedFile(partitionValues: InternalRow,
                            filePath: StoragePath,
                            start: Long,
                            length: Long): PartitionedFile = {
    sparkAdapter.getSparkPartitionedFileUtils.createPartitionedFile(
      partitionValues, filePath, start, length)
  }
}
