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
import org.apache.hudi.HoodieBaseRelation.BaseFileReader
import org.apache.hudi.HoodieBootstrapRelation.validate
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.isMetaField
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

case class HoodieBootstrapSplit(dataFile: PartitionedFile, skeletonFile: Option[PartitionedFile] = None) extends HoodieFileSplit

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
  *                   else read data from tablePath using HoodiFileIndex.
  * @param metaClient Hoodie table meta client
  * @param optParams DataSource options passed by the user
  */
case class HoodieBootstrapRelation(override val sqlContext: SQLContext,
                                   private val userSchema: Option[StructType],
                                   private val globPaths: Seq[Path],
                                   override val metaClient: HoodieTableMetaClient,
                                   override val optParams: Map[String, String],
                                   private val prunedDataSchema: Option[StructType] = None)
  extends HoodieBaseRelation(sqlContext, metaClient, optParams, userSchema, prunedDataSchema) {

  override type FileSplit = HoodieBootstrapSplit
  override type Relation = HoodieBootstrapRelation

  private val resolver = sqlContext.sparkSession.sessionState.analyzer.resolver

  private lazy val skeletonSchema = HoodieSparkUtils.getMetaSchema

  override val mandatoryFields: Seq[String] = Seq.empty

  protected override def collectFileSplits(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[FileSplit] = {
    val fileSlices = listLatestFileSlices(globPaths, partitionFilters, dataFilters)
    fileSlices.map { fileSlice =>
      val baseFile = fileSlice.getBaseFile.get()

      if (baseFile.getBootstrapBaseFile.isPresent) {
        val partitionValues =
          getPartitionColumnsAsInternalRowInternal(baseFile.getFileStatus, extractPartitionValuesFromPartitionPath = true)
        val dataFile = PartitionedFile(partitionValues, baseFile.getBootstrapBaseFile.get().getPath, 0, baseFile.getBootstrapBaseFile.get().getFileLen)
        val skeletonFile = Option(PartitionedFile(InternalRow.empty, baseFile.getPath, 0, baseFile.getFileLen))

        HoodieBootstrapSplit(dataFile, skeletonFile)
      } else {
        val dataFile = PartitionedFile(getPartitionColumnsAsInternalRow(baseFile.getFileStatus), baseFile.getPath, 0, baseFile.getFileLen)
        HoodieBootstrapSplit(dataFile)
      }
    }
  }

  protected override def composeRDD(fileSplits: Seq[FileSplit],
                                    tableSchema: HoodieTableSchema,
                                    requiredSchema: HoodieTableSchema,
                                    requestedColumns: Array[String],
                                    filters: Array[Filter]): RDD[InternalRow] = {
    val requiredSkeletonFileSchema =
      StructType(skeletonSchema.filter(f => requestedColumns.exists(col => resolver(f.name, col))))

    val (bootstrapDataFileReader, bootstrapSkeletonFileReader) =
      createBootstrapFileReaders(tableSchema, requiredSchema, requiredSkeletonFileSchema, filters)

    // TODO elaborate
    val regularFileReader = createRegularFileReader(tableSchema, requiredSchema, filters)

    new HoodieBootstrapRDD(sqlContext.sparkSession, bootstrapDataFileReader, bootstrapSkeletonFileReader, regularFileReader,
      requestedColumns, fileSplits)
  }

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
      dataSchema = new HoodieTableSchema(bootstrapDataFileSchema),
      partitionSchema = partitionSchema,
      requiredDataSchema = new HoodieTableSchema(requiredBootstrapDataFileSchema),
      // TODO elaborate (we can't filter as we need record sequences to be aligned b/w data and skeleton files)
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
      dataSchema = new HoodieTableSchema(skeletonSchema),
      // TODO elaborate (we don't want partition-values to be injected by Spark)
      partitionSchema = StructType(Seq.empty),
      requiredDataSchema = new HoodieTableSchema(requiredSkeletonFileSchema),
      // TODO elaborate (we can't filter as we need record sequences to be aligned b/w data and skeleton files)
      filters = if (requiredBootstrapDataFileSchema.isEmpty) filters else Seq(),
      options = optParams,
      hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf(),
      // TODO elaborate
      shouldAppendPartitionValuesOverride = Some(false)
    )

    (bootstrapDataFileReader, boostrapSkeletonFileReader)
  }

  private def createRegularFileReader(tableSchema: HoodieTableSchema,
                                     requiredSchema: HoodieTableSchema,
                                     filters: Array[Filter]): BaseFileReader = {
    // NOTE: "Data" schema in here refers to the whole table's schema that doesn't include only partition
    //       columns, as opposed to data file schema not including any meta-fields columns in case of
    //       Bootstrap relation
    val (partitionSchema, dataSchema, requiredDataSchema) =
      tryPrunePartitionColumns(tableSchema, requiredSchema)

    // TODO elaborate (this a normal parquet reader for files not requiring combining w/ skeleton)
    createBaseFileReader(
      spark = sqlContext.sparkSession,
      dataSchema = dataSchema,
      partitionSchema = partitionSchema,
      requiredDataSchema = requiredDataSchema,
      filters = filters,
      options = optParams,
      hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf()
    )
  }

  override def updatePrunedDataSchema(prunedSchema: StructType): HoodieBootstrapRelation =
    this.copy(prunedDataSchema = Some(prunedSchema))
}


object HoodieBootstrapRelation {

  private def validate(requiredDataSchema: HoodieTableSchema, requiredDataFileSchema: StructType, requiredSkeletonFileSchema: StructType): Unit = {
    val requiredDataColumns: Seq[String] = requiredDataSchema.structTypeSchema.fieldNames.toSeq
    val combinedColumns = (requiredSkeletonFileSchema.fieldNames ++ requiredDataFileSchema.fieldNames).toSeq

    // NOTE: Here we validate that all required data columns are covered by the combination of the columns
    //       from both skeleton file and the corresponding data file
    checkState(combinedColumns.sorted == requiredDataColumns.sorted)
  }

}
