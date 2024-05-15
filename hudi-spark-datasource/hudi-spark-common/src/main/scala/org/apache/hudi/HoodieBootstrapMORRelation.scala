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

import org.apache.hudi.common.model.{FileSlice, HoodieLogFile}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.storage.StoragePath

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

case class HoodieBootstrapMORSplit(dataFile: PartitionedFile, skeletonFile: Option[PartitionedFile],
                                   logFiles: List[HoodieLogFile]) extends BaseHoodieBootstrapSplit

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
case class HoodieBootstrapMORRelation(override val sqlContext: SQLContext,
                                      private val userSchema: Option[StructType],
                                      private val globPaths: Seq[StoragePath],
                                      override val metaClient: HoodieTableMetaClient,
                                      override val optParams: Map[String, String],
                                      private val prunedDataSchema: Option[StructType] = None)
  extends BaseHoodieBootstrapRelation(sqlContext, userSchema, globPaths, metaClient,
    optParams, prunedDataSchema) {

  override type Relation = HoodieBootstrapMORRelation

  protected lazy val mandatoryFieldsForMerging: Seq[String] =
    Seq(recordKeyField) ++ preCombineFieldOpt.map(Seq(_)).getOrElse(Seq())

  override lazy val mandatoryFields: Seq[String] = mandatoryFieldsForMerging

  protected override def getFileSlices(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[FileSlice] = {
    if (globPaths.isEmpty) {
      fileIndex.listFileSlices(HoodieFileIndex.
        convertFilterForTimestampKeyGenerator(metaClient, partitionFilters)).values.flatten.toSeq
    } else {
      listLatestFileSlices(globPaths, partitionFilters, dataFilters)
    }
  }

  protected override def createFileSplit(fileSlice: FileSlice, dataFile: PartitionedFile, skeletonFile: Option[PartitionedFile]): FileSplit = {
    HoodieBootstrapMORSplit(dataFile, skeletonFile, fileSlice.getLogFiles.sorted(HoodieLogFile.getLogFileComparator).iterator().asScala.toList)
  }

  protected override def composeRDD(fileSplits: Seq[FileSplit],
                                    tableSchema: HoodieTableSchema,
                                    requiredSchema: HoodieTableSchema,
                                    requestedColumns: Array[String],
                                    filters: Array[Filter]): RDD[InternalRow] = {

    val (bootstrapDataFileReader, bootstrapSkeletonFileReader, regularFileReader) = getFileReaders(tableSchema,
      requiredSchema, requestedColumns, filters)
    new HoodieBootstrapMORRDD(
      sqlContext.sparkSession,
      config = jobConf,
      bootstrapDataFileReader = bootstrapDataFileReader,
      bootstrapSkeletonFileReader = bootstrapSkeletonFileReader,
      regularFileReader = regularFileReader,
      tableSchema = tableSchema,
      requiredSchema = requiredSchema,
      tableState = tableState, fileSplits)
  }


  override def updatePrunedDataSchema(prunedSchema: StructType): HoodieBootstrapMORRelation =
    this.copy(prunedDataSchema = Some(prunedSchema))
}
