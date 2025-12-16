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

import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.MergeOnReadSnapshotRelation.{createPartitionedFile, isProjectionCompatible}
import org.apache.hudi.common.model.{FileSlice, HoodieLogFile, OverwriteWithLatestAvroPayload}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.storage.StoragePath

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
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

case class MergeOnReadSnapshotRelation(override val sqlContext: SQLContext,
                                       override val optParams: Map[String, String],
                                       override val metaClient: HoodieTableMetaClient,
                                       private val userSchema: Option[StructType],
                                       private val prunedDataSchema: Option[StructType] = None)
  extends BaseMergeOnReadSnapshotRelation(sqlContext, optParams, metaClient, userSchema, prunedDataSchema) {

  override type Relation = MergeOnReadSnapshotRelation

  override def updatePrunedDataSchema(prunedSchema: StructType): Relation =
    this.copy(prunedDataSchema = Some(prunedSchema))

  override protected def shouldIncludeLogFiles(): Boolean = {
    true
  }

}

/**
 * Base implementation of the Merge-on-Read snapshot relation
 *
 * NOTE: Reason this is extracted as a standalone base class is such that both MOR
 *       Snapshot and Incremental relations could inherit from it while both being Scala
 *       case classes
 */
abstract class BaseMergeOnReadSnapshotRelation(sqlContext: SQLContext,
                                               optParams: Map[String, String],
                                               metaClient: HoodieTableMetaClient,
                                               userSchema: Option[StructType],
                                               prunedDataSchema: Option[StructType])
  extends HoodieBaseRelation(sqlContext, metaClient, optParams, userSchema, prunedDataSchema) {

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
    Seq(recordKeyField) ++ orderingFields

  override lazy val mandatoryFields: Seq[String] = mandatoryFieldsForMerging

  protected val mergeType: String = optParams.getOrElse(DataSourceReadOptions.REALTIME_MERGE.key,
    DataSourceReadOptions.REALTIME_MERGE.defaultValue)

  /**
   * Determines whether relation's schema could be pruned by Spark's Optimizer
   */
  override def canPruneRelationSchema: Boolean =
    super.canPruneRelationSchema && isProjectionCompatible(tableState)

  protected override def composeRDD(fileSplits: Seq[HoodieMergeOnReadFileSplit],
                                    tableSchema: HoodieTableSchema,
                                    requiredSchema: HoodieTableSchema,
                                    requestedColumns: Array[String],
                                    filters: Array[Filter]): RDD[InternalRow] = {
    val requiredFilters = Seq.empty
    val optionalFilters = filters
    val readers = createBaseFileReaders(tableSchema, requiredSchema, requestedColumns, requiredFilters, optionalFilters)
    val confWithSchema = embedInternalSchema(new Configuration(conf), internalSchemaOpt)

    new HoodieMergeOnReadRDDV2(
      sqlContext.sparkContext,
      config = new JobConf(confWithSchema),
      sqlConf = sqlContext.sparkSession.sessionState.conf,
      fileReaders = readers,
      tableSchema = tableSchema,
      requiredSchema = requiredSchema,
      tableState = tableState,
      mergeType = mergeType,
      fileSplits = fileSplits,
      optionalFilters = optionalFilters,
      metaClient = metaClient,
      options = optParams)
  }

  protected override def collectFileSplits(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): List[HoodieMergeOnReadFileSplit] = {
    val convertedPartitionFilters =
      HoodieFileIndex.convertFilterForTimestampKeyGenerator(metaClient, partitionFilters)
    val fileSlices = fileIndex.filterFileSlices(dataFilters, convertedPartitionFilters).flatMap(s => s._2)
    buildSplits(fileSlices)
  }

  protected def buildSplits(fileSlices: Seq[FileSlice]): List[HoodieMergeOnReadFileSplit] = {
    fileSlices.map { fileSlice =>
      val baseFile = toScalaOption(fileSlice.getBaseFile)
      val logFiles = fileSlice.getLogFiles.sorted(HoodieLogFile.getLogFileComparator).iterator().asScala.toList

      val partitionedBaseFile = baseFile.map { file =>
        createPartitionedFile(
          getPartitionColumnsAsInternalRow(file.getPathInfo), file.getPathInfo.getPath, 0, file.getFileLen)
      }

      HoodieMergeOnReadFileSplit(partitionedBaseFile, logFiles)
    }.toList
  }
}

object MergeOnReadSnapshotRelation extends SparkAdapterSupport {

  /**
   * List of [[HoodieRecordPayload]] classes capable of merging projected records:
   * in some cases, when for example, user is only interested in a handful of columns rather
   * than the full row we will be able to optimize data throughput by only fetching the required
   * columns. However, to properly fulfil MOR semantic particular [[HoodieRecordPayload]] in
   * question should be able to merge records based on just such projected representation (including
   * columns required for merging, such as primary-key, pre-combine key, etc)
   */
  private val projectionCompatiblePayloadClasses: Set[String] = Seq(
    classOf[OverwriteWithLatestAvroPayload]
  ).map(_.getName).toSet

  def isProjectionCompatible(tableState: HoodieTableState): Boolean =
    projectionCompatiblePayloadClasses.contains(tableState.recordPayloadClassName)

  def createPartitionedFile(partitionValues: InternalRow,
                            filePath: StoragePath,
                            start: Long,
                            length: Long): PartitionedFile = {
    sparkAdapter.getSparkPartitionedFileUtils.createPartitionedFile(
      partitionValues, filePath, start, length)
  }
}
