/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getWritePartitionPaths
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SQLContext

import scala.collection.JavaConverters._

/**
 * Relation, that implements the Hoodie incremental view.
 *
 * Implemented for Copy_on_write storage.
 *
 */
case class IncrementalRelation(override val sqlContext: SQLContext,
                               override val optParams: Map[String, String],
                               private val userSchema: Option[StructType],
                               override val metaClient: HoodieTableMetaClient,
                               private val prunedDataSchema: Option[StructType] = None)
  extends AbstractBaseFileOnlyRelation(sqlContext, metaClient, optParams, userSchema, Seq(), prunedDataSchema)
    with HoodieIncrementalRelationTrait {

  override type Relation = IncrementalRelation

  override def imbueConfigs(sqlContext: SQLContext): Unit = {
    super.imbueConfigs(sqlContext)
    // TODO(HUDI-3639) vectorized reader has to be disabled to make sure IncrementalRelation is working properly
    sqlContext.sparkSession.sessionState.conf.setConfString("spark.sql.parquet.enableVectorizedReader", "false")
  }
  override def updatePrunedDataSchema(prunedSchema: StructType): Relation =
    this.copy(prunedDataSchema = Some(prunedSchema))

  protected override def timeline: HoodieTimeline = {
    if (fullTableScan) {
      metaClient.getCommitsAndCompactionTimeline
    } else if (useStateTransitionTime) {
      metaClient.getCommitsAndCompactionTimeline.findInstantsInRangeByStateTransitionTs(startTimestamp, endTimestamp)
    } else {
      metaClient.getCommitsAndCompactionTimeline.findInstantsInRange(startTimestamp, endTimestamp)
    }
  }

  protected override def composeRDD(fileSplits: Seq[HoodieBaseFileSplit],
                                    tableSchema: HoodieTableSchema,
                                    requiredSchema: HoodieTableSchema,
                                    requestedColumns: Array[String],
                                    filters: Array[Filter]): RDD[InternalRow] = {
    super.composeRDD(fileSplits, tableSchema, requiredSchema, requestedColumns,
      filters ++ incrementalSpanRecordFilters)
  }

  protected override def collectFileSplits(partitionFilters: Seq[Expression],
                                           dataFilters: Seq[Expression]): Seq[HoodieBaseFileSplit] = {
    if (includedCommits.isEmpty) {
      List()
    } else {
      val fileSlices = if (fullTableScan) {
        listLatestFileSlices(Seq(), partitionFilters, dataFilters)
      } else {
        val latestCommit = includedCommits.last.getTimestamp

        val fsView = new HoodieTableFileSystemView(metaClient, timeline, affectedFilesInCommits)

        val modifiedPartitions = getWritePartitionPaths(commitsMetadata)

        modifiedPartitions.asScala.flatMap { relativePartitionPath =>
          fsView.getLatestMergedFileSlicesBeforeOrOn(relativePartitionPath, latestCommit).iterator().asScala
        }.toSeq
      }

      buildSplits(fileSlices)
    }
  }
}
