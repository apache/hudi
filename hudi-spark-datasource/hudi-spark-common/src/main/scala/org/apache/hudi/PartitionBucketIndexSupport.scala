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

import org.apache.hudi.BaseHoodieTableFileIndex.PartitionPath
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.{FileSlice, PartitionBucketIndexHashingConfig}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.index.bucket.partition.PartitionBucketIndexCalculator

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.slf4j.LoggerFactory

class PartitionBucketIndexSupport(spark: SparkSession,
                         metadataConfig: HoodieMetadataConfig,
                         metaClient: HoodieTableMetaClient,
                         specifiedQueryInstant: Option[String] = Option.empty)
  extends BucketIndexSupport(spark, metadataConfig, metaClient) {

  private val log = LoggerFactory.getLogger(getClass)
  private def initCalc(): Option[PartitionBucketIndexCalculator] = {
    if (specifiedQueryInstant.isDefined) {
      val hashingConfigOption = PartitionBucketIndexHashingConfig.loadingLatestHashingConfigBeforeOrOn(metaClient, specifiedQueryInstant.get)
      if (hashingConfigOption.isPresent) {
        Option.apply(PartitionBucketIndexCalculator.getInstance(hashingConfigOption.get().getExpressions,
          hashingConfigOption.get().getRule,
          hashingConfigOption.get().getDefaultBucketNumber))
      } else {
        // could be empty, if user upgrade to partition level bucket index after specifiedQueryInstant
        Option.empty
      }
    } else {
      // specifiedQueryInstant is null or empty, so load latest hashing config directly
      val hashingConfig = PartitionBucketIndexHashingConfig.loadingLatestHashingConfig(metaClient)
      Option.apply(PartitionBucketIndexCalculator.getInstance(hashingConfig.getExpressions,
        hashingConfig.getRule, hashingConfig.getDefaultBucketNumber))
    }
  }


  private val calc: Option[PartitionBucketIndexCalculator] = initCalc()

  override def computeCandidateFileNames(fileIndex: HoodieFileIndex,
                                         queryFilters: Seq[Expression],
                                         queryReferencedColumns: Seq[String],
                                         prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])],
                                         shouldPushDownFilesFilter: Boolean): Option[Set[String]] = {
    if (calc.isEmpty) {
      // fall back to BucketIndexSupport
      super.computeCandidateFileNames(fileIndex, queryFilters, queryReferencedColumns, prunedPartitionsAndFileSlices, shouldPushDownFilesFilter)
    } else {
      Option.apply(prunedPartitionsAndFileSlices.flatMap(v => {
        val partitionPathOption = v._1
        val fileSlices = v._2
        val numBuckets = calc.get.computeNumBuckets(partitionPathOption.getOrElse(new PartitionPath("", Array())).path)
        val bucketIdsBitMapByFilter = filterQueriesWithBucketHashField(queryFilters, numBuckets)
        if (bucketIdsBitMapByFilter.isDefined && bucketIdsBitMapByFilter.get.cardinality() > 0) {
          val allFilesName = getPrunedPartitionsAndFileNames(fileIndex, Seq((partitionPathOption, fileSlices)))._2
          getCandidateFiles(allFilesName, bucketIdsBitMapByFilter.get)
        } else {
          Seq()
        }}).toSet)
    }
  }
}

object PartitionBucketIndexSupport {
  val INDEX_NAME = "BUCKET"
}

