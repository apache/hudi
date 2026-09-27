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

package org.apache.spark.sql.hudi.analysis

import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.index.vector.{ScoredVectorPostingMatch, VectorDistanceMetric, VectorIndexArbiter, VectorIndexMdtSearchUtils, VectorIndexMetadataCache, VectorIndexOptions, VectorPostingMatch, VectorStalePolicy}
import org.apache.hudi.common.model.{FileSlice, HoodieIndexDefinition, HoodieRecord}
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.timeline.{HoodieTimeline, InstantComparison}
import org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.data.HoodieJavaRDD
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.metadata.{HoodieTableMetadata, HoodieTableMetadataUtil}

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.HoodieVectorSearchTableValuedFunction
import org.apache.spark.sql.catalyst.plans.logical.HoodieVectorSearchTableValuedFunction.{DistanceMetric, SearchAlgorithm}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, col, monotonically_increasing_id, row_number}
import org.apache.spark.sql.hudi.command.exception.HoodieAnalysisException
import org.apache.spark.sql.types.{ArrayType, ByteType, DataType, DoubleType, FloatType, Metadata, StructField, StructType}
import org.apache.spark.util.LongAccumulator
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

/** Exact reranking and file-slice resolution for the IVF/RaBitQ search plan. */
private[analysis] object VectorExactSearchPlanner extends SparkAdapterSupport {
  import HoodieVectorSearchPlanBuilder._
  import IvfRaBitQMdtSearchAlgorithm.{isLogResidentCandidate, isPartitionedRecordIndex, toVectorMetric, VectorSearchPlanContext}

  private val LOG = LoggerFactory.getLogger(getClass)

  def buildRaBitQExactCandidateDf(
      spark: SparkSession,
      ctx: VectorSearchPlanContext,
      outputSchema: StructType,
      embeddingCol: String,
      vectorSchema: HoodieSchema.Vector,
      queryVector: Array[Float],
      exactQueryVector: Array[Double],
      k: Int): DataFrame = {
    val planStartNs = System.nanoTime()
    val storageConf = HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf())
    val cacheLoadStartNs = System.nanoTime()
    val cache = ctx.cache
    val mergedOptions = ctx.tuningOptions
    val metric = toVectorMetric(ctx.metric)
    val residualEncoding = cache.isResidualEncoding
    val numProbes = math.min(math.max(1, VectorIndexOptions.getNumProbes(mergedOptions.asJava)), cache.numClusters())
    val topClusters = cache.findTopClusters(queryVector, numProbes, metric)
    val topClusterSeq = topClusters.toSeq
    val cacheLoadMs = elapsedMs(cacheLoadStartNs)

    // Shard counts from cache — no extra MDT IO.
    val shardCounts = cache.getShardCounts(topClusters)
    val shardCountsScala = shardCounts.asScala.map { case (clusterId, count) =>
      clusterId.intValue() -> count.intValue()
    }.toMap
    val postingPrefixCount = VectorIndexMdtSearchUtils.buildPostingPrefixes(cache.getGenerationId, shardCounts).size()
    LOG.info(
      s"[vector_search][plan] basePath=${ctx.basePath} indexPartition=${ctx.indexPartition} cacheLoadMs=$cacheLoadMs " +
        s"generationId=${cache.getGenerationId} dimension=${cache.getDimension} numClusters=${cache.numClusters()} " +
        s"numProbes=$numProbes refineFactor=${VectorIndexOptions.getRefineFactor(mergedOptions.asJava)} residualEncoding=$residualEncoding " +
        s"topClusters=${summarizeInts(topClusterSeq)} shardCounts=${summarizeShardCounts(shardCountsScala)} " +
        s"postingPrefixes=$postingPrefixCount")
    if (shardCounts.isEmpty) {
      sparkAdapter.getUnsafeUtils.createDataFrameFromRDD(
        spark, spark.sparkContext.emptyRDD[InternalRow], outputSchema)
    } else {
      val refineK = math.max(k, k * VectorIndexOptions.getRefineFactor(mergedOptions.asJava))
      val emptyDf = sparkAdapter.getUnsafeUtils.createDataFrameFromRDD(
        spark, spark.sparkContext.emptyRDD[InternalRow], outputSchema)

      val topCandidates = VectorIndexMdtSearchUtils.scanPostingCandidates(
        ctx.metadataTable, ctx.indexPartition, cache.getGenerationId, shardCounts,
        queryVector, cache.getDimension, cache.getQuantizerSeed, cache.getRaBitQBits, cache.isAssumeNormalized,
        metric, VectorIndexOptions.isRaBitQAsymmetricScoring(mergedOptions.asJava),
        residualEncoding, if (residualEncoding) cache.getCentroids else null, refineK)
      val topCandidateMaterializeStartNs = System.nanoTime()
      val materializedCandidates = try {
        topCandidates.collectAsList().asScala.toSeq
      } finally {
        topCandidates.unpersistWithDependencies()
      }
      val topCandidateList = materializedCandidates
        .filter(candidate => candidate.getFileGroupId != null && candidate.getFileGroupId.nonEmpty)
      val candidatesWithoutLocator = materializedCandidates.size - topCandidateList.size
      LOG.info(
        s"[vector_search][stage][materialize_top_candidates] refineK=$refineK rerankCandidates=${materializedCandidates.size} " +
          s"candidatesWithLocators=${topCandidateList.size} candidatesWithoutLocators=$candidatesWithoutLocator " +
          s"distinctFileGroups=${topCandidateList.map(_.getFileGroupId).distinct.size} elapsedMs=${elapsedMs(topCandidateMaterializeStartNs)}")
      if (candidatesWithoutLocator > 0) {
        throw new HoodieAnalysisException(
          s"Vector exact search found $candidatesWithoutLocator rerank candidates without file-group locators. " +
            s"Exact rerank requires per-candidate posting locators.")
      }

      // RFC-109 RLI finalist arbiter. Exact mode uses refreshed RLI locations for every live
      // candidate: SERVE retains positional trust; STALE is resolved by key in the live base file.
      val arbiterEnabled = VectorIndexOptions.isFinalistArbiterEnabled(mergedOptions.asJava)
      val serveCandidates: Seq[ScoredVectorPostingMatch] =
        if (arbiterEnabled) {
          val arb = VectorIndexMdtSearchUtils.arbitrateMaterializedFinalists(
            ctx.metadataTable, topCandidateList.asJava, isPartitionedRecordIndex(ctx))
          val staleFailPolicy = VectorIndexOptions.isStaleLocatorPolicyFail(mergedOptions.asJava)
          LOG.info(
            s"[vector_search][exact][arbiter] arbiterExclusions{stale=${arb.staleCount}, deleted=${arb.deletedCount}} " +
              s"serve=${arb.serve.size} stalePolicy=${if (staleFailPolicy) "fail" else "fallback"}")
          if (staleFailPolicy && !arb.stale.isEmpty) {
            throw new HoodieAnalysisException(
              s"Vector exact search rejected ${arb.staleCount} stale finalist(s) because " +
                "vector.stale.locator.policy=FAIL.")
          }
          arb.serve.asScala.toSeq ++ arb.stale.asScala.toSeq
        } else {
          topCandidateList
        }

      if (serveCandidates.isEmpty) {
        emptyDf
      } else {
        val candidatesByPartition = serveCandidates
          .groupBy(candidate => Option(candidate.getLocation).map(_.getPartitionPath).getOrElse(""))
          .map { case (partitionPath, candidates) =>
            partitionPath -> candidates.flatMap(candidate => Option(candidate.getLocation).map(_.getFileId)).toSet
          }
        val fileSliceMap = preResolveFileSlices(
          ctx.metadataTable, ctx.metaClient, ctx.timeline, ctx.latestInstantTime, candidatesByPartition)
        if (fileSliceMap.isEmpty) {
          throw new HoodieAnalysisException(
            s"Vector exact search resolved zero file slices for ${serveCandidates.size} rerank candidates " +
              s"across ${candidatesByPartition.values.map(_.size).sum} file groups. Candidate locators are present, so this indicates " +
              s"a file-slice resolution bug or stale vector index.")
        }
        val bFileSliceMap = spark.sparkContext.broadcast(fileSliceMap)
        // MOR log-resident candidates carry their raw embedding in a log block, not the base Parquet
        // file, so the positional fetcher cannot read them. Split them out and materialize them via a
        // merged (base + log) read; base-resident candidates keep the fast positional path.
        val (logResidentCandidates, baseCandidates) = serveCandidates.partition { candidate =>
          bFileSliceMap.value.get(candidate.getLocation.getFileId).exists { slice =>
            isLogResidentCandidate(candidate, slice.getBaseInstantTime, slice.getLogFiles.findAny().isPresent)
          }
        }
        val exactFetchParallelism = math.max(
          1,
          math.min(refineK, math.max(1, spark.sparkContext.defaultParallelism * 4)))
        val groupedCandidates = baseCandidates.groupBy(_.getLocation.getFileId).toSeq.sortBy(_._1)
        val targetBatchesPerGroup = math.max(1, exactFetchParallelism / math.max(1, groupedCandidates.size))
        val fetchBatches = groupedCandidates.flatMap { case (fgId, candidates) =>
          val sorted = candidates.sortBy(candidate => candidate.getLocation.getPosition)
          val batchSize = math.max(1, math.ceil(sorted.size.toDouble / targetBatchesPerGroup).toInt)
          sorted.grouped(batchSize).map(batch => (fgId, batch)).toSeq
        }
        val byFetchBatch: RDD[(String, Seq[ScoredVectorPostingMatch])] =
          if (fetchBatches.isEmpty) {
            spark.sparkContext.emptyRDD[(String, Seq[ScoredVectorPostingMatch])]
          } else {
            spark.sparkContext.parallelize(fetchBatches, math.min(exactFetchParallelism, fetchBatches.size))
          }
        LOG.info(
          s"[vector_search][stage][exact_read_plan] rerankCandidates=${serveCandidates.size} " +
            s"candidateGroups=${groupedCandidates.size} resolvedSlices=${fileSliceMap.size} " +
            s"fileGroupPartitions=${byFetchBatch.getNumPartitions} exactFetchParallelism=$exactFetchParallelism")

        val vectorOnlyProjection = mergedOptions.get(VECTOR_ONLY_PROJECTION_OPT)
          .orElse(spark.conf.getOption(VECTOR_ONLY_PROJECTION_SPARK_CONF))
          .exists(_.equalsIgnoreCase("true"))
        val fetchSkippedLogResidentAcc: LongAccumulator =
          spark.sparkContext.longAccumulator("vector_fetch_skipped_log_resident")
        val fetchKeyMismatchAcc: LongAccumulator = spark.sparkContext.longAccumulator("vector_fetch_key_mismatch")
        val rowsMaterializedAcc: LongAccumulator = spark.sparkContext.longAccumulator("vector_rows_materialized")
        val exactRows: RDD[InternalRow] = byFetchBatch.mapPartitions { batches =>
          val startNs = System.nanoTime()
          val rows = ArrayBuffer.empty[InternalRow]
          var batchCount = 0
          var resolvedSlices = 0
          var missingSlices = 0
          var candidateKeys = 0L
          var baseFiles = 0
          var logFiles = 0L
          var totalSliceBytes = 0L
          var staleCandidates = 0L
          var fetchSkippedLogResident = 0L
          var fetchKeyMismatch = 0L
          var keyLookupRowsDecoded = 0L
          var metrics = PositionalParquetFetcher.FetchMetrics()
          val fetcher = new PositionalParquetFetcher(
            storageConf.newInstance().unwrap(),
            embeddingCol,
            vectorSchema,
            exactQueryVector,
            ctx.metric,
            outputSchema,
            mergedOptions.getOrElse("vector.exact_fetch.offset_index_missing_policy", "fallback").equalsIgnoreCase("fail"),
            vectorOnlyProjection,
            VectorIndexOptions.shouldVerifyFetchKeys(mergedOptions.asJava))
          val keyLocator = new ParquetRecordKeyLocator(storageConf.newInstance().unwrap())
          while (batches.hasNext) {
            val (fgId, candidates) = batches.next()
            batchCount += 1
            val fileSliceOpt = bFileSliceMap.value.get(fgId)
            if (fileSliceOpt.isEmpty) {
              missingSlices += 1
              throw new HoodieAnalysisException(
                s"Vector exact search could not resolve file group '$fgId' for ${candidates.size} rerank candidates. " +
                  s"No record-level-index fallback is active in this path.")
            } else {
              resolvedSlices += 1
              val fileSlice = fileSliceOpt.get
              val logFileCount = fileSlice.getLogFiles.count()
              val currentBaseInstantTime = fileSlice.getBaseInstantTime
              val logResident = candidates.filter(candidate =>
                isLogResidentCandidate(candidate, currentBaseInstantTime, logFileCount > 0))
              if (logResident.nonEmpty) {
                fetchSkippedLogResident += logResident.size
                fetchSkippedLogResidentAcc.add(logResident.size)
                LOG.warn(
                  s"Vector exact fetch skipped ${logResident.size} log-resident candidate(s) in file group '$fgId'; " +
                    "they remain a result shortfall and the retained candidate pool will refill where candidates remain")
              }
              val liveCandidates = candidates.filterNot(logResident.toSet)
              staleCandidates += liveCandidates.count(candidate =>
                candidate.getArbiterDecision == VectorIndexArbiter.Decision.STALE)
              if (liveCandidates.nonEmpty) {
                val positionalCandidates = liveCandidates.filter { candidate =>
                  val location = candidate.getLocation
                  location != null && location.getPosition >= 0 &&
                    candidate.getArbiterDecision != VectorIndexArbiter.Decision.STALE &&
                    (location.getInstantTime == null || location.getInstantTime == currentBaseInstantTime)
                }
                val keyCandidates = liveCandidates.filterNot(positionalCandidates.toSet)
                val recordKeys = liveCandidates.map(_.getRecordKey).toSet
                candidateKeys += recordKeys.size
                if (fileSlice.getBaseFile.isEmpty) {
                  throw new HoodieAnalysisException(
                    s"Vector exact key fallback requires a base Parquet file for file group '$fgId'; " +
                      "the remaining candidates are not classified as log-resident.")
                }
                baseFiles += 1
                logFiles += logFileCount
                totalSliceBytes += fileSlice.getTotalFileSize
                val baseFilePath = fileSlice.getBaseFile.get.getPath
                val locatedByKey = keyLocator.locate(baseFilePath, keyCandidates)
                keyLookupRowsDecoded += locatedByKey.metrics.rowsDecoded
                if (locatedByKey.metrics.located != keyCandidates.size) {
                  throw new HoodieAnalysisException(
                    s"Vector exact key fallback located ${locatedByKey.metrics.located} of ${keyCandidates.size} " +
                      s"candidate key(s) in the live base file for file group '$fgId'.")
                }
                val firstFetch = fetcher.fetch(baseFilePath, positionalCandidates ++ locatedByKey.candidates)
                rows ++= firstFetch.rows
                metrics = metrics.add(firstFetch.metrics)
                rowsMaterializedAcc.add(firstFetch.metrics.rowsMaterialized)
                if (firstFetch.retryByKey.nonEmpty) {
                  fetchKeyMismatch += firstFetch.retryByKey.size
                  fetchKeyMismatchAcc.add(firstFetch.retryByKey.size)
                  val retries = keyLocator.locate(baseFilePath, firstFetch.retryByKey)
                  keyLookupRowsDecoded += retries.metrics.rowsDecoded
                  if (retries.metrics.located != firstFetch.retryByKey.size) {
                    throw new HoodieAnalysisException(
                      s"Vector exact fetch could not relocate ${firstFetch.retryByKey.size - retries.metrics.located} " +
                        s"key mismatch(es) in file group '$fgId'.")
                  }
                  val retryFetch = fetcher.fetch(baseFilePath, retries.candidates)
                  if (retryFetch.retryByKey.nonEmpty) {
                    throw new HoodieAnalysisException(
                      s"Vector exact fetch could not self-heal ${retryFetch.retryByKey.size} key mismatch(es) " +
                        s"in file group '$fgId' after key-based relocation.")
                  }
                  rows ++= retryFetch.rows
                  metrics = metrics.add(retryFetch.metrics)
                  rowsMaterializedAcc.add(retryFetch.metrics.rowsMaterialized)
                }
              }
            }
          }
          LOG.info(
            s"[vector_search][stage][exact_read] rerankCandidates=$candidateKeys candidateFileCount=$resolvedSlices " +
              s"fetchBatches=$batchCount resolvedSlices=$resolvedSlices missingSlices=$missingSlices " +
              s"rowsDecoded=${metrics.rowsDecoded} rowsMaterialized=${metrics.rowsMaterialized} " +
              s"keyLookupRowsDecoded=$keyLookupRowsDecoded fetchKeyMismatch=$fetchKeyMismatch " +
              s"fetchSkippedLogResident=$fetchSkippedLogResident " +
              s"rowGroupsTotal=${metrics.rowGroupsTotal} rowGroupsSelected=${metrics.rowGroupsSelected} " +
              s"pagesInSelectedRowGroups=${metrics.pagesInSelectedRowGroups} pagesSelected=${metrics.pagesSelected} " +
              s"offsetIndexHits=${metrics.offsetIndexHits} offsetIndexMissing=${metrics.offsetIndexMissing} " +
              s"rangedGets=${metrics.rangedGets} pageBytesFetched=${metrics.pageBytesFetched} " +
              s"physicalBytesRead=${metrics.physicalBytesRead} footerReads=${metrics.footerReads} footerCacheHits=${metrics.footerCacheHits} " +
              s"fetchKeyMismatchMetric=${metrics.fetchKeyMismatch} staleCandidates=$staleCandidates " +
              s"baseFiles=$baseFiles logFiles=$logFiles " +
              s"sliceBytes=$totalSliceBytes footerMs=${metrics.footerMs} offsetIndexMs=${metrics.offsetIndexMs} " +
              s"fetchWaitMs=${metrics.fetchWaitMs} decodeMs=${metrics.decodeMs} scoreMs=${metrics.scoreMs} elapsedMs=${elapsedMs(startNs)}")
          rows.iterator
        }
        val logResidentRows: Seq[InternalRow] =
          if (logResidentCandidates.isEmpty) {
            Seq.empty
          } else {
            val dataSchema = new TableSchemaResolver(ctx.metaClient).getTableSchema(true)
            val scorer = new VectorExactScorer(vectorSchema, exactQueryVector, ctx.metric)
            val logFetcher = new LogResidentVectorFetcher(
              storageConf, ctx.metaClient, dataSchema, ctx.latestInstantTime,
              embeddingCol, outputSchema, scorer)
            val fetched = logResidentCandidates
              .groupBy(candidate =>
                (Option(candidate.getLocation.getPartitionPath).getOrElse(""), candidate.getLocation.getFileId))
              .flatMap { case ((partitionPath, fgId), candidates) =>
                logFetcher.fetchSlice(partitionPath, fileSliceMap(fgId), candidates.map(_.getRecordKey).toSet)
              }.toSeq
            LOG.info(
              s"[vector_search][exact][log_resident] candidates=${logResidentCandidates.size} " +
                s"materialized=${fetched.size} " +
                s"fileGroups=${logResidentCandidates.map(_.getLocation.getFileId).distinct.size}")
            fetched
          }
        val combinedRows: RDD[InternalRow] =
          if (logResidentRows.isEmpty) exactRows
          else exactRows.union(spark.sparkContext.parallelize(logResidentRows))
        LOG.info(s"[vector_search][plan] exact candidate DF planned in ${elapsedMs(planStartNs)} ms")
        sparkAdapter.getUnsafeUtils.createDataFrameFromRDD(spark, combinedRows, outputSchema)
      }
    }
  }

  private def preResolveFileSlices(
      metadataTable: HoodieTableMetadata,
      metaClient: HoodieTableMetaClient,
      timeline: HoodieTimeline,
      latestInstantTime: String,
      candidatesByPartition: Map[String, Set[String]]): Map[String, FileSlice] = {
    val startNs = System.nanoTime()
    val fsView = new HoodieTableFileSystemView(metadataTable, metaClient, timeline)
    try {
      val result = scala.collection.mutable.Map.empty[String, FileSlice]
      var baseFiles = 0
      var logFiles = 0L
      var totalSliceBytes = 0L
      candidatesByPartition.foreach { case (partitionPath, candidateFgIds) =>
        val sliceIter = fsView.getLatestFileSlicesBeforeOrOn(partitionPath, latestInstantTime, true).iterator()
        while (sliceIter.hasNext) {
          val slice = sliceIter.next()
          val fgId = slice.getFileGroupId.getFileId
          if (candidateFgIds.contains(fgId)) {
            result(fgId) = slice
            if (slice.getBaseFile.isPresent) {
              baseFiles += 1
            }
            logFiles += slice.getLogFiles.count()
            totalSliceBytes += slice.getTotalFileSize
          }
        }
      }
      LOG.info(
        s"[vector_search][stage][resolve_file_slices] candidatePartitions=${candidatesByPartition.size} " +
          s"candidateFileGroups=${candidatesByPartition.values.map(_.size).sum} resolvedSlices=${result.size} " +
          s"baseFiles=$baseFiles logFiles=$logFiles sliceBytes=$totalSliceBytes elapsedMs=${elapsedMs(startNs)}")
      result.toMap
    } finally {
      fsView.close()
    }
  }

}
