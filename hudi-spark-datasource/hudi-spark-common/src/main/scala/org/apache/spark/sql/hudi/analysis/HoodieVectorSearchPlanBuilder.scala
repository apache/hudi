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

/**
 * IVF + RaBitQ vector search backed by native metadata-table posting prefix lookups.
 *
 * The algorithm keeps Spark SQL out of the posting scan: it probes centroids, converts
 * the selected clusters to MDT posting prefixes, reads posting rows with HFile-friendly
 * prefix lookups, reduces with RaBitQ approximate scores before record-location lookup,
 * then exact-reranks the fetched Hudi rows.
 */
object IvfRaBitQMdtSearchAlgorithm extends VectorSearchAlgorithm with SparkAdapterSupport {
  private val APPROXIMATE_CANDIDATE_FLOOR = 32

  private[analysis] def approximateCandidateHeapSize(k: Int): Int =
    math.max(k, APPROXIMATE_CANDIDATE_FLOOR)

  import HoodieVectorSearchPlanBuilder._

  private[analysis] def isLogResidentCandidate(
      candidate: ScoredVectorPostingMatch,
      currentBaseInstant: String,
      sliceHasLogs: Boolean): Boolean = {
    // A negative position is the authoritative log-resident signal: base-file records always carry a
    // non-negative row position, so a candidate with position < 0 in a slice that still has log files
    // lives in a log block and cannot be materialized from the base Parquet. The candidate's instant
    // is NOT a reliable discriminator here -- a log delta can share the slice's base instant time.
    val location = candidate.getLocation
    sliceHasLogs && location != null && location.getPosition < 0
  }

  private[analysis] def approximateArbitratedCandidate(
      candidate: ScoredVectorPostingMatch): Option[ScoredVectorPostingMatch] =
    candidate.getArbiterDecision match {
      case VectorIndexArbiter.Decision.SERVE => Some(candidate)
      // A delta carries the updated vector code. Refreshing only its locator is safe and preserves
      // the new score; a stale packed-block row carries old code and must never be resurrected.
      case VectorIndexArbiter.Decision.STALE if candidate.isDelta => Some(candidate)
      case _ => None
    }

  private[analysis] case class FreshnessLag(firstUnmarkedInstant: String, lagCount: Int)

  private[analysis] def completedSourceWriteTimeline(timeline: HoodieTimeline): HoodieTimeline =
    timeline.getWriteTimeline.filterCompletedInstants()

  private[analysis] def freshnessLag(
      completedWriteInstants: Seq[String],
      coveredInstant: Option[String]): Option[FreshnessLag] = {
    val uncovered = completedWriteInstants.filter(instant => coveredInstant.forall(covered =>
      compareTimestamps(covered, InstantComparison.LESSER_THAN, instant)))
    uncovered.headOption.map(first => FreshnessLag(first, uncovered.size))
  }

  override val name: String = "ivf_rabitq_mdt"

  private[analysis] case class VectorSearchPlanContext(
      basePath: String,
      metaClient: HoodieTableMetaClient,
      timeline: HoodieTimeline,
      latestInstantTime: String,
      indexPartition: String,
      indexDefinition: HoodieIndexDefinition,
      metadataTable: HoodieTableMetadata,
      cache: VectorIndexMetadataCache,
      tuningOptions: Map[String, String],
      metric: DistanceMetric.Value)

  private[analysis] def isPartitionedRecordIndex(ctx: VectorSearchPlanContext): Boolean =
    false

  private val STRUCTURAL_OPTION_KEYS: Set[String] = Set(
    VectorIndexOptions.METRIC,
    VectorIndexOptions.QUANTIZER,
    VectorIndexOptions.RABITQ_BITS,
    VectorIndexOptions.RABITQ_SEED,
    VectorIndexOptions.RABITQ_ASSUME_NORMALIZED)

  private[analysis] def resolvedTuningOptions(
      indexOptions: Map[String, String],
      runtimeOptions: Map[String, String]): Map[String, String] = {
    val inherited = indexOptions
      .filterNot { case (key, _) => STRUCTURAL_OPTION_KEYS.contains(key) }
      // An explicit runtime mode change gets that mode's freshness default unless the caller also
      // overrides freshness. Otherwise a persisted exact-mode default (FAIL) would leak into an
      // approximate query whose contract default is WARN.
      .filterNot { case (key, _) =>
        key == VectorIndexOptions.FRESHNESS_POLICY &&
          runtimeOptions.contains(VectorIndexOptions.QUERY_MODE) &&
          !runtimeOptions.contains(VectorIndexOptions.FRESHNESS_POLICY)
      }
    inherited ++ runtimeOptions
  }

  private def resolvePlanContext(
      spark: SparkSession,
      basePath: String,
      embeddingCol: String,
      vectorSchema: HoodieSchema.Vector,
      callerMetric: DistanceMetric.Value,
      runtimeOptions: Map[String, String]): VectorSearchPlanContext = {
    val illegal = runtimeOptions.keySet.intersect(STRUCTURAL_OPTION_KEYS)
    if (illegal.nonEmpty) {
      throw new HoodieAnalysisException(
        s"Options ${illegal.toSeq.sorted.mkString(", ")} describe vector index structure and cannot be set at query time; " +
          "they are read from the active index generation.")
    }

    val storageConf = HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf())
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(storageConf.newInstance())
      .setBasePath(basePath)
      .build()
    // Freshness markers are emitted for completed source writes only. Keep the query comparand on
    // exactly that action family: clean, rollback, savepoint, restore, and indexing instants must
    // never make an otherwise-covered vector generation look stale.
    val timeline = completedSourceWriteTimeline(metaClient.getActiveTimeline)
    val latestInstantTime = timeline.lastInstant().orElseThrow(() =>
      new HoodieAnalysisException(s"No completed commits found for table '$basePath'.")).requestedTime()
    val (indexPartition, indexDefinition) = resolveVectorIndexDefinition(metaClient, embeddingCol)
    // Build the metadata-reader config from the active session properties instead of bare
    // defaults. Prior to this, newBuilder().enable(true).build() silently dropped every
    // hoodie.* metadata/hfile read-side conf set on the session (e.g. the HFile block-cache
    // sizing), so the vector TVF always read on defaults. fromProperties restores operator
    // control; enable(true) still forces MDT on for the search path regardless of session.
    val metadataProps = TypedProperties.fromMap(spark.sessionState.conf.getAllConfs.asJava)
    val metadataConfig = HoodieMetadataConfig.newBuilder()
      .fromProperties(metadataProps)
      .enable(true)
      .build()
    val engineContext = new HoodieSparkEngineContext(new JavaSparkContext(spark.sparkContext), spark.sqlContext)
    val metadataTable = metaClient.getTableFormat.getMetadataFactory
      .create(engineContext, metaClient.getStorage, metadataConfig, basePath)
    val cache = getOrLoadMetadataCache(metaClient, metadataTable, indexPartition, vectorSchema, latestInstantTime)
    if (cache == null || cache.getGenerationId < 0) {
      throw new HoodieAnalysisException(
        s"Vector index '$indexPartition' does not contain valid generation metadata.")
    }

    val indexOptions = Option(indexDefinition.getIndexOptions).map(_.asScala.toMap).getOrElse(Map.empty)
    val indexMetric = toSqlMetric(VectorIndexOptions.getMetric(indexOptions.asJava))
    val authoritativeMetric = validateMetricAuthority(callerMetric, indexMetric, cache)
    val tuningOptions = resolvedTuningOptions(indexOptions, runtimeOptions)
    VectorSearchPlanContext(
      basePath, metaClient, timeline, latestInstantTime, indexPartition, indexDefinition,
      metadataTable, cache, tuningOptions, authoritativeMetric)
  }

  private def validateMetricAuthority(
      callerMetric: DistanceMetric.Value,
      indexMetric: DistanceMetric.Value,
      cache: VectorIndexMetadataCache): DistanceMetric.Value = {
    if (callerMetric == indexMetric) {
      callerMetric
    } else {
      val normalizedEquivalent = cache.isAssumeNormalized &&
        Set(callerMetric, indexMetric) == Set(DistanceMetric.COSINE, DistanceMetric.DOT_PRODUCT)
      if (normalizedEquivalent) {
        callerMetric
      } else {
        throw new HoodieAnalysisException(
          s"Query requests metric '$callerMetric' but the vector index generation was built for " +
            s"'$indexMetric' (assumeNormalized=${cache.isAssumeNormalized}).")
      }
    }
  }

  private def toSqlMetric(metric: VectorDistanceMetric): DistanceMetric.Value = metric match {
    case VectorDistanceMetric.COSINE => DistanceMetric.COSINE
    case VectorDistanceMetric.L2 => DistanceMetric.L2
    case VectorDistanceMetric.DOT_PRODUCT => DistanceMetric.DOT_PRODUCT
  }

  private[analysis] def toVectorMetric(metric: DistanceMetric.Value): VectorDistanceMetric = metric match {
    case DistanceMetric.COSINE => VectorDistanceMetric.COSINE
    case DistanceMetric.L2 => VectorDistanceMetric.L2
    case DistanceMetric.DOT_PRODUCT => VectorDistanceMetric.DOT_PRODUCT
  }

  override def buildSingleQueryPlan(
      spark: SparkSession,
      corpusTable: VectorSearchTable,
      embeddingCol: String,
      queryVector: Array[Double],
      k: Int,
      metric: DistanceMetric.Value,
      runtimeOptions: Map[String, String] = Map.empty): LogicalPlan = {
    val corpusDf = corpusTable.df
    validateEmbeddingColumn(corpusDf, embeddingCol)
    validateQueryVectorDimension(corpusDf, embeddingCol, queryVector.length)

    val basePath = corpusTable.basePath.getOrElse {
      throw new HoodieAnalysisException(
        s"Search algorithm '$name' requires a Hudi table identifier or table path, not a temporary view.")
    }
    if (!corpusDf.columns.contains(HoodieRecord.RECORD_KEY_METADATA_FIELD)) {
      throw new HoodieAnalysisException(
        s"Search algorithm '$name' requires Hudi meta fields, but '${HoodieRecord.RECORD_KEY_METADATA_FIELD}' is missing.")
    }

    val vectorSchema = extractVectorSchema(corpusDf, embeddingCol).getOrElse {
      throw new HoodieAnalysisException(
        s"Search algorithm '$name' requires '$embeddingCol' to carry VECTOR metadata.")
    }
    val queryFloat = queryVector.map(_.toFloat)

    val contextStartNs = System.nanoTime()
    val ctx = resolvePlanContext(spark, basePath, embeddingCol, vectorSchema, metric, runtimeOptions)
    LOG.info(
      s"[vector_search][plan_context] basePath=$basePath latestInstant=${ctx.latestInstantTime} " +
        s"elapsedMs=${elapsedMs(contextStartNs)}")
    val coveredInstant = ctx.cache.getLastContiguousSourceInstant
    val lag = freshnessLag(
      ctx.timeline.getInstantsAsStream.iterator.asScala.map(_.requestedTime()).toSeq,
      Option(coveredInstant))
    val staleFallbackPlan = lag.map { freshnessLag =>
      val freshnessPolicy = VectorIndexOptions.getFreshnessPolicy(ctx.tuningOptions.asJava)
      val firstUnmarkedInstant = freshnessLag.firstUnmarkedInstant
      val lagCount = freshnessLag.lagCount
      val staleMessage =
        s"Vector index generation ${ctx.cache.getGenerationId} covers source writes through " +
          s"${Option(coveredInstant).getOrElse("<none>")} but the latest source data-write instant is ${ctx.latestInstantTime}; " +
          s"first unmarked instant=$firstUnmarkedInstant, lag count=$lagCount. " +
          s"Run vector-index catch-up/replay (or rebuild the index until replay is available) to repair the marker gap"
      freshnessPolicy match {
        case VectorStalePolicy.FAIL =>
          throw new HoodieAnalysisException(staleMessage)
        case VectorStalePolicy.WARN =>
          LOG.warn(s"$staleMessage; continuing because vector.freshness.policy=WARN")
          None
        case VectorStalePolicy.FALLBACK =>
          LOG.warn(s"$staleMessage; using brute-force exact fallback")
          Some(BruteForceSearchAlgorithm.buildSingleQueryPlan(
            spark, corpusTable, embeddingCol, queryVector, k, metric, runtimeOptions))
      }
    }.flatten
    staleFallbackPlan.getOrElse {
      if (ctx.cache.getDimension != vectorSchema.getDimension) {
        throw new HoodieAnalysisException(
          s"Vector index dimension (${ctx.cache.getDimension}) does not match column '$embeddingCol' dimension (${vectorSchema.getDimension}).")
      }
      val approxMode = VectorIndexOptions.isApproximateSearchMode(ctx.tuningOptions.asJava)

      if (approxMode) {
        buildApproximatePlan(spark, ctx, queryFloat, k)
      } else {
        val exactOutputSchema = StructType(
          corpusDf.schema.fields.filterNot(_.name.equalsIgnoreCase(embeddingCol)) :+
            StructField(DISTANCE_COL, DoubleType, nullable = false))
        val candidateDf = VectorExactSearchPlanner.buildRaBitQExactCandidateDf(
          spark, ctx, exactOutputSchema, embeddingCol, vectorSchema, queryFloat, queryVector, k)

        candidateDf
          .orderBy(col(DISTANCE_COL).asc)
          .limit(k)
          .queryExecution.analyzed
      }
    }
  }

  override def buildBatchQueryPlan(
      spark: SparkSession,
      corpusTable: VectorSearchTable,
      corpusEmbeddingCol: String,
      queryDf: DataFrame,
      queryEmbeddingCol: String,
      k: Int,
      metric: DistanceMetric.Value,
      runtimeOptions: Map[String, String] = Map.empty): LogicalPlan = {
    throw new HoodieAnalysisException(
      s"Search algorithm '$name' currently supports single-query hudi_vector_search only.")
  }

  /**
   * Approximate-only path: scores postings via RaBitQ and returns
   * (record_key, partition_path, file_group_id, approx_distance) without
   * reading the base table at all.
   */
  private def buildApproximatePlan(
      spark: SparkSession,
      ctx: VectorSearchPlanContext,
      queryVector: Array[Float],
      k: Int): LogicalPlan = {
    import spark.implicits._
    val planStartNs = System.nanoTime()
    val cache = ctx.cache
    val mergedOptions = ctx.tuningOptions
    val metric = toVectorMetric(ctx.metric)
    val residualEncoding = cache.isResidualEncoding
    val numProbes = math.min(math.max(1, VectorIndexOptions.getNumProbes(mergedOptions.asJava)), cache.numClusters())
    val centroidProbeStartNs = System.nanoTime()
    val topClusters = cache.findTopClusters(queryVector, numProbes, metric)
    val centroidProbeMs = elapsedMs(centroidProbeStartNs)
    val shardLookupStartNs = System.nanoTime()
    val shardCounts = cache.getShardCounts(topClusters)
    val shardLookupMs = elapsedMs(shardLookupStartNs)
    // Approximate-only search does not exact-rerank, so applying refine_factor here only bloats
    // every task-local heap and both global reductions. Keep a small floor because local pruning
    // precedes record-key overlay and tombstone removal; retaining exactly k could underfill results
    // when duplicate versions occupy the local winners.
    val candidateHeapSize = approximateCandidateHeapSize(k)
    val postingPlanStartNs = System.nanoTime()
    val topCandidates = VectorIndexMdtSearchUtils.scanPostingCandidates(
      ctx.metadataTable, ctx.indexPartition, cache.getGenerationId, shardCounts,
      queryVector, cache.getDimension, cache.getQuantizerSeed, cache.getRaBitQBits, cache.isAssumeNormalized,
      metric, VectorIndexOptions.isRaBitQAsymmetricScoring(mergedOptions.asJava),
      residualEncoding, if (residualEncoding) cache.getCentroids else null, candidateHeapSize)
    val postingPlanMs = elapsedMs(postingPlanStartNs)
    val resultRddPlanStartNs = System.nanoTime()
    val resultSchema = new StructType(Array(
      StructField(HoodieRecord.RECORD_KEY_METADATA_FIELD, org.apache.spark.sql.types.DataTypes.StringType, nullable = false, Metadata.empty),
      StructField(HoodieRecord.PARTITION_PATH_METADATA_FIELD, org.apache.spark.sql.types.DataTypes.StringType, nullable = true, Metadata.empty),
      StructField(FILE_GROUP_ID_COL, org.apache.spark.sql.types.DataTypes.StringType, nullable = true, Metadata.empty),
      StructField(DISTANCE_COL, org.apache.spark.sql.types.DataTypes.DoubleType, nullable = false, Metadata.empty)
    ))
    // RFC-109 RLI finalist arbiter (freshness gate). Default-off: dormant until the commit-time
    // delta writer lands. When enabled, exclude STALE (rewritten/moved) and DELETED finalists so
    // approx candidate-gen never serves a posting that no longer faithfully represents a live row.
    val arbiterEnabled = VectorIndexOptions.isFinalistArbiterEnabled(mergedOptions.asJava)
    val candidateRdd: RDD[ScoredVectorPostingMatch] =
      if (arbiterEnabled) {
        val arbitrated = VectorIndexMdtSearchUtils.arbitrateFinalists(
          ctx.metadataTable, topCandidates, isPartitionedRecordIndex(ctx))
        val staleAcc: LongAccumulator = spark.sparkContext.longAccumulator("vector_arbiter_stale")
        val deletedAcc: LongAccumulator = spark.sparkContext.longAccumulator("vector_arbiter_deleted")
        HoodieJavaRDD.getJavaRDD(arbitrated).rdd.mapPartitions { it =>
          var stale = 0L
          var deleted = 0L
          val kept = ArrayBuffer.empty[ScoredVectorPostingMatch]
          it.foreach { c =>
            approximateArbitratedCandidate(c) match {
              case Some(candidate) => kept += candidate
              case None if c.getArbiterDecision == VectorIndexArbiter.Decision.STALE => stale += 1L
              case None => deleted += 1L
            }
          }
          staleAcc.add(stale)
          deletedAcc.add(deleted)
          if (stale > 0L || deleted > 0L) {
            LOG.info(s"[vector_search][approximate][arbiter] arbiterExclusions{stale=$stale, deleted=$deleted} kept=${kept.size}")
          }
          kept.iterator
        }
      } else {
        HoodieJavaRDD.getJavaRDD(topCandidates).rdd
      }
    val resultRows = candidateRdd.map { c =>
      val liveLocation = Option(c.getLocation)
      Row(
        c.getRecordKey,
        liveLocation.map(_.getPartitionPath).orElse(Option(c.getPartitionPath)).getOrElse(""),
        liveLocation.map(_.getFileId).orElse(Option(c.getFileGroupId)).getOrElse(""),
        c.getApproxDistance.toDouble
      )
    }
    val resultRddPlanMs = elapsedMs(resultRddPlanStartNs)
    val sparkAnalysisStartNs = System.nanoTime()
    val analyzedPlan = spark.createDataFrame(resultRows, resultSchema)
      .orderBy(col(DISTANCE_COL).asc)
      .limit(k)
      .queryExecution.analyzed
    val sparkAnalysisMs = elapsedMs(sparkAnalysisStartNs)
    LOG.info(
      s"[vector_search][approximate] basePath=${ctx.basePath} probedClusters=${topClusters.length} " +
        s"candidateHeapSize=$candidateHeapSize residualEncoding=$residualEncoding arbiter=$arbiterEnabled " +
        s"centroidProbeMs=$centroidProbeMs shardLookupMs=$shardLookupMs postingPlanMs=$postingPlanMs " +
        s"resultRddPlanMs=$resultRddPlanMs sparkAnalysisMs=$sparkAnalysisMs planMs=${elapsedMs(planStartNs)}")
    analyzedPlan
  }

  private def resolveVectorIndexDefinition(
      metaClient: HoodieTableMetaClient,
      embeddingCol: String): (String, HoodieIndexDefinition) = {
    if (!metaClient.getIndexMetadata.isPresent) {
      throw new HoodieAnalysisException("No Hudi index metadata is available for the table.")
    }
    val matches = metaClient.getIndexMetadata.get.getIndexDefinitions.asScala
      .filter { case (_, definition) =>
        definition.getIndexType == HoodieTableMetadataUtil.PARTITION_NAME_VECTOR_INDEX
      }
      .filter { case (_, definition) =>
        Option(definition.getSourceFields).exists(_.asScala.exists(_.equalsIgnoreCase(embeddingCol)))
      }
      .toSeq
      .sortBy(_._1)
    matches match {
      case Seq() =>
        throw new HoodieAnalysisException(
          s"No vector index found for embedding column '$embeddingCol'.")
      case Seq(single) =>
        single
      case multiple =>
        throw new HoodieAnalysisException(
          s"Multiple vector indexes found for embedding column '$embeddingCol': ${multiple.map(_._1).mkString(", ")}. " +
            "Specify the index explicitly or drop the stale index.")
    }
  }

  private case class MetadataCacheKey(basePath: String, indexPartition: String)

  @volatile private var metadataCaches: Map[MetadataCacheKey, VectorIndexMetadataCache] = Map.empty

  private[analysis] def resetMetadataCaches(): Unit = {
    metadataCaches = Map.empty
  }

  private[analysis] def metadataCacheSize: Int = metadataCaches.size

  private def getOrLoadMetadataCache(
      metaClient: HoodieTableMetaClient,
      metadataTable: HoodieTableMetadata,
      indexPartition: String,
      vectorSchema: HoodieSchema.Vector,
      currentInstant: String): VectorIndexMetadataCache = {
    val cacheKey = MetadataCacheKey(metaClient.getBasePath.toString, indexPartition)

    metadataCaches.synchronized {
      metadataCaches.get(cacheKey) match {
        case Some(existing) if !existing.isStaleFor(currentInstant) => existing
        case existingOpt =>
          // Query hot path only needs manifest, centroids, and quantizer. Loading all
          // cluster-stat rows is expensive at 16K+ clusters on object storage; shard
          // counts fall back to the active generation manifest.
          val loaded = VectorIndexMetadataCache.load(metadataTable, indexPartition, vectorSchema, currentInstant, false)
          if (loaded == null) {
            existingOpt.orNull
          } else {
            existingOpt match {
              case Some(existing) if Option(existing.getLoadInstant).exists(_.compareTo(loaded.getLoadInstant) > 0) =>
                existing
              case _ =>
                metadataCaches = metadataCaches.updated(cacheKey, loaded)
                loaded
            }
          }
      }
    }
  }
}
