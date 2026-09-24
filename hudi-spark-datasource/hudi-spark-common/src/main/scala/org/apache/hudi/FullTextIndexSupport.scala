/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi

import org.apache.hudi.DataSourceReadOptions.{QUERY_TYPE, TIME_TRAVEL_AS_OF_INSTANT}
import org.apache.hudi.FullTextIndexSupport.{extractConstraints, TokenConstraint}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.data.HoodieListData
import org.apache.hudi.common.model.{FileSlice, HoodieIndexDefinition}
import org.apache.hudi.common.model.HoodieTableQueryType.SNAPSHOT
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.InstantComparison
import org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps
import org.apache.hudi.core.read.BaseHoodieTableFileIndex
import org.apache.hudi.metadata.{BaseTableMetadata, FullTextIndexUtils, HoodieMetadataPayload, RawKey}
import org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_FULL_TEXT_INDEX

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, Expression, Literal}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.hudi.fulltext.{HudiHasAllTokens, HudiHasAnyTokens, HudiHasToken}
import org.apache.spark.sql.types.StringType
import org.roaringbitmap.RoaringBitmap

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Prunes file slices with the full-text index for hudi_has_token / hudi_has_all_tokens / hudi_has_any_tokens
 * predicates on an indexed column.
 *
 * <p>A slice is pruned only when the index view being read covers exactly its base file: the coverage marker
 * of that (file id, base instant) unit is present. Slices whose unit is not covered (the index view is behind
 * or ahead of the queried slices, or the file was never indexed) and slices with log files are always kept.
 * For an AND over several tokens, the row-position bitmaps of the non-dense tokens are intersected within each
 * file, so a covered file is kept only if some row holds every token.
 */
// scalastyle:off return
class FullTextIndexSupport(spark: SparkSession,
                           metadataConfig: HoodieMetadataConfig,
                           metaClient: HoodieTableMetaClient) extends SparkBaseIndexSupport(spark, metadataConfig, metaClient) {

  override def getIndexName: String = FullTextIndexSupport.INDEX_NAME

  override def isIndexAvailable: Boolean = metadataConfig.isEnabled && fullTextIndexDefinitions.nonEmpty

  override def invalidateCaches(): Unit = {}

  /**
   * The index only describes the units of the latest snapshot, so it is used for snapshot queries on the
   * latest completed instant only; incremental and older time-travel reads are not pruned by it.
   */
  override def supportsQueryType(options: Map[String, String]): Boolean = {
    if (!options.getOrElse(QUERY_TYPE.key, QUERY_TYPE.defaultValue).equalsIgnoreCase(SNAPSHOT.name)) {
      false
    } else {
      options.get(TIME_TRAVEL_AS_OF_INSTANT.key).forall { instant =>
        val lastCompleted = metaClient.getCommitsTimeline.filterCompletedInstants.lastInstant
        lastCompleted.isPresent && compareTimestamps(HoodieSqlCommonUtils.formatQueryInstant(instant),
          InstantComparison.GREATER_THAN_OR_EQUALS, lastCompleted.get.requestedTime)
      }
    }
  }

  private def fullTextIndexDefinitions: Seq[HoodieIndexDefinition] = {
    if (!metaClient.getIndexMetadata.isPresent) {
      Seq.empty
    } else {
      val completed = metaClient.getTableConfig.getMetadataPartitions
      metaClient.getIndexMetadata.get.getIndexDefinitions.values.asScala.toSeq
        .filter(d => d.getIndexType == PARTITION_NAME_FULL_TEXT_INDEX && completed.contains(d.getIndexName))
    }
  }

  override def computeCandidateFileNames(fileIndex: HoodieFileIndex,
                                         queryFilters: Seq[Expression],
                                         queryReferencedColumns: Seq[String],
                                         prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])],
                                         shouldPushDownFilesFilter: Boolean): Option[Set[String]] = {
    val indexByColumn = fullTextIndexDefinitions.map(d => d.getSourceFields.get(0).toLowerCase -> d.getIndexName).toMap
    val constraints = queryFilters.flatMap(f => extractConstraints(f)).filter(c => indexByColumn.contains(c.column))
    if (constraints.isEmpty) {
      return None
    }

    val slices = prunedPartitionsAndFileSlices.flatMap(_._2)
    val (withLogs, baseOnly) = slices.partition(s => s.hasLogFiles || !s.getBaseFile.isPresent)
    val sliceUnits: Map[String, String] = baseOnly.map(s => s.getFileId -> FullTextIndexUtils.baseUnit(s.getBaseInstantTime)).toMap

    var candidates: Option[Set[String]] = None
    constraints.groupBy(c => indexByColumn(c.column)).foreach { case (indexPartition, indexConstraints) =>
      // Only units whose coverage marker is visible can be pruned by this index view.
      val markers = readExact(indexPartition, sliceUnits.map { case (fileId, unit) => FullTextIndexUtils.markerKey(fileId, unit) }.toSeq)
      val coveredUnits = sliceUnits.filter { case (fileId, unit) => markers.contains(FullTextIndexUtils.markerKey(fileId, unit)) }
      val uncovered = sliceUnits.keySet -- coveredUnits.keySet
      indexConstraints.foreach { c =>
        val tokens = FullTextIndexUtils.tokenize(c.query).asScala.toSeq
        if (tokens.nonEmpty) {
          val files = candidateFileIds(indexPartition, tokens, c.matchAll, coveredUnits) ++ uncovered
          candidates = Some(candidates.map(_.intersect(files)).getOrElse(files))
        }
      }
    }
    candidates.map { fileIds =>
      val keptBaseFiles = baseOnly.filter(s => fileIds.contains(s.getFileId)).map(_.getBaseFile.get.getFileName)
      val keptWithLogs = withLogs.flatMap { s =>
        Option(s.getBaseFile.orElse(null)).map(_.getFileName).toSeq ++ s.getLogFiles.iterator().asScala.map(_.getFileName)
      }
      (keptBaseFiles ++ keptWithLogs).toSet
    }
  }

  private def candidateFileIds(indexPartition: String, tokens: Seq[String], matchAll: Boolean,
                               liveUnits: Map[String, String]): Set[String] = {
    // token -> (fileId -> dense) over live units only
    val presence = mutable.Map[String, mutable.Map[String, Boolean]]()
    tokens.foreach(t => presence(t) = mutable.Map.empty)
    val prefixKeys = tokens.map(t => FullTextRawKey(FullTextIndexUtils.presencePrefix(t)))
    metadataTable.getRecordsByKeyPrefixes(HoodieListData.eager(prefixKeys.asJava), indexPartition, true)
      .collectAsList().asScala.foreach { record =>
        val Array(term, fileId, unit) = FullTextIndexUtils.parseKey(record.getRecordKey)
        if (liveUnits.get(fileId).contains(unit) && presence.contains(term)) {
          presence(term)(fileId) = record.getData.getFullTextIndexMetadata.getDense
        }
      }

    if (!matchAll) {
      return presence.values.flatMap(_.keySet).toSet
    }
    val filesWithAllTokens = presence.values.map(_.keySet.toSet).reduce(_ intersect _)
    if (tokens.size == 1 || filesWithAllTokens.isEmpty) {
      return filesWithAllTokens
    }

    val needed = for {
      fileId <- filesWithAllTokens.toSeq
      token <- tokens if !presence(token)(fileId)
    } yield (fileId, FullTextIndexUtils.positionsKey(token, fileId, liveUnits(fileId)))
    val bitmaps: Map[String, RoaringBitmap] = readExact(indexPartition, needed.map(_._2)).flatMap { case (key, payload) =>
      Option(payload.getFullTextIndexMetadata).flatMap(info => Option(info.getPositions))
        .map(buffer => key -> FullTextIndexUtils.deserialize(buffer))
    }
    filesWithAllTokens.filter { fileId =>
      val keys = needed.filter(_._1 == fileId).map(_._2)
      if (keys.isEmpty) {
        true
      } else if (!keys.forall(bitmaps.contains)) {
        // A missing positions entry cannot prove absence; keep the file.
        true
      } else {
        !keys.map(bitmaps).reduce((a, b) => RoaringBitmap.and(a, b)).isEmpty
      }
    }
  }

  /** Reads the entries stored under exactly these keys; absent keys are absent from the result. */
  private def readExact(indexPartition: String, keys: Seq[String]): Map[String, HoodieMetadataPayload] = {
    if (keys.isEmpty) {
      return Map.empty
    }
    val rawKeys = HoodieListData.eager(keys.map(k => FullTextRawKey(k): RawKey).asJava)
    val records = metadataTable match {
      case base: BaseTableMetadata =>
        val pairs = base.readIndexRecordsWithKeys(rawKeys, indexPartition)
        try pairs.collectAsList().asScala.map(p => p.getKey -> p.getValue) finally pairs.unpersistWithDependencies()
      case other =>
        other.getRecordsByKeyPrefixes(rawKeys, indexPartition, true).collectAsList().asScala
          .map(r => r.getRecordKey -> r.getData)
    }
    val wanted = keys.toSet
    records.filter { case (key, payload) => wanted.contains(key) && !payload.isDeleted }.toMap
  }
}

// scalastyle:on return

object FullTextIndexSupport {
  val INDEX_NAME = "full_text_index"

  case class TokenConstraint(column: String, query: String, matchAll: Boolean)

  /**
   * Extracts token constraints from a pushed-down data filter. Only top-level conjuncts with a column
   * reference and a string literal are used; anything else is left to row-level evaluation.
   */
  def extractConstraints(filter: Expression): Seq[TokenConstraint] = filter match {
    case And(left, right) => extractConstraints(left) ++ extractConstraints(right)
    case HudiHasToken(a: AttributeReference, Literal(q, StringType)) if q != null => Seq(TokenConstraint(a.name.toLowerCase, q.toString, matchAll = true))
    case HudiHasAllTokens(a: AttributeReference, Literal(q, StringType)) if q != null => Seq(TokenConstraint(a.name.toLowerCase, q.toString, matchAll = true))
    case HudiHasAnyTokens(a: AttributeReference, Literal(q, StringType)) if q != null => Seq(TokenConstraint(a.name.toLowerCase, q.toString, matchAll = false))
    case _ => Seq.empty
  }
}

case class FullTextRawKey(key: String) extends RawKey {
  override def encode(): String = key
}
