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
import org.apache.hudi.FullTextIndexSupport.{evaluate, indexLeaves, mayMatch, rowLevelUseful, toCondition, AndCond, IndexLeaf, IndexView, MAX_PREFIX_POSITION_TERMS, PrefixLeaf, TokenLeaf}
import org.apache.hudi.avro.model.HoodieFullTextIndexInfo
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
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, Expression, Not, Or}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.hudi.fulltext.{HudiHasAnyTokens, HudiHasPhrase, HudiHasTokenPrefix, TokenPredicate}
import org.apache.spark.sql.types.StringType
import org.roaringbitmap.RoaringBitmap

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

/**
 * Prunes file slices with the full-text index for the token predicates (hudi_has_token, hudi_has_all_tokens,
 * hudi_has_any_tokens, hudi_has_token_prefix, hudi_has_phrase) on an indexed column, combined with AND, OR and NOT.
 *
 * <p>A slice is pruned only when the index view being read covers exactly its base file: the coverage marker
 * of that (file id, base instant) unit is present. Slices whose unit is not covered (the index view is behind
 * or ahead of the queried slices, or the file was never indexed) and slices with log files are always kept.
 * Within each covered file, the filter is evaluated on row positions where they are stored, so a file is kept only
 * if some row may satisfy it; predicates the index cannot answer count as satisfied by every row.
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
    val condition = AndCond(queryFilters.map(f => toCondition(f, indexByColumn)))
    val leaves = indexLeaves(condition)
    if (leaves.isEmpty) {
      return None
    }

    val slices = prunedPartitionsAndFileSlices.flatMap(_._2)
    val (withLogs, baseOnly) = slices.partition(s => s.hasLogFiles || !s.getBaseFile.isPresent)
    val sliceUnits: Map[String, String] = baseOnly.map(s => s.getFileId -> FullTextIndexUtils.baseUnit(s.getBaseInstantTime)).toMap

    val views: Map[String, IndexView] = leaves.groupBy(_.indexPartition).map { case (indexPartition, partitionLeaves) =>
      // Only units whose coverage marker is visible can be pruned by this index view.
      val markers = readExact(indexPartition, sliceUnits.map { case (fileId, unit) => FullTextIndexUtils.markerKey(fileId, unit) }.toSeq)
      val covered = sliceUnits.filter { case (fileId, unit) => markers.contains(FullTextIndexUtils.markerKey(fileId, unit)) }
      val rowCounts = covered.map { case (fileId, unit) =>
        fileId -> markers(FullTextIndexUtils.markerKey(fileId, unit)).getFullTextIndexMetadata.getRowCount.toLong
      }
      val presence = readPresence(indexPartition, partitionLeaves, covered)
      val prefixTerms = partitionLeaves.collect { case PrefixLeaf(_, prefix) =>
        prefix -> presence.keys.filter(_.startsWith(prefix)).toSeq
      }.toMap
      indexPartition -> IndexView(covered, rowCounts, presence, prefixTerms)
    }

    // File-level pass on presence entries, then a row-level pass on positions for the files left.
    val fileLevel = sliceUnits.keySet.filter(fileId => mayMatch(evaluate(condition, fileId, views, Map.empty)))
    val candidates = if (!rowLevelUseful(condition)) {
      fileLevel
    } else {
      val bitmaps = readPositions(fileLevel, leaves, views)
      fileLevel.filter(fileId => mayMatch(evaluate(condition, fileId, views, bitmaps)))
    }

    if (candidates.size == sliceUnits.size) {
      // Nothing pruned: leave the filters to the other index supports.
      return None
    }
    val keptBaseFiles = baseOnly.filter(s => candidates.contains(s.getFileId)).map(_.getBaseFile.get.getFileName)
    val keptWithLogs = withLogs.flatMap { s =>
      Option(s.getBaseFile.orElse(null)).map(_.getFileName).toSeq ++ s.getLogFiles.iterator().asScala.map(_.getFileName)
    }
    Some((keptBaseFiles ++ keptWithLogs).toSet)
  }

  /** Presence entries of the leaves' terms, for covered units only: term -> file id -> entry. */
  private def readPresence(indexPartition: String, leaves: Seq[IndexLeaf],
                           covered: Map[String, String]): Map[String, Map[String, HoodieFullTextIndexInfo]] = {
    val prefixes = leaves.map {
      case TokenLeaf(_, token) => FullTextIndexUtils.presencePrefix(token)
      case PrefixLeaf(_, prefix) => FullTextIndexUtils.presenceTermPrefix(prefix)
    }.distinct.sorted
    // The HFile reader only seeks forward, so a prefix extending a shorter one in the batch is dropped:
    // the shorter prefix already returns its entries.
    val minimal = prefixes.foldLeft(Vector.empty[String]) { (kept, p) => if (kept.exists(p.startsWith)) kept else kept :+ p }
    val presence = mutable.Map[String, mutable.Map[String, HoodieFullTextIndexInfo]]()
    metadataTable.getRecordsByKeyPrefixes(HoodieListData.eager(minimal.map(p => FullTextRawKey(p): RawKey).asJava), indexPartition, true)
      .collectAsList().asScala.foreach { record =>
        val Array(term, fileId, unit) = FullTextIndexUtils.parseKey(record.getRecordKey)
        if (covered.get(fileId).contains(unit)) {
          presence.getOrElseUpdate(term, mutable.Map.empty)(fileId) = record.getData.getFullTextIndexMetadata
        }
      }
    presence.map { case (term, files) => term -> files.toMap }.toMap
  }

  /** Positions of the leaves' non-dense terms in the given files: (index partition, term, file id) -> rows. */
  private def readPositions(fileIds: Set[String], leaves: Seq[IndexLeaf],
                            views: Map[String, IndexView]): Map[(String, String, String), RoaringBitmap] = {
    leaves.distinct.groupBy(_.indexPartition).flatMap { case (indexPartition, partitionLeaves) =>
      val view = views(indexPartition)
      // A prefix matching too many terms is evaluated on presence only.
      val terms = partitionLeaves.flatMap {
        case TokenLeaf(_, token) => Seq(token)
        case PrefixLeaf(_, prefix) => view.prefixTerms(prefix).filter(_ => view.prefixTerms(prefix).size <= MAX_PREFIX_POSITION_TERMS)
      }.distinct
      val keys = for {
        term <- terms
        fileId <- fileIds.toSeq
        info <- view.presence.get(term).flatMap(_.get(fileId)).toSeq if !info.getDense
      } yield FullTextIndexUtils.positionsKey(term, fileId, view.covered(fileId))
      readExact(indexPartition, keys).flatMap { case (key, payload) =>
        val Array(term, fileId, _) = FullTextIndexUtils.parseKey(key)
        Option(payload.getFullTextIndexMetadata).flatMap(info => Option(info.getPositions))
          .map(buffer => (indexPartition, term, fileId) -> FullTextIndexUtils.deserialize(buffer))
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

  /** A pushed-down filter, with full-text predicates on indexed columns as leaves and anything else as Unknown. */
  sealed trait Condition
  case class AndCond(children: Seq[Condition]) extends Condition
  case class OrCond(children: Seq[Condition]) extends Condition
  case class NotCond(child: Condition) extends Condition
  /** Satisfied only by rows satisfying the child, but not surely by all of them: keeps the child's sup, drops its sub. */
  case class SubsetOf(child: Condition) extends Condition
  case object Unknown extends Condition
  sealed trait IndexLeaf extends Condition {
    def indexPartition: String
  }
  case class TokenLeaf(indexPartition: String, token: String) extends IndexLeaf
  case class PrefixLeaf(indexPartition: String, prefix: String) extends IndexLeaf

  /** Above this many matching terms, a prefix is evaluated on presence entries only, without positions. */
  val MAX_PREFIX_POSITION_TERMS = 1000

  /**
   * One index partition as seen by a query: covered units (file id -> unit), their row counts from the coverage
   * markers, presence entries (term -> file id -> entry), and the terms each queried prefix expands to.
   */
  case class IndexView(covered: Map[String, String], rowCounts: Map[String, Long],
                       presence: Map[String, Map[String, HoodieFullTextIndexInfo]], prefixTerms: Map[String, Seq[String]])

  /**
   * Rows of one file that may satisfy a condition (sup) and rows that surely do (sub); None is every row.
   * Pruning keeps a file unless sup is empty. NOT is evaluated as the complement of sub, so sub must never
   * hold a row that does not satisfy the condition; NOT's own sub is left empty because rows with a null
   * text satisfy neither a predicate nor its negation.
   */
  case class Rows(sup: Option[RoaringBitmap], sub: Option[RoaringBitmap])

  private def noRows: Option[RoaringBitmap] = Some(new RoaringBitmap())

  private val unknownRows: Rows = Rows(None, noRows)

  def mayMatch(rows: Rows): Boolean = rows.sup.forall(!_.isEmpty)

  def toCondition(filter: Expression, indexByColumn: Map[String, String]): Condition = filter match {
    case And(left, right) => AndCond(Seq(toCondition(left, indexByColumn), toCondition(right, indexByColumn)))
    case Or(left, right) => OrCond(Seq(toCondition(left, indexByColumn), toCondition(right, indexByColumn)))
    case Not(child) => NotCond(toCondition(child, indexByColumn))
    case p: TokenPredicate => (p.text, constantString(p.query)) match {
      case (a: AttributeReference, Some(query)) if indexByColumn.contains(a.name.toLowerCase) =>
        val indexPartition = indexByColumn(a.name.toLowerCase)
        val tokens = FullTextIndexUtils.tokenize(query).asScala.toSeq
        p match {
          case _ if tokens.isEmpty => Unknown
          case _: HudiHasAnyTokens => OrCond(tokens.map(TokenLeaf(indexPartition, _)))
          case _: HudiHasTokenPrefix => if (tokens.size == 1) PrefixLeaf(indexPartition, tokens.head) else Unknown
          case _: HudiHasPhrase =>
            // A row with the phrase holds all of its indexed tokens, but a row holding them may lack the phrase
            // (order, adjacency, repeated or over-long tokens), so only a one-token phrase is exact.
            val and = AndCond(tokens.map(TokenLeaf(indexPartition, _)))
            if (FullTextIndexUtils.tokenSequence(query).size == 1 && tokens.size == 1) and else SubsetOf(and)
          case _ => AndCond(tokens.map(TokenLeaf(indexPartition, _)))
        }
      case _ => Unknown
    }
    case _ => Unknown
  }

  private def constantString(e: Expression): Option[String] =
    if (e.foldable && e.dataType == StringType) Try(e.eval()).toOption.flatMap(Option(_)).map(_.toString) else None

  def indexLeaves(c: Condition): Seq[IndexLeaf] = c match {
    case AndCond(cs) => cs.flatMap(indexLeaves)
    case OrCond(cs) => cs.flatMap(indexLeaves)
    case NotCond(child) => indexLeaves(child)
    case SubsetOf(child) => indexLeaves(child)
    case leaf: IndexLeaf => Seq(leaf)
    case Unknown => Seq.empty
  }

  /** Whether positions can prune more than presence: an AND of indexed terms, or a NOT over one. */
  def rowLevelUseful(c: Condition): Boolean = c match {
    case AndCond(cs) => cs.count(indexLeaves(_).nonEmpty) >= 2 || cs.exists(rowLevelUseful)
    case OrCond(cs) => cs.exists(rowLevelUseful)
    case NotCond(child) => indexLeaves(child).nonEmpty
    case SubsetOf(child) => rowLevelUseful(child)
    case _ => false
  }

  /** Evaluates the condition on one file; without positions every present term counts for every row. */
  def evaluate(c: Condition, fileId: String, views: Map[String, IndexView],
               positions: Map[(String, String, String), RoaringBitmap]): Rows = c match {
    case AndCond(cs) => cs.map(evaluate(_, fileId, views, positions)).foldLeft(Rows(None, None)) { (a, b) =>
      Rows(intersect(a.sup, b.sup), intersect(a.sub, b.sub))
    }
    case OrCond(cs) => cs.map(evaluate(_, fileId, views, positions)).foldLeft(Rows(noRows, noRows)) { (a, b) =>
      Rows(union(a.sup, b.sup), union(a.sub, b.sub))
    }
    case NotCond(child) =>
      Rows(complement(evaluate(child, fileId, views, positions).sub, rowCount(fileId, views)), noRows)
    case SubsetOf(child) =>
      Rows(evaluate(child, fileId, views, positions).sup, noRows)
    case TokenLeaf(indexPartition, token) =>
      val view = views(indexPartition)
      if (!view.covered.contains(fileId)) {
        unknownRows
      } else {
        view.presence.get(token).flatMap(_.get(fileId)) match {
          case None => Rows(noRows, noRows)
          case Some(info) =>
            positions.get((indexPartition, token, fileId)) match {
              case Some(rows) => Rows(Some(rows), Some(rows))
              case None => Rows(None, if (inEveryRow(info)) None else noRows)
            }
        }
      }
    case PrefixLeaf(indexPartition, prefix) =>
      val view = views(indexPartition)
      if (!view.covered.contains(fileId)) {
        unknownRows
      } else {
        // Any of the terms starting with the prefix; none at all means no row matches.
        evaluate(OrCond(view.prefixTerms(prefix).map(TokenLeaf(indexPartition, _))), fileId, views, positions)
      }
    case Unknown => unknownRows
  }

  private def inEveryRow(info: HoodieFullTextIndexInfo): Boolean = info.getCardinality == info.getRowCount

  private def rowCount(fileId: String, views: Map[String, IndexView]): Option[Long] =
    views.values.flatMap(_.rowCounts.get(fileId)).headOption

  private def intersect(a: Option[RoaringBitmap], b: Option[RoaringBitmap]): Option[RoaringBitmap] = (a, b) match {
    case (None, _) => b
    case (_, None) => a
    case (Some(x), Some(y)) => Some(RoaringBitmap.and(x, y))
  }

  private def union(a: Option[RoaringBitmap], b: Option[RoaringBitmap]): Option[RoaringBitmap] = (a, b) match {
    case (Some(x), Some(y)) => Some(RoaringBitmap.or(x, y))
    case _ => None
  }

  private def complement(rows: Option[RoaringBitmap], rowCount: Option[Long]): Option[RoaringBitmap] = (rows, rowCount) match {
    case (None, _) => noRows
    case (Some(r), _) if r.isEmpty => None
    case (Some(r), Some(n)) => Some(RoaringBitmap.flip(r, 0L, n))
    case (Some(_), None) => None
  }
}

case class FullTextRawKey(key: String) extends RawKey {
  override def encode(): String = key
}
