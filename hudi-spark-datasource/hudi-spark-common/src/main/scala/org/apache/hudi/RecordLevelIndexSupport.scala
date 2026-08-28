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

import org.apache.hudi.DataSourceReadOptions.{QUERY_TYPE, TIME_TRAVEL_AS_OF_INSTANT}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.data.HoodiePairData
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{FileSlice, HoodieRecordGlobalLocation}
import org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField
import org.apache.hudi.common.model.HoodieTableQueryType.SNAPSHOT
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.InstantComparison
import org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps
import org.apache.hudi.common.util.HoodieDataUtils
import org.apache.hudi.core.index.record.HoodieRecordIndex
import org.apache.hudi.core.read.BaseHoodieTableFileIndex
import org.apache.hudi.keygen.KeyGenerator
import org.apache.hudi.metadata.HoodieTableMetadataUtil
import org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX
import org.apache.hudi.storage.StoragePath

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, Cast, EqualTo, Expression, In, Literal}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Base class for data skipping based on the Record Level Index (RLI) in the metadata table.
 *
 * The RLI maps a record key to the file group that stores it. The actual metadata lookup differs between
 * a global RLI ([[GlobalRecordLevelIndexSupport]]) and a partitioned RLI
 * ([[PartitionedRecordLevelIndexSupport]]); subclasses implement [[lookupCandidateFilesForRecordKeys]].
 * Use [[RecordLevelIndexSupport.create]] to instantiate the right implementation for a table.
 */
abstract class RecordLevelIndexSupport(spark: SparkSession,
                                       metadataConfig: HoodieMetadataConfig,
                                       metaClient: HoodieTableMetaClient)
  extends SparkBaseIndexSupport(spark, metadataConfig, metaClient) {

  override def getIndexName: String = RecordLevelIndexSupport.INDEX_NAME

  override def computeCandidateFileNames(fileIndex: HoodieFileIndex,
                                         queryFilters: Seq[Expression],
                                         queryReferencedColumns: Seq[String],
                                         prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])],
                                         shouldPushDownFilesFilter: Boolean
                                        ): Option[Set[String]] = {
    lazy val (_, recordKeys) = filterQueriesWithRecordKey(queryFilters)
    if (recordKeys.nonEmpty) {
      lookupCandidateFilesForRecordKeys(fileIndex, prunedPartitionsAndFileSlices, recordKeys)
    } else {
      Option.empty
    }
  }

  /**
   * Looks up the candidate files which may store the provided record keys from the record level index.
   * Implemented differently for a global vs a partitioned RLI.
   *
   * @param fileIndex                     the file index of the query
   * @param prunedPartitionsAndFileSlices already pruned partitions and file slices
   * @param recordKeys                    the record key literals extracted from the query filters
   * @return the set of candidate file names, or [[None]] if the index could not be used and pruning
   *         should be skipped (falling back to other indexes).
   */
  protected def lookupCandidateFilesForRecordKeys(fileIndex: HoodieFileIndex,
                                                  prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])],
                                                  recordKeys: List[String]): Option[Set[String]]

  override def invalidateCaches(): Unit = {
    // no caches for this index type, do nothing
  }

  /**
   * Builds a map from fileId to data-table partition path from record index lookup results.
   */
  protected def collectFileIdToPartitionMap(recordIndexData: HoodiePairData[String, HoodieRecordGlobalLocation]): mutable.Map[String, String] = {
    val recordKeyLocationsList = HoodieDataUtils.dedupeAndCollectAsList(recordIndexData)
    val fileIdToPartitionMap: mutable.Map[String, String] = mutable.Map.empty
    for (recordKeyLocation <- recordKeyLocationsList.asScala) {
      val location = recordKeyLocation.getValue
      fileIdToPartitionMap.put(location.getFileId, location.getPartitionPath)
    }
    fileIdToPartitionMap
  }

  /**
   * Filters the input files, keeping only those whose fileId is present in the record index lookup results.
   */
  protected def filterCandidateFiles(allFiles: Seq[StoragePath], fileIdToPartitionMap: mutable.Map[String, String]): Set[String] = {
    val candidateFiles: mutable.Set[String] = mutable.Set.empty
    for (file <- allFiles) {
      val fileId = FSUtils.getFileIdFromFilePath(file)
      if (fileIdToPartitionMap.contains(fileId)) {
        candidateFiles += file.getName
      }
    }
    candidateFiles.toSet
  }

  /**
   * Return true if metadata table is enabled and record index metadata partition is available.
   */
  def isIndexAvailable: Boolean = {
    metadataConfig.isEnabled && metaClient.getTableConfig.getMetadataPartitions.contains(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX)
  }

  /**
   * Returns true if the query type is supported by the index.
   */
  override def supportsQueryType(options: Map[String, String]): Boolean = {
    if (!options.getOrElse(QUERY_TYPE.key, QUERY_TYPE.defaultValue).equalsIgnoreCase(SNAPSHOT.name)) {
      // Disallow RLI for non-snapshot query types
      false
    } else {
      // Now handle the time-travel case for snapshot queries
      options.get(TIME_TRAVEL_AS_OF_INSTANT.key)
        .fold {
          // No time travel instant specified, so allow if it's a snapshot query
          true
        } { instant =>
          // Check if the as.of.instant is greater than or equal to the last completed instant.
          // We can still use RLI for data skipping for the latest snapshot.
          compareTimestamps(HoodieSqlCommonUtils.formatQueryInstant(instant),
            InstantComparison.GREATER_THAN_OR_EQUALS, metaClient.getCommitsTimeline.filterCompletedInstants.lastInstant.get.requestedTime)
        }
    }
  }
}

object RecordLevelIndexSupport {
  val INDEX_NAME = "RECORD_LEVEL"

  /**
   * Upper bound on the number of candidate data-table partitions eligible for a partitioned RLI lookup.
   *
   * Unlike the global RLI (a single lookup over all keys), the partitioned variant performs one metadata-table read
   * per candidate partition. When a query does not filter on the partition column the candidate set can span many
   * partitions, and fanning out a lookup to each one can add latency that outweighs the skipping benefit. Once the
   * candidate partition count exceeds this threshold, pruning is skipped.
   */
  private[hudi] val MAX_PARTITIONS = 10

  /**
   * Creates the [[RecordLevelIndexSupport]] implementation matching the table's record level index:
   * [[PartitionedRecordLevelIndexSupport]] when the RLI is partitioned, otherwise
   * [[GlobalRecordLevelIndexSupport]].
   */
  def create(spark: SparkSession,
             metadataConfig: HoodieMetadataConfig,
             metaClient: HoodieTableMetaClient): RecordLevelIndexSupport = {
    val isPartitioned = metaClient.getIndexForMetadataPartition(PARTITION_NAME_RECORD_INDEX)
      .map[Boolean](indexDef => HoodieRecordIndex.isPartitioned(indexDef))
      .orElse(false)
    if (isPartitioned) {
      new PartitionedRecordLevelIndexSupport(spark, metadataConfig, metaClient)
    } else {
      new GlobalRecordLevelIndexSupport(spark, metadataConfig, metaClient)
    }
  }

  private def getDefaultAttributeFetcher(): Function1[Expression, Expression] = {
    expr => expr
  }

  def getSimpleLiteralGenerator(): Function2[AttributeReference, Literal, String] = {
    (_, lit) => lit.value.toString
  }

  def getComplexKeyLiteralGenerator(): Function2[AttributeReference, Literal, String] = {
    (attr: AttributeReference, lit: Literal) => attr.name + KeyGenerator.DEFAULT_COLUMN_VALUE_SEPARATOR + lit.value.toString
  }

  /**
   * If the input query is an EqualTo or IN query on simple record key columns, the function returns a tuple of
   * list of the query and list of record key literals present in the query otherwise returns an empty option.
   *
   * @param queryFilter The query that need to be filtered.
   * @return Tuple of filtered query and list of record key literals that need to be matched
   */
  def filterQueryWithRecordKey(queryFilter: Expression, recordKeyOpt: Option[String]): Option[(Expression, List[String])] = {
    filterQueryWithRecordKey(queryFilter, recordKeyOpt, getDefaultAttributeFetcher())
  }

  def filterQueryWithRecordKey(queryFilter: Expression, recordKeyOpt: Option[String],
                               literalGenerator: Function2[AttributeReference, Literal, String]): Option[(Expression, List[String])] = {
    filterQueryWithRecordKey(queryFilter, recordKeyOpt, literalGenerator, getDefaultAttributeFetcher())._1
  }

  def filterQueryWithRecordKey(queryFilter: Expression, recordKeyOpt: Option[String], attributeFetcher: Function1[Expression, Expression]): Option[(Expression, List[String])] = {
    filterQueryWithRecordKey(queryFilter, recordKeyOpt, getSimpleLiteralGenerator(), attributeFetcher)._1
  }

  def filterQueryWithRecordKey(queryFilter: Expression, recordKeyOpt: Option[String], literalGenerator: Function2[AttributeReference, Literal, String],
                               attributeFetcher: Function1[Expression, Expression]): (Option[(Expression, List[String])], Boolean) = {
    queryFilter match {
      case equalToQuery: EqualTo =>
        val attributeLiteralTuple = getAttributeLiteralTuple(attributeFetcher.apply(equalToQuery.left), attributeFetcher.apply(equalToQuery.right)).orNull
        if (attributeLiteralTuple != null) {
          val attribute = attributeLiteralTuple._1
          val literal = attributeLiteralTuple._2
          if (attribute != null && attribute.name != null && attributeMatchesRecordKey(attribute.name, recordKeyOpt)) {
            val recordKeyLiteral = literalGenerator.apply(attribute, literal)
            (Option.apply(EqualTo(attribute, literal), List.apply(recordKeyLiteral)), true)
          } else {
            (Option.empty, true)
          }
        } else {
          (Option.empty, true)
        }

      case inQuery: In =>
        var validINQuery = true
        val attributeOpt = Option.apply(
          attributeFetcher.apply(inQuery.value) match {
            case attribute: AttributeReference =>
              if (!attributeMatchesRecordKey(attribute.name, recordKeyOpt)) {
                validINQuery = false
                null
              } else {
                attribute
              }
            case _ =>
              validINQuery = false
              null
          })
        var literals: List[String] = List.empty
        inQuery.list.foreach {
          case literal: Literal if attributeOpt.isDefined =>
            val recordKeyLiteral = literalGenerator.apply(attributeOpt.get, literal)
            literals = literals :+ recordKeyLiteral
          case _ => validINQuery = false
        }
        if (validINQuery) {
          (Option.apply(In(attributeOpt.get, inQuery.list), literals), true)
        } else {
          (Option.empty, true)
        }

      // Handle And expression (composite filter)
      case andQuery: And =>
        val leftResult = filterQueryWithRecordKey(andQuery.left, recordKeyOpt, literalGenerator, attributeFetcher)
        val rightResult = filterQueryWithRecordKey(andQuery.right, recordKeyOpt, literalGenerator, attributeFetcher)

        val isSupported = leftResult._2 && rightResult._2
        if (!isSupported) {
          (Option.empty, false)
        } else {
          // If both left and right filters are valid, concatenate their results
          (leftResult._1, rightResult._1) match {
            case (Some((leftExp, leftKeys)), Some((rightExp, rightKeys))) =>
              // Return concatenated expressions and record keys
              (Option.apply(And(leftExp, rightExp), leftKeys ++ rightKeys), true)
            case (Some((leftExp, leftKeys)), None) =>
              // Return concatenated expressions and record keys
              (Option.apply(leftExp, leftKeys), true)
            case (None, Some((rightExp, rightKeys))) =>
              // Return concatenated expressions and record keys
              (Option.apply(rightExp, rightKeys), true)
            case _ => (Option.empty, true)
          }
        }

      case _ => (Option.empty, false)
    }
  }

  /**
   * Returns the list of storage paths from the pruned partitions and file slices.
   *
   * @param prunedPartitionsAndFileSlices - List of pruned partitions and file slices
   * @return List of storage paths
   */
  def getPrunedStoragePaths(prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])],
                            fileIndex: HoodieFileIndex): Seq[StoragePath] = {
    if (prunedPartitionsAndFileSlices.isEmpty) {
      fileIndex.inputFiles.map(strPath => new StoragePath(strPath)).toSeq
    } else {
      prunedPartitionsAndFileSlices
        .flatMap { case (_, fileSlices) =>
          fileSlices
        }
        .flatMap { fileSlice =>
          val baseFileOption = Option(fileSlice.getBaseFile.orElse(null))
          val logFiles = if (fileIndex.includeLogFiles) {
            fileSlice.getLogFiles.iterator().asScala
          } else {
            Iterator.empty
          }
          val baseFilePaths = baseFileOption.map(baseFile => baseFile.getStoragePath).toSeq
          val logFilePaths = logFiles.map(logFile => logFile.getPath).toSeq

          baseFilePaths ++ logFilePaths
        }
    }
  }

  /**
   * Returns the attribute and literal pair given the operands of a binary operator. The pair is returned only if one of
   * the operand is an attribute and other is literal. In other cases it returns an empty Option.
   * @param expression1 - Left operand of the binary operator
   * @param expression2 - Right operand of the binary operator
   * @return Attribute and literal pair
   */
  private def getAttributeLiteralTuple(expression1: Expression, expression2: Expression): Option[(AttributeReference, Literal)] = {
    expression1 match {
      case attr: AttributeReference => expression2 match {
        case literal: Literal =>
          Option.apply(attr, literal)
        case _ =>
          Option.empty
      }
      case literal: Literal => expression2 match {
        case attr: AttributeReference =>
          Option.apply(attr, literal)
        case cast: Cast if cast.child.isInstanceOf[AttributeReference] =>
          Option.apply(cast.child.asInstanceOf[AttributeReference], literal)
        case _ =>
          Option.empty
      }
      case cast: Cast if cast.child.isInstanceOf[AttributeReference] => expression2 match {
        case literal: Literal =>
          Option.apply(cast.child.asInstanceOf[AttributeReference], literal)
        case _ =>
          Option.empty
      }
      case _ => Option.empty
    }
  }

  /**
   * Matches the configured simple record key with the input attribute name.
   * @param attributeName The attribute name provided in the query
   * @return true if input attribute name matches the configured simple record key
   */
  private def attributeMatchesRecordKey(attributeName: String, recordKeyOpt: Option[String]): Boolean = {
    if (recordKeyOpt.isDefined && recordKeyOpt.get == attributeName) {
      true
    } else {
      HoodieMetadataField.RECORD_KEY_METADATA_FIELD.getFieldName == attributeName
    }
  }
}
