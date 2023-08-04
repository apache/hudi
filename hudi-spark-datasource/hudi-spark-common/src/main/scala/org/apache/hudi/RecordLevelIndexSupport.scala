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

import org.apache.hadoop.fs.FileStatus
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.metadata.{HoodieTableMetadata, HoodieTableMetadataUtil}
import org.apache.hudi.util.JFunction
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, Literal}

import scala.collection.{JavaConverters, mutable}

class RecordLevelIndexSupport(spark: SparkSession,
                              metadataConfig: HoodieMetadataConfig,
                              metaClient: HoodieTableMetaClient) {

  @transient private lazy val engineCtx = new HoodieSparkEngineContext(new JavaSparkContext(spark.sparkContext))
  @transient private lazy val metadataTable: HoodieTableMetadata =
    HoodieTableMetadata.create(engineCtx, metadataConfig, metaClient.getBasePathV2.toString)

  /**
   * Returns the list of candidate files which should be queried after pruning based on query filters.
   * @param allFiles - List of all files which needs to be considered for the query
   * @param queryFilters - Input query filters. List of candidate files are pruned based on these query filters.
   * @return Sequence of file names which need to be queried
   */
  def getCandidateFiles(allFiles: Seq[FileStatus], queryFilters: Seq[Expression]): Set[String] = {
    val (_, recordKeys) = filterQueryFiltersWithRecordKey(queryFilters)
    val recordKeyLocationsMap = metadataTable.readRecordIndex(JavaConverters.seqAsJavaListConverter(recordKeys).asJava)
    val fileIdToPartitionMap: mutable.Map[String, String] = mutable.Map.empty
    val candidateFiles: mutable.Set[String] = mutable.Set.empty
    for (location <- JavaConverters.collectionAsScalaIterableConverter(recordKeyLocationsMap.values()).asScala) {
      fileIdToPartitionMap.put(location.getFileId, location.getPartitionPath)
    }
    for (file <- allFiles) {
      val fileId = FSUtils.getFileIdFromFilePath(file.getPath)
      val partitionOpt = fileIdToPartitionMap.get(fileId)
      if (partitionOpt.isDefined) {
        candidateFiles += file.getPath.getName
      }
    }
    candidateFiles.toSet
  }

  /**
   * Returns the configured record key for the table if it is a simple record key else returns empty option.
   */
  private def getRecordKeyConfig: Option[String] = {
    val recordKeysOpt: org.apache.hudi.common.util.Option[Array[String]] = metaClient.getTableConfig.getRecordKeyFields
    val recordKeyOpt = recordKeysOpt.map[String](JFunction.toJavaFunction[Array[String], String](arr =>
      if (arr.length == 1) {
        arr(0)
      } else {
        null
      }))
    Option.apply(recordKeyOpt.orElse(null))
  }

  /**
   * Matches the configured simple record key with the input attribute name.
   * @param attributeName The attribute name provided in the query
   * @return true if input attribute name matches the configured simple record key
   */
  private def attributeMatchesRecordKey(attributeName: String): Boolean = {
    val recordKeyOpt = getRecordKeyConfig
    if (recordKeyOpt.isDefined && recordKeyOpt.get == attributeName) {
      true
    } else {
      HoodieMetadataField.RECORD_KEY_METADATA_FIELD.getFieldName == recordKeyOpt.get
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
        case _ =>
          Option.empty
      }
      case _ => Option.empty
    }

  }

  /**
   * Given query filters, it filters the EqualTo queries on simple record key columns and returns a tuple of list of such
   * queries and list of record key literals present in the query.
   * @param queryFilters The queries that need to be filtered.
   * @return Tuple of List of filtered queries and list of record key literals that need to be matched
   */
  private def filterQueryFiltersWithRecordKey(queryFilters: Seq[Expression]): (List[Expression], List[String]) = {
    var recordKeyQueries: List[Expression] = List.empty
    var recordKeys: List[String] = List.empty
    for (query <- queryFilters) {
      query match {
        case equalToQuery: EqualTo =>
          val (attribute, literal) = getAttributeLiteralTuple(equalToQuery.left, equalToQuery.right).orNull
          if (attribute != null && attribute.name != null && attributeMatchesRecordKey(attribute.name)) {
            recordKeys = recordKeys :+ literal.value.toString
            recordKeyQueries = recordKeyQueries :+ equalToQuery
          }
        case _ =>
      }
    }
    Tuple2.apply(recordKeyQueries, recordKeys)
  }

  /**
   * Returns true in cases when metadata table is enabled and Record Level Index is built.
   */
  def isIndexApplicable(queryFilters: Seq[Expression]): Boolean = {
    isIndexAvailable && filterQueryFiltersWithRecordKey(queryFilters)._1.nonEmpty
  }

  /**
   * Return true if metadata table is enabled and record index metadata partition is available.
   */
  private def isIndexAvailable: Boolean = {
    metadataConfig.enabled && metaClient.getTableConfig.getMetadataPartitions.contains(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX)
  }
}
