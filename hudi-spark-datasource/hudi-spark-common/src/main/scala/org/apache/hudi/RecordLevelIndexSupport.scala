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

  def getRecordKey: Option[String] = {
    val recordKeysOpt: org.apache.hudi.common.util.Option[Array[String]] = metaClient.getTableConfig.getRecordKeyFields
    val recordKeyOpt = recordKeysOpt.map[String](JFunction.toJavaFunction[Array[String], String](arr =>
      if (arr.length == 1) {
        arr(0)
      } else {
        null
      }))
    Option.apply(recordKeyOpt.orElse(null))
  }

  def attributeMatchesRecordKey(attributeName: String): Boolean = {
    val recordKeyOpt = getRecordKey
    if (recordKeyOpt.isDefined && recordKeyOpt.get == attributeName) {
      return true
    }
    HoodieMetadataField.RECORD_KEY_METADATA_FIELD.getFieldName == recordKeyOpt.get
  }

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

  private def isIndexAvailable: Boolean = {
    metadataConfig.enabled && metaClient.getTableConfig.getMetadataPartitions.contains(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX)
  }
}
