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

import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.FileSlice
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.collection.Pair
import org.apache.hudi.config.HoodieIndexConfig
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.index.HoodieIndex.IndexType
import org.apache.hudi.index.bucket.BucketIdentifier
import org.apache.hudi.keygen.KeyGenerator
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory

import org.apache.avro.generic.GenericData
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, EmptyRow, Expression, Literal}
import org.apache.spark.sql.types.{DoubleType, FloatType}
import org.apache.spark.util.collection.BitSet
import org.slf4j.LoggerFactory

import scala.collection.{mutable, JavaConverters}

class BucketIndexSupport(spark: SparkSession,
                         metadataConfig: HoodieMetadataConfig,
                         metaClient: HoodieTableMetaClient)
  extends SparkBaseIndexSupport (spark, metadataConfig, metaClient) {

  private val log = LoggerFactory.getLogger(getClass)

  private lazy val keyGenerator = {
    val props = TypedProperties.copy(metadataConfig.getProps())
    TypedProperties.putAll(props, metaClient.getTableConfig.getProps)
    HoodieSparkKeyGeneratorFactory.createKeyGenerator(props)
  }

  private lazy val avroSchema = new TableSchemaResolver(metaClient).getTableAvroSchema(false)

  override def getIndexName: String = BucketIndexSupport.INDEX_NAME

  /**
   * Return true if table can use bucket index
   * - has bucket hash field
   * - table is bucket index writer
   * - only support simple bucket engine
   */
  def isIndexAvailable: Boolean = {
    indexBucketHashFieldsOpt.isDefined &&
      metadataConfig.getStringOrDefault(HoodieIndexConfig.INDEX_TYPE, "").equalsIgnoreCase(IndexType.BUCKET.name()) &&
      metadataConfig.getStringOrDefault(HoodieIndexConfig.BUCKET_INDEX_ENGINE_TYPE).equalsIgnoreCase(HoodieIndex.BucketIndexEngineType.SIMPLE.name()) &&
      metadataConfig.getBooleanOrDefault(HoodieIndexConfig.BUCKET_QUERY_INDEX)
  }

  override def invalidateCaches(): Unit = {
    // no caches for this index type, do nothing
  }

  override def computeCandidateFileNames(fileIndex: HoodieFileIndex,
                                         queryFilters: Seq[Expression],
                                         queryReferencedColumns: Seq[String],
                                         prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])],
                                         shouldPushDownFilesFilter: Boolean): Option[Set[String]] = {

    val bucketIdsBitMapByFilter = filterQueriesWithBucketHashField(queryFilters)

    if (bucketIdsBitMapByFilter.isDefined && bucketIdsBitMapByFilter.get.cardinality() > 0) {
      val allFilesName = getPrunedPartitionsAndFileNames(fileIndex, prunedPartitionsAndFileSlices)._2
      Option.apply(getCandidateFiles(allFilesName, bucketIdsBitMapByFilter.get))
    } else {
      Option.empty
    }
  }

  def getCandidateFiles(allFilesName: Set[String], bucketIds: BitSet): Set[String] = {
    val candidateFiles: mutable.Set[String] = mutable.Set.empty
    for (fileName <- allFilesName) {
      val fileId = FSUtils.getFileIdFromFileName(fileName)
      val fileBucketId = BucketIdentifier.bucketIdFromFileId(fileId)
      if (bucketIds.get(fileBucketId)) {
        candidateFiles += fileName
      }
    }
    candidateFiles.toSet
  }

  def filterQueriesWithBucketHashField(queryFilters: Seq[Expression],
                                       bucketNumber: Int = metadataConfig.getIntOrDefault(HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS)): Option[BitSet] = {
    if (indexBucketHashFieldsOpt.isEmpty || queryFilters.isEmpty) {
      None
    } else {
      var matchedBuckets: BitSet = null
      if (indexBucketHashFieldsOpt.get.size == 1) {
        matchedBuckets = getBucketsBySingleHashFields(queryFilters.reduce(And), indexBucketHashFieldsOpt.get.get(0), bucketNumber)
      } else {
        matchedBuckets = getBucketsByMultipleHashFields(queryFilters,
          JavaConverters.asScalaBufferConverter(indexBucketHashFieldsOpt.get).asScala.toSet, bucketNumber)
      }

      val numBucketsSelected = matchedBuckets.cardinality()

      // None means all the buckets need to be scanned
      if (numBucketsSelected == bucketNumber) {
        log.info("The query predicates do not include equality expressions for all the hashing fields, fall back to the other indices")
        None
      } else {
        Some(matchedBuckets)
      }
    }
  }

  // multiple hash fields only support Equality expression by And
  private def getBucketsByMultipleHashFields(queryFilters: Seq[Expression], indexBucketHashFields: Set[String], numBuckets: Int): BitSet = {
    val hashValuePairs = queryFilters.map(expr => getEqualityFieldPair(expr, indexBucketHashFields)).filter(pair => pair != null)
    val matchedBuckets = new BitSet(numBuckets)
    if (hashValuePairs.size != indexBucketHashFields.size) {
      matchedBuckets.setUntil(numBuckets)
    } else {
      val record = new GenericData.Record(avroSchema)
      hashValuePairs.foreach(p => record.put(p.getKey, p.getValue))
      val hoodieKey = keyGenerator.getKey(record)
      matchedBuckets.set(BucketIdentifier.getBucketId(hoodieKey.getRecordKey, indexBucketHashFieldsOpt.get, numBuckets))
    }
    matchedBuckets
  }

  private def getEqualityFieldPair(expr: Expression, equalityFields: Set[String]): Pair[String, Any] = {
    expr match {
      case expressions.Equality(a: Attribute, Literal(v, _)) if equalityFields.contains(a.name) =>
        Pair.of(a.name, v)
      case _ =>
        null
    }
  }

  private def getBucketsBySingleHashFields(expr: Expression, bucketColumnName: String, numBuckets: Int): BitSet = {

    def getBucketNumber(attr: Attribute, v: Any): Int = {
      val record = new GenericData.Record(avroSchema)
      record.put(attr.name, v)
      val hoodieKey = keyGenerator.getKey(record)
      BucketIdentifier.getBucketId(hoodieKey.getRecordKey, indexBucketHashFieldsOpt.get, numBuckets)
    }

    def getBucketSetFromIterable(attr: Attribute, iter: Iterable[Any]): BitSet = {
      val matchedBuckets = new BitSet(numBuckets)
      iter
        .map(v => getBucketNumber(attr, v))
        .foreach(bucketNum => matchedBuckets.set(bucketNum))
      matchedBuckets
    }

    def getBucketSetFromValue(attr: Attribute, v: Any): BitSet = {
      val matchedBuckets = new BitSet(numBuckets)
      matchedBuckets.set(getBucketNumber(attr, v))
      matchedBuckets
    }

    expr match {
      case expressions.Equality(a: Attribute, Literal(v, _)) if a.name == bucketColumnName =>
        getBucketSetFromValue(a, v)
      case expressions.In(a: Attribute, list)
        if list.forall(_.isInstanceOf[Literal]) && a.name == bucketColumnName =>
        getBucketSetFromIterable(a, list.map(e => e.eval(EmptyRow)))
      case expressions.InSet(a: Attribute, hset) if a.name == bucketColumnName =>
        getBucketSetFromIterable(a, hset)
      case expressions.IsNull(a: Attribute) if a.name == bucketColumnName =>
        getBucketSetFromValue(a, null)
      case expressions.IsNaN(a: Attribute)
        if a.name == bucketColumnName && a.dataType == FloatType =>
        getBucketSetFromValue(a, Float.NaN)
      case expressions.IsNaN(a: Attribute)
        if a.name == bucketColumnName && a.dataType == DoubleType =>
        getBucketSetFromValue(a, Double.NaN)
      case expressions.And(left, right) =>
        getBucketsBySingleHashFields(left, bucketColumnName, numBuckets) &
          getBucketsBySingleHashFields(right, bucketColumnName, numBuckets)
      case expressions.Or(left, right) =>
        getBucketsBySingleHashFields(left, bucketColumnName, numBuckets) |
          getBucketsBySingleHashFields(right, bucketColumnName, numBuckets)
      case _ =>
        val matchedBuckets = new BitSet(numBuckets)
        matchedBuckets.setUntil(numBuckets)
        matchedBuckets
    }
  }

  /**
   * returns the configured bucket field for the table,
   * will fall back to record key fields if the bucket fields are not set up.
   */
  private val indexBucketHashFieldsOpt: Option[java.util.List[String]] = {
    val bucketHashFields = metadataConfig.getStringOrDefault(HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD,
      metadataConfig.getString(HoodieTableConfig.RECORDKEY_FIELDS))
    if (bucketHashFields == null || bucketHashFields.isEmpty) {
      Option.apply(null)
    } else {
      Option.apply(JavaConverters.seqAsJavaListConverter(bucketHashFields.split(",")).asJava)
    }
  }

  def getKeyGenerator: KeyGenerator = {
    keyGenerator
  }
}

object BucketIndexSupport {
  val INDEX_NAME = "BUCKET"
}

