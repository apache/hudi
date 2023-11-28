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

package org.apache.spark.sql.hudi

import org.apache.avro.generic.GenericData
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hudi.{AvroConversionUtils, BucketIndexSupport}
import org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.HoodieIndexConfig
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.index.bucket.BucketIdentifier
import org.apache.hudi.keygen.{ComplexKeyGenerator, NonpartitionedKeyGenerator}
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.spark.sql.HoodieCatalystExpressionUtils
import org.apache.spark.sql.catalyst.encoders.DummyExpressionHolder
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, VarcharType}

class TestBucketIndexSupport extends HoodieSparkSqlTestBase with PredicateHelper {

  val sourceTableSchema: StructType =
    StructType(
      Seq(
        StructField("A", LongType),
        StructField("B", StringType),
        StructField("C", LongType),
        StructField("D", VarcharType(32))
      )
    )

  test("Test Single Hash Fields Expression") {
    withTempDir { tmp =>
      val bucketNumber = 19
      val configProperties = new TypedProperties()
      configProperties.setProperty(HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD.key, "A")
      configProperties.setProperty(HoodieTableConfig.RECORDKEY_FIELDS.key, "A")
      configProperties.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key, "A")
      configProperties.setProperty(HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key, String.valueOf(bucketNumber))
      val metadataConfig = HoodieMetadataConfig.newBuilder
        .fromProperties(configProperties)
        .enable(configProperties.getBoolean(ENABLE.key, true)).build()
      val bucketIndexSupport = new BucketIndexSupport(metadataConfig, sourceTableSchema)
      val keyGenerator = bucketIndexSupport.getKeyGenerator
      assert(keyGenerator.isInstanceOf[NonpartitionedKeyGenerator])

      // init
      val testKeyGenerator = new NonpartitionedKeyGenerator(configProperties)
      val avroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(sourceTableSchema, "record", "")
      var record = new GenericData.Record(avroSchema)
      record.put("A", "1")
      val bucket1Id4 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record), "A", bucketNumber)
      record = new GenericData.Record(avroSchema)
      record.put("A", "2")
      val bucket2Id5 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record), "A", bucketNumber)
      record = new GenericData.Record(avroSchema)
      record.put("A", "3")
      val bucket3Id6 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record), "A", bucketNumber)
      record = new GenericData.Record(avroSchema)
      record.put("A", "4")
      val bucket4Id7 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record), "A", bucketNumber)
      record = new GenericData.Record(avroSchema)
      record.put("A", "5")
      val bucket5Id8 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record), "A", bucketNumber)
      assert(bucket1Id4 == 4 && bucket2Id5 == 5 && bucket3Id6 == 6 && bucket4Id7 == 7 && bucket5Id8 == 8)
      // fileIdStr
      val bucket1Id4FileId = BucketIdentifier.newBucketFileIdPrefix(bucket1Id4)
      val bucket2Id5FileId = BucketIdentifier.newBucketFileIdPrefix(bucket2Id5)
      val bucket3Id6FileId = BucketIdentifier.newBucketFileIdPrefix(bucket3Id6)
      val bucket4Id7FileId = BucketIdentifier.newBucketFileIdPrefix(bucket4Id7)
      val bucket5Id8FileId = BucketIdentifier.newBucketFileIdPrefix(bucket5Id8)

      val allFileStatus = Array.apply(
        new FileStatus(0, false, 0, 0, 0, 0,
          null, null, null, new Path(tmp.getCanonicalPath, bucket1Id4FileId)),
        new FileStatus(0, false, 0, 0, 0, 0,
          null, null, null, new Path(tmp.getCanonicalPath, bucket2Id5FileId)),
        new FileStatus(0, false, 0, 0, 0, 0,
          null, null, null, new Path(tmp.getCanonicalPath, bucket3Id6FileId)),
        new FileStatus(0, false, 0, 0, 0, 0,
          null, null, null, new Path(tmp.getCanonicalPath, bucket4Id7FileId)),
        new FileStatus(0, false, 0, 0, 0, 0,
          null, null, null, new Path(tmp.getCanonicalPath, bucket5Id8FileId)))

      var equalTo = "A = 3"
      exprBucketAnswerCheck(bucketIndexSupport, equalTo, List.apply(bucket3Id6), fallback = false)
      exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket3Id6FileId), allFileStatus, fallback = false)
      equalTo = "A = 3 And A = 4 and B = '6'"
      exprBucketAnswerCheck(bucketIndexSupport, equalTo, List.empty, fallback = false)
      exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.empty, allFileStatus, fallback = false)
      equalTo = "A = 5 And B = 'abc'"
      exprBucketAnswerCheck(bucketIndexSupport, equalTo, List.apply(bucket5Id8), fallback = false)
      exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket5Id8FileId), allFileStatus, fallback = false)
      equalTo = "A = C and A = 1"
      exprBucketAnswerCheck(bucketIndexSupport, equalTo, List.apply(bucket1Id4), fallback = false)
      exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket1Id4FileId), allFileStatus, fallback = false)
      equalTo = "A = 5 Or A = 2"
      exprBucketAnswerCheck(bucketIndexSupport, equalTo, List.apply(bucket5Id8, bucket2Id5), fallback = false)
      exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket5Id8FileId, bucket2Id5FileId), allFileStatus, fallback = false)
      equalTo = "A = 5 Or A = 2 Or A = 8"
      exprBucketAnswerCheck(bucketIndexSupport, equalTo, List.apply(bucket5Id8, bucket2Id5), fallback = false)
      exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket5Id8FileId, bucket2Id5FileId), allFileStatus, fallback = false)
      equalTo = "A = 5 Or (A = 2 and B = 'abc')"
      exprBucketAnswerCheck(bucketIndexSupport, equalTo, List.apply(bucket5Id8, bucket2Id5), fallback = false)
      exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket5Id8FileId, bucket2Id5FileId), allFileStatus, fallback = false)
      equalTo = "A = 5 And (A = 2 Or B = 'abc')"
      exprBucketAnswerCheck(bucketIndexSupport, equalTo, List.apply(bucket5Id8), fallback = false)
      exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket5Id8FileId), allFileStatus, fallback = false)
      equalTo = "A = 5 And (A = 2 Or B = 'abc')"
      exprBucketAnswerCheck(bucketIndexSupport, equalTo, List.apply(bucket5Id8), fallback = false)
      exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket5Id8FileId), allFileStatus, fallback = false)

      var inExpr = "A in (3)"
      exprBucketAnswerCheck(bucketIndexSupport, inExpr, List.apply(bucket3Id6), fallback = false)
      exprFilePathAnswerCheck(bucketIndexSupport, inExpr, Set.apply(bucket3Id6FileId), allFileStatus, fallback = false)
      inExpr = "A in (3, 5)"
      exprBucketAnswerCheck(bucketIndexSupport, inExpr, List.apply(bucket3Id6, bucket5Id8), fallback = false)
      exprFilePathAnswerCheck(bucketIndexSupport, inExpr, Set.apply(bucket3Id6FileId, bucket5Id8FileId), allFileStatus, fallback = false)

      var complexExpr = "A = 3 And A in (3)"
      exprBucketAnswerCheck(bucketIndexSupport, complexExpr, List.apply(bucket3Id6), fallback = false)
      exprFilePathAnswerCheck(bucketIndexSupport, complexExpr, Set.apply(bucket3Id6FileId), allFileStatus, fallback = false)
      complexExpr = "A = 3 Or A in (3, 5)"
      exprBucketAnswerCheck(bucketIndexSupport, complexExpr, List.apply(bucket3Id6, bucket5Id8), fallback = false)
      exprFilePathAnswerCheck(bucketIndexSupport, complexExpr, Set.apply(bucket3Id6FileId, bucket5Id8FileId), allFileStatus, fallback = false)
      complexExpr = "A = 3 Or A in (5, 2)"
      exprBucketAnswerCheck(bucketIndexSupport, complexExpr, List.apply(bucket3Id6, bucket5Id8, bucket2Id5), fallback = false)
      exprFilePathAnswerCheck(bucketIndexSupport, complexExpr, Set.apply(bucket3Id6FileId, bucket5Id8FileId, bucket2Id5FileId), allFileStatus, fallback = false)
      complexExpr = "A = 3 and C in (3, 5)"
      exprBucketAnswerCheck(bucketIndexSupport, complexExpr, List.apply(bucket3Id6), fallback = false)
      exprFilePathAnswerCheck(bucketIndexSupport, complexExpr, Set.apply(bucket3Id6FileId), allFileStatus, fallback = false)


      // fall back other index
      var fallBack = "B = 'abc'"
      exprBucketAnswerCheck(bucketIndexSupport, fallBack, List.empty, fallback = true)
      fallBack = "A = 5 Or B = 'abc'"
      exprBucketAnswerCheck(bucketIndexSupport, fallBack, List.empty, fallback = true)
      fallBack = "A = C"
      exprBucketAnswerCheck(bucketIndexSupport, fallBack, List.empty, fallback = true)
      // todo optimize this scene
      fallBack = "A = C and C = 1"
      exprBucketAnswerCheck(bucketIndexSupport, fallBack, List.empty, fallback = true)

      // in
      fallBack = "C in (3)"
      exprBucketAnswerCheck(bucketIndexSupport, fallBack, List.empty, fallback = true)

      // complex
      fallBack = "A = 3 Or C in (3, 5)"
      exprBucketAnswerCheck(bucketIndexSupport, fallBack, List.empty, fallback = true)
    }
  }

  test("Test Multiple HashFields Expression") {
    withTempDir { tmp =>
      val bucketNumber = 19
      val configProperties = new TypedProperties()
      configProperties.setProperty(HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD.key, "A,B")
      configProperties.setProperty(HoodieTableConfig.RECORDKEY_FIELDS.key, "A,B")
      configProperties.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key, "A,B")
      configProperties.setProperty(HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key, String.valueOf(bucketNumber))
      val metadataConfig = HoodieMetadataConfig.newBuilder
        .fromProperties(configProperties)
        .enable(configProperties.getBoolean(ENABLE.key, true)).build()
      val bucketIndexSupport = new BucketIndexSupport(metadataConfig, sourceTableSchema)
      val keyGenerator = bucketIndexSupport.getKeyGenerator
      assert(keyGenerator.isInstanceOf[NonpartitionedKeyGenerator])

      // init
      val testKeyGenerator = new NonpartitionedKeyGenerator(configProperties)
      val avroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(sourceTableSchema, "record", "")
      var record = new GenericData.Record(avroSchema)
      record.put("A", "1")
      record.put("B", "2")
      val bucket1Id4 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record), "A,B", bucketNumber)
      record = new GenericData.Record(avroSchema)
      record.put("A", "2")
      record.put("B", "3")
      val bucket2Id5 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record), "A,B", bucketNumber)
      record = new GenericData.Record(avroSchema)
      record.put("A", "3")
      record.put("B", "4")
      val bucket3Id6 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record), "A,B", bucketNumber)
      record = new GenericData.Record(avroSchema)
      record.put("A", "4")
      record.put("B", "5")
      val bucket4Id7 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record), "A,B", bucketNumber)
      record = new GenericData.Record(avroSchema)
      record.put("A", "5")
      record.put("B", "6")
      val bucket5Id8 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record), "A,B", bucketNumber)
      assert(bucket1Id4 == 3 && bucket2Id5 == 16 && bucket3Id6 == 10 && bucket4Id7 == 4 && bucket5Id8 == 17)
      // fileIdStr
      val bucket1Id4FileId = BucketIdentifier.newBucketFileIdPrefix(bucket1Id4)
      val bucket2Id5FileId = BucketIdentifier.newBucketFileIdPrefix(bucket2Id5)
      val bucket3Id6FileId = BucketIdentifier.newBucketFileIdPrefix(bucket3Id6)
      val bucket4Id7FileId = BucketIdentifier.newBucketFileIdPrefix(bucket4Id7)
      val bucket5Id8FileId = BucketIdentifier.newBucketFileIdPrefix(bucket5Id8)

      val allFileStatus = Array.apply(
        new FileStatus(0, false, 0, 0, 0, 0,
          null, null, null, new Path(tmp.getCanonicalPath, bucket1Id4FileId)),
        new FileStatus(0, false, 0, 0, 0, 0,
          null, null, null, new Path(tmp.getCanonicalPath, bucket2Id5FileId)),
        new FileStatus(0, false, 0, 0, 0, 0,
          null, null, null, new Path(tmp.getCanonicalPath, bucket3Id6FileId)),
        new FileStatus(0, false, 0, 0, 0, 0,
          null, null, null, new Path(tmp.getCanonicalPath, bucket4Id7FileId)),
        new FileStatus(0, false, 0, 0, 0, 0,
          null, null, null, new Path(tmp.getCanonicalPath, bucket5Id8FileId)))

      var equalTo = "A = 2 and B = '3'"
      exprBucketAnswerCheck(bucketIndexSupport, equalTo, List.apply(bucket2Id5), fallback = false)
      exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket2Id5FileId), allFileStatus, fallback = false)

      equalTo = "A = 4 and B = '5'"
      exprBucketAnswerCheck(bucketIndexSupport, equalTo, List.apply(bucket4Id7), fallback = false)
      exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket4Id7FileId), allFileStatus, fallback = false)

      // fall back other index
      var fallBack = "B = '5'"
      exprBucketAnswerCheck(bucketIndexSupport, fallBack, List.empty, fallback = true)
      fallBack = "A = 3"
      exprBucketAnswerCheck(bucketIndexSupport, fallBack, List.empty, fallback = true)
      fallBack = "A = 3 or B = '4'"
      exprBucketAnswerCheck(bucketIndexSupport, fallBack, List.empty, fallback = true)
    }
  }


  test("Test Multiple Hash Fields Expression With ComplexKey") {
    withTempDir { tmp =>
      val bucketNumber = 19
      val configProperties = new TypedProperties()
      configProperties.setProperty(HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD.key, "A,B")
      configProperties.setProperty(HoodieTableConfig.RECORDKEY_FIELDS.key, "A,B")
      configProperties.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key, "A,B")
      configProperties.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key, "C")
      configProperties.setProperty(HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key, String.valueOf(bucketNumber))
      val metadataConfig = HoodieMetadataConfig.newBuilder
        .fromProperties(configProperties)
        .enable(configProperties.getBoolean(ENABLE.key, true)).build()
      val bucketIndexSupport = new BucketIndexSupport(metadataConfig, sourceTableSchema)
      val keyGenerator = bucketIndexSupport.getKeyGenerator
      assert(keyGenerator.isInstanceOf[ComplexKeyGenerator])

      // init
      val testKeyGenerator = new ComplexKeyGenerator(configProperties)
      val avroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(sourceTableSchema, "record", "")
      var record = new GenericData.Record(avroSchema)
      record.put("A", "1")
      record.put("B", "2")
      val bucket1Id4 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record), "A,B", bucketNumber)
      record = new GenericData.Record(avroSchema)
      record.put("A", "2")
      record.put("B", "3")
      val bucket2Id5 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record), "A,B", bucketNumber)
      record = new GenericData.Record(avroSchema)
      record.put("A", "3")
      record.put("B", "4")
      val bucket3Id6 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record), "A,B", bucketNumber)
      record = new GenericData.Record(avroSchema)
      record.put("A", "4")
      record.put("B", "5")
      val bucket4Id7 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record), "A,B", bucketNumber)
      record = new GenericData.Record(avroSchema)
      record.put("A", "5")
      record.put("B", "6")
      val bucket5Id8 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record), "A,B", bucketNumber)
      assert(bucket1Id4 == 3 && bucket2Id5 == 16 && bucket3Id6 == 10 && bucket4Id7 == 4 && bucket5Id8 == 17)
      // fileIdStr
      val bucket1Id4FileId = BucketIdentifier.newBucketFileIdPrefix(bucket1Id4)
      val bucket2Id5FileId = BucketIdentifier.newBucketFileIdPrefix(bucket2Id5)
      val bucket3Id6FileId = BucketIdentifier.newBucketFileIdPrefix(bucket3Id6)
      val bucket4Id7FileId = BucketIdentifier.newBucketFileIdPrefix(bucket4Id7)
      val bucket5Id8FileId = BucketIdentifier.newBucketFileIdPrefix(bucket5Id8)

      val allFileStatus = Array.apply(
        new FileStatus(0, false, 0, 0, 0, 0,
          null, null, null, new Path(tmp.getCanonicalPath, bucket1Id4FileId)),
        new FileStatus(0, false, 0, 0, 0, 0,
          null, null, null, new Path(tmp.getCanonicalPath, bucket2Id5FileId)),
        new FileStatus(0, false, 0, 0, 0, 0,
          null, null, null, new Path(tmp.getCanonicalPath, bucket3Id6FileId)),
        new FileStatus(0, false, 0, 0, 0, 0,
          null, null, null, new Path(tmp.getCanonicalPath, bucket4Id7FileId)),
        new FileStatus(0, false, 0, 0, 0, 0,
          null, null, null, new Path(tmp.getCanonicalPath, bucket5Id8FileId)))

      var equalTo = "A = 2 and B = '3'"
      exprBucketAnswerCheck(bucketIndexSupport, equalTo, List.apply(bucket2Id5), fallback = false)
      exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket2Id5FileId), allFileStatus, fallback = false)

      equalTo = "A = 4 and B = '5'"
      exprBucketAnswerCheck(bucketIndexSupport, equalTo, List.apply(bucket4Id7), fallback = false)
      exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket4Id7FileId), allFileStatus, fallback = false)

      // fall back other index
      var fallBack = "B = '5'"
      exprBucketAnswerCheck(bucketIndexSupport, fallBack, List.empty, fallback = true)
      fallBack = "A = 3"
      exprBucketAnswerCheck(bucketIndexSupport, fallBack, List.empty, fallback = true)
      fallBack = "A = 3 or B = '4'"
      exprBucketAnswerCheck(bucketIndexSupport, fallBack, List.empty, fallback = true)
    }
  }

  def exprBucketAnswerCheck(bucketIndexSupport: BucketIndexSupport, exprRaw: String, expectResult: List[Int], fallback: Boolean): Unit = {
    val resolveExpr = HoodieCatalystExpressionUtils.resolveExpr(spark, exprRaw, sourceTableSchema)
    val optimizerPlan = spark.sessionState.optimizer.execute(DummyExpressionHolder(Seq(resolveExpr)))
    val optimizerExpr = optimizerPlan.asInstanceOf[DummyExpressionHolder].exprs.head

    val bucketSet = bucketIndexSupport.filterQueriesWithBucketHashField(splitConjunctivePredicates(optimizerExpr))
    if (fallback) {
      // will match all file, set is None, fallback other index
      assert(bucketSet.isEmpty)
    }
    if (expectResult.nonEmpty) {
      assert(bucketSet.isDefined)
      // can not check size, because Or can contain other file
      //      assert(bucketSet.get.cardinality() == expectResult.size)
      expectResult.foreach(expectId => assert(bucketSet.get.get(expectId)))
    } else {
      assert(bucketSet.isEmpty || bucketSet.get.cardinality() == 0)
    }
  }

  def exprFilePathAnswerCheck(bucketIndexSupport: BucketIndexSupport, exprRaw: String, expectResult: Set[String],
                              allFileStatus: Seq[FileStatus], fallback: Boolean): Unit = {
    val resolveExpr = HoodieCatalystExpressionUtils.resolveExpr(spark, exprRaw, sourceTableSchema)
    val optimizerPlan = spark.sessionState.optimizer.execute(DummyExpressionHolder(Seq(resolveExpr)))
    val optimizerExpr = optimizerPlan.asInstanceOf[DummyExpressionHolder].exprs.head

    val bucketSet = bucketIndexSupport.filterQueriesWithBucketHashField(splitConjunctivePredicates(optimizerExpr))
    if (fallback) {
      // will match all file, set is None, fallback other index
      assert(bucketSet.isEmpty)
    }
    if (expectResult.nonEmpty) {
      assert(bucketSet.isDefined)
      // can not check size, because Or can contain other file
      //      assert(bucketSet.get.cardinality() == expectResult.size)
      val candidateFiles = bucketIndexSupport.getCandidateFiles(allFileStatus, bucketSet.get)
      assert(expectResult == candidateFiles)
    } else {
      assert(bucketSet.isEmpty || bucketSet.get.cardinality() == 0)
    }
  }

  test("Test BucketQuery Is Available") {
    val configProperties = new TypedProperties()
    configProperties.setProperty(HoodieTableConfig.RECORDKEY_FIELDS.key(), "A")
    configProperties.setProperty(HoodieIndexConfig.INDEX_TYPE.key(), "BUCKET")
    val bucketNumber = 19
    val metadataConfig = HoodieMetadataConfig.newBuilder
      .fromProperties(configProperties)
      .enable(configProperties.getBoolean(ENABLE.key, false)).build()
    val bucketIndexSupport = new BucketIndexSupport(metadataConfig, sourceTableSchema)

    assert(!bucketIndexSupport.isIndexAvailable)
    metadataConfig.setValue(HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key(), String.valueOf(bucketNumber))
    assert(bucketIndexSupport.isIndexAvailable)
    metadataConfig.setValue(HoodieTableConfig.RECORDKEY_FIELDS.key(), "A,B")
    assert(bucketIndexSupport.isIndexAvailable)
    metadataConfig.setValue(HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD.key(), "A")
    assert(bucketIndexSupport.isIndexAvailable)
    metadataConfig.setValue(HoodieIndexConfig.INDEX_TYPE.key(), "SIMPLE")
    assert(!bucketIndexSupport.isIndexAvailable)
    metadataConfig.setValue(HoodieIndexConfig.INDEX_TYPE.key(), "BUCKET")
    assert(bucketIndexSupport.isIndexAvailable)
    metadataConfig.setValue(HoodieIndexConfig.BUCKET_INDEX_ENGINE_TYPE, HoodieIndex.BucketIndexEngineType.CONSISTENT_HASHING.name())
    assert(!bucketIndexSupport.isIndexAvailable)
    metadataConfig.setValue(HoodieIndexConfig.BUCKET_INDEX_ENGINE_TYPE, HoodieIndex.BucketIndexEngineType.SIMPLE.name())
    assert(bucketIndexSupport.isIndexAvailable)
    metadataConfig.setValue(HoodieIndexConfig.BUCKET_QUERY_INDEX, "false")
    assert(!bucketIndexSupport.isIndexAvailable)
    metadataConfig.setValue(HoodieIndexConfig.BUCKET_QUERY_INDEX, "true")
    assert(bucketIndexSupport.isIndexAvailable)
  }
}
