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

package org.apache.hudi.functional

import org.apache.hudi.{BaseHoodieTableFileIndex, HoodieFileIndex, HoodieSparkUtils, PartitionBucketIndexSupport}
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{FileSlice, HoodieBaseFile, PartitionBucketIndexHashingConfig}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.HoodieIndexConfig
import org.apache.hudi.index.bucket.BucketIdentifier
import org.apache.hudi.keygen.NonpartitionedKeyGenerator
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.storage.{StoragePath, StoragePathInfo}

import org.apache.avro.generic.GenericData
import org.apache.spark.sql.HoodieCatalystExpressionUtils
import org.apache.spark.sql.catalyst.encoders.DummyExpressionHolder
import org.junit.jupiter.api.{BeforeEach, Tag, Test}
import org.mockito.Mockito

@Tag("functional")
class TestPartitionBucketIndexSupport extends TestBucketIndexSupport {

  private val DEFAULT_RULE = "regex"
  private val EXPRESSION_BUCKET_NUMBER = 19
  private val DEFAULT_EXPRESSIONS = "\\d{4}\\-(06\\-(01|17|18)|11\\-(01|10|11))," + EXPRESSION_BUCKET_NUMBER
  private val DEFAULT_BUCKET_NUMBER = 10
  private val DEFAULT_PARTITION_PATH = Array("2025-06-17", "2025-06-18")
  private var fileIndex: HoodieFileIndex = null
  @BeforeEach
  override def setUp(): Unit = {
    super.setUp()
    PartitionBucketIndexHashingConfig.saveHashingConfig(metaClient, DEFAULT_EXPRESSIONS, DEFAULT_RULE, DEFAULT_BUCKET_NUMBER, null)
    fileIndex = Mockito.mock(classOf[HoodieFileIndex])
  }

  @Test
  override def testSingleHashFieldsExpression: Unit = {
    val configProperties = new TypedProperties()
    configProperties.setProperty(HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD.key, "A")
    configProperties.setProperty(HoodieTableConfig.RECORDKEY_FIELDS.key, "A")
    configProperties.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key, "A")

    configProperties.setProperty(HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key, String.valueOf(DEFAULT_BUCKET_NUMBER))
    metaClient.getTableConfig.setValue(HoodieTableConfig.CREATE_SCHEMA.key(), avroSchemaStr)
    val metadataConfig = HoodieMetadataConfig.newBuilder
      .fromProperties(configProperties)
      .enable(configProperties.getBoolean(HoodieMetadataConfig.ENABLE.key, true)).build()
    val bucketIndexSupport: PartitionBucketIndexSupport = new PartitionBucketIndexSupport(spark, metadataConfig, metaClient)


    // init
    val testKeyGenerator = new NonpartitionedKeyGenerator(configProperties)
    var record = new GenericData.Record(schema.toAvroSchema)
    record.put("A", "1")
    val bucket1Id4 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record).getRecordKey, "A", EXPRESSION_BUCKET_NUMBER)
    record = new GenericData.Record(schema.toAvroSchema)
    record.put("A", "2")
    val bucket2Id5 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record).getRecordKey, "A", EXPRESSION_BUCKET_NUMBER)
    record = new GenericData.Record(schema.toAvroSchema)
    record.put("A", "3")
    val bucket3Id6 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record).getRecordKey, "A", EXPRESSION_BUCKET_NUMBER)
    record = new GenericData.Record(schema.toAvroSchema)
    record.put("A", "4")
    val bucket4Id7 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record).getRecordKey, "A", EXPRESSION_BUCKET_NUMBER)
    record = new GenericData.Record(schema.toAvroSchema)
    record.put("A", "5")
    val bucket5Id8 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record).getRecordKey, "A", EXPRESSION_BUCKET_NUMBER)
    assert(bucket1Id4 == 4 && bucket2Id5 == 5 && bucket3Id6 == 6 && bucket4Id7 == 7 && bucket5Id8 == 8)

    // fileIdStr
    val token = FSUtils.makeWriteToken(1, 0, 1)
    val bucket1Id4FileName = FSUtils.makeBaseFileName("00000000000000000", token, BucketIdentifier.newBucketFileIdPrefix(bucket1Id4) + "-0", HoodieTableConfig.BASE_FILE_FORMAT.defaultValue.getFileExtension)
    val bucket2Id5FileName = FSUtils.makeBaseFileName("00000000000000000", token, BucketIdentifier.newBucketFileIdPrefix(bucket2Id5) + "-0", HoodieTableConfig.BASE_FILE_FORMAT.defaultValue.getFileExtension)
    val bucket3Id6FileName = FSUtils.makeBaseFileName("00000000000000000", token, BucketIdentifier.newBucketFileIdPrefix(bucket3Id6) + "-0", HoodieTableConfig.BASE_FILE_FORMAT.defaultValue.getFileExtension)
    val bucket4Id7FileName = FSUtils.makeBaseFileName("00000000000000000", token, BucketIdentifier.newBucketFileIdPrefix(bucket4Id7) + "-0", HoodieTableConfig.BASE_FILE_FORMAT.defaultValue.getFileExtension)
    val bucket5Id8FileName = FSUtils.makeBaseFileName("00000000000000000", token, BucketIdentifier.newBucketFileIdPrefix(bucket5Id8) + "-0", HoodieTableConfig.BASE_FILE_FORMAT.defaultValue.getFileExtension)


    val allFileNames = Set.apply(bucket1Id4FileName, bucket2Id5FileName, bucket3Id6FileName, bucket4Id7FileName, bucket5Id8FileName)
    var equalTo = "A = 3"
    exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket3Id6FileName), allFileNames)
    equalTo = "A = 3 And A = 4 and B = '6'"
    exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.empty, allFileNames)
    equalTo = "A = 5 And B = 'abc'"
    exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket5Id8FileName), allFileNames)
    equalTo = "A = C and A = 1"
    exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket1Id4FileName), allFileNames)
    equalTo = "A = 5 Or A = 2"
    exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket5Id8FileName, bucket2Id5FileName), allFileNames)
    equalTo = "A = 5 Or A = 2 Or A = 8"
    exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket5Id8FileName, bucket2Id5FileName), allFileNames)
    equalTo = "A = 5 Or (A = 2 and B = 'abc')"
    exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket5Id8FileName, bucket2Id5FileName), allFileNames)
    equalTo = "A = 5 And (A = 2 Or B = 'abc')"
    exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket5Id8FileName), allFileNames)
    equalTo = "A = 5 And (A = 2 Or B = 'abc')"
    exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket5Id8FileName), allFileNames)
  }

  @Test
  override def testMultipleHashFieldsExpress(): Unit = {
    val configProperties = new TypedProperties()
    configProperties.setProperty(HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD.key, "A,B")
    configProperties.setProperty(HoodieTableConfig.RECORDKEY_FIELDS.key, "A,B")
    configProperties.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key, "A,B")
    configProperties.setProperty(HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key, String.valueOf(DEFAULT_BUCKET_NUMBER))
    metaClient.getTableConfig.setValue(HoodieTableConfig.CREATE_SCHEMA.key(), avroSchemaStr)
    val metadataConfig = HoodieMetadataConfig.newBuilder
      .fromProperties(configProperties)
      .enable(configProperties.getBoolean(HoodieMetadataConfig.ENABLE.key, true)).build()
    val bucketIndexSupport = new PartitionBucketIndexSupport(spark, metadataConfig, metaClient)
    val keyGenerator = bucketIndexSupport.getKeyGenerator
    assert(keyGenerator.isInstanceOf[NonpartitionedKeyGenerator])

    // init
    val testKeyGenerator = new NonpartitionedKeyGenerator(configProperties)
    var record = new GenericData.Record(schema.toAvroSchema)
    record.put("A", "1")
    record.put("B", "2")
    val bucket1Id4 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record).getRecordKey, "A,B", EXPRESSION_BUCKET_NUMBER)
    record = new GenericData.Record(schema.toAvroSchema)
    record.put("A", "2")
    record.put("B", "3")
    val bucket2Id5 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record).getRecordKey, "A,B", EXPRESSION_BUCKET_NUMBER)
    record = new GenericData.Record(schema.toAvroSchema)
    record.put("A", "3")
    record.put("B", "4")
    val bucket3Id6 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record).getRecordKey, "A,B", EXPRESSION_BUCKET_NUMBER)
    record = new GenericData.Record(schema.toAvroSchema)
    record.put("A", "4")
    record.put("B", "5")
    val bucket4Id7 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record).getRecordKey, "A,B", EXPRESSION_BUCKET_NUMBER)
    record = new GenericData.Record(schema.toAvroSchema)
    record.put("A", "5")
    record.put("B", "6")
    val bucket5Id8 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record).getRecordKey, "A,B", EXPRESSION_BUCKET_NUMBER)
    assert(bucket1Id4 == 3 && bucket2Id5 == 16 && bucket3Id6 == 10 && bucket4Id7 == 4 && bucket5Id8 == 17)

    // fileIdStr
    val token = FSUtils.makeWriteToken(1, 0, 1)
    val bucket1Id4FileName = FSUtils.makeBaseFileName("00000000000000000", token, BucketIdentifier.newBucketFileIdPrefix(bucket1Id4) + "-0", HoodieTableConfig.BASE_FILE_FORMAT.defaultValue.getFileExtension)
    val bucket2Id5FileName = FSUtils.makeBaseFileName("00000000000000000", token, BucketIdentifier.newBucketFileIdPrefix(bucket2Id5) + "-0", HoodieTableConfig.BASE_FILE_FORMAT.defaultValue.getFileExtension)
    val bucket3Id6FileName = FSUtils.makeBaseFileName("00000000000000000", token, BucketIdentifier.newBucketFileIdPrefix(bucket3Id6) + "-0", HoodieTableConfig.BASE_FILE_FORMAT.defaultValue.getFileExtension)
    val bucket4Id7FileName = FSUtils.makeBaseFileName("00000000000000000", token, BucketIdentifier.newBucketFileIdPrefix(bucket4Id7) + "-0", HoodieTableConfig.BASE_FILE_FORMAT.defaultValue.getFileExtension)
    val bucket5Id8FileName = FSUtils.makeBaseFileName("00000000000000000", token, BucketIdentifier.newBucketFileIdPrefix(bucket5Id8) + "-0", HoodieTableConfig.BASE_FILE_FORMAT.defaultValue.getFileExtension)

    val allFileNames = Set.apply(bucket1Id4FileName, bucket2Id5FileName, bucket3Id6FileName, bucket4Id7FileName, bucket5Id8FileName)

    var equalTo = "A = 2 and B = '3'"
    exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket2Id5FileName), allFileNames)

    equalTo = "A = 4 and B = '5'"
    exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket4Id7FileName), allFileNames)
  }

  def exprFilePathAnswerCheck(bucketIndexSupport: PartitionBucketIndexSupport, exprRaw: String, expectResult: Set[String],
                              allFileStatus: Set[String]): Unit = {
    if (!HoodieSparkUtils.gteqSpark4_0) { // TODO (HUDI-9403)
      val resolveExpr = HoodieCatalystExpressionUtils.resolveExpr(spark, exprRaw, structSchema)
      val optimizerPlan = spark.sessionState.optimizer.execute(DummyExpressionHolder(Seq(resolveExpr)))
      val optimizerExpr = optimizerPlan.asInstanceOf[DummyExpressionHolder].exprs.head

      // split input files into different partitions
      val partitionPath1 = DEFAULT_PARTITION_PATH(0)
      val allFileSlices1: Seq[FileSlice] = allFileStatus.slice(0, 3).map(fileName => {
        val slice = new FileSlice(partitionPath1, "00000000000000000", FSUtils.getFileId(fileName))
        slice.setBaseFile(new HoodieBaseFile(new StoragePathInfo(new StoragePath(fileName), 0L, false, 0, 0, 0)))
        slice
      }).toSeq

      val partitionPath2 = DEFAULT_PARTITION_PATH(1)
      val allFileSlices2: Seq[FileSlice] = allFileStatus.slice(3, 5).map(fileName => {
        val slice = new FileSlice(partitionPath1, "00000000000000000", FSUtils.getFileId(fileName))
        slice.setBaseFile(new HoodieBaseFile(new StoragePathInfo(new StoragePath(fileName), 0L, false, 0, 0, 0)))
        slice
      }).toSeq

      val input = Seq((Option.apply(new BaseHoodieTableFileIndex.PartitionPath(partitionPath1, Array())), allFileSlices1),
        (Option.apply(new BaseHoodieTableFileIndex.PartitionPath(partitionPath2, Array())), allFileSlices2))
      val candidate = bucketIndexSupport.computeCandidateFileNames(fileIndex, splitConjunctivePredicates(optimizerExpr),
        Seq(), input, false)

      assert(candidate.get.equals(expectResult))
    }
  }
}
