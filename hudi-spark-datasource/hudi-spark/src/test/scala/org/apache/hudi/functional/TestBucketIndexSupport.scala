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

import org.apache.hudi.BucketIndexSupport
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.table.view.{FileSystemViewManager, FileSystemViewStorageConfig}
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.index.bucket.BucketIdentifier
import org.apache.hudi.index.bucket.partition.NumBucketsFunction
import org.apache.hudi.keygen.{ComplexKeyGenerator, NonpartitionedKeyGenerator}
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hudi.timeline.service.TimelineService

import org.apache.avro.generic.GenericData
import org.apache.spark.sql.{BucketPartitionUtils, HoodieCatalystExpressionUtils, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.types._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Tag, Test}

import java.util

@Tag("functional")
class TestBucketIndexSupport extends HoodieSparkClientTestBase with PredicateHelper {

  var spark: SparkSession = _

  val avroSchemaStr = "{\"namespace\": \"example.avro\", \"type\": \"record\", " + "\"name\": \"logicalTypes\"," +
    "\"fields\": [" +
    "{\"name\": \"A\", \"type\": [\"null\", \"long\"], \"default\": null}," +
    "{\"name\": \"B\", \"type\": [\"null\", \"string\"], \"default\": null}," +
    "{\"name\": \"C\", \"type\": [\"null\", \"long\"], \"default\": null}," +
    "{\"name\": \"D\", \"type\": [\"null\", \"string\"], \"default\": null}" +
    "]}"

  val structSchema: StructType =
    StructType(
      Seq(
        StructField("A", LongType),
        StructField("B", StringType),
        StructField("C", LongType),
        StructField("D", VarcharType(32))
      )
    )

  val schema = HoodieSchema.parse(avroSchemaStr)

  @BeforeEach
  override def setUp() {
    initPath()
    initSparkContexts()

    setTableName("hoodie_test")
    initMetaClient()

    spark = sqlContext.sparkSession
  }

  @AfterEach
  override def tearDown() = {
    cleanupSparkContexts()
  }

  @Test
  def testBucketIndexRemotePartitioner(): Unit = {
    val numBuckets = 511
    val config = HoodieWriteConfig.newBuilder
      .withPath(basePath).
      withIndexConfig(HoodieIndexConfig.newBuilder()
        .withBucketNum(numBuckets.toString)
        .enableBucketRemotePartitioner(true).build())
      .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder
        .withRemoteServerPort(incrementTimelineServicePortToUse).build)
      .build

    val timelineService = new TimelineService(HadoopFSUtils.getStorageConf,
      TimelineService.Config.builder.enableMarkerRequests(true).enableRemotePartitioner(true)
      .serverPort(config.getViewStorageConfig.getRemoteViewServerPort).build,
      FileSystemViewManager.createViewManager(context, config.getMetadataConfig, config.getViewStorageConfig, config.getCommonConfig))

    timelineService.startService
    this.timelineService = timelineService

    timelineService.startService()
    val numBucketsFunction = NumBucketsFunction.fromWriteConfig(config)
    val partitionNum = 1533
    val partitioner = BucketPartitionUtils.getRemotePartitioner(config.getViewStorageConfig, numBucketsFunction, partitionNum)
    val dataPartitions = List("dt=20250501", "dt=20250502", "dt=20250503", "dt=20250504", "dt=20250505", "dt=20250506")
    val res = new util.HashMap[Int, Int]
    dataPartitions.foreach(dataPartition => {
      for (i <- 1 to numBuckets) {
        // for bucket id from 1 to numBuckets
        // mock 1000 spark partitions
        val sparkPartition = partitioner.getPartition((dataPartition, i))
        if (res.containsKey(sparkPartition)) {
          val value = res.get(sparkPartition) + 1
          res.put(sparkPartition, value)
        } else {
          res.put(sparkPartition, 1)
        }
      }
    })
    timelineService.close()

    res.values().stream().forEach(value => {
      assert(value == (numBuckets*dataPartitions.size/partitionNum))
    })
  }

  @Test
  def testSingleHashFieldsExpression: Unit = {
    val bucketNumber = 19
    val configProperties = new TypedProperties()
    configProperties.setProperty(HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD.key, "A")
    configProperties.setProperty(HoodieTableConfig.RECORDKEY_FIELDS.key, "A")
    configProperties.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key, "A")
    configProperties.setProperty(HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key, String.valueOf(bucketNumber))
    metaClient.getTableConfig.setValue(HoodieTableConfig.CREATE_SCHEMA.key(), avroSchemaStr)
    val metadataConfig = HoodieMetadataConfig.newBuilder
      .fromProperties(configProperties)
      .enable(configProperties.getBoolean(ENABLE.key, true)).build()
    val bucketIndexSupport = new BucketIndexSupport(spark, metadataConfig, metaClient)
    val keyGenerator = bucketIndexSupport.getKeyGenerator
    assert(keyGenerator.isInstanceOf[NonpartitionedKeyGenerator])

    // init
    val testKeyGenerator = new NonpartitionedKeyGenerator(configProperties)
    var record = new GenericData.Record(schema.toAvroSchema)
    record.put("A", "1")
    val bucket1Id4 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record).getRecordKey, "A", bucketNumber)
    record = new GenericData.Record(schema.toAvroSchema)
    record.put("A", "2")
    val bucket2Id5 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record).getRecordKey, "A", bucketNumber)
    record = new GenericData.Record(schema.toAvroSchema)
    record.put("A", "3")
    val bucket3Id6 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record).getRecordKey, "A", bucketNumber)
    record = new GenericData.Record(schema.toAvroSchema)
    record.put("A", "4")
    val bucket4Id7 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record).getRecordKey, "A", bucketNumber)
    record = new GenericData.Record(schema.toAvroSchema)
    record.put("A", "5")
    val bucket5Id8 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record).getRecordKey, "A", bucketNumber)
    assert(bucket1Id4 == 4 && bucket2Id5 == 5 && bucket3Id6 == 6 && bucket4Id7 == 7 && bucket5Id8 == 8)

    // fileIdStr
    val token = FSUtils.makeWriteToken(1, 0, 1)
    val bucket1Id4FileName = FSUtils.makeBaseFileName("00000000000000000", token, BucketIdentifier.newBucketFileIdPrefix(bucket1Id4) + "-0",HoodieTableConfig.BASE_FILE_FORMAT.defaultValue.getFileExtension)
    val bucket2Id5FileName = FSUtils.makeBaseFileName("00000000000000000", token, BucketIdentifier.newBucketFileIdPrefix(bucket2Id5) + "-0",HoodieTableConfig.BASE_FILE_FORMAT.defaultValue.getFileExtension)
    val bucket3Id6FileName = FSUtils.makeBaseFileName("00000000000000000", token, BucketIdentifier.newBucketFileIdPrefix(bucket3Id6) + "-0",HoodieTableConfig.BASE_FILE_FORMAT.defaultValue.getFileExtension)
    val bucket4Id7FileName = FSUtils.makeBaseFileName("00000000000000000", token, BucketIdentifier.newBucketFileIdPrefix(bucket4Id7) + "-0",HoodieTableConfig.BASE_FILE_FORMAT.defaultValue.getFileExtension)
    val bucket5Id8FileName = FSUtils.makeBaseFileName("00000000000000000", token, BucketIdentifier.newBucketFileIdPrefix(bucket5Id8) + "-0",HoodieTableConfig.BASE_FILE_FORMAT.defaultValue.getFileExtension)


    val allFileNames = Set.apply(bucket1Id4FileName, bucket2Id5FileName, bucket3Id6FileName, bucket4Id7FileName, bucket5Id8FileName)

    var equalTo = "A = 3"
    exprBucketAnswerCheck(bucketIndexSupport, equalTo, List.apply(bucket3Id6), fallback = false)
    exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket3Id6FileName), allFileNames, fallback = false)
    equalTo = "A = 3 And A = 4 and B = '6'"
    exprBucketAnswerCheck(bucketIndexSupport, equalTo, List.empty, fallback = false)
    exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.empty, allFileNames, fallback = false)
    equalTo = "A = 5 And B = 'abc'"
    exprBucketAnswerCheck(bucketIndexSupport, equalTo, List.apply(bucket5Id8), fallback = false)
    exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket5Id8FileName), allFileNames, fallback = false)
    equalTo = "A = C and A = 1"
    exprBucketAnswerCheck(bucketIndexSupport, equalTo, List.apply(bucket1Id4), fallback = false)
    exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket1Id4FileName), allFileNames, fallback = false)
    equalTo = "A = 5 Or A = 2"
    exprBucketAnswerCheck(bucketIndexSupport, equalTo, List.apply(bucket5Id8, bucket2Id5), fallback = false)
    exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket5Id8FileName, bucket2Id5FileName), allFileNames, fallback = false)
    equalTo = "A = 5 Or A = 2 Or A = 8"
    exprBucketAnswerCheck(bucketIndexSupport, equalTo, List.apply(bucket5Id8, bucket2Id5), fallback = false)
    exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket5Id8FileName, bucket2Id5FileName), allFileNames, fallback = false)
    equalTo = "A = 5 Or (A = 2 and B = 'abc')"
    exprBucketAnswerCheck(bucketIndexSupport, equalTo, List.apply(bucket5Id8, bucket2Id5), fallback = false)
    exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket5Id8FileName, bucket2Id5FileName), allFileNames, fallback = false)
    equalTo = "A = 5 And (A = 2 Or B = 'abc')"
    exprBucketAnswerCheck(bucketIndexSupport, equalTo, List.apply(bucket5Id8), fallback = false)
    exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket5Id8FileName), allFileNames, fallback = false)
    equalTo = "A = 5 And (A = 2 Or B = 'abc')"
    exprBucketAnswerCheck(bucketIndexSupport, equalTo, List.apply(bucket5Id8), fallback = false)
    exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket5Id8FileName), allFileNames, fallback = false)

    var inExpr = "A in (3)"
    exprBucketAnswerCheck(bucketIndexSupport, inExpr, List.apply(bucket3Id6), fallback = false)
    exprFilePathAnswerCheck(bucketIndexSupport, inExpr, Set.apply(bucket3Id6FileName), allFileNames, fallback = false)
    inExpr = "A in (3, 5)"
    exprBucketAnswerCheck(bucketIndexSupport, inExpr, List.apply(bucket3Id6, bucket5Id8), fallback = false)
    exprFilePathAnswerCheck(bucketIndexSupport, inExpr, Set.apply(bucket3Id6FileName, bucket5Id8FileName), allFileNames, fallback = false)

    var complexExpr = "A = 3 And A in (3)"
    exprBucketAnswerCheck(bucketIndexSupport, complexExpr, List.apply(bucket3Id6), fallback = false)
    exprFilePathAnswerCheck(bucketIndexSupport, complexExpr, Set.apply(bucket3Id6FileName), allFileNames, fallback = false)
    complexExpr = "A = 3 Or A in (3, 5)"
    exprBucketAnswerCheck(bucketIndexSupport, complexExpr, List.apply(bucket3Id6, bucket5Id8), fallback = false)
    exprFilePathAnswerCheck(bucketIndexSupport, complexExpr, Set.apply(bucket3Id6FileName, bucket5Id8FileName), allFileNames, fallback = false)
    complexExpr = "A = 3 Or A in (5, 2)"
    exprBucketAnswerCheck(bucketIndexSupport, complexExpr, List.apply(bucket3Id6, bucket5Id8, bucket2Id5), fallback = false)
    exprFilePathAnswerCheck(bucketIndexSupport, complexExpr, Set.apply(bucket3Id6FileName, bucket5Id8FileName, bucket2Id5FileName), allFileNames, fallback = false)
    complexExpr = "A = 3 and C in (3, 5)"
    exprBucketAnswerCheck(bucketIndexSupport, complexExpr, List.apply(bucket3Id6), fallback = false)
    exprFilePathAnswerCheck(bucketIndexSupport, complexExpr, Set.apply(bucket3Id6FileName), allFileNames, fallback = false)


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

  @Test
  def testMultipleHashFieldsExpress(): Unit = {
    val bucketNumber = 19
    val configProperties = new TypedProperties()
    configProperties.setProperty(HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD.key, "A,B")
    configProperties.setProperty(HoodieTableConfig.RECORDKEY_FIELDS.key, "A,B")
    configProperties.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key, "A,B")
    configProperties.setProperty(HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key, String.valueOf(bucketNumber))
    metaClient.getTableConfig.setValue(HoodieTableConfig.CREATE_SCHEMA.key(), avroSchemaStr)
    val metadataConfig = HoodieMetadataConfig.newBuilder
      .fromProperties(configProperties)
      .enable(configProperties.getBoolean(ENABLE.key, true)).build()
    val bucketIndexSupport = new BucketIndexSupport(spark, metadataConfig, metaClient)
    val keyGenerator = bucketIndexSupport.getKeyGenerator
    assert(keyGenerator.isInstanceOf[NonpartitionedKeyGenerator])

    // init
    val testKeyGenerator = new NonpartitionedKeyGenerator(configProperties)
    var record = new GenericData.Record(schema.toAvroSchema)
    record.put("A", "1")
    record.put("B", "2")
    val bucket1Id4 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record).getRecordKey, "A,B", bucketNumber)
    record = new GenericData.Record(schema.toAvroSchema)
    record.put("A", "2")
    record.put("B", "3")
    val bucket2Id5 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record).getRecordKey, "A,B", bucketNumber)
    record = new GenericData.Record(schema.toAvroSchema)
    record.put("A", "3")
    record.put("B", "4")
    val bucket3Id6 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record).getRecordKey, "A,B", bucketNumber)
    record = new GenericData.Record(schema.toAvroSchema)
    record.put("A", "4")
    record.put("B", "5")
    val bucket4Id7 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record).getRecordKey, "A,B", bucketNumber)
    record = new GenericData.Record(schema.toAvroSchema)
    record.put("A", "5")
    record.put("B", "6")
    val bucket5Id8 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record).getRecordKey, "A,B", bucketNumber)
    assert(bucket1Id4 == 3 && bucket2Id5 == 16 && bucket3Id6 == 10 && bucket4Id7 == 4 && bucket5Id8 == 17)

    // fileIdStr
    val token = FSUtils.makeWriteToken(1, 0, 1)
    val bucket1Id4FileName = FSUtils.makeBaseFileName("00000000000000000", token, BucketIdentifier.newBucketFileIdPrefix(bucket1Id4) + "-0",HoodieTableConfig.BASE_FILE_FORMAT.defaultValue.getFileExtension)
    val bucket2Id5FileName = FSUtils.makeBaseFileName("00000000000000000", token, BucketIdentifier.newBucketFileIdPrefix(bucket2Id5) + "-0",HoodieTableConfig.BASE_FILE_FORMAT.defaultValue.getFileExtension)
    val bucket3Id6FileName = FSUtils.makeBaseFileName("00000000000000000", token, BucketIdentifier.newBucketFileIdPrefix(bucket3Id6) + "-0",HoodieTableConfig.BASE_FILE_FORMAT.defaultValue.getFileExtension)
    val bucket4Id7FileName = FSUtils.makeBaseFileName("00000000000000000", token, BucketIdentifier.newBucketFileIdPrefix(bucket4Id7) + "-0",HoodieTableConfig.BASE_FILE_FORMAT.defaultValue.getFileExtension)
    val bucket5Id8FileName = FSUtils.makeBaseFileName("00000000000000000", token, BucketIdentifier.newBucketFileIdPrefix(bucket5Id8) + "-0",HoodieTableConfig.BASE_FILE_FORMAT.defaultValue.getFileExtension)

    val allFileNames = Set.apply(bucket1Id4FileName, bucket2Id5FileName, bucket3Id6FileName, bucket4Id7FileName, bucket5Id8FileName)

    var equalTo = "A = 2 and B = '3'"
    exprBucketAnswerCheck(bucketIndexSupport, equalTo, List.apply(bucket2Id5), fallback = false)
    exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket2Id5FileName), allFileNames, fallback = false)

    equalTo = "A = 4 and B = '5'"
    exprBucketAnswerCheck(bucketIndexSupport, equalTo, List.apply(bucket4Id7), fallback = false)
    exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket4Id7FileName), allFileNames, fallback = false)

    // fall back other index
    var fallBack = "B = '5'"
    exprBucketAnswerCheck(bucketIndexSupport, fallBack, List.empty, fallback = true)
    fallBack = "A = 3"
    exprBucketAnswerCheck(bucketIndexSupport, fallBack, List.empty, fallback = true)
    fallBack = "A = 3 or B = '4'"
    exprBucketAnswerCheck(bucketIndexSupport, fallBack, List.empty, fallback = true)
  }


  def testMultipleHashFieldsExpressionWithComplexKey(): Unit = {
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
    val bucketIndexSupport = new BucketIndexSupport(spark, metadataConfig, metaClient)
    val keyGenerator = bucketIndexSupport.getKeyGenerator
    assert(keyGenerator.isInstanceOf[ComplexKeyGenerator])

    // init
    val testKeyGenerator = new ComplexKeyGenerator(configProperties)
    var record = new GenericData.Record(schema.toAvroSchema)
    record.put("A", "1")
    record.put("B", "2")
    val bucket1Id4 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record).getRecordKey, "A,B", bucketNumber)
    record = new GenericData.Record(schema.toAvroSchema)
    record.put("A", "2")
    record.put("B", "3")
    val bucket2Id5 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record).getRecordKey, "A,B", bucketNumber)
    record = new GenericData.Record(schema.toAvroSchema)
    record.put("A", "3")
    record.put("B", "4")
    val bucket3Id6 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record).getRecordKey, "A,B", bucketNumber)
    record = new GenericData.Record(schema.toAvroSchema)
    record.put("A", "4")
    record.put("B", "5")
    val bucket4Id7 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record).getRecordKey, "A,B", bucketNumber)
    record = new GenericData.Record(schema.toAvroSchema)
    record.put("A", "5")
    record.put("B", "6")
    val bucket5Id8 = BucketIdentifier.getBucketId(testKeyGenerator.getKey(record).getRecordKey, "A,B", bucketNumber)
    assert(bucket1Id4 == 3 && bucket2Id5 == 16 && bucket3Id6 == 10 && bucket4Id7 == 4 && bucket5Id8 == 17)

    // fileIdStr
    val token = FSUtils.makeWriteToken(1, 0, 1)
    val bucket1Id4FileName = FSUtils.makeBaseFileName("00000000000000000", token, BucketIdentifier.newBucketFileIdPrefix(bucket1Id4) + "-0",HoodieTableConfig.BASE_FILE_FORMAT.defaultValue.getFileExtension)
    val bucket2Id5FileName = FSUtils.makeBaseFileName("00000000000000000", token, BucketIdentifier.newBucketFileIdPrefix(bucket2Id5) + "-0",HoodieTableConfig.BASE_FILE_FORMAT.defaultValue.getFileExtension)
    val bucket3Id6FileName = FSUtils.makeBaseFileName("00000000000000000", token, BucketIdentifier.newBucketFileIdPrefix(bucket3Id6) + "-0",HoodieTableConfig.BASE_FILE_FORMAT.defaultValue.getFileExtension)
    val bucket4Id7FileName = FSUtils.makeBaseFileName("00000000000000000", token, BucketIdentifier.newBucketFileIdPrefix(bucket4Id7) + "-0",HoodieTableConfig.BASE_FILE_FORMAT.defaultValue.getFileExtension)
    val bucket5Id8FileName = FSUtils.makeBaseFileName("00000000000000000", token, BucketIdentifier.newBucketFileIdPrefix(bucket5Id8) + "-0",HoodieTableConfig.BASE_FILE_FORMAT.defaultValue.getFileExtension)

    val allFileNames = Set.apply(bucket1Id4FileName, bucket2Id5FileName, bucket3Id6FileName, bucket4Id7FileName, bucket5Id8FileName)

    var equalTo = "A = 2 and B = '3'"
    exprBucketAnswerCheck(bucketIndexSupport, equalTo, List.apply(bucket2Id5), fallback = false)
    exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket2Id5FileName), allFileNames, fallback = false)

    equalTo = "A = 4 and B = '5'"
    exprBucketAnswerCheck(bucketIndexSupport, equalTo, List.apply(bucket4Id7), fallback = false)
    exprFilePathAnswerCheck(bucketIndexSupport, equalTo, Set.apply(bucket4Id7FileName), allFileNames, fallback = false)

    // fall back other index
    var fallBack = "B = '5'"
    exprBucketAnswerCheck(bucketIndexSupport, fallBack, List.empty, fallback = true)
    fallBack = "A = 3"
    exprBucketAnswerCheck(bucketIndexSupport, fallBack, List.empty, fallback = true)
    fallBack = "A = 3 or B = '4'"
    exprBucketAnswerCheck(bucketIndexSupport, fallBack, List.empty, fallback = true)
  }

  def exprBucketAnswerCheck(bucketIndexSupport: BucketIndexSupport, exprRaw: String, expectResult: List[Int], fallback: Boolean): Unit = {
    val resolveExpr = HoodieCatalystExpressionUtils.resolveExpr(spark, exprRaw, structSchema)
    val dummyExpressionHolder = HoodieDummyExpressionHolder(Seq(resolveExpr), resolveExpr.references.toSeq)
    val optimizerPlan = spark.sessionState.optimizer.execute(dummyExpressionHolder)
    val optimizerExpr = optimizerPlan.asInstanceOf[HoodieDummyExpressionHolder].exprs.head

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
                              allFileStatus: Set[String], fallback: Boolean): Unit = {
    val resolveExpr = HoodieCatalystExpressionUtils.resolveExpr(spark, exprRaw, structSchema)
    val dummyExpressionHolder = HoodieDummyExpressionHolder(Seq(resolveExpr), resolveExpr.references.toSeq)
    val optimizerPlan = spark.sessionState.optimizer.execute(dummyExpressionHolder)
    val optimizerExpr = optimizerPlan.asInstanceOf[HoodieDummyExpressionHolder].exprs.head

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

  @Test
  def testBucketQueryIsAvaliable(): Unit = {
    val configProperties = new TypedProperties()
    configProperties.setProperty(HoodieTableConfig.RECORDKEY_FIELDS.key(), "A")
    configProperties.setProperty(HoodieIndexConfig.INDEX_TYPE.key(), "BUCKET")
    val metadataConfig = HoodieMetadataConfig.newBuilder
      .fromProperties(configProperties)
      .enable(configProperties.getBoolean(ENABLE.key, false)).build()
    val bucketIndexSupport = new BucketIndexSupport(spark, metadataConfig, metaClient)

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

// SPARK-44219 added extra rule to validate expressions against its children's references to check if there are dangling references
// This is forked from Spark's [[DummyExpressionHolder]], which always set the output to Nil and would fail this test
case class HoodieDummyExpressionHolder(exprs: Seq[Expression], output: Seq[Attribute]) extends LeafNode {
  override lazy val resolved = true
}
