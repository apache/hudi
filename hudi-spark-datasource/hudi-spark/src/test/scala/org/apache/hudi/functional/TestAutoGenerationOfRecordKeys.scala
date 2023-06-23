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

package org.apache.hudi.functional

import org.apache.hadoop.fs.FileSystem
import org.apache.hudi.HoodieConversionUtils.toJavaOption
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.{HoodieRecord, HoodieTableType, WriteOperationType}
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.common.util
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.ExceptionUtil.getRootCause
import org.apache.hudi.exception.{HoodieException, HoodieKeyGeneratorException}
import org.apache.hudi.functional.CommonOptionUtils._
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.keygen.{ComplexKeyGenerator, KeyGenUtils, NonpartitionedKeyGenerator, SimpleKeyGenerator, TimestampBasedKeyGenerator}
import org.apache.hudi.keygen.constant.KeyGeneratorOptions.Config
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hudi.util.JFunction
import org.apache.hudi.{DataSourceWriteOptions, HoodieDataSourceHelpers, ScalaAssertionSupport}
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.{SaveMode, SparkSession, SparkSessionExtensions}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, EnumSource}

import java.util.function.Consumer
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class TestAutoGenerationOfRecordKeys extends HoodieSparkClientTestBase with ScalaAssertionSupport {
  var spark: SparkSession = null
  val verificationCol: String = "driver"
  val updatedVerificationVal: String = "driver_update"

  override def getSparkSessionExtensionsInjector: util.Option[Consumer[SparkSessionExtensions]] =
    toJavaOption(
      Some(
        JFunction.toJavaConsumer((receiver: SparkSessionExtensions) => new HoodieSparkSessionExtension().apply(receiver)))
    )

  @BeforeEach override def setUp() {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initFileSystem()
  }

  @AfterEach override def tearDown() = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
    FileSystem.closeAll()
    System.gc()
  }

  @ParameterizedTest
  @CsvSource(value = Array(
    "AVRO,insert,COPY_ON_WRITE", "AVRO,bulk_insert,COPY_ON_WRITE", "AVRO,insert,MERGE_ON_READ", "AVRO,bulk_insert,MERGE_ON_READ"
  ))
  def testRecordKeysAutoGen(recordType: HoodieRecordType, op: String, tableType: HoodieTableType): Unit = {
    testRecordKeysAutoGenInternal(recordType, op, tableType)
  }

  @Test
  def testRecordKeyAutoGenWithTimestampBasedKeyGen(): Unit = {
    testRecordKeysAutoGenInternal(HoodieRecordType.AVRO, "insert", HoodieTableType.COPY_ON_WRITE,
      classOf[TimestampBasedKeyGenerator].getName)
  }

  @Test
  def testRecordKeyAutoGenWithComplexKeyGen(): Unit = {
    testRecordKeysAutoGenInternal(HoodieRecordType.AVRO, "insert", HoodieTableType.COPY_ON_WRITE,
      classOf[ComplexKeyGenerator].getName,
      complexPartitionPath = true)
  }

  @Test
  def testRecordKeyAutoGenWithNonPartitionedKeyGen(): Unit = {
    testRecordKeysAutoGenInternal(HoodieRecordType.AVRO, "insert", HoodieTableType.COPY_ON_WRITE,
      classOf[NonpartitionedKeyGenerator].getName, complexPartitionPath = false, nonPartitionedDataset = true)
  }

  def testRecordKeysAutoGenInternal(recordType: HoodieRecordType, op: String = "insert", tableType: HoodieTableType = HoodieTableType.COPY_ON_WRITE,
                                    keyGenClass: String = classOf[SimpleKeyGenerator].getCanonicalName,
                                    complexPartitionPath: Boolean = false, nonPartitionedDataset: Boolean = false): Unit = {
    val (vanillaWriteOpts, readOpts) = getWriterReaderOpts(recordType)

    var options: Map[String, String] = vanillaWriteOpts ++ Map(
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> keyGenClass)

    val isTimestampBasedKeyGen: Boolean = classOf[TimestampBasedKeyGenerator].getName.equals(keyGenClass)
    if (isTimestampBasedKeyGen) {
      options += Config.TIMESTAMP_TYPE_FIELD_PROP -> "DATE_STRING"
      options += Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP -> "yyyy/MM/dd"
      options += Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP -> "yyyyMMdd"
    }

    if (complexPartitionPath) {
      options += KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key() -> "rider,_hoodie_is_deleted"
    }
    if (nonPartitionedDataset) {
      options = options -- Seq(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key())
    }

    // add partition Id and instant time
    options += KeyGenUtils.RECORD_KEY_GEN_PARTITION_ID_CONFIG -> "1"
    options += KeyGenUtils.RECORD_KEY_GEN_INSTANT_TIME_CONFIG -> "100"

    // NOTE: In this test we deliberately removing record-key configuration
    //       to validate Hudi is handling this case appropriately
    val writeOpts = options -- Seq(DataSourceWriteOptions.RECORDKEY_FIELD.key)

    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 5)).toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    inputDF.cache

    //
    // Step #1: Persist first batch with auto-gen'd record-keys
    //

    inputDF.write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, op)
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), tableType.name())
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))

    //
    // Step #2: Persist *same* batch with auto-gen'd record-keys (new record keys should
    //          be generated this time)
    //
    val inputDF2 = inputDF
    inputDF2.write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, op)
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), tableType.name())
      .mode(SaveMode.Append)
      .save(basePath)

    val readDF = spark.read.format("hudi")
      .options(readOpts)
      .load(basePath)
    readDF.cache

    val recordKeys = readDF.select(HoodieRecord.RECORD_KEY_METADATA_FIELD)
      .distinct()
      .collectAsList()
      .map(_.getString(0))

    // Validate auto-gen'd keys are globally unique
    assertEquals(10, recordKeys.size)

    // validate entire batch is present in snapshot read
    val expectedInputDf = inputDF.union(inputDF2).drop("partition","rider","_hoodie_is_deleted")
    val actualDf = readDF.drop(HoodieRecord.HOODIE_META_COLUMNS.asScala: _*).drop("partition","rider","_hoodie_is_deleted")
    assertEquals(expectedInputDf.except(actualDf).count, 0)
  }

  @ParameterizedTest
  @CsvSource(value = Array(
    "hoodie.populate.meta.fields,false","hoodie.combine.before.insert,true","hoodie.datasource.write.insert.drop.duplicates,true"
  ))
  def testRecordKeysAutoGenInvalidParams(configKey: String, configValue: String): Unit = {
    val (writeOpts, _) = getWriterReaderOpts(HoodieRecordType.AVRO)

    // NOTE: In this test we deliberately removing record-key configuration
    //       to validate Hudi is handling this case appropriately
    var opts = writeOpts -- Seq(DataSourceWriteOptions.RECORDKEY_FIELD.key)

    // add partition Id and instant time
    opts += KeyGenUtils.RECORD_KEY_GEN_PARTITION_ID_CONFIG -> "1"
    opts += KeyGenUtils.RECORD_KEY_GEN_INSTANT_TIME_CONFIG -> "100"

    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 1)).toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    val e = assertThrows(classOf[HoodieKeyGeneratorException]) {
      inputDF.write.format("hudi")
        .options(opts)
        .option(DataSourceWriteOptions.OPERATION.key, "insert")
        .option(configKey, configValue)
        .mode(SaveMode.Overwrite)
        .save(basePath)
    }

    assertTrue(getRootCause(e).getMessage.contains(configKey + " is not supported with auto generation of record keys"))
  }


  @Test
  def testRecordKeysAutoGenEnableToDisable(): Unit = {
    val (vanillaWriteOpts, readOpts) = getWriterReaderOpts(HoodieRecordType.AVRO)

    var options: Map[String, String] = vanillaWriteOpts ++ Map(
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> classOf[SimpleKeyGenerator].getCanonicalName)

    // NOTE: In this test we deliberately removing record-key configuration
    //       to validate Hudi is handling this case appropriately
    var writeOpts = options -- Seq(DataSourceWriteOptions.RECORDKEY_FIELD.key)

    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 5)).toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    inputDF.cache


    // add partition Id and instant time
    writeOpts += KeyGenUtils.RECORD_KEY_GEN_PARTITION_ID_CONFIG -> "1"
    writeOpts += KeyGenUtils.RECORD_KEY_GEN_INSTANT_TIME_CONFIG -> "100"

    //
    // Step #1: Persist first batch with auto-gen'd record-keys
    //
    inputDF.write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, "insert")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))

    //
    // Step #2: Insert w/ explicit record key config. Should fail since we can't modify this property.
    //
    val e = assertThrows(classOf[HoodieException]) {
      val inputDF2 = inputDF
      inputDF2.write.format("hudi")
        .options(writeOpts ++ Map(
          DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key"
        ))
        .option(DataSourceWriteOptions.OPERATION.key, "insert")
        .mode(SaveMode.Append)
        .save(basePath)
    }

    val expectedMsg = s"RecordKey:\t_row_key\tnull"
    assertTrue(getRootCause(e).getMessage.contains(expectedMsg))
  }
}
