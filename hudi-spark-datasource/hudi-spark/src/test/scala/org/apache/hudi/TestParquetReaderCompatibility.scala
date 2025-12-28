/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import org.apache.hudi.TestParquetReaderCompatibility.{SparkSetting, TestScenario, ThreeLevel, TwoLevel}
import org.apache.hudi.TestParquetReaderCompatibility.NullabilityEnum.{NotNullable, Nullability, Nullable}
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.table.{HoodieTableConfig, ParquetTableSchemaResolver}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.common.util.ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.io.storage.HoodieIOFactory
import org.apache.hudi.metadata.HoodieBackedTableMetadata
import org.apache.hudi.storage.{HoodieStorageUtils, StoragePath}
import org.apache.hudi.storage.hadoop.{HadoopStorageConfiguration, HoodieHadoopStorage}
import org.apache.hudi.testutils.HoodieClientTestUtils

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.schema.OriginalType
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructField, StructType}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.mutable

object TestParquetReaderCompatibility {
  val listFieldName = "internal_list"
  abstract class SparkSetting {
    def value: String
    def overrideConf(conf: SparkConf): Unit
  }

  // Explicitly set 2 level
  case object TwoLevel extends SparkSetting {
    val value: String = "TwoLevel"
    def overrideConf(conf: SparkConf): Unit = {
      conf.set("spark.hadoop.parquet.avro.write-old-list-structure", true.toString)
    }
  }

  // Explicitly set 3 level
  case object ThreeLevel extends SparkSetting {
    val value: String = "ThreeLevel"

    def overrideConf(conf: SparkConf): Unit = {
      conf.set("spark.hadoop.parquet.avro.write-old-list-structure", false.toString)
    }
  }

  // Set nothing(likely most users do so) - default is 2 level inside Avro code.
  case object Default extends SparkSetting {
    def value: String = "TwoLevel"

    def overrideConf(conf: SparkConf): Unit = {}
  }

  val cases: Seq[SparkSetting] = Seq(TwoLevel, ThreeLevel, Default)

  object NullabilityEnum extends Enumeration {
    type Nullability = Value
    val Nullable: NullabilityEnum.Value = Value("Nullable")
    val NotNullable: NullabilityEnum.Value = Value("NotNullable")
  }

  case class TestScenario(initialLevel: SparkSetting, listNullability: NullabilityEnum.Nullability, targetLevel: SparkSetting, itemsNullability: NullabilityEnum.Nullability)

  /**
   * Here we are generating all possible combinations of settings, including default.
   **/
  def allPossibleCombinations: Seq[TestScenario] = {
    val allPossibleCombinations = for (
      initialLevel <- cases;
      listNullability <- NullabilityEnum.values.toSeq;
      targetLevel <- cases;
      itemsNullability <- NullabilityEnum.values.toSeq
    ) yield TestScenario(initialLevel, listNullability, targetLevel, itemsNullability)
    allPossibleCombinations.filter {
      case c => {
        val notAllowedNulls = Seq(TwoLevel, Default)
        // It's not allowed to have NULLs inside lists for 2 level lists(this matches explicit setting or default).
        !(c.itemsNullability == Nullable && (notAllowedNulls.contains(c.targetLevel) || notAllowedNulls.contains(c.initialLevel)))
      }
    }
  }

  /**
   * Predefined scenarios which fail/lead to dataloss without the fix.
   **/
  def selectedCombinations: Seq[TestScenario] = {
    Seq(
        // This scenario leads to silent dataloss mentioned here - https://github.com/apache/hudi/pull/11450 - basically all arrays
        // which are not updated in the incoming batch are set to null.
        TestScenario(initialLevel = TwoLevel, listNullability = Nullable, targetLevel = ThreeLevel, itemsNullability = NotNullable),
        // This scenario leads to exception mentioned here https://github.com/apache/hudi/pull/11450 - the only difference with silent dataloss
        // is that writer does not allow wrongly-read null to be written into new file, so write fails.
        TestScenario(initialLevel = TwoLevel, listNullability = NotNullable, targetLevel = ThreeLevel, itemsNullability = NotNullable),
        // This is reverse version of scenario TwoLevel -> ThreeLevel with nullable list value - leads to silent data loss.
        TestScenario(initialLevel = ThreeLevel, listNullability = Nullable, targetLevel = TwoLevel, itemsNullability = NotNullable),
        // This is reverse version of scenario TwoLevel -> ThreeLevel with not nullable list value - leads to exception.
        TestScenario(initialLevel = ThreeLevel, listNullability = NotNullable, targetLevel = TwoLevel, itemsNullability = NotNullable)
    )
  }
  def testSource: java.util.stream.Stream[TestScenario] = {
    val scenarios = if(runAllPossible) {
      allPossibleCombinations
    } else {
      selectedCombinations
    }
    scenarios.asJava.stream()
  }

  /**
   * Change the value to run on highlighted ones.
   **/
  def runAllPossible = true
}

/**
 * Ensure after switch from reading file with schema with which file was written to deduced schema(RFC 46)
 * different list levels can interoperate.
 **/
class TestParquetReaderCompatibility extends HoodieSparkWriterTestBase {
  /*
  * Generate schema with required nullability constraints.
  * The interesting part is that if list is the last element in the schema - different errors will be thrown.
  **/
  private def getSchemaWithParameters(listNullability: Nullability, listElementNullability: Nullability): StructType = {
    val listNullable = listNullability == Nullable
    val listElementsNullable = listElementNullability == Nullable
    val schema = StructType(Array(
      StructField("key", LongType, nullable = false),
      StructField("partition", StringType, nullable = false),
      StructField(TestParquetReaderCompatibility.listFieldName, ArrayType(LongType, listElementsNullable), listNullable),
      StructField("ts", LongType, nullable = false)
    ))
    schema
  }
  private def defaultPartition = "p1"

  private def generateRowsWithParameters(listNullability: Nullability, listElementNullability: Nullability, combineValue: Long = 1L, dummyCount: Int = 10): Map[Long, Row] = {
    val listNullable = listNullability == Nullable
    val listElementsNullable = listElementNullability == Nullable
    val res = mutable.Map[Long, Row]()
    var key = 1L
    for (_ <- 1 to dummyCount) {
      res += key -> Row(key, defaultPartition, Seq(100L), combineValue)
      key += 1
    }
    res += key -> Row(key, defaultPartition, Seq(1L, 2L), combineValue)
    key += 1
    if (listNullable) {
      res += key -> Row(key, defaultPartition, null, combineValue)
      key += 1
    }
    if (listElementsNullable) {
      res += key -> Row(key, defaultPartition, Seq(1L, null), combineValue)
      key += 1
    }
    res.toMap
  }

  private def createSparkSessionWithListLevel(listType: SparkSetting): SparkSession = {
    val conf = new SparkConf()
    listType.overrideConf(conf)
    val spark = SparkSession.builder()
      .config(HoodieClientTestUtils.getSparkConfForTest("hoodie_test"))
      .config(conf)
      .getOrCreate()
    spark
  }

  /**
   * Test interoperability of different parquet list types and their nullability.
   **/
  @ParameterizedTest
  @MethodSource(Array("testSource"))
  def testAvroListUpdate(input: TestScenario): Unit = {
    spark.stop()
    val path = tempBasePath + "_avro_list_update"
    val options = Map(
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "key",
      HoodieTableConfig.ORDERING_FIELDS.key -> "ts",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieWriteConfig.TBL_NAME.key -> hoodieFooTableName,
      "path" -> path
    )
    val initialLevel = input.initialLevel
    val listNullability = input.listNullability
    val targetLevel = input.targetLevel
    val itemsNullability = input.itemsNullability
    val structType = getSchemaWithParameters(listNullability, itemsNullability)
    val initialRecords = generateRowsWithParameters(listNullability, itemsNullability)

    val firstWriteSession = createSparkSessionWithListLevel(initialLevel)
    try {
      HoodieSparkSqlWriter.write(
        firstWriteSession.sqlContext,
        SaveMode.Overwrite,
        options,
        firstWriteSession.createDataFrame(firstWriteSession.sparkContext.parallelize(initialRecords.values.toSeq), structType)
      )

      val firstWriteLevels = getListLevelsFromPath(firstWriteSession, path)
      assert(firstWriteLevels.size == 1, s"Expected only one level, got $firstWriteLevels")
      assert(firstWriteLevels.head == initialLevel.value, s"Expected level $initialLevel, got $firstWriteLevels")
    } finally {
      firstWriteSession.close()
    }

    val updateRecords = generateRowsWithParameters(listNullability, itemsNullability, 2L, 1)
    val secondWriteSession = createSparkSessionWithListLevel(targetLevel)
    var expectedRecordsWithSchema: Seq[Row] = Seq()
    try {
      HoodieSparkSqlWriter.write(
        secondWriteSession.sqlContext,
        SaveMode.Append,
        options,
        secondWriteSession.createDataFrame(secondWriteSession.sparkContext.parallelize(updateRecords.values.toSeq), structType)
      )
      val secondWriteLevels = getListLevelsFromPath(secondWriteSession, path)
      assert(secondWriteLevels.size == 1, s"Expected only one level, got $secondWriteLevels")
      assert(secondWriteLevels.head == targetLevel.value, s"Expected level $targetLevel, got $secondWriteLevels")

      val expectedRecords = (initialRecords ++ updateRecords).values.toSeq
      expectedRecordsWithSchema = dropMetaFields(
        secondWriteSession.createDataFrame(secondWriteSession.sparkContext.parallelize(expectedRecords), structType)
      ).collect().toSeq
    } finally {
      secondWriteSession.close()
    }

    val readSessionWithInitLevel = createSparkSessionWithListLevel(initialLevel)
    try {
      compareResults(expectedRecordsWithSchema, readSessionWithInitLevel, path)
    } finally {
      readSessionWithInitLevel.close()
    }

    val readSessionWithTargetLevel = createSparkSessionWithListLevel(targetLevel)
    try {
      compareResults(expectedRecordsWithSchema, readSessionWithTargetLevel, path)
    } finally {
      readSessionWithTargetLevel.close()
    }

    initSparkContext()
  }

  private def compareResults(expectedRecords: Seq[Row], sparkSession: SparkSession, path: String): Unit = {
    implicit object RowOrdering extends Ordering[Row] {
      def compare(a: Row, b: Row): Int = {
        val firstId = a.getLong(a.fieldIndex("key"))
        val secondId = b.getLong(b.fieldIndex("key"))
        firstId.compareTo(secondId)
      }
    }
    val expectedSorted = expectedRecords.sorted
    val readRecords = dropMetaFields(sparkSession.read.format("hudi").load(path)).collect().toSeq.sorted
    assert(readRecords.length == expectedSorted.length, s"Expected ${expectedSorted.length} records, got ${readRecords.length}")
    val recordsEqual = readRecords == expectedSorted
    val explanationStr = if (!recordsEqual) {
      readRecords.zipWithIndex.map {
        case (row, index) => {
          val expectedRow = expectedSorted(index)
          if (row != expectedRow) {
            s"Difference: Expected $expectedRow, got $row"
          } else {
            s"Equals: expected $expectedRow, got $row"
          }
        }
      }.mkString("\n")
    } else {
      ""
    }
    assert(recordsEqual, explanationStr)
  }

  private def getListLevelsFromPath(spark: SparkSession, path: String): Set[String] = {
    val engineContext = new HoodieSparkEngineContext(spark.sparkContext, spark.sqlContext)
    val metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build()
    val baseTableMetadata = new HoodieBackedTableMetadata(
      engineContext, HoodieTestUtils.getDefaultStorage, metadataConfig, s"$path", false)
    val fileStatuses = baseTableMetadata.getAllFilesInPartitions(Collections.singletonList(s"$path/$defaultPartition"))

    fileStatuses.asScala.flatMap(_._2.asScala).map(_.getPath).map(path => getListType(spark.sparkContext.hadoopConfiguration, path)).toSet
  }

  private def getListType(hadoopConf: Configuration, path: StoragePath): String = {
    val reader = HoodieIOFactory.getIOFactory(HoodieStorageUtils.getStorage(path, HadoopFSUtils.getStorageConf(hadoopConf))).getReaderFactory(HoodieRecordType.AVRO).getFileReader(DEFAULT_HUDI_CONFIG_FOR_READER, path)
    //TODO boundary to revisit in later pr to use HoodieSchema directly
    val schema = ParquetTableSchemaResolver.convertAvroSchemaToParquet(reader.getSchema.toAvroSchema, hadoopConf)

    val list = schema.getFields.asScala.find(_.getName == TestParquetReaderCompatibility.listFieldName).get
    val groupType = list.asGroupType()
    val originalType = groupType.getOriginalType
    val isThreeLevel = originalType == OriginalType.LIST && !(groupType.getType(0).getName == "array")

    if (isThreeLevel) {
      ThreeLevel.value
    } else {
      TwoLevel.value
    }
  }

}
