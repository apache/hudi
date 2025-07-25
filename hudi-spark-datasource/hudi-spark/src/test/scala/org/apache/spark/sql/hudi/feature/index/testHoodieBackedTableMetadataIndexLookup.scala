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

package org.apache.spark.sql.hudi.feature.index

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.data.HoodieListData
import org.apache.hudi.common.engine.EngineType
import org.apache.hudi.common.model.HoodieRecordLocation
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.util.HoodieDataUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.data.HoodieJavaRDD
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, MetadataPartitionType}

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.util.Utils

import java.io.File

import scala.collection.JavaConverters._

/**
 * Base class for testing HoodieBackedTableMetadata index lookup functionality.
 * Provides shared setup and common test utilities.
 */
abstract class HoodieBackedTableMetadataIndexLookupTestBase extends HoodieSparkSqlTestBase {

  // Shared test data for all tests
  protected var tableName: String = _
  protected var basePath: String = _
  protected var metaClient: HoodieTableMetaClient = _
  protected var hoodieBackedTableMetadata: HoodieBackedTableMetadata = _
  protected var testData: Seq[Seq[Any]] = _
  protected var tmpDir: File = _
  private val createTableStatementProvider = () =>
    s"""
       |create table if not exists $tableName (
       |  id string,
       |  name string,
       |  price double,
       |  ts long
       |) using hudi
       | options (
       |  primaryKey ='id',
       |  type = 'cow',
       |  preCombineField = 'ts',
       |  hoodie.metadata.enable = 'true',
       |  hoodie.metadata.record.index.enable = 'true',
       |  hoodie.metadata.index.column.stats.enable = 'true',
       |  hoodie.metadata.index.secondary.enable = 'true',
       |  hoodie.metadata.record.index.min.filegroup.count = '${getNumFileIndexGroup}',
       |  hoodie.metadata.record.index.max.filegroup.count = '${getNumFileIndexGroup}',
       |  hoodie.write.table.version = '${getTableVersion}',
       |  hoodie.datasource.write.payload.class = 'org.apache.hudi.common.model.OverwriteWithLatestAvroPayload'
       | )
       | location '$basePath'
       """.stripMargin
  /**
   * Get the table version for this test implementation
   */
  protected def getTableVersion: String

  protected def getNumFileIndexGroup: String = {
    "1"
  }

  /**
   * Get the expected index version for this test implementation
   */
  protected def getExpectedIndexVersion: String

  /**
   * Override test method to ensure table exists before each test runs
   * This compensates for the parent class dropping tables after each test
   */
  override protected def test(testName: String, testTags: org.scalatest.Tag*)(testFun: => Any)(implicit pos: org.scalactic.source.Position): Unit = {
    super.test(testName, testTags: _*) {
      // Ensure table exists before running the test
      ensureTableExists()
      // Run the actual test
      testFun
    }
  }

  /**
   * Ensure table exists - compensates for parent class cleanup
   */
  private def ensureTableExists(): Unit = {
    spark.sql(createTableStatementProvider.apply())
  }

  /**
   * Setup method that runs once before all tests
   */
  override protected def beforeAll(): Unit = {
    super.beforeAll()

    // Create shared temporary directory
    tmpDir = Utils.createTempDir()

    // Setup shared test data
    setupSharedTestData()
  }

  /**
   * Teardown method that runs once after all tests
   */
  override protected def afterAll(): Unit = {
    // Cleanup shared resources
    cleanupSharedResources()

    // Cleanup temporary directory
    if (tmpDir != null) {
      Utils.deleteRecursively(tmpDir)
    }

    super.afterAll()
  }

  /**
   * Setup shared test data that will be used across all tests
   */
  private def setupSharedTestData(): Unit = {
    tableName = generateTableName
    basePath = s"${tmpDir.getCanonicalPath}/$tableName"

    spark.sql("set hoodie.embed.timeline.server=false")

    // Create table with specified version
    spark.sql(createTableStatementProvider.apply())

    // Insert initial test data
    spark.sql(s"insert into $tableName values('1', 'b1', 10, 1000)")
    spark.sql(s"insert into $tableName values('2', 'b2', 20, 1000)")
    spark.sql(s"insert into $tableName" + " values('$', '$', 30, 1000)")

    val props = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
      DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "ts",
      HoodieWriteConfig.TBL_NAME.key -> tableName,
      DataSourceWriteOptions.OPERATION.key -> DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL
    )

    val writeConfig = HoodieWriteConfig.newBuilder()
      .withEngineType(EngineType.JAVA)
      .withPath(basePath)
      .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
      .withProps(props.asJava)
      .build()

    metaClient = HoodieTableMetaClient.builder().setBasePath(basePath)
      .setConf(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf())).build()

    // Secondary index is created by default for non record key column when index type is not specified
    testData = Seq(
      Seq("b1", "b1", 10, 1000),
      Seq("b2", "b2", 20, 1000),
      Seq("$", "$", 30, 1000)
    )

    // Create secondary indexes on name and price columns
    spark.sql(s"set hoodie.write.table.version = ${getTableVersion}")
    spark.sql(s"set hoodie.metadata.record.index.min.filegroup.count = ${getNumFileIndexGroup}")
    spark.sql(s"set hoodie.metadata.record.index.max.filegroup.count = ${getNumFileIndexGroup}")
    spark.sql(s"create index idx_name on $tableName (name)")
    spark.sql(s"create index idx_price on $tableName (price)")

    checkAnswer(s"show indexes from $tableName")(
      Seq("column_stats", "column_stats", ""),
      Seq("secondary_index_idx_name", "secondary_index", "name"),
      Seq("secondary_index_idx_price", "secondary_index", "price"),
      Seq("record_index", "record_index", "")
    )

    // Verify the data in the table matches expected data
    //    checkAnswer(s"select id, name, price, ts from $tableName")(testData: _*)

    // Verify the table version
    metaClient.reload()
    val jsc = new JavaSparkContext(spark.sparkContext)
    val sqlContext = new SQLContext(spark)
    val context = new HoodieSparkEngineContext(jsc, sqlContext)
    hoodieBackedTableMetadata = new HoodieBackedTableMetadata(
      context, metaClient.getStorage, writeConfig.getMetadataConfig, basePath, true)

  }

  /**
   * Cleanup shared resources
   */
  private def cleanupSharedResources(): Unit = {
    if (hoodieBackedTableMetadata != null) {
      hoodieBackedTableMetadata.close()
      hoodieBackedTableMetadata = null
    }

    // Reset other data members
    tableName = null
    basePath = null
    metaClient = null
    testData = null
  }

  /**
   * Test record index with mapping functionality
   */
  protected def testReadRecordIndex(): Unit = {
    // Case 1: Empty input
    val emptyResult = hoodieBackedTableMetadata.readRecordIndex(HoodieListData.eager(List.empty[String].asJava))
    assert(emptyResult.collectAsList().isEmpty, "Empty input should return empty result")

    // Case 2: All existing keys
    val allKeys = HoodieListData.eager(List("1", "2", "$").asJava)
    val allResult = hoodieBackedTableMetadata.readRecordIndex(allKeys).collectAsList().asScala
    assert(allResult.size == 3, "Should return 3 results for 3 existing keys")

    // Validate keys
    val resultKeys = allResult.map(_.getKey()).toSet
    assert(resultKeys == Set("1", "2", "$"), "Keys should match input keys")

    // Validate HoodieRecordGlobalLocation structure
    allResult.foreach { pair =>
      val key = pair.getKey
      val location = pair.getValue

      // Validate location is not null
      assert(location != null, s"Location for key $key should not be null")

      // Validate location fields
      assert(location.getPartitionPath != null, s"Partition path for key $key should not be null")
      assert(location.getInstantTime != null, s"Instant time for key $key should not be null")
      assert(location.getFileId != null, s"File ID for key $key should not be null")

      // Validate position (should be valid or INVALID_POSITION)
      assert(location.getPosition >= HoodieRecordLocation.INVALID_POSITION,
        s"Position for key $key should be >= INVALID_POSITION")
    }

    // Case 3: Non-existing keys
    val nonExistKeys = HoodieListData.eager(List("100", "200").asJava)
    val nonExistResult = hoodieBackedTableMetadata.readRecordIndex(nonExistKeys).collectAsList().asScala
    assert(nonExistResult.isEmpty, "Non-existing keys should return empty result")

    // Case 4: Mix of existing and non-existing keys
    val mixedKeys = HoodieListData.eager(List("1", "100", "2", "200").asJava)
    val mixedResult = hoodieBackedTableMetadata.readRecordIndex(mixedKeys).collectAsList().asScala
    assert(mixedResult.size == 2, "Should return 2 results for 2 existing keys")
    val mixedResultKeys = mixedResult.map(_.getKey()).toSet
    assert(mixedResultKeys == Set("1", "2"), "Should only return existing keys")

    // Case 5: Duplicate keys
    val dupKeys = HoodieListData.eager(List("1", "1", "2", "2", "$", "$").asJava)
    val dupResult = hoodieBackedTableMetadata.readRecordIndex(dupKeys).collectAsList().asScala
    assert(dupResult.size == 3, "Should return 3 unique results for duplicate keys")
    val dupResultKeys = dupResult.map(_.getKey()).toSet
    assert(dupResultKeys == Set("1", "2", "$"), "Should deduplicate keys")

    // Case 6: Use parallelized RDD
    val jsc = new JavaSparkContext(spark.sparkContext)
    val context = new HoodieSparkEngineContext(jsc, new SQLContext(spark))
    val rddKeys = HoodieJavaRDD.of(List("1", "2", "$").asJava, context, 2)
    val rddResult = hoodieBackedTableMetadata.readRecordIndex(rddKeys)
    assert(rddResult.collectAsList().asScala.size == 3, "RDD input should return 3 results")
  }

  /**
   * Test secondary index result functionality
   */
  protected def testReadSecondaryIndexLocations(): Unit = {
    // Get the secondary index partition name
    val secondaryIndexName = "secondary_index_idx_name"

    // Case 1: Empty input
    val emptyResult = hoodieBackedTableMetadata.readSecondaryIndexLocations(
      HoodieListData.eager(List.empty[String].asJava), secondaryIndexName)
    assert(emptyResult.collectAsList().isEmpty, s"Empty input should return empty result for table version ${getTableVersion}")

    // Case 2: All existing secondary keys
    val allSecondaryKeys = HoodieListData.eager(List("b1", "b2", "$").asJava)
    val allResult = hoodieBackedTableMetadata.readSecondaryIndexLocations(allSecondaryKeys, secondaryIndexName).collectAsList().asScala
    assert(allResult.size == 3, s"Should return 3 results for 3 existing secondary keys in table version ${getTableVersion}")

    // Validate HoodieRecordGlobalLocation structure
    allResult.foreach { location =>
      // Validate location is not null
      assert(location != null, s"Location should not be null for table version ${getTableVersion}")

      // Validate location fields
      assert(location.getPartitionPath != null, s"Partition path should not be null for table version ${getTableVersion}")
      assert(location.getInstantTime != null, s"Instant time should not be null for table version ${getTableVersion}")
      assert(location.getFileId != null, s"File ID should not be null for table version ${getTableVersion}")

      // Validate position (should be valid or INVALID_POSITION)
      assert(location.getPosition >= HoodieRecordLocation.INVALID_POSITION,
        s"Position should be >= INVALID_POSITION for table version ${getTableVersion}")
    }

    // Case 3: Non-existing secondary keys
    val nonExistKeys = HoodieListData.eager(List("non_exist_1", "non_exist_2").asJava)
    val nonExistResult = hoodieBackedTableMetadata.readSecondaryIndexLocations(nonExistKeys, secondaryIndexName).collectAsList().asScala
    assert(nonExistResult.isEmpty, s"Non-existing secondary keys should return empty result for table version ${getTableVersion}")

    // Case 4: Mix of existing and non-existing secondary keys
    val mixedKeys = HoodieListData.eager(List("b1", "non_exist_1", "b2", "non_exist_2").asJava)
    val mixedResult = hoodieBackedTableMetadata.readSecondaryIndexLocations(mixedKeys, secondaryIndexName).collectAsList().asScala
    assert(mixedResult.size == 2, s"Should return 2 results for 2 existing secondary keys in table version ${getTableVersion}")

    // Case 5: Duplicate secondary keys
    val dupKeys = HoodieListData.eager(List("b1", "b1", "b2", "b2", "$", "$").asJava)
    val dupResult = hoodieBackedTableMetadata.readSecondaryIndexLocations(dupKeys, secondaryIndexName).collectAsList().asScala
    assert(dupResult.size == 3, s"Should return 3 unique results for duplicate secondary keys in table version ${getTableVersion}")

    // Case 6: Test with different secondary index (price column)
    val priceIndexName = "secondary_index_idx_price"
    val priceKeys = HoodieListData.eager(List("10.0", "20.0", "30.0").asJava)
    val priceResult = hoodieBackedTableMetadata.readSecondaryIndexLocations(priceKeys, priceIndexName).collectAsList().asScala
    assert(priceResult.size == 3, s"Should return 3 results for price secondary keys in table version ${getTableVersion}")

    // Case 7: Test invalid secondary index partition name
    val invalidIndexName = "non_existent_index"
    checkExceptionContain(() => {
      hoodieBackedTableMetadata.readSecondaryIndexLocations(
        HoodieListData.eager(List("b1").asJava), invalidIndexName)
    })("No MetadataPartitionType for partition path: non_existent_index")

    // Case 8: Test version-specific behavior differences
    testVersionSpecificBehavior()

    // Case 9: Test large number of keys to exercise multiple file slices path
    val largeKeyList = (1 to 100).map(i => s"large_key_$i").asJava
    val largeKeys = HoodieListData.eager(largeKeyList)
    val largeResult = hoodieBackedTableMetadata.readSecondaryIndexLocations(largeKeys, secondaryIndexName)
    // Should not throw exception, even if no results found
    assert(largeResult.collectAsList().isEmpty, "Large key list should return empty result for non-existing keys")
  }

  /**
   * Test version-specific behavior differences
   * Override in subclasses to implement version-specific tests
   */
  protected def testVersionSpecificBehavior(): Unit = {
    // Default implementation - can be overridden by subclasses
  }

  /**
   * Test secondary index records functionality
   */
  protected def testGetSecondaryIndexRecords(): Unit = {
    val secondaryIndexName = "secondary_index_idx_name"

    // Test with existing secondary keys
    val existingKeys = HoodieListData.eager(List("b1", "b2", "$").asJava)
    val result = hoodieBackedTableMetadata.getSecondaryIndexRecords(existingKeys, secondaryIndexName)
    val resultMap = HoodieDataUtils.dedupeAndCollectAsMap(result)

    assert(resultMap.size == 3, s"Should return 3 results for existing secondary keys in table version ${getTableVersion}")

    // Validate that each secondary key maps to a set of record keys
    resultMap.asScala.foreach { case (secondaryKey, recordKeys) =>
      assert(recordKeys.asScala.nonEmpty, s"Secondary key $secondaryKey should map to at least one record key")
      assert(recordKeys.size == 1, s"Secondary key $secondaryKey should map to exactly one record key in this test")
    }

    // Test with non-existing secondary keys
    val nonExistingKeys = HoodieListData.eager(List("non_exist_1", "non_exist_2").asJava)
    val nonExistingResult = hoodieBackedTableMetadata.getSecondaryIndexRecords(nonExistingKeys, secondaryIndexName)
    val nonExistingMap = HoodieDataUtils.dedupeAndCollectAsMap(nonExistingResult)
    assert(nonExistingMap.isEmpty, s"Should return empty result for non-existing secondary keys in table version ${getTableVersion}")
  }
}

/**
 * Test class for table version 8 (V1)
 */
class HoodieBackedTableMetadataIndexLookupV8TestBase extends HoodieBackedTableMetadataIndexLookupTestBase {

  override protected def getTableVersion: String = "8"

  override protected def getExpectedIndexVersion: String = "V1"

  override protected def getNumFileIndexGroup: String = {
    "10"
  }

  override protected def testVersionSpecificBehavior(): Unit = {
    // For version 1, test that it only supports HoodieListData
    val secondaryIndexName = "secondary_index_idx_name"
    val jsc = new JavaSparkContext(spark.sparkContext)
    val context = new HoodieSparkEngineContext(jsc, new SQLContext(spark))
    val rddKeys = HoodieJavaRDD.of(List("b1").asJava, context, 1)
    checkExceptionContain(() => {
      hoodieBackedTableMetadata.readSecondaryIndexLocations(rddKeys, secondaryIndexName)
    })("only support HoodieListData")
  }
}

class HoodieBackedTableMetadataIndexLookupV8Test1Fg extends HoodieBackedTableMetadataIndexLookupV8TestBase {
  override protected def getNumFileIndexGroup: String = {
    "1"
  }

  test("Unit test Index join API - Version 8") {
    testGetSecondaryIndexRecords()
  }

  test("Exhaustive test for readRecordIndex - Version 8") {
    testReadRecordIndex()
  }

  test("Exhaustive test for readSecondaryIndexResult - Version 8") {
    testReadSecondaryIndexLocations()
  }
}

class HoodieBackedTableMetadataIndexLookupV8Test10Fg extends HoodieBackedTableMetadataIndexLookupV8TestBase {
  override protected def getNumFileIndexGroup: String = {
    "10"
  }

  test("Unit test Index join API - Version 8") {
    testGetSecondaryIndexRecords()
  }

  test("Exhaustive test for readRecordIndex - Version 8") {
    testReadRecordIndex()
  }

  test("Exhaustive test for readSecondaryIndexResult - Version 8") {
    testReadSecondaryIndexLocations()
  }
}

/**
 * Test class for table version 9 (V2)
 */
class HoodieBackedTableMetadataIndexLookupV9TestBase extends HoodieBackedTableMetadataIndexLookupTestBase {

  override protected def getTableVersion: String = "9"

  override protected def getExpectedIndexVersion: String = "V2"

  override protected def testVersionSpecificBehavior(): Unit = {
    // For version 2, test that it supports both HoodieListData and RDD
    val secondaryIndexName = "secondary_index_idx_name"
    val jsc = new JavaSparkContext(spark.sparkContext)
    val context = new HoodieSparkEngineContext(jsc, new SQLContext(spark))
    val rddKeys = HoodieJavaRDD.of(List("b1", "b2", "$").asJava, context, 2)
    val rddResult = hoodieBackedTableMetadata.readSecondaryIndexLocations(rddKeys, secondaryIndexName)
    val locations = rddResult.collectAsList()
    assert(locations.asScala.size == 3, "Version 2 should support RDD input")

    // Test case for null values in secondary index
    testNullValueInSecondaryIndex()
  }

  /**
   * Test case for handling null values in secondary index
   */
  def testNullValueInSecondaryIndex(): Unit = {
    val secondaryIndexName = "secondary_index_idx_name"

    // Insert record with null value for the indexed column
    spark.sql(s"insert into $tableName values('null_record', null, null, 1002)")
    // Everytime we insert need to manually reset the MDT object.
    hoodieBackedTableMetadata.reset()
    // Read MDT SI records using hudi_metadata() to verify records are successfully written
    val mdtResult = spark.sql(s"select key from hudi_metadata('$tableName') where type=${MetadataPartitionType.SECONDARY_INDEX.getRecordType} order by key")
    val mdtRows = mdtResult.collect()

    val expectedRows = Seq(
      "\u0000$null_record",
      "\u0000$null_record",
      "10.0$1",
      "20.0$2",
      "30.0$\\$",
      "\\$$\\$",
      "b1$1",
      "b2$2"
    )

    assert(mdtRows.length == expectedRows.length, s"Expected ${expectedRows} rows but got ${mdtRows}")
    mdtRows.map(_.getString(0)).zip(expectedRows).foreach { case (actual, expected) =>
      assert(actual == expected, s"Row mismatch: expected $expected but got $actual")
    }

    // Test SI index lookup API searching for null value
    val nullKeys = HoodieListData.eager(List(null.asInstanceOf[String]).asJava)
    val nullResult = hoodieBackedTableMetadata.readSecondaryIndexLocations(nullKeys, secondaryIndexName)
    val nullLocations = nullResult.collectAsList().asScala

    // Verify that null lookup returns exactly 1 result (for the null_record we inserted)
    assert(nullLocations.size == 1, s"Secondary index lookup should return exactly 1 result for null value, but found ${nullLocations.size}")

    // Verify that the returned locations are valid
    nullLocations.foreach { location =>
      assert(location != null, "Location for null key should not be null")
      assert(location.getPartitionPath != null, "Partition path for null key should not be null")
      assert(location.getInstantTime != null, "Instant time for null key should not be null")
      assert(location.getFileId != null, "File ID for null key should not be null")
      assert(location.getPosition >= HoodieRecordLocation.INVALID_POSITION,
        "Position for null key should be >= INVALID_POSITION")
    }

    // Test getSecondaryIndexRecords API with null value
    val nullRecordsResult = hoodieBackedTableMetadata.getSecondaryIndexRecords(nullKeys, secondaryIndexName)
    val nullRecordsMap = HoodieDataUtils.dedupeAndCollectAsMap(nullRecordsResult)

    // Verify that null key maps to record keys.
    // Assert it is map with key as "null_record" -> value as set of "null"
    assert(nullRecordsMap.keySet().asScala.toList.equals(Seq("null_record")))
    assert(nullRecordsMap.get("null_record").asScala.toList.equals(Seq(null)))
    // Clean up the test data
    spark.sql(s"delete from $tableName where id = 'null_record'")
    hoodieBackedTableMetadata.reset()
  }
}

class HoodieBackedTableMetadataIndexLookupV9Test1Fg extends HoodieBackedTableMetadataIndexLookupV9TestBase {
  override protected def getNumFileIndexGroup: String = {
    "1"
  }

  test("Unit test Index join API - Version 9") {
    testGetSecondaryIndexRecords()
  }

  test("Exhaustive test for readRecordIndex - Version 9") {
    testReadRecordIndex()
  }

  test("Exhaustive test for readSecondaryIndexResult - Version 9") {
    testReadSecondaryIndexLocations()
  }
}

class HoodieBackedTableMetadataIndexLookupV9Test10Fg extends HoodieBackedTableMetadataIndexLookupV9TestBase {
  override protected def getNumFileIndexGroup: String = {
    "10"
  }

  test("Unit test Index join API - Version 9") {
    testGetSecondaryIndexRecords()
  }

  test("Exhaustive test for readRecordIndex - Version 9") {
    testReadRecordIndex()
  }

  test("Exhaustive test for readSecondaryIndexResult - Version 9") {
    testReadSecondaryIndexLocations()
  }
}
