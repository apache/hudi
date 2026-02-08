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

import org.apache.hudi.DataSourceReadOptions.{FILE_INDEX_PARTITION_LISTING_VIA_CATALOG, QUERY_TYPE, QUERY_TYPE_SNAPSHOT_OPT_VAL}
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.datasources.NoopCache
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertNull, assertTrue}

import java.util.Collections

import scala.collection.JavaConverters._

/**
 * Tests for SparkHoodieTableFileIndex HMS partition listing feature.
 *
 * This test verifies that:
 * 1. When FILE_INDEX_PARTITION_LISTING_VIA_CATALOG is true and MDT is not available,
 *    partition listing comes from the HMS (external catalog)
 * 2. When FILE_INDEX_PARTITION_LISTING_VIA_CATALOG is false, HMS listing is not used
 * 3. When MDT is available, HMS listing is not used (even if the config is enabled)
 */
class TestSparkHoodieTableFileIndexHmsListing extends HoodieSparkSqlTestBase {

  test("Test HMS partition listing config is properly wired") {
    val tableName = generateTableName
    val databaseName = "default"

    // Create a partitioned table using SQL (which registers it with the catalog)
    spark.sql(
      s"""
         |CREATE TABLE $tableName (
         |  id INT,
         |  name STRING,
         |  price DOUBLE,
         |  ts LONG,
         |  dt STRING
         |) USING hudi
         |PARTITIONED BY (dt)
         |TBLPROPERTIES (
         |  primaryKey = 'id',
         |  orderingFields = 'ts',
         |  hoodie.metadata.enable = 'false'
         |)
       """.stripMargin)

    // Insert data into multiple partitions
    spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 10.0, 1000, '2023-01-01')")
    spark.sql(s"INSERT INTO $tableName VALUES (2, 'a2', 20.0, 2000, '2023-01-02')")
    spark.sql(s"INSERT INTO $tableName VALUES (3, 'a3', 30.0, 3000, '2023-01-03')")

    // Get the table path from the catalog
    val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
    val tablePath = table.storage.properties("path")

    // Verify partitions are registered in the catalog
    val catalogPartitions = spark.sessionState.catalog.externalCatalog
      .listPartitions(databaseName, tableName)
    assertEquals(3, catalogPartitions.size, "Should have 3 partitions in the catalog")

    // Create metaClient
    val metaClient = createMetaClient(spark, tablePath)

    // Test Case 1: HMS listing disabled (default) - getMatchingPartitionPathsFromCatalog should return null
    {
      val configProperties = new TypedProperties()
      configProperties.setProperty("path", tablePath)
      configProperties.setProperty(QUERY_TYPE.key, QUERY_TYPE_SNAPSHOT_OPT_VAL)
      configProperties.setProperty(FILE_INDEX_PARTITION_LISTING_VIA_CATALOG.key, "false")
      configProperties.setProperty(HoodieMetadataConfig.ENABLE.key, "false")

      val fileIndex = new SparkHoodieTableFileIndex(
        spark,
        metaClient,
        None,
        configProperties,
        Seq(new StoragePath(tablePath)),
        None,
        NoopCache
      )

      // Call the protected method via reflection to test it directly
      val method = classOf[SparkHoodieTableFileIndex].getDeclaredMethod(
        "getMatchingPartitionPathsFromCatalog", classOf[java.util.List[String]])
      method.setAccessible(true)
      val result = method.invoke(fileIndex, Collections.singletonList(""))

      assertNull(result, "Should return null when HMS listing is disabled")
    }

    // Test Case 2: HMS listing enabled and MDT disabled - should return partitions from catalog
    {
      val configProperties = new TypedProperties()
      configProperties.setProperty("path", tablePath)
      configProperties.setProperty(QUERY_TYPE.key, QUERY_TYPE_SNAPSHOT_OPT_VAL)
      configProperties.setProperty(FILE_INDEX_PARTITION_LISTING_VIA_CATALOG.key, "true")
      configProperties.setProperty(HoodieMetadataConfig.ENABLE.key, "false")

      val fileIndex = new SparkHoodieTableFileIndex(
        spark,
        metaClient,
        None,
        configProperties,
        Seq(new StoragePath(tablePath)),
        None,
        NoopCache
      )

      // Call the protected method via reflection
      val method = classOf[SparkHoodieTableFileIndex].getDeclaredMethod(
        "getMatchingPartitionPathsFromCatalog", classOf[java.util.List[String]])
      method.setAccessible(true)
      val result = method.invoke(fileIndex, Collections.singletonList("")).asInstanceOf[java.util.List[String]]

      assertNotNull(result, "Should return partition list when HMS listing is enabled and MDT is disabled")
      assertEquals(3, result.size(), "Should return 3 partitions from the catalog")

      // Verify the partition paths are correct
      val partitionPaths = result.asScala.toSet
      assertTrue(partitionPaths.contains("dt=2023-01-01"), "Should contain partition dt=2023-01-01")
      assertTrue(partitionPaths.contains("dt=2023-01-02"), "Should contain partition dt=2023-01-02")
      assertTrue(partitionPaths.contains("dt=2023-01-03"), "Should contain partition dt=2023-01-03")
    }

    // Test Case 3: HMS listing with prefix filtering
    {
      val configProperties = new TypedProperties()
      configProperties.setProperty("path", tablePath)
      configProperties.setProperty(QUERY_TYPE.key, QUERY_TYPE_SNAPSHOT_OPT_VAL)
      configProperties.setProperty(FILE_INDEX_PARTITION_LISTING_VIA_CATALOG.key, "true")
      configProperties.setProperty(HoodieMetadataConfig.ENABLE.key, "false")

      val fileIndex = new SparkHoodieTableFileIndex(
        spark,
        metaClient,
        None,
        configProperties,
        Seq(new StoragePath(tablePath)),
        None,
        NoopCache
      )

      // Call with a specific prefix that should match only one partition
      val method = classOf[SparkHoodieTableFileIndex].getDeclaredMethod(
        "getMatchingPartitionPathsFromCatalog", classOf[java.util.List[String]])
      method.setAccessible(true)
      val result = method.invoke(fileIndex, Collections.singletonList("dt=2023-01-01")).asInstanceOf[java.util.List[String]]

      assertNotNull(result, "Should return partition list")
      assertEquals(1, result.size(), "Should return only 1 partition matching the prefix")
      assertEquals("dt=2023-01-01", result.get(0), "Should return the matching partition")
    }
  }

  test("Test HMS partition listing is skipped when MDT is available") {
    val tableName = generateTableName

    // Create a partitioned table with MDT enabled
    spark.sql(
      s"""
         |CREATE TABLE $tableName (
         |  id INT,
         |  name STRING,
         |  price DOUBLE,
         |  ts LONG,
         |  dt STRING
         |) USING hudi
         |PARTITIONED BY (dt)
         |TBLPROPERTIES (
         |  primaryKey = 'id',
         |  orderingFields = 'ts',
         |  hoodie.metadata.enable = 'true'
         |)
       """.stripMargin)

    // Insert data to create MDT
    spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 10.0, 1000, '2023-01-01')")
    spark.sql(s"INSERT INTO $tableName VALUES (2, 'a2', 20.0, 2000, '2023-01-02')")

    // Get the table path
    val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
    val tablePath = table.storage.properties("path")

    // Create metaClient and reload to pick up MDT
    val metaClient = createMetaClient(spark, tablePath)

    // Verify MDT is available
    assertTrue(metaClient.getTableConfig.isMetadataTableAvailable,
      "MDT should be available after insert")

    // Test: Even with HMS listing enabled, should return null when MDT is available
    val configProperties = new TypedProperties()
    configProperties.setProperty("path", tablePath)
    configProperties.setProperty(QUERY_TYPE.key, QUERY_TYPE_SNAPSHOT_OPT_VAL)
    configProperties.setProperty(FILE_INDEX_PARTITION_LISTING_VIA_CATALOG.key, "true")
    configProperties.setProperty(HoodieMetadataConfig.ENABLE.key, "true")

    val fileIndex = new SparkHoodieTableFileIndex(
      spark,
      metaClient,
      None,
      configProperties,
      Seq(new StoragePath(tablePath)),
      None,
      NoopCache
    )

    // Call the protected method via reflection
    val method = classOf[SparkHoodieTableFileIndex].getDeclaredMethod(
      "getMatchingPartitionPathsFromCatalog", classOf[java.util.List[String]])
    method.setAccessible(true)
    val result = method.invoke(fileIndex, Collections.singletonList(""))

    assertNull(result, "Should return null when MDT is available (prefer MDT over HMS listing)")
  }

  test("Test end-to-end partition pruning with HMS listing") {
    val tableName = generateTableName

    // Create a partitioned table with MDT disabled
    spark.sql(
      s"""
         |CREATE TABLE $tableName (
         |  id INT,
         |  name STRING,
         |  price DOUBLE,
         |  ts LONG,
         |  dt STRING
         |) USING hudi
         |PARTITIONED BY (dt)
         |TBLPROPERTIES (
         |  primaryKey = 'id',
         |  orderingFields = 'ts',
         |  hoodie.metadata.enable = 'false'
         |)
       """.stripMargin)

    // Insert data into multiple partitions
    spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 10.0, 1000, '2023-01-01')")
    spark.sql(s"INSERT INTO $tableName VALUES (2, 'a2', 20.0, 2000, '2023-01-02')")
    spark.sql(s"INSERT INTO $tableName VALUES (3, 'a3', 30.0, 3000, '2023-01-03')")

    // Read with HMS listing enabled
    val df = spark.read.format("hudi")
      .option(FILE_INDEX_PARTITION_LISTING_VIA_CATALOG.key, "true")
      .option(HoodieMetadataConfig.ENABLE.key, "false")
      .load(spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
        .storage.properties("path"))

    // Verify we can read all data
    assertEquals(3, df.count(), "Should read all 3 records")

    // Verify partition pruning works
    assertEquals(1, df.filter("dt = '2023-01-01'").count(), "Should read 1 record from partition 2023-01-01")
    assertEquals(2, df.filter("dt >= '2023-01-02'").count(), "Should read 2 records from partitions >= 2023-01-02")
  }
}
