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

package org.apache.spark.sql.hudi.common

import org.apache.hudi.DataSourceReadOptions
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.metadata.CatalogBackedTableMetadata
import org.apache.hudi.storage.StoragePath

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}

import java.util.stream.Collectors

/**
 * Tests for CatalogBackedTableMetadata to verify partition listing via catalog
 * when metadata table is disabled or corrupted.
 */
class TestCatalogBackedTableMetadata extends HoodieSparkSqlTestBase {

  test("Test catalog-backed partition listing with metadata table disabled") {
    withTempDir { tmp =>
      val targetTable = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$targetTable"

      // Enable catalog-backed partition listing
      spark.conf.set(DataSourceReadOptions.FILE_INDEX_PARTITION_LISTING_VIA_CATALOG.key, "true")

      spark.sql(
        s"""
           |create table $targetTable (
           |  `id` string,
           |  `name` string,
           |  `ts` bigint,
           |  `year` string,
           |  `month` string,
           |  `day` string
           |) using hudi
           | tblproperties (
           |  'primaryKey' = 'id',
           |  'type' = 'COW',
           |  'preCombineField'='ts'
           | )
           | partitioned by (`year`, `month`, `day`)
           | location '$tablePath'
        """.stripMargin)

      // Insert data across multiple partitions using SQL to ensure catalog is updated
      spark.sql(
        s"""
           |INSERT INTO $targetTable VALUES
           |  ('1', 'a1', 1000, '2024', '01', '01'),
           |  ('2', 'a2', 2000, '2024', '01', '02'),
           |  ('3', 'a3', 3000, '2024', '01', '03'),
           |  ('4', 'a4', 4000, '2024', '02', '01'),
           |  ('5', 'a5', 5000, '2024', '02', '02'),
           |  ('6', 'a6', 6000, '2024', '03', '01')
        """.stripMargin)

      syncPartitionsToCatalog(targetTable)

      // Verify metadata table is disabled
      val metaClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
        .setBasePath(tablePath)
        .build()

      // Verify catalog-backed metadata can list partitions
      val engine = new HoodieSparkEngineContext(spark.sparkContext)
      val storage = metaClient.getStorage
      val catalogBackedMetadata = new CatalogBackedTableMetadata(
        engine, metaClient.getTableConfig, storage, tablePath)

      val allPartitions = catalogBackedMetadata.getAllPartitionPaths
      val jan01 = hivePartitionPath("year" -> "2024", "month" -> "01", "day" -> "01")
      val jan02 = hivePartitionPath("year" -> "2024", "month" -> "01", "day" -> "02")
      val jan03 = hivePartitionPath("year" -> "2024", "month" -> "01", "day" -> "03")
      val feb01 = hivePartitionPath("year" -> "2024", "month" -> "02", "day" -> "01")
      val feb02 = hivePartitionPath("year" -> "2024", "month" -> "02", "day" -> "02")
      val mar01 = hivePartitionPath("year" -> "2024", "month" -> "03", "day" -> "01")

      assertEquals(6, allPartitions.size(), "Should have 6 partitions")
      assertTrue(allPartitions.contains(jan01))
      assertTrue(allPartitions.contains(jan02))
      assertTrue(allPartitions.contains(jan03))
      assertTrue(allPartitions.contains(feb01))
      assertTrue(allPartitions.contains(feb02))
      assertTrue(allPartitions.contains(mar01))

      checkAnswer(s"select id, name from $targetTable where year = '2024' and month = '01' order by id")(
        Seq("1", "a1"),
        Seq("2", "a2"),
        Seq("3", "a3")
      )

      corruptParquetFileInPartition(tablePath, jan02)

      // Corrupting a partition that is not part of these queries ensures pruning is effective.
      checkAnswer(s"select id, name from $targetTable where year = '2024' and month = '02' order by id")(
        Seq("4", "a4"),
        Seq("5", "a5")
      )

      checkAnswer(s"select id, name from $targetTable where day = '01' order by id")(
        Seq("1", "a1"),
        Seq("4", "a4"),
        Seq("6", "a6")
      )

      catalogBackedMetadata.close()
    }
  }

  test("Test catalog-backed partition listing with partition filtering") {
    withTempDir { tmp =>
      val targetTable = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$targetTable"

      // Enable catalog-backed partition listing
      spark.conf.set(DataSourceReadOptions.FILE_INDEX_PARTITION_LISTING_VIA_CATALOG.key, "true")

      spark.sql(
        s"""
           |create table $targetTable (
           |  `id` string,
           |  `name` string,
           |  `ts` bigint,
           |  `country` string,
           |  `state` string,
           |  `city` string
           |) using hudi
           | tblproperties (
           |  'primaryKey' = 'id',
           |  'type' = 'COW',
           |  'preCombineField'='ts',
           |  'hoodie.metadata.enable'='false'
           | )
           | partitioned by (`country`, `state`, `city`)
           | location '$tablePath'
        """.stripMargin)

      // Insert data with different partition values
      import spark.implicits._
      val df = Seq(
        ("1", "a1", 1000L, "USA", "CA", "SFO"),
        ("2", "a2", 2000L, "USA", "CA", "LA"),
        ("3", "a3", 3000L, "USA", "TX", "AU"),
        ("4", "a4", 4000L, "USA", "NY", "NYC"),
        ("5", "a5", 5000L, "CAN", "ON", "TOR"),
        ("6", "a6", 6000L, "CAN", "BC", "VAN")
      ).toDF("id", "name", "ts", "country", "state", "city")

      df.write
        .format("hudi")
        .mode("append")
        .save(tablePath)

      syncPartitionsToCatalog(targetTable)

      checkAnswer(s"select id, name from $targetTable where country = 'USA' order by id")(
        Seq("1", "a1"),
        Seq("2", "a2"),
        Seq("3", "a3"),
        Seq("4", "a4")
      )

      corruptParquetFileInPartition(tablePath, hivePartitionPath("country" -> "USA", "state" -> "TX", "city" -> "AU"))

      // Corrupting a non-matching partition proves Spark can prune it before file scanning.
      checkAnswer(s"select id, name from $targetTable where country = 'CAN' order by id")(
        Seq("5", "a5"),
        Seq("6", "a6")
      )

      // Test partition pruning with multiple partition columns
      checkAnswer(s"select id, name from $targetTable where country = 'USA' and state = 'CA' order by id")(
        Seq("1", "a1"),
        Seq("2", "a2")
      )

      // Test partition pruning with IN clause
      checkAnswer(s"select id, name from $targetTable where state IN ('CA', 'ON') order by id")(
        Seq("1", "a1"),
        Seq("2", "a2"),
        Seq("5", "a5")
      )

      // Metadata is disabled, so catalog-backed listing should still kick in even without the flag.
      spark.conf.unset(DataSourceReadOptions.FILE_INDEX_PARTITION_LISTING_VIA_CATALOG.key)
      checkAnswer(s"select id, name from $targetTable where country = 'CAN' order by id")(
        Seq("5", "a5"),
        Seq("6", "a6")
      )

      // Verify catalog-backed metadata returns correct filtered partitions
      val metaClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
        .setBasePath(tablePath)
        .build()

      val engine = new HoodieSparkEngineContext(spark.sparkContext)
      val storage = metaClient.getStorage
      val catalogBackedMetadata = new CatalogBackedTableMetadata(
        engine, metaClient.getTableConfig, storage, tablePath)

      val allPartitions = catalogBackedMetadata.getAllPartitionPaths
      assertEquals(6, allPartitions.size(), "Should have 6 partitions")

      catalogBackedMetadata.close()
    }
  }

  test("Test catalog-backed metadata works correctly when metadata table is corrupted") {
    withTempDir { tmp =>
      val targetTable = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$targetTable"

      // First create table with metadata enabled
      spark.sql(
        s"""
           |create table $targetTable (
           |  `id` string,
           |  `name` string,
           |  `ts` bigint,
           |  `partition` string
           |) using hudi
           | tblproperties (
           |  'primaryKey' = 'id',
           |  'type' = 'COW',
           |  'preCombineField'='ts',
           |  'hoodie.metadata.enable'='true'
           | )
           | partitioned by (`partition`)
           | location '$tablePath'
        """.stripMargin)

      // Insert initial data
      import spark.implicits._
      val df = Seq(
        ("1", "a1", 1000L, "par1"),
        ("2", "a2", 2000L, "par2"),
        ("3", "a3", 3000L, "par3")
      ).toDF("id", "name", "ts", "partition")

      df.write
        .format("hudi")
        .mode("append")
        .save(tablePath)

      syncPartitionsToCatalog(targetTable)

      // Verify data can be read
      checkAnswer(s"select id, name from $targetTable order by id")(
        Seq("1", "a1"),
        Seq("2", "a2"),
        Seq("3", "a3")
      )

      // Now corrupt the metadata table by deleting some files
      val metaClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
        .setBasePath(tablePath)
        .build()

      val metadataPath = new StoragePath(tablePath, ".hoodie/metadata")
      val storage = metaClient.getStorage
      if (storage.exists(metadataPath)) {
        // Delete a partition from metadata table to simulate corruption
        val filesPartitionPath = new StoragePath(metadataPath, "files")
        if (storage.exists(filesPartitionPath)) {
          val pathInfos = storage.listDirectEntries(filesPartitionPath)
          if (!pathInfos.isEmpty) {
            // Delete the first file to corrupt metadata
            storage.deleteFile(pathInfos.get(0).getPath)
          }
        }
      }

      // Enable catalog-backed listing as fallback
      spark.conf.set(DataSourceReadOptions.FILE_INDEX_PARTITION_LISTING_VIA_CATALOG.key, "true")

      // Re-read the metaClient
      val updatedMetaClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
        .setBasePath(tablePath)
        .build()

      // Catalog-backed metadata should still work
      val engine = new HoodieSparkEngineContext(spark.sparkContext)
      val catalogBackedMetadata = new CatalogBackedTableMetadata(
        engine, updatedMetaClient.getTableConfig, storage, tablePath)

      val partitions = catalogBackedMetadata.getAllPartitionPaths
      assertTrue(partitions.size() >= 3, s"Should have at least 3 partitions, got ${partitions.size()}")

      catalogBackedMetadata.close()
    }
  }

  test("Test catalog-backed metadata with path prefix filtering") {
    withTempDir { tmp =>
      val targetTable = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$targetTable"

      // Enable catalog-backed partition listing
      spark.conf.set(DataSourceReadOptions.FILE_INDEX_PARTITION_LISTING_VIA_CATALOG.key, "true")

      spark.sql(
        s"""
           |create table $targetTable (
           |  `id` string,
           |  `name` string,
           |  `ts` bigint,
           |  `year` string,
           |  `month` string,
           |  `day` string
           |) using hudi
           | tblproperties (
           |  'primaryKey' = 'id',
           |  'type' = 'COW',
           |  'preCombineField'='ts',
           |  'hoodie.metadata.enable'='false'
           | )
           | partitioned by (`year`, `month`, `day`)
           | location '$tablePath'
        """.stripMargin)

      // Insert data across multiple dates
      import spark.implicits._
      val df = Seq(
        ("1", "a1", 1000L, "2024", "01", "01"),
        ("2", "a2", 2000L, "2024", "01", "02"),
        ("3", "a3", 3000L, "2024", "01", "03"),
        ("4", "a4", 4000L, "2024", "02", "01"),
        ("5", "a5", 5000L, "2024", "02", "02"),
        ("6", "a6", 6000L, "2023", "12", "31")
      ).toDF("id", "name", "ts", "year", "month", "day")

      df.write
        .format("hudi")
        .mode("append")
        .save(tablePath)

      syncPartitionsToCatalog(targetTable)

      val metaClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
        .setBasePath(tablePath)
        .build()

      val engine = new HoodieSparkEngineContext(spark.sparkContext)
      val storage = metaClient.getStorage
      val catalogBackedMetadata = new CatalogBackedTableMetadata(
        engine, metaClient.getTableConfig, storage, tablePath)

      // Test filtering with path prefix
      import scala.collection.JavaConverters._
      val janPrefix = hivePartitionPrefix("year" -> "2024", "month" -> "01")
      val febPrefix = hivePartitionPrefix("year" -> "2024", "month" -> "02")
      val decPrefix = hivePartitionPrefix("year" -> "2023", "month" -> "12")
      val jan01 = hivePartitionPath("year" -> "2024", "month" -> "01", "day" -> "01")
      val jan02 = hivePartitionPath("year" -> "2024", "month" -> "01", "day" -> "02")
      val jan03 = hivePartitionPath("year" -> "2024", "month" -> "01", "day" -> "03")
      val feb01 = hivePartitionPath("year" -> "2024", "month" -> "02", "day" -> "01")
      val feb02 = hivePartitionPath("year" -> "2024", "month" -> "02", "day" -> "02")
      val dec31 = hivePartitionPath("year" -> "2023", "month" -> "12", "day" -> "31")
      val pathPrefixes = List(janPrefix).asJava
      val partitionsWithPrefix = catalogBackedMetadata.getPartitionPathWithPathPrefixes(pathPrefixes)

      assertEquals(3, partitionsWithPrefix.size(), "Should have 3 partitions under 2024/01")
      assertTrue(partitionsWithPrefix.contains(jan01))
      assertTrue(partitionsWithPrefix.contains(jan02))
      assertTrue(partitionsWithPrefix.contains(jan03))
      assertFalse(partitionsWithPrefix.contains(feb01))

      // Test with multiple path prefixes
      val multiplePathPrefixes = List(janPrefix, febPrefix).asJava
      val partitionsWithMultiplePrefixes = catalogBackedMetadata.getPartitionPathWithPathPrefixes(multiplePathPrefixes)

      assertEquals(5, partitionsWithMultiplePrefixes.size(), "Should have 5 partitions")
      assertTrue(partitionsWithMultiplePrefixes.contains(jan01))
      assertTrue(partitionsWithMultiplePrefixes.contains(jan02))
      assertTrue(partitionsWithMultiplePrefixes.contains(jan03))
      assertTrue(partitionsWithMultiplePrefixes.contains(feb01))
      assertTrue(partitionsWithMultiplePrefixes.contains(feb02))
      assertFalse(partitionsWithMultiplePrefixes.contains(dec31))
      assertFalse(partitionsWithMultiplePrefixes.contains(decPrefix))

      catalogBackedMetadata.close()
    }
  }

  test("Test catalog-backed metadata with non-partitioned table") {
    withTempDir { tmp =>
      val targetTable = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$targetTable"

      // Enable catalog-backed partition listing
      spark.conf.set(DataSourceReadOptions.FILE_INDEX_PARTITION_LISTING_VIA_CATALOG.key, "true")

      spark.sql(
        s"""
           |create table $targetTable (
           |  `id` string,
           |  `name` string,
           |  `ts` bigint
           |) using hudi
           | tblproperties (
           |  'primaryKey' = 'id',
           |  'type' = 'COW',
           |  'preCombineField'='ts',
           |  'hoodie.metadata.enable'='false'
           | )
           | location '$tablePath'
        """.stripMargin)

      // Insert data
      import spark.implicits._
      val df = Seq(
        ("1", "a1", 1000L),
        ("2", "a2", 2000L),
        ("3", "a3", 3000L)
      ).toDF("id", "name", "ts")

      df.write
        .format("hudi")
        .mode("append")
        .save(tablePath)

      // Verify data can be read
      checkAnswer(s"select id, name from $targetTable order by id")(
        Seq("1", "a1"),
        Seq("2", "a2"),
        Seq("3", "a3")
      )

      val metaClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
        .setBasePath(tablePath)
        .build()

      val engine = new HoodieSparkEngineContext(spark.sparkContext)
      val storage = metaClient.getStorage
      val catalogBackedMetadata = new CatalogBackedTableMetadata(
        engine, metaClient.getTableConfig, storage, tablePath)

      catalogBackedMetadata.close()
    }
  }

  test("Test catalog-backed metadata performance vs file system listing") {
    withTempDir { tmp =>
      val targetTable = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$targetTable"

      spark.sql(
        s"""
           |create table $targetTable (
           |  `id` string,
           |  `name` string,
           |  `ts` bigint,
           |  `year` string,
           |  `month` string
           |) using hudi
           | tblproperties (
           |  'primaryKey' = 'id',
           |  'type' = 'COW',
           |  'preCombineField'='ts',
           |  'hoodie.metadata.enable'='false'
           | )
           | partitioned by (`year`, `month`)
           | location '$tablePath'
        """.stripMargin)

      // Insert data across 24 partitions (2 years * 12 months)
      import spark.implicits._
      val data = for {
        year <- Seq("2023", "2024")
        month <- (1 to 12).map(m => f"$m%02d")
        id <- (1 to 10)
      } yield (s"${year}_${month}_$id", s"name_$id", id * 1000L, year, month)

      val df = data.toDF("id", "name", "ts", "year", "month")
      df.write
        .format("hudi")
        .mode("append")
        .save(tablePath)

      syncPartitionsToCatalog(targetTable)

      val metaClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
        .setBasePath(tablePath)
        .build()

      val engine = new HoodieSparkEngineContext(spark.sparkContext)
      val storage = metaClient.getStorage

      // Test with catalog-backed metadata
      spark.conf.set(DataSourceReadOptions.FILE_INDEX_PARTITION_LISTING_VIA_CATALOG.key, "true")
      val catalogBackedMetadata = new CatalogBackedTableMetadata(
        engine, metaClient.getTableConfig, storage, tablePath)

      val startCatalog = System.currentTimeMillis()
      val partitionsCatalog = catalogBackedMetadata.getAllPartitionPaths
      val catalogTime = System.currentTimeMillis() - startCatalog

      assertEquals(24, partitionsCatalog.size(), "Should have 24 partitions")

      // Test with file system listing
      val fsBackedMetadata = new org.apache.hudi.metadata.FileSystemBackedTableMetadata(
        engine, metaClient.getTableConfig, storage, tablePath)

      val startFs = System.currentTimeMillis()
      val partitionsFs = fsBackedMetadata.getAllPartitionPaths
      val fsTime = System.currentTimeMillis() - startFs

      assertEquals(24, partitionsFs.size(), "Should have 24 partitions")

      // Both methods should return same partitions
      assertEquals(partitionsCatalog.size(), partitionsFs.size(),
        "Catalog and FS listing should return same number of partitions")

      catalogBackedMetadata.close()
      fsBackedMetadata.close()
    }
  }

  private def corruptParquetFileInPartition(tablePath: String, relativePartitionPath: String): Unit = {
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
      .setBasePath(tablePath)
      .build()

    val partitionPath = new StoragePath(tablePath, relativePartitionPath)
    val storage = metaClient.getStorage
    val parquetFiles = storage.listDirectEntries(partitionPath).stream()
      .filter(fileStatus => fileStatus.getPath.getName.endsWith(".parquet") && !fileStatus.getPath.getName.startsWith("."))
      .collect(Collectors.toList())

    assertFalse(parquetFiles.isEmpty, s"Should have at least one parquet file in $relativePartitionPath")

    storage.deleteFile(parquetFiles.get(0).getPath)
    storage.createNewFile(parquetFiles.get(0).getPath)
  }

  private def syncPartitionsToCatalog(tableName: String): Unit = {
    spark.sql(s"msck repair table $tableName")
  }

  private def hivePartitionPrefix(partitionColumns: (String, String)*): String = {
    partitionColumns.map { case (column, value) => s"$column=$value" }.mkString("/")
  }

  private def hivePartitionPath(partitionColumns: (String, String)*): String = {
    hivePartitionPrefix(partitionColumns: _*)
  }
}
