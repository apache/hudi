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

package org.apache.hudi.utilities.benchmarking;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test cases for {@link MetadataBenchmarkingTool}.
 */
public class TestMetadataBenchmarkingTool {

  private static final Logger LOG = LoggerFactory.getLogger(TestMetadataBenchmarkingTool.class);
  private static SparkSession sparkSession;

  @BeforeAll
  public static void setUpClass() {
    // Initialize SparkSession for tests
    sparkSession = SparkSession.builder()
        .appName("TestMetadataBenchmarkingTool")
        .master("local[2]")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .getOrCreate();

    LOG.info("SparkSession and EngineContext initialized for tests");
  }

  @AfterAll
  public static void tearDownClass() {
    if (sparkSession != null) {
      sparkSession.stop();
      sparkSession = null;
      LOG.info("SparkSession stopped");
    }
  }

  @Test
  public void testMetadataBenchmarkingToolRunWithTwoColumns(@TempDir Path tempDir) {
    LOG.info("Running MetadataBenchmarkingTool test with temp directory: {}", tempDir);

    // Create config for MetadataBenchmarkingTool with 2 columns (tenantID & age)
    MetadataBenchmarkingTool.Config config = new MetadataBenchmarkingTool.Config();
    config.tableBasePath = tempDir.resolve("test_table_2cols").toString();
    config.numColumnsToIndex = 2; // tenantID & age
    config.colStatsFileGroupCount = 10;
    config.numFiles = 100;
    config.numPartitions = 3;

    LOG.info("Test config: tableBasePath={}, numFiles={}, numPartitions={}, numColumnsToIndex={}, colStatsFileGroupCount={}",
        config.tableBasePath, config.numFiles, config.numPartitions, config.numColumnsToIndex, config.colStatsFileGroupCount);

    // Run MetadataBenchmarkingTool
    assertDoesNotThrow(() -> {
      try (MetadataBenchmarkingTool metadataBenchmarkingTool = new MetadataBenchmarkingTool(sparkSession, config)) {
        metadataBenchmarkingTool.run();
      }
    }, "MetadataBenchmarkingTool.run1() should complete without throwing exceptions");

    LOG.info("MetadataBenchmarkingTool test with 2 columns completed successfully");
  }

  @Test
  public void testMetadataBenchmarkingToolRunWithOneColumn(@TempDir Path tempDir) {
    LOG.info("Running MetadataBenchmarkingTool test with temp directory: {}", tempDir);

    // Create config for MetadataBenchmarkingTool with 1 column (tenantID only)
    MetadataBenchmarkingTool.Config config = new MetadataBenchmarkingTool.Config();
    config.tableBasePath = tempDir.resolve("test_table_1col").toString();
    config.numColumnsToIndex = 1; // tenantID only
    config.colStatsFileGroupCount = 10;
    config.numFiles = 100;
    config.numPartitions = 3;

    LOG.info("Test config: tableBasePath={}, numFiles={}, numPartitions={}, numColumnsToIndex={}, colStatsFileGroupCount={}",
        config.tableBasePath, config.numFiles, config.numPartitions, config.numColumnsToIndex, config.colStatsFileGroupCount);

    // Run MetadataBenchmarkingTool
    assertDoesNotThrow(() -> {
      try (MetadataBenchmarkingTool metadataBenchmarkingTool = new MetadataBenchmarkingTool(sparkSession, config)) {
        metadataBenchmarkingTool.run();
      }
    }, "MetadataBenchmarkingTool.run1() should complete without throwing exceptions");

    LOG.info("MetadataBenchmarkingTool test with 1 column completed successfully");
  }

  /**
   * Test getPartitionFilter method with various numPartitions values.
   * Validates that the filter correctly excludes ~25% of partitions.
   */
  @ParameterizedTest
  @CsvSource({
      "1, ''",                           // Single partition - no filtering possible
      "2, dt > '2020-01-01'",            // 2 partitions: exclude 1 (50%), keeps 1
      "3, dt > '2020-01-01'",            // 3 partitions: ceil(0.75)=1 excluded, keeps 2 (66%)
      "4, dt > '2020-01-01'",            // 4 partitions: ceil(1)=1 excluded, keeps 3 (75%)
      "10, dt > '2020-01-03'",           // 10 partitions: ceil(2.5)=3 excluded, keeps 7 (70%)
      "100, dt > '2020-01-25'"           // 100 partitions: ceil(25)=25 excluded, keeps 75 (75%)
  })
  public void testGetPartitionFilter(int numPartitions, String expectedFilter) throws Exception {
    LOG.info("Testing getPartitionFilter with numPartitions={}", numPartitions);

    // Create config with specified numPartitions
    MetadataBenchmarkingTool.Config config = new MetadataBenchmarkingTool.Config();
    config.tableBasePath = "/tmp/test_partition_filter";
    config.numPartitions = numPartitions;
    config.numColumnsToIndex = 1;
    config.colStatsFileGroupCount = 1;
    config.numFiles = 10;

    // Use reflection to call private getPartitionFilter method
    MetadataBenchmarkingTool tool = new MetadataBenchmarkingTool(sparkSession, config);
    Method method = MetadataBenchmarkingTool.class.getDeclaredMethod("getPartitionFilter");
    method.setAccessible(true);
    String actualFilter = (String) method.invoke(tool);

    LOG.info("numPartitions={}, expectedFilter='{}', actualFilter='{}'", numPartitions, expectedFilter, actualFilter);

    assertEquals(expectedFilter, actualFilter,
        String.format("Partition filter mismatch for numPartitions=%d", numPartitions));

    // Calculate and log the expected data retention percentage
    if (numPartitions > 1) {
      int numPartitionsToExclude = (int) Math.ceil(numPartitions * 0.25);
      int partitionsKept = numPartitions - numPartitionsToExclude;
      double retentionPercentage = (partitionsKept * 100.0) / numPartitions;
      LOG.info("numPartitions={}: excluding {} partitions, keeping {} partitions ({}%)",
          numPartitions, numPartitionsToExclude, partitionsKept, String.format("%.1f", retentionPercentage));
    }

    tool.close();
  }
}

