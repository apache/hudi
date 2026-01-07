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

package org.apache.hudi.utilities;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Test cases for {@link HoodieMDTStats}.
 */
public class TestHoodieMDTStats {

  private static final Logger LOG = LoggerFactory.getLogger(TestHoodieMDTStats.class);
  private static SparkSession sparkSession;

  @BeforeAll
  public static void setUpClass() {
    // Initialize SparkSession for tests
    sparkSession = SparkSession.builder()
        .appName("TestHoodieMDTStats")
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
  public void testHoodieMDTStatsRun(@TempDir Path tempDir) {
    LOG.info("Running HoodieMDTStats test with temp directory: {}", tempDir);

    // Create config for HoodieMDTStats
    HoodieMDTStats.Config config = new HoodieMDTStats.Config();
    config.tableBasePath = tempDir.resolve("test_table").toString();
    config.colsToIndex = "age,salary";
    config.colStatsFileGroupCount = 10;
    config.numFiles = 1000;
    config.filesPerCommit = 1000;
    config.numPartitions = 3;

    LOG.info("Test config: tableBasePath={}, numFiles={}, numPartitions={}, numColumnsToIndex={}, colStatsFileGroupCount={}",
        config.tableBasePath, config.numFiles, config.numPartitions, config.colsToIndex, config.colStatsFileGroupCount);

    // Run HoodieMDTStats
    assertDoesNotThrow(() -> {
      try (HoodieMDTStats hoodieMDTStats = new HoodieMDTStats(sparkSession, config)) {
        hoodieMDTStats.run();
      }
    }, "HoodieMDTStats.run() should complete without throwing exceptions");

    LOG.info("HoodieMDTStats test completed successfully");
  }
}

