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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.config.HoodieStreamerConfig;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression guard for the record-size sampler: the sample must be written straight from its RDD and
 * never routed through the driver. The original sampler did {@code collect()} then
 * {@code jsc.parallelize(samples, 1)}, which shipped the whole sample through the driver and blew
 * {@code spark.rpc.message.maxSize} for large-record tables (and {@code spark.driver.maxResultSize}
 * on the collect leg).
 *
 * <p>This test pins {@code spark.driver.maxResultSize} far below the sample's collected size on a
 * dedicated context (the shared functional harness runs at 2 GB and cannot be lowered per test).
 * Any implementation that collects the sample to the driver overruns that budget and fails here;
 * the RDD-based implementation keeps the sample in the data plane, so the driver only ever receives
 * the single bulk-insert {@code WriteStatus} plus small counts, stays under the budget, and produces
 * the estimate. {@code spark.driver.maxResultSize} is enforced in {@code local[N]}, so this runs in
 * normal CI without a real cluster.
 */
class TestSampleWritesDriverBudget {

  // Enough that collecting the sample far exceeds the driver budget below; the RDD path's driver
  // footprint is O(1) (one bulk-insert WriteStatus plus small counts), independent of record count.
  // Guard margin relies on trip records being well over 512k/5000 (~105) serialized bytes each; they
  // are ~250+, so a collect-based sampler returns ~1.3 MiB to the driver and trips the budget.
  private static final int RECORD_COUNT = 5000;
  // Far below the collected sample size (~1.3 MiB here), well above the RDD path's driver footprint.
  private static final String DRIVER_RESULT_BUDGET = "512k";

  private static JavaSparkContext jsc;

  @TempDir
  Path tempDir;

  private HoodieTestDataGenerator dataGen;

  @BeforeAll
  static void setUpSpark() {
    SparkConf conf = new SparkConf()
        .setAppName("sample-writes-driver-budget")
        .setMaster("local[2]")
        .set("spark.ui.enabled", "false")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
        .set("spark.driver.maxResultSize", DRIVER_RESULT_BUDGET);
    jsc = new JavaSparkContext(conf);
  }

  @AfterAll
  static void tearDownSpark() {
    if (jsc != null) {
      jsc.close();
      jsc = null;
    }
  }

  @BeforeEach
  void setUp() {
    dataGen = new HoodieTestDataGenerator(0xDEED);
  }

  @AfterEach
  void tearDown() {
    if (dataGen != null) {
      dataGen.close();
    }
  }

  @Test
  void estimatesWithoutRoutingSampleThroughDriver() throws IOException {
    String basePath = tempDir.toAbsolutePath().toString();
    HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName("sample_writes_budget")
        .setPayloadClass(HoodieAvroPayload.class)
        .initTable(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()), basePath);

    TypedProperties props = new TypedProperties();
    props.put(HoodieStreamerConfig.SAMPLE_WRITES_ENABLED.key(), "true");
    // Keep the whole batch in the sample so a collect would clearly exceed the driver budget.
    props.put(HoodieStreamerConfig.SAMPLE_WRITES_SIZE.key(), String.valueOf(RECORD_COUNT));
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withProperties(props)
        .forTable("sample_writes_budget")
        .withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .build();

    String commitTime = HoodieTestDataGenerator.getCommitTimeAtUTC(1);
    JavaRDD<HoodieRecord> records = jsc.parallelize(dataGen.generateInserts(commitTime, RECORD_COUNT), 4);

    Option<HoodieWriteConfig> writeConfigOpt =
        SparkSampleWritesUtils.getWriteConfigWithRecordSizeEstimate(jsc, Option.of(records), writeConfig);

    assertTrue(writeConfigOpt.isPresent(),
        "Record-size estimate must be produced without collecting the sample to the driver "
            + "(a collect-based sampler exceeds spark.driver.maxResultSize=" + DRIVER_RESULT_BUDGET + ").");
  }
}
