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

package com.uber.hoodie.index;

import com.uber.hoodie.common.HoodieClientTestUtils;
import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieHBaseIndexConfig;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieStorageConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.index.hbase.DefaultHBaseQPSResourceAllocator;
import com.uber.hoodie.index.hbase.HBaseIndex;
import com.uber.hoodie.index.hbase.HBaseIndexQPSResourceAllocator;
import java.io.File;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestHBaseQPSResourceAllocator {
  private static JavaSparkContext jsc = null;
  private static String tableName = "test_table";
  private String basePath = null;
  private static HBaseTestingUtility utility;
  private static Configuration hbaseConfig;
  private static String QPS_TEST_SUFFIX_PATH = "qps_test_suffix";

  @AfterClass
  public static void clean() {
    if (jsc != null) {
      jsc.stop();
    }
  }

  @BeforeClass
  public static void init() throws Exception {
    utility = new HBaseTestingUtility();
    utility.startMiniCluster();
    hbaseConfig = utility.getConnection().getConfiguration();
    jsc = new JavaSparkContext(HoodieClientTestUtils.getSparkConfForTest("TestQPSResourceAllocator"));
  }

  @After
  public void clear() {
    if (basePath != null) {
      new File(basePath).delete();
    }
  }

  @Before
  public void before() throws Exception {
    // Create a temp folder as the base path
    TemporaryFolder folder = new TemporaryFolder();
    folder.create();
    basePath = folder.getRoot().getAbsolutePath() + QPS_TEST_SUFFIX_PATH;
    // Initialize table
    HoodieTestUtils.init(jsc.hadoopConfiguration(), basePath);
  }

  @Test
  public void testsDefaultQPSResourceAllocator() {
    HoodieWriteConfig config = getConfig(Optional.empty());
    HBaseIndex index = new HBaseIndex(config);
    HBaseIndexQPSResourceAllocator hBaseIndexQPSResourceAllocator = index.createQPSResourceAllocator(config);
    Assert.assertEquals(hBaseIndexQPSResourceAllocator.getClass().getName(),
        DefaultHBaseQPSResourceAllocator.class.getName());
    Assert.assertEquals(config.getHbaseIndexQPSFraction(),
        hBaseIndexQPSResourceAllocator.acquireQPSResources(config.getHbaseIndexQPSFraction(), 100), 0.0f);
  }

  @Test
  public void testsExplicitDefaultQPSResourceAllocator() {
    HoodieWriteConfig config = getConfig(Optional.of(HoodieHBaseIndexConfig.DEFAULT_HBASE_INDEX_QPS_ALLOCATOR_CLASS));
    HBaseIndex index = new HBaseIndex(config);
    HBaseIndexQPSResourceAllocator hBaseIndexQPSResourceAllocator = index.createQPSResourceAllocator(config);
    Assert.assertEquals(hBaseIndexQPSResourceAllocator.getClass().getName(),
        DefaultHBaseQPSResourceAllocator.class.getName());
    Assert.assertEquals(config.getHbaseIndexQPSFraction(),
        hBaseIndexQPSResourceAllocator.acquireQPSResources(config.getHbaseIndexQPSFraction(), 100), 0.0f);
  }

  @Test
  public void testsInvalidQPSResourceAllocator() {
    HoodieWriteConfig config = getConfig(Optional.of("InvalidResourceAllocatorClassName"));
    HBaseIndex index = new HBaseIndex(config);
    HBaseIndexQPSResourceAllocator hBaseIndexQPSResourceAllocator = index.createQPSResourceAllocator(config);
    Assert.assertEquals(hBaseIndexQPSResourceAllocator.getClass().getName(),
        DefaultHBaseQPSResourceAllocator.class.getName());
    Assert.assertEquals(config.getHbaseIndexQPSFraction(),
        hBaseIndexQPSResourceAllocator.acquireQPSResources(config.getHbaseIndexQPSFraction(), 100), 0.0f);
  }

  private HoodieWriteConfig getConfig(Optional<String> resourceAllocatorClass) {
    HoodieHBaseIndexConfig hoodieHBaseIndexConfig = getConfigWithResourceAllocator(resourceAllocatorClass);
    return getConfigBuilder(hoodieHBaseIndexConfig).build();
  }

  private HoodieWriteConfig.Builder getConfigBuilder(HoodieHBaseIndexConfig hoodieHBaseIndexConfig) {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
               .withParallelism(1, 1).withCompactionConfig(
                   HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024).withInlineCompaction(false)
                       .build()).withAutoCommit(false)
               .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1024 * 1024).build())
               .forTable("test-trip-table").withIndexConfig(
                   HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.HBASE)
                       .withHBaseIndexConfig(hoodieHBaseIndexConfig)
                       .build());
  }

  private HoodieHBaseIndexConfig getConfigWithResourceAllocator(Optional<String> resourceAllocatorClass) {
    HoodieHBaseIndexConfig.Builder builder =
        new HoodieHBaseIndexConfig.Builder()
            .hbaseZkPort(Integer.valueOf(hbaseConfig.get("hbase.zookeeper.property.clientPort")))
            .hbaseZkQuorum(hbaseConfig.get("hbase.zookeeper.quorum")).hbaseTableName(tableName)
            .hbaseIndexGetBatchSize(100);
    if (resourceAllocatorClass.isPresent()) {
      builder.withQPSResourceAllocatorType(resourceAllocatorClass.get());
    }
    return builder.build();
  }
}
