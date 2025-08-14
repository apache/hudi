/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utils;

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.keygen.SimpleAvroKeyGenerator;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link StreamerUtil}.
 */
class TestStreamerUtil {

  @TempDir
  File tempFile;

  @Test
  void testInitTableWithSpecificVersion() throws IOException {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());

    // Test for partitioned table.
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "p0,p1");
    conf.set(FlinkOptions.WRITE_TABLE_VERSION, HoodieTableVersion.SIX.versionCode());
    StreamerUtil.initTableIfNotExists(conf);

    // Validate the partition fields & preCombineField in hoodie.properties.
    HoodieTableMetaClient metaClient1 = HoodieTestUtils.createMetaClient(tempFile.getAbsolutePath());
    assertArrayEquals(metaClient1.getTableConfig().getPartitionFields().get(), new String[] {"p0", "p1"});
    assertNotNull(metaClient1.getTableConfig().getKeyGeneratorClassName());
    assertEquals(HoodieTableVersion.SIX, metaClient1.getTableConfig().getTableVersion());
  }

  @Test
  void testInitTableIfNotExists() throws IOException {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());

    // Test for partitioned table.
    conf.set(FlinkOptions.ORDERING_FIELDS, "ts");
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "p0,p1");
    StreamerUtil.initTableIfNotExists(conf);

    // Validate the partition fields & preCombineField in hoodie.properties.
    HoodieTableMetaClient metaClient1 = HoodieTestUtils.createMetaClient(tempFile.getAbsolutePath());
    assertTrue(metaClient1.getTableConfig().getPartitionFields().isPresent(),
        "Missing partition columns in the hoodie.properties.");
    assertArrayEquals(metaClient1.getTableConfig().getPartitionFields().get(), new String[] {"p0", "p1"});
    assertEquals(metaClient1.getTableConfig().getOrderingFieldsStr().get(), "ts");
    assertEquals(metaClient1.getTableConfig().getKeyGeneratorClassName(), SimpleAvroKeyGenerator.class.getName());
    assertEquals(HoodieTableVersion.current(), metaClient1.getTableConfig().getTableVersion());

    // Test for non-partitioned table.
    conf.removeConfig(FlinkOptions.PARTITION_PATH_FIELD);
    FileIOUtils.deleteDirectory(tempFile);
    StreamerUtil.initTableIfNotExists(conf);
    HoodieTableMetaClient metaClient2 = HoodieTestUtils.createMetaClient(tempFile.getAbsolutePath());
    assertFalse(metaClient2.getTableConfig().getPartitionFields().isPresent());
    assertEquals(metaClient2.getTableConfig().getKeyGeneratorClassName(), SimpleAvroKeyGenerator.class.getName());
  }

  @Test
  void testMedianInstantTime() {
    String higher = "20210705125921";
    String lower = "20210705125806";
    String expectedMedianInstant = "20210705125844499";
    String median1 = StreamerUtil.medianInstantTime(higher, lower).get();
    assertThat(median1, is(expectedMedianInstant));
    // test symmetry
    assertThrows(IllegalArgumentException.class,
        () -> StreamerUtil.medianInstantTime(lower, higher),
        "The first argument should have newer instant time");
    // test very near instant time
    assertFalse(StreamerUtil.medianInstantTime("20211116115634", "20211116115633").isPresent());
  }

  @Test
  void testInstantTimeDiff() {
    String higher = "20210705125921";
    String lower = "20210705125806";
    long diff = StreamerUtil.instantTimeDiffSeconds(higher, lower);
    assertThat(diff, is(75L));
  }

  @Test
  public void testAddCheckpointIdIntoMetadata() {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());

    // Test for write extra metadata.
    conf.set(FlinkOptions.WRITE_EXTRA_METADATA_ENABLED, true);

    HashMap<String, String> metadata = new HashMap<>();
    StreamerUtil.addFlinkCheckpointIdIntoMetaData(conf, metadata, 123L);
    assertEquals(metadata.get(StreamerUtil.FLINK_CHECKPOINT_ID), "123");
  }

  @Test
  void testTableExist() throws IOException {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    String basePath = tempFile.getAbsolutePath();

    assertFalse(StreamerUtil.tableExists(basePath, HadoopConfigurations.getHadoopConf(conf)));

    try (FileSystem fs = HadoopFSUtils.getFs(basePath, HadoopConfigurations.getHadoopConf(conf))) {
      fs.mkdirs(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME));
      assertFalse(StreamerUtil.tableExists(basePath, HadoopConfigurations.getHadoopConf(conf)));

      fs.create(new Path(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME), HoodieTableConfig.HOODIE_PROPERTIES_FILE));
      assertTrue(StreamerUtil.tableExists(basePath, HadoopConfigurations.getHadoopConf(conf)));
    }
  }
}

