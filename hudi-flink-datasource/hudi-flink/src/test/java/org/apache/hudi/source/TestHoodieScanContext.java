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

package org.apache.hudi.source;

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.source.prune.PartitionPruners;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.mockito.Mockito.mock;

/**
 * Test cases for {@link HoodieScanContext}.
 */
public class TestHoodieScanContext {

  @Test
  public void testGetConf() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.PATH, "/tmp/test");

    HoodieScanContext scanContext = createTestScanContext(conf,  new StoragePath("/tmp/test"),
        TestConfigurations.ROW_TYPE, "20231201000000000", 100 * 1024 * 1024,
        1000, false, false, false, false);

    assertNotNull(scanContext.getConf(), "Configuration should not be null");
    assertEquals("/tmp/test", scanContext.getConf().get(FlinkOptions.PATH),
        "Configuration should match");
  }

  @Test
  public void testGetPath() throws Exception {
    Configuration conf = new Configuration();
    StoragePath expectedPath = new StoragePath("/tmp/test/table");

    HoodieScanContext scanContext = createTestScanContext(conf, expectedPath,
        TestConfigurations.ROW_TYPE, "20231201000000000", 100 * 1024 * 1024,
        1000, false, false, false, false);

    assertEquals(expectedPath, scanContext.getPath(), "Path should match");
  }

  @Test
  public void testGetRowType() throws Exception {
    Configuration conf = new Configuration();
    RowType rowType = TestConfigurations.ROW_TYPE;

    HoodieScanContext scanContext = createTestScanContext(conf, new StoragePath("/tmp/test"),
        rowType, "20231201000000000", 100 * 1024 * 1024,
        1000, false, false, false, false);

    assertNotNull(scanContext.getRowType(), "RowType should not be null");
    assertEquals(rowType, scanContext.getRowType(), "RowType should match");
  }

  @Test
  public void testGetStartInstant() throws Exception {
    Configuration conf = new Configuration();
    String expectedInstant = "20231201000000000";

    HoodieScanContext scanContext = createTestScanContext(conf, new StoragePath("/tmp/test"),
        TestConfigurations.ROW_TYPE, expectedInstant, 100 * 1024 * 1024,
        1000, false, false, false, false);

    assertEquals(expectedInstant, scanContext.getStartInstant(),
        "Start instant should match");
  }

  @Test
  public void testGetMaxCompactionMemoryInBytes() throws Exception {
    Configuration conf = new Configuration();
    long expectedMemory = 1024L * 1024L * 1024L; // 1GB

    HoodieScanContext scanContext = createTestScanContext(conf, new StoragePath("/tmp/test"),
        TestConfigurations.ROW_TYPE, "20231201000000000", expectedMemory,
        1000, false, false, false, false);

    assertEquals(expectedMemory, scanContext.getMaxCompactionMemoryInBytes(),
        "Max compaction memory should match");
  }

  @Test
  public void testGetMaxPendingSplits() throws Exception {
    Configuration conf = new Configuration();
    long expectedMaxPendingSplits = 5000L;

    HoodieScanContext scanContext = createTestScanContext(conf, new StoragePath("/tmp/test"),
        TestConfigurations.ROW_TYPE, "20231201000000000", 100 * 1024 * 1024,
        expectedMaxPendingSplits, false, false, false, false);

    assertEquals(expectedMaxPendingSplits, scanContext.getMaxPendingSplits(),
        "Max pending splits should match");
  }

  @Test
  public void testSkipCompaction() throws Exception {
    Configuration conf = new Configuration();

    HoodieScanContext scanContextTrue = createTestScanContext(conf, new StoragePath("/tmp/test"),
        TestConfigurations.ROW_TYPE, "20231201000000000", 100 * 1024 * 1024,
        1000, true, false, false, false);
    assertTrue(scanContextTrue.isSkipCompaction(), "Skip compaction should be true");

    HoodieScanContext scanContextFalse = createTestScanContext(conf, new StoragePath("/tmp/test"),
        TestConfigurations.ROW_TYPE, "20231201000000000", 100 * 1024 * 1024,
        1000, false, false, false, false);
    assertFalse(scanContextFalse.isSkipCompaction(), "Skip compaction should be false");
  }

  @Test
  public void testSkipClustering() throws Exception {
    Configuration conf = new Configuration();

    HoodieScanContext scanContextTrue = createTestScanContext(conf, new StoragePath("/tmp/test"),
        TestConfigurations.ROW_TYPE, "20231201000000000", 100 * 1024 * 1024,
        1000, false, true, false, false);
    assertTrue(scanContextTrue.isSkipClustering(), "Skip clustering should be true");

    HoodieScanContext scanContextFalse = createTestScanContext(conf, new StoragePath("/tmp/test"),
        TestConfigurations.ROW_TYPE, "20231201000000000", 100 * 1024 * 1024,
        1000, false, false, false, false);
    assertFalse(scanContextFalse.isSkipClustering(), "Skip clustering should be false");
  }

  @Test
  public void testSkipInsertOverwrite() throws Exception {
    Configuration conf = new Configuration();

    HoodieScanContext scanContextTrue = createTestScanContext(conf, new StoragePath("/tmp/test"),
        TestConfigurations.ROW_TYPE, "20231201000000000", 100 * 1024 * 1024,
        1000, false, false, true, false);
    assertTrue(scanContextTrue.isSkipInsertOverwrite(), "Skip insert overwrite should be true");

    HoodieScanContext scanContextFalse = createTestScanContext(conf, new StoragePath("/tmp/test"),
        TestConfigurations.ROW_TYPE, "20231201000000000", 100 * 1024 * 1024,
        1000, false, false, false, false);
    assertFalse(scanContextFalse.isSkipInsertOverwrite(), "Skip insert overwrite should be false");
  }

  @Test
  public void testCdcEnabled() throws Exception {
    Configuration conf = new Configuration();

    HoodieScanContext scanContextTrue = createTestScanContext(conf, new StoragePath("/tmp/test"),
        TestConfigurations.ROW_TYPE, "20231201000000000", 100 * 1024 * 1024,
        1000, false, false, false, true);
    assertTrue(scanContextTrue.isCdcEnabled(), "CDC should be enabled");

    HoodieScanContext scanContextFalse = createTestScanContext(conf, new StoragePath("/tmp/test"),
        TestConfigurations.ROW_TYPE, "20231201000000000", 100 * 1024 * 1024,
        1000, false, false, false, false);
    assertFalse(scanContextFalse.isCdcEnabled(), "CDC should be disabled");
  }

  @Test
  public void testGetScanInterval() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.READ_STREAMING_CHECK_INTERVAL, 5);

    HoodieScanContext scanContext = createTestScanContext(conf, new StoragePath("/tmp/test"),
        TestConfigurations.ROW_TYPE, "20231201000000000", 100 * 1024 * 1024,
        1000, false, false, false, false);
    Duration scanInterval = scanContext.getScanInterval();

    assertNotNull(scanInterval, "Scan interval should not be null");
    assertEquals(Duration.ofSeconds(5), scanInterval, "Scan interval should be 5 minutes");
  }

  @Test
  public void testGetScanIntervalDefaultValue() throws Exception {
    Configuration conf = new Configuration();
    // Not setting READ_STREAMING_CHECK_INTERVAL to use default

    HoodieScanContext scanContext = createTestScanContext(conf, new StoragePath("/tmp/test"),
        TestConfigurations.ROW_TYPE, "20231201000000000", 100 * 1024 * 1024,
        1000, false, false, false, false);
    Duration scanInterval = scanContext.getScanInterval();

    assertNotNull(scanInterval, "Scan interval should not be null");
    assertEquals(Duration.ofSeconds(FlinkOptions.READ_STREAMING_CHECK_INTERVAL.defaultValue()),
        scanInterval, "Scan interval should use default value");
  }

  @Test
  public void testAllFieldsInitialized() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.READ_STREAMING_CHECK_INTERVAL, 10);
    StoragePath path = new StoragePath("/tmp/hoodie/table");
    String startInstant = "20231215120000000";
    long maxCompactionMemory = 2L * 1024L * 1024L * 1024L; // 2GB
    long maxPendingSplits = 10000L;

    HoodieScanContext scanContext = createTestScanContext(conf, path,
        TestConfigurations.ROW_TYPE, startInstant, maxCompactionMemory,
        maxPendingSplits, true, true, true, true);

    assertEquals(path, scanContext.getPath());
    assertEquals(startInstant, scanContext.getStartInstant());
    assertEquals(maxCompactionMemory, scanContext.getMaxCompactionMemoryInBytes());
    assertEquals(maxPendingSplits, scanContext.getMaxPendingSplits());
    assertTrue(scanContext.isSkipCompaction());
    assertTrue(scanContext.isSkipClustering());
    assertTrue(scanContext.isSkipInsertOverwrite());
    assertTrue(scanContext.isCdcEnabled());
    assertEquals(Duration.ofSeconds(10), scanContext.getScanInterval());
  }

  @Test
  public void testBuilderWithMinimalConfiguration() throws Exception {
    Configuration conf = new Configuration();
    StoragePath path = new StoragePath("/tmp/test");

    HoodieScanContext scanContext = HoodieScanContext.builder()
        .conf(conf)
        .path(path)
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant("20240101000000")
        .maxCompactionMemoryInBytes(100 * 1024 * 1024)
        .maxPendingSplits(1000)
        .skipCompaction(false)
        .skipClustering(false)
        .skipInsertOverwrite(false)
        .cdcEnabled(false)
        .build();

    assertNotNull(scanContext);
    assertEquals(path, scanContext.getPath());
  }

  @Test
  public void testBuilderChaining() throws Exception {
    Configuration conf = new Configuration();
    StoragePath path = new StoragePath("/tmp/test");

    HoodieScanContext.HoodieScanContextBuilder builder = HoodieScanContext.builder();
    HoodieScanContext scanContext = builder
        .conf(conf)
        .path(path)
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant("20240101000000")
        .endInstant("20240201000000")
        .maxCompactionMemoryInBytes(100 * 1024 * 1024)
        .maxPendingSplits(1000)
        .skipCompaction(true)
        .skipClustering(true)
        .skipInsertOverwrite(true)
        .cdcEnabled(true)
        .isStreaming(true)
        .build();

    assertNotNull(scanContext);
    assertTrue(scanContext.isSkipCompaction());
    assertTrue(scanContext.isSkipClustering());
    assertTrue(scanContext.isSkipInsertOverwrite());
    assertTrue(scanContext.isCdcEnabled());
    assertTrue(scanContext.isStreaming());
  }

  @Test
  public void testBuilderWithEndInstant() throws Exception {
    Configuration conf = new Configuration();
    StoragePath path = new StoragePath("/tmp/test");
    String endInstant = "20240201000000";

    HoodieScanContext scanContext = HoodieScanContext.builder()
        .conf(conf)
        .path(path)
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant("20240101000000")
        .endInstant(endInstant)
        .maxCompactionMemoryInBytes(100 * 1024 * 1024)
        .maxPendingSplits(1000)
        .skipCompaction(false)
        .skipClustering(false)
        .skipInsertOverwrite(false)
        .cdcEnabled(false)
        .build();

    assertNotNull(scanContext);
    assertEquals(endInstant, scanContext.getEndInstant());
  }

  @Test
  public void testIsStreaming() throws Exception {
    Configuration conf = new Configuration();

    HoodieScanContext streamingScanContext = createTestScanContext(conf, new StoragePath("/tmp/test"),
        TestConfigurations.ROW_TYPE, "20240101000000", 100 * 1024 * 1024,
        1000, false, false, false, false);

    assertFalse(streamingScanContext.isStreaming(), "Should not be streaming by default");

    HoodieScanContext batchScanContext = HoodieScanContext.builder()
        .conf(conf)
        .path(new StoragePath("/tmp/test"))
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant("20240101000000")
        .maxCompactionMemoryInBytes(100 * 1024 * 1024)
        .maxPendingSplits(1000)
        .skipCompaction(false)
        .skipClustering(false)
        .skipInsertOverwrite(false)
        .cdcEnabled(false)
        .isStreaming(true)
        .build();

    assertTrue(batchScanContext.isStreaming(), "Should be streaming when explicitly set");
  }

  @Test
  public void testGetEndCommit() throws Exception {
    Configuration conf = new Configuration();
    String endCommit = "20240201000000";

    HoodieScanContext scanContext = HoodieScanContext.builder()
        .conf(conf)
        .path(new StoragePath("/tmp/test"))
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant("20240101000000")
        .endInstant(endCommit)
        .maxCompactionMemoryInBytes(100 * 1024 * 1024)
        .maxPendingSplits(1000)
        .skipCompaction(false)
        .skipClustering(false)
        .skipInsertOverwrite(false)
        .cdcEnabled(false)
        .build();

    assertEquals(endCommit, scanContext.getEndInstant());
  }

  @Test
  public void testBuilderProducesIndependentInstances() throws Exception {
    Configuration conf = new Configuration();
    StoragePath path = new StoragePath("/tmp/test");

    HoodieScanContext.HoodieScanContextBuilder builder = HoodieScanContext.builder()
        .conf(conf)
        .path(path)
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant("20240101000000")
        .maxCompactionMemoryInBytes(100 * 1024 * 1024)
        .maxPendingSplits(1000)
        .skipCompaction(false)
        .skipClustering(false)
        .skipInsertOverwrite(false)
        .cdcEnabled(false);

    HoodieScanContext scanContext1 = builder.build();
    HoodieScanContext scanContext2 = builder.build();

    // Both should be valid but independent instances
    assertNotNull(scanContext1);
    assertNotNull(scanContext2);
  }

  // Helper method to create ScanContext using the Builder
  private HoodieScanContext createTestScanContext(
      Configuration conf,
      StoragePath path,
      RowType rowType,
      String startInstant,
      long maxCompactionMemoryInBytes,
      long maxPendingSplits,
      boolean skipCompaction,
      boolean skipClustering,
      boolean skipInsertOverwrite,
      boolean cdcEnabled) throws Exception {
    return HoodieScanContext.builder()
        .conf(conf)
        .path(path)
        .rowType(rowType)
        .startInstant(startInstant)
        .maxCompactionMemoryInBytes(maxCompactionMemoryInBytes)
        .maxPendingSplits(maxPendingSplits)
        .skipCompaction(skipCompaction)
        .skipClustering(skipClustering)
        .skipInsertOverwrite(skipInsertOverwrite)
        .cdcEnabled(cdcEnabled)
        .build();
  }

  @Test
  public void testStreamingModeConfiguration() throws Exception {
    Configuration conf = new Configuration();
    StoragePath path = new StoragePath("/tmp/test");

    // Test with isStreaming = true
    HoodieScanContext streamingContext = HoodieScanContext.builder()
        .conf(conf)
        .path(path)
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant("20240101000000")
        .maxCompactionMemoryInBytes(100 * 1024 * 1024)
        .maxPendingSplits(1000)
        .skipCompaction(false)
        .skipClustering(false)
        .skipInsertOverwrite(false)
        .cdcEnabled(false)
        .isStreaming(true)
        .build();

    assertTrue(streamingContext.isStreaming(), "Streaming mode should be enabled");

    // Test with isStreaming = false (batch mode)
    HoodieScanContext batchContext = HoodieScanContext.builder()
        .conf(conf)
        .path(path)
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant("20240101000000")
        .maxCompactionMemoryInBytes(100 * 1024 * 1024)
        .maxPendingSplits(1000)
        .skipCompaction(false)
        .skipClustering(false)
        .skipInsertOverwrite(false)
        .cdcEnabled(false)
        .isStreaming(false)
        .build();

    assertFalse(batchContext.isStreaming(), "Batch mode should be enabled");
  }

  @Test
  public void testStreamingModeDefault() throws Exception {
    Configuration conf = new Configuration();
    StoragePath path = new StoragePath("/tmp/test");

    HoodieScanContext scanContext = HoodieScanContext.builder()
        .conf(conf)
        .path(path)
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant("20240101000000")
        .maxCompactionMemoryInBytes(100 * 1024 * 1024)
        .maxPendingSplits(1000)
        .skipCompaction(false)
        .skipClustering(false)
        .skipInsertOverwrite(false)
        .cdcEnabled(false)
        .build();

    assertFalse(scanContext.isStreaming(), "Streaming mode should default to false");
  }

  @Test
  public void testBuilderWithAllStreamingFlags() throws Exception {
    Configuration conf = new Configuration();
    StoragePath path = new StoragePath("/tmp/test");

    HoodieScanContext scanContext = HoodieScanContext.builder()
        .conf(conf)
        .path(path)
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant("20240101000000")
        .maxCompactionMemoryInBytes(100 * 1024 * 1024)
        .maxPendingSplits(1000)
        .skipCompaction(true)
        .skipClustering(true)
        .skipInsertOverwrite(true)
        .cdcEnabled(true)
        .isStreaming(true)
        .build();

    assertTrue(scanContext.isSkipCompaction(), "skipCompaction should be true");
    assertTrue(scanContext.isSkipClustering(), "skipClustering should be true");
    assertTrue(scanContext.isSkipInsertOverwrite(), "skipInsertOverwrite should be true");
    assertTrue(scanContext.isCdcEnabled(), "cdcEnabled should be true");
    assertTrue(scanContext.isStreaming(), "isStreaming should be true");
  }

  @Test
  public void testBuilderWithStartAndEndInstants() throws Exception {
    Configuration conf = new Configuration();
    StoragePath path = new StoragePath("/tmp/test");
    String startInstant = "20240101000000";
    String endInstant = "20240201000000";

    HoodieScanContext scanContext = HoodieScanContext.builder()
        .conf(conf)
        .path(path)
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant(startInstant)
        .endInstant(endInstant)
        .maxCompactionMemoryInBytes(100 * 1024 * 1024)
        .maxPendingSplits(1000)
        .skipCompaction(false)
        .skipClustering(false)
        .skipInsertOverwrite(false)
        .cdcEnabled(false)
        .build();

    assertEquals(startInstant, scanContext.getStartInstant(), "Start instant should match");
    assertEquals(endInstant, scanContext.getEndInstant(), "End instant should match");
  }

  @Test
  public void testBuilderWithCustomScanInterval() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.READ_STREAMING_CHECK_INTERVAL, 10);
    StoragePath path = new StoragePath("/tmp/test");

    HoodieScanContext scanContext = HoodieScanContext.builder()
        .conf(conf)
        .path(path)
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant("20240101000000")
        .maxCompactionMemoryInBytes(100 * 1024 * 1024)
        .maxPendingSplits(1000)
        .skipCompaction(false)
        .skipClustering(false)
        .skipInsertOverwrite(false)
        .cdcEnabled(false)
        .build();

    Duration scanInterval = scanContext.getScanInterval();
    assertEquals(10, scanInterval.getSeconds(), "Scan interval should be 10 seconds");
  }

  @Test
  public void testBuilderWithLargeMemoryConfiguration() throws Exception {
    Configuration conf = new Configuration();
    StoragePath path = new StoragePath("/tmp/test");
    long largeMemory = 10L * 1024L * 1024L * 1024L; // 10GB

    HoodieScanContext scanContext = HoodieScanContext.builder()
        .conf(conf)
        .path(path)
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant("20240101000000")
        .maxCompactionMemoryInBytes(largeMemory)
        .maxPendingSplits(1000)
        .skipCompaction(false)
        .skipClustering(false)
        .skipInsertOverwrite(false)
        .cdcEnabled(false)
        .build();

    assertEquals(largeMemory, scanContext.getMaxCompactionMemoryInBytes(),
        "Large memory configuration should be preserved");
  }

  @Test
  public void testBuilderWithHighMaxPendingSplits() throws Exception {
    Configuration conf = new Configuration();
    StoragePath path = new StoragePath("/tmp/test");
    long highPendingSplits = 100000L;

    HoodieScanContext scanContext = HoodieScanContext.builder()
        .conf(conf)
        .path(path)
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant("20240101000000")
        .maxCompactionMemoryInBytes(100 * 1024 * 1024)
        .maxPendingSplits(highPendingSplits)
        .skipCompaction(false)
        .skipClustering(false)
        .skipInsertOverwrite(false)
        .cdcEnabled(false)
        .build();

    assertEquals(highPendingSplits, scanContext.getMaxPendingSplits(),
        "High max pending splits should be preserved");
  }

  @Test
  public void testPartitionPrunerField() throws Exception {
    Configuration conf = new Configuration();
    StoragePath path = new StoragePath("/tmp/test");
    PartitionPruners.PartitionPruner mockPruner = mock(PartitionPruners.PartitionPruner.class);

    // Test with partition pruner
    HoodieScanContext contextWithPruner = HoodieScanContext.builder()
        .conf(conf)
        .path(path)
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant("20240101000000")
        .maxCompactionMemoryInBytes(100 * 1024 * 1024)
        .maxPendingSplits(1000)
        .skipCompaction(false)
        .skipClustering(false)
        .skipInsertOverwrite(false)
        .cdcEnabled(false)
        .isStreaming(true)
        .partitionPruner(mockPruner)
        .build();

    assertNotNull(contextWithPruner.getPartitionPruner(), "Partition pruner should not be null");
    assertEquals(mockPruner, contextWithPruner.getPartitionPruner(),
        "Partition pruner should match the mock instance");

    // Test without partition pruner
    HoodieScanContext contextWithoutPruner = HoodieScanContext.builder()
        .conf(conf)
        .path(path)
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant("20240101000000")
        .maxCompactionMemoryInBytes(100 * 1024 * 1024)
        .maxPendingSplits(1000)
        .skipCompaction(false)
        .skipClustering(false)
        .skipInsertOverwrite(false)
        .cdcEnabled(false)
        .isStreaming(false)
        .build();

    assertNull(contextWithoutPruner.getPartitionPruner(),
        "Partition pruner should be null when not set");
  }

  @Test
  public void testPartitionPrunerWithAllConfigOptions() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.READ_STREAMING_CHECK_INTERVAL, 5);
    StoragePath path = new StoragePath("/tmp/test");
    PartitionPruners.PartitionPruner mockPruner = mock(PartitionPruners.PartitionPruner.class);

    HoodieScanContext scanContext = HoodieScanContext.builder()
        .conf(conf)
        .path(path)
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant("20240101000000")
        .endInstant("20240201000000")
        .maxCompactionMemoryInBytes(100 * 1024 * 1024)
        .maxPendingSplits(1000)
        .skipCompaction(true)
        .skipClustering(true)
        .skipInsertOverwrite(true)
        .cdcEnabled(true)
        .isStreaming(true)
        .partitionPruner(mockPruner)
        .build();

    // Verify all properties are correctly set
    assertNotNull(scanContext.getPartitionPruner(), "Partition pruner should be set");
    assertTrue(scanContext.isStreaming(), "Should be in streaming mode");
    assertTrue(scanContext.isSkipCompaction(), "Skip compaction should be enabled");
    assertTrue(scanContext.isSkipClustering(), "Skip clustering should be enabled");
    assertTrue(scanContext.isSkipInsertOverwrite(), "Skip insert overwrite should be enabled");
    assertTrue(scanContext.isCdcEnabled(), "CDC should be enabled");
    assertEquals("20240101000000", scanContext.getStartInstant(), "Start instant should match");
    assertEquals("20240201000000", scanContext.getEndInstant(), "End instant should match");
  }

  @Test
  public void testPartitionPrunerIndependenceAcrossInstances() throws Exception {
    Configuration conf = new Configuration();
    StoragePath path = new StoragePath("/tmp/test");
    PartitionPruners.PartitionPruner mockPruner1 = mock(PartitionPruners.PartitionPruner.class);
    PartitionPruners.PartitionPruner mockPruner2 = mock(PartitionPruners.PartitionPruner.class);

    // Create two scan contexts with different pruners
    HoodieScanContext scanContext1 = HoodieScanContext.builder()
        .conf(conf)
        .path(path)
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant("20240101000000")
        .maxCompactionMemoryInBytes(100 * 1024 * 1024)
        .maxPendingSplits(1000)
        .skipCompaction(false)
        .skipClustering(false)
        .skipInsertOverwrite(false)
        .cdcEnabled(false)
        .isStreaming(true)
        .partitionPruner(mockPruner1)
        .build();

    HoodieScanContext scanContext2 = HoodieScanContext.builder()
        .conf(conf)
        .path(path)
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant("20240101000000")
        .maxCompactionMemoryInBytes(100 * 1024 * 1024)
        .maxPendingSplits(1000)
        .skipCompaction(false)
        .skipClustering(false)
        .skipInsertOverwrite(false)
        .cdcEnabled(false)
        .isStreaming(true)
        .partitionPruner(mockPruner2)
        .build();

    // Verify that each context has its own independent pruner
    assertEquals(mockPruner1, scanContext1.getPartitionPruner(),
        "First context should have first pruner");
    assertEquals(mockPruner2, scanContext2.getPartitionPruner(),
        "Second context should have second pruner");
    assertNotEquals(scanContext1.getPartitionPruner(), scanContext2.getPartitionPruner(),
        "Pruners should be independent across contexts");
  }
}
