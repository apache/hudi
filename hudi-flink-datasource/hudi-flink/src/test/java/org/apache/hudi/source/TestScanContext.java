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
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link ScanContext}.
 */
public class TestScanContext {

  @Test
  public void testGetConf() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.PATH, "/tmp/test");

    ScanContext scanContext = createTestScanContext(conf,  new Path("/tmp/test"),
        TestConfigurations.ROW_TYPE, "20231201000000000", 100 * 1024 * 1024,
        1000, false, false, false, false);

    assertNotNull(scanContext.getConf(), "Configuration should not be null");
    assertEquals("/tmp/test", scanContext.getConf().get(FlinkOptions.PATH),
        "Configuration should match");
  }

  @Test
  public void testGetPath() throws Exception {
    Configuration conf = new Configuration();
    Path expectedPath = new Path("/tmp/test/table");

    ScanContext scanContext = createTestScanContext(conf, expectedPath,
        TestConfigurations.ROW_TYPE, "20231201000000000", 100 * 1024 * 1024,
        1000, false, false, false, false);

    assertEquals(expectedPath, scanContext.getPath(), "Path should match");
  }

  @Test
  public void testGetRowType() throws Exception {
    Configuration conf = new Configuration();
    RowType rowType = TestConfigurations.ROW_TYPE;

    ScanContext scanContext = createTestScanContext(conf, new Path("/tmp/test"),
        rowType, "20231201000000000", 100 * 1024 * 1024,
        1000, false, false, false, false);

    assertNotNull(scanContext.getRowType(), "RowType should not be null");
    assertEquals(rowType, scanContext.getRowType(), "RowType should match");
  }

  @Test
  public void testGetStartInstant() throws Exception {
    Configuration conf = new Configuration();
    String expectedInstant = "20231201000000000";

    ScanContext scanContext = createTestScanContext(conf, new Path("/tmp/test"),
        TestConfigurations.ROW_TYPE, expectedInstant, 100 * 1024 * 1024,
        1000, false, false, false, false);

    assertEquals(expectedInstant, scanContext.getStartCommit(),
        "Start instant should match");
  }

  @Test
  public void testGetMaxCompactionMemoryInBytes() throws Exception {
    Configuration conf = new Configuration();
    long expectedMemory = 1024L * 1024L * 1024L; // 1GB

    ScanContext scanContext = createTestScanContext(conf, new Path("/tmp/test"),
        TestConfigurations.ROW_TYPE, "20231201000000000", expectedMemory,
        1000, false, false, false, false);

    assertEquals(expectedMemory, scanContext.getMaxCompactionMemoryInBytes(),
        "Max compaction memory should match");
  }

  @Test
  public void testGetMaxPendingSplits() throws Exception {
    Configuration conf = new Configuration();
    long expectedMaxPendingSplits = 5000L;

    ScanContext scanContext = createTestScanContext(conf, new Path("/tmp/test"),
        TestConfigurations.ROW_TYPE, "20231201000000000", 100 * 1024 * 1024,
        expectedMaxPendingSplits, false, false, false, false);

    assertEquals(expectedMaxPendingSplits, scanContext.getMaxPendingSplits(),
        "Max pending splits should match");
  }

  @Test
  public void testSkipCompaction() throws Exception {
    Configuration conf = new Configuration();

    ScanContext scanContextTrue = createTestScanContext(conf, new Path("/tmp/test"),
        TestConfigurations.ROW_TYPE, "20231201000000000", 100 * 1024 * 1024,
        1000, true, false, false, false);
    assertTrue(scanContextTrue.skipCompaction(), "Skip compaction should be true");

    ScanContext scanContextFalse = createTestScanContext(conf, new Path("/tmp/test"),
        TestConfigurations.ROW_TYPE, "20231201000000000", 100 * 1024 * 1024,
        1000, false, false, false, false);
    assertFalse(scanContextFalse.skipCompaction(), "Skip compaction should be false");
  }

  @Test
  public void testSkipClustering() throws Exception {
    Configuration conf = new Configuration();

    ScanContext scanContextTrue = createTestScanContext(conf, new Path("/tmp/test"),
        TestConfigurations.ROW_TYPE, "20231201000000000", 100 * 1024 * 1024,
        1000, false, true, false, false);
    assertTrue(scanContextTrue.skipClustering(), "Skip clustering should be true");

    ScanContext scanContextFalse = createTestScanContext(conf, new Path("/tmp/test"),
        TestConfigurations.ROW_TYPE, "20231201000000000", 100 * 1024 * 1024,
        1000, false, false, false, false);
    assertFalse(scanContextFalse.skipClustering(), "Skip clustering should be false");
  }

  @Test
  public void testSkipInsertOverwrite() throws Exception {
    Configuration conf = new Configuration();

    ScanContext scanContextTrue = createTestScanContext(conf, new Path("/tmp/test"),
        TestConfigurations.ROW_TYPE, "20231201000000000", 100 * 1024 * 1024,
        1000, false, false, true, false);
    assertTrue(scanContextTrue.skipInsertOverwrite(), "Skip insert overwrite should be true");

    ScanContext scanContextFalse = createTestScanContext(conf, new Path("/tmp/test"),
        TestConfigurations.ROW_TYPE, "20231201000000000", 100 * 1024 * 1024,
        1000, false, false, false, false);
    assertFalse(scanContextFalse.skipInsertOverwrite(), "Skip insert overwrite should be false");
  }

  @Test
  public void testCdcEnabled() throws Exception {
    Configuration conf = new Configuration();

    ScanContext scanContextTrue = createTestScanContext(conf, new Path("/tmp/test"),
        TestConfigurations.ROW_TYPE, "20231201000000000", 100 * 1024 * 1024,
        1000, false, false, false, true);
    assertTrue(scanContextTrue.cdcEnabled(), "CDC should be enabled");

    ScanContext scanContextFalse = createTestScanContext(conf, new Path("/tmp/test"),
        TestConfigurations.ROW_TYPE, "20231201000000000", 100 * 1024 * 1024,
        1000, false, false, false, false);
    assertFalse(scanContextFalse.cdcEnabled(), "CDC should be disabled");
  }

  @Test
  public void testGetScanInterval() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.READ_STREAMING_CHECK_INTERVAL, 5);

    ScanContext scanContext = createTestScanContext(conf, new Path("/tmp/test"),
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

    ScanContext scanContext = createTestScanContext(conf, new Path("/tmp/test"),
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
    Path path = new Path("/tmp/hoodie/table");
    String startInstant = "20231215120000000";
    long maxCompactionMemory = 2L * 1024L * 1024L * 1024L; // 2GB
    long maxPendingSplits = 10000L;

    ScanContext scanContext = createTestScanContext(conf, path,
        TestConfigurations.ROW_TYPE, startInstant, maxCompactionMemory,
        maxPendingSplits, true, true, true, true);

    assertEquals(path, scanContext.getPath());
    assertEquals(startInstant, scanContext.getStartCommit());
    assertEquals(maxCompactionMemory, scanContext.getMaxCompactionMemoryInBytes());
    assertEquals(maxPendingSplits, scanContext.getMaxPendingSplits());
    assertTrue(scanContext.skipCompaction());
    assertTrue(scanContext.skipClustering());
    assertTrue(scanContext.skipInsertOverwrite());
    assertTrue(scanContext.cdcEnabled());
    assertEquals(Duration.ofSeconds(10), scanContext.getScanInterval());
  }

  // Helper method to create ScanContext using the Builder
  private ScanContext createTestScanContext(
      Configuration conf,
      Path path,
      RowType rowType,
      String startInstant,
      long maxCompactionMemoryInBytes,
      long maxPendingSplits,
      boolean skipCompaction,
      boolean skipClustering,
      boolean skipInsertOverwrite,
      boolean cdcEnabled) throws Exception {
    return new ScanContext.Builder()
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
}
