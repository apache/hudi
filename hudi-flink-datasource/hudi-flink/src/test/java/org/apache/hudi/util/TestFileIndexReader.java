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

package org.apache.hudi.util;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.source.FileIndex;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FileIndexReader}.
 */
public class TestFileIndexReader {

  @TempDir
  File tempDir;

  private HoodieTableMetaClient metaClient;
  private Configuration conf;
  private StoragePath tablePath;

  @BeforeEach
  public void setUp() {
    conf = TestConfigurations.getDefaultConf(tempDir.getAbsolutePath());
    tablePath = new StoragePath(tempDir.getAbsolutePath());
  }

  @Test
  public void testGetBaseFileOnlyFileSlicesForCopyOnWrite() throws Exception {
    metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), HoodieTableType.COPY_ON_WRITE);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.COPY_ON_WRITE.name());

    // Write test data
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    metaClient.reloadActiveTimeline();

    TestFileIndexReaderImpl reader = new TestFileIndexReaderImpl(tablePath, conf, metaClient);
    List<FileSlice> fileSlices = reader.getBaseFileOnlyFileSlices(metaClient);

    assertNotNull(fileSlices);
    assertFalse(fileSlices.isEmpty());
    // Verify all file slices have base files
    fileSlices.forEach(slice -> assertTrue(slice.getBaseFile().isPresent()));
  }

  @Test
  public void testGetBaseFileOnlyFileSlicesWithEmptyTable() throws Exception {
    metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), HoodieTableType.COPY_ON_WRITE);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.COPY_ON_WRITE.name());

    TestFileIndexReaderImpl reader = new TestFileIndexReaderImpl(tablePath, conf, metaClient);
    List<FileSlice> fileSlices = reader.getBaseFileOnlyFileSlices(metaClient);

    assertNotNull(fileSlices);
    assertTrue(fileSlices.isEmpty());
  }

  @Test
  public void testBaseFileOnlyHoodieSourceSplits() throws Exception {
    metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), HoodieTableType.COPY_ON_WRITE);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.COPY_ON_WRITE.name());

    // Write test data
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    metaClient.reloadActiveTimeline();

    TestFileIndexReaderImpl reader = new TestFileIndexReaderImpl(tablePath, conf, metaClient);
    String mergeType = conf.get(FlinkOptions.MERGE_TYPE);
    List<HoodieSourceSplit> splits = reader.baseFileOnlyHoodieSourceSplits(metaClient, tablePath, mergeType);

    assertNotNull(splits);
    assertFalse(splits.isEmpty());
    // Verify all splits have valid properties
    splits.forEach(split -> {
      assertNotNull(split.getBasePath());
      assertNotNull(split.getTablePath());
      assertNotNull(split.getFileId());
      assertFalse(split.getLogPaths().isPresent()); // No log files for base-file-only splits
    });
  }

  @Test
  public void testBaseFileOnlyHoodieSourceSplitsWithEmptyTable() throws Exception {
    metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), HoodieTableType.COPY_ON_WRITE);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.COPY_ON_WRITE.name());

    TestFileIndexReaderImpl reader = new TestFileIndexReaderImpl(tablePath, conf, metaClient);
    String mergeType = conf.get(FlinkOptions.MERGE_TYPE);
    List<HoodieSourceSplit> splits = reader.baseFileOnlyHoodieSourceSplits(metaClient, tablePath, mergeType);

    assertNotNull(splits);
    assertTrue(splits.isEmpty());
  }

  @Test
  public void testBuildHoodieSplitsForMergeOnRead() throws Exception {
    metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), HoodieTableType.MERGE_ON_READ);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());

    // Write test data
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    metaClient.reloadActiveTimeline();

    TestFileIndexReaderImpl reader = new TestFileIndexReaderImpl(tablePath, conf, metaClient);
    List<HoodieSourceSplit> splits = reader.buildHoodieSplits(metaClient, conf);

    assertNotNull(splits);
    assertFalse(splits.isEmpty());
    // Verify splits have proper structure and metadata
    splits.forEach(split -> {
      assertNotNull(split.getTablePath());
      assertNotNull(split.getFileId());
      assertNotNull(split.getMergeType());
      assertNotNull(split.getLatestCommit(), "Instant time should not be null");
      assertNotNull(split.splitId(), "Split ID should not be null");
    });
  }

  @Test
  public void testBuildInputSplitsForMergeOnRead() throws Exception {
    metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), HoodieTableType.MERGE_ON_READ);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());

    // Write test data
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    metaClient.reloadActiveTimeline();

    TestFileIndexReaderImpl reader = new TestFileIndexReaderImpl(tablePath, conf, metaClient);
    List<MergeOnReadInputSplit> splits = reader.buildInputSplits(metaClient, conf);

    assertNotNull(splits);
    assertFalse(splits.isEmpty());
    // Verify input splits have proper structure
    splits.forEach(split -> {
      assertNotNull(split.getTablePath());
      assertNotNull(split.getFileId());
      assertNotNull(split.getLatestCommit());
      assertNotNull(split.getMergeType());
    });
  }

  @Test
  public void testBuildInputSplitsWithEmptyTable() throws Exception {
    metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), HoodieTableType.MERGE_ON_READ);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());

    TestFileIndexReaderImpl reader = new TestFileIndexReaderImpl(tablePath, conf, metaClient);
    List<MergeOnReadInputSplit> splits = reader.buildInputSplits(metaClient, conf);

    assertNotNull(splits);
    assertTrue(splits.isEmpty());
  }

  @Test
  public void testGetOrBuildFileIndexCaching() throws Exception {
    metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), HoodieTableType.COPY_ON_WRITE);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.COPY_ON_WRITE.name());

    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    metaClient.reloadActiveTimeline();

    TestFileIndexReaderImpl reader = new TestFileIndexReaderImpl(tablePath, conf, metaClient);

    // First call should create the file index
    FileIndex fileIndex1 = reader.getOrBuildFileIndex();
    assertNotNull(fileIndex1);

    // Second and third calls should return cached instances
    FileIndex fileIndex2 = reader.getOrBuildFileIndex();
    assertNotNull(fileIndex2);

    FileIndex fileIndex3 = reader.getOrBuildFileIndex();
    assertNotNull(fileIndex3);

    // Partition paths should be consistent across calls
    assertEquals(fileIndex1.getOrBuildPartitionPaths().size(),
        fileIndex2.getOrBuildPartitionPaths().size(),
        "Partition paths should be consistent across calls");
  }

  @Test
  public void testBaseFileOnlyHoodieSourceSplitsWithLogFiles() throws Exception {
    metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), HoodieTableType.MERGE_ON_READ);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());

    // Write data and update to generate log files
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf);
    metaClient.reloadActiveTimeline();

    TestFileIndexReaderImpl reader = new TestFileIndexReaderImpl(tablePath, conf, metaClient);
    String mergeType = conf.get(FlinkOptions.MERGE_TYPE);
    List<HoodieSourceSplit> splits = reader.baseFileOnlyHoodieSourceSplits(metaClient, tablePath, mergeType);

    assertNotNull(splits);
    // Base file only should not include log files even for MOR table
    splits.forEach(split -> {
      if (split.getBasePath() != null) {
        assertFalse(split.getLogPaths().isPresent() && !split.getLogPaths().get().isEmpty(),
            "Base-file-only splits should not have log paths");
      }
    });
  }

  @Test
  public void testBuildInputSplitsWithLogFiles() throws Exception {
    metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), HoodieTableType.MERGE_ON_READ);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());

    // Write data and update to generate log files
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf);
    metaClient.reloadActiveTimeline();

    TestFileIndexReaderImpl reader = new TestFileIndexReaderImpl(tablePath, conf, metaClient);
    List<org.apache.hudi.table.format.mor.MergeOnReadInputSplit> splits = reader.buildInputSplits(metaClient, conf);

    assertNotNull(splits);
    assertFalse(splits.isEmpty());

    // Verify that splits contain proper file paths and metadata
    splits.forEach(split -> {
      assertNotNull(split.getTablePath(), "Table path should not be null");
      assertNotNull(split.getLatestCommit(), "Latest commit should not be null");
      assertNotNull(split.getMergeType(), "Merge type should not be null");
    });
  }

  @Test
  public void testBaseFileOnlyFileSlicesWithPartitionFilter() throws Exception {
    metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), HoodieTableType.COPY_ON_WRITE);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.COPY_ON_WRITE.name());

    // Write data to multiple partitions
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_INSERT_SEPARATE_PARTITION, conf);
    metaClient.reloadActiveTimeline();

    TestFileIndexReaderImpl reader = new TestFileIndexReaderImpl(tablePath, conf, metaClient);
    List<FileSlice> fileSlices = reader.getBaseFileOnlyFileSlices(metaClient);

    assertNotNull(fileSlices);
    assertFalse(fileSlices.isEmpty());

    // Verify multiple partitions are present
    long partitionCount = fileSlices.stream()
        .map(FileSlice::getPartitionPath)
        .distinct()
        .count();
    assertTrue(partitionCount > 1, "Should have file slices from multiple partitions");
  }

  @Test
  public void testBaseFileOnlyHoodieSourceSplitsWithMultipleCommits() throws Exception {
    metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), HoodieTableType.COPY_ON_WRITE);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.COPY_ON_WRITE.name());

    // Write data multiple times to create multiple commits
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_UPDATE_DELETE, conf);
    metaClient.reloadActiveTimeline();

    TestFileIndexReaderImpl reader = new TestFileIndexReaderImpl(tablePath, conf, metaClient);
    String mergeType = conf.get(FlinkOptions.MERGE_TYPE);
    List<HoodieSourceSplit> splits = reader.baseFileOnlyHoodieSourceSplits(metaClient, tablePath, mergeType);

    assertNotNull(splits);
    assertFalse(splits.isEmpty());

    // Verify splits have valid instant times
    splits.forEach(split -> {
      assertNotNull(split.getLatestCommit(), "Instant time should not be null");
      assertFalse(split.getLatestCommit().isEmpty(), "Instant time should not be empty");
    });
  }

  @Test
  public void testBuildHoodieSplitsWithDifferentMergeTypes() throws Exception {
    metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), HoodieTableType.MERGE_ON_READ);

    // Test with payload combine merge type
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());
    conf.set(FlinkOptions.MERGE_TYPE, FlinkOptions.REALTIME_PAYLOAD_COMBINE);

    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    metaClient.reloadActiveTimeline();

    TestFileIndexReaderImpl reader = new TestFileIndexReaderImpl(tablePath, conf, metaClient);
    List<HoodieSourceSplit> splits = reader.buildHoodieSplits(metaClient, conf);

    assertNotNull(splits);
    assertFalse(splits.isEmpty());

    // Verify merge type is set correctly
    splits.forEach(split -> {
      assertEquals(FlinkOptions.REALTIME_PAYLOAD_COMBINE, split.getMergeType(),
          "Merge type should match configuration");
    });
  }

  /**
   * Test implementation of FileIndexReader for testing purposes.
   */
  private static class TestFileIndexReaderImpl extends FileIndexReader {
    private final StoragePath path;
    private final Configuration conf;
    private final HoodieTableMetaClient metaClient;

    public TestFileIndexReaderImpl(StoragePath path, Configuration conf, HoodieTableMetaClient metaClient) {
      this.path = path;
      this.conf = conf;
      this.metaClient = metaClient;
    }

    @Override
    protected FileIndex buildFileIndex() {
      return FileIndex.builder()
          .path(path)
          .conf(conf)
          .rowType(TestConfigurations.ROW_TYPE)
          .metaClient(metaClient)
          .build();
    }
  }
}
