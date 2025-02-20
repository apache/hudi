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

package org.apache.hudi.common.table.timeline.versioning.v2;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCommitMetadataSerDeV2 {

  private static final String TEST_PARTITION_PATH = "2023/01/01";
  private static final String TEST_FILE_ID = "test-file-id";
  private static final String TEST_PREV_COMMIT = "000001";
  private static final String TEST_BASE_FILE = "test-base-file";

  private String testPath;

  @BeforeEach
  public void setUp() {
    testPath = "file:///path/1/";
  }

  @Test
  public void testEmptyMetadataSerDe() throws Exception {
    // Create empty commit metadata
    HoodieCommitMetadata emptyMetadata = new HoodieCommitMetadata();

    // Create SerDe instance
    CommitMetadataSerDeV2 serDe = new CommitMetadataSerDeV2();

    // Create test instant
    HoodieInstant instant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, "commit", "001");

    // Serialize
    Option<byte[]> serialized = serDe.serialize(emptyMetadata);
    assertTrue(serialized.isPresent());

    // Deserialize
    HoodieCommitMetadata deserialized = serDe.deserialize(instant, Option.of(new ByteArrayInputStream(serialized.get())), () -> true, HoodieCommitMetadata.class);
    
    // Verify
    assertNotNull(deserialized);
    assertEquals(0, deserialized.getPartitionToWriteStats().size());
    assertEquals(false, deserialized.getCompacted());
    assertEquals(WriteOperationType.UNKNOWN, deserialized.getOperationType());
    assertTrue(deserialized.getExtraMetadata().isEmpty());
  }

  @Test
  public void testPopulatedMetadataSerDe() throws Exception {
    // Create populated commit metadata
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();

    // Add write stats
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId(TEST_FILE_ID);
    writeStat.setPath(testPath);
    writeStat.setPrevCommit(TEST_PREV_COMMIT);
    writeStat.setNumWrites(100);
    writeStat.setNumUpdateWrites(50);
    writeStat.setTotalWriteBytes(1024);
    writeStat.setTotalWriteErrors(0);
    writeStat.setPartitionPath(TEST_PARTITION_PATH);
    writeStat.setFileSizeInBytes(2048);
    writeStat.setPrevBaseFile(TEST_BASE_FILE);

    // Set event times
    writeStat.setMinEventTime(1000L);
    writeStat.setMaxEventTime(2000L);

    // Add new field values
    writeStat.setTotalLogFilesCompacted(5L);
    writeStat.setTotalLogReadTimeMs(150L);
    writeStat.setTotalLogSizeCompacted(1024L);
    writeStat.setTempPath("temp/path/file1");

    // Add CDC stats
    Map<String, Long> cdcStats = new HashMap<>();
    cdcStats.put("cdc-file-1.log", 512L);
    writeStat.setCdcStats(cdcStats);

    // Add runtime stats
    HoodieWriteStat.RuntimeStats runtimeStats = new HoodieWriteStat.RuntimeStats();
    runtimeStats.setTotalScanTime(100);
    runtimeStats.setTotalCreateTime(200);
    runtimeStats.setTotalUpsertTime(300);
    writeStat.setRuntimeStats(runtimeStats);

    metadata.addWriteStat(TEST_PARTITION_PATH, writeStat);

    // Set other metadata fields
    metadata.setOperationType(WriteOperationType.INSERT);
    metadata.setCompacted(true);
    metadata.addMetadata("test-key", "test-value");

    // Create SerDe instance
    CommitMetadataSerDeV2 serDe = new CommitMetadataSerDeV2();

    // Create test instant
    HoodieInstant instant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, "commit", "001");

    // Serialize
    Option<byte[]> serialized = serDe.serialize(metadata);
    assertTrue(serialized.isPresent());

    // Deserialize
    HoodieCommitMetadata deserialized = serDe.deserialize(
        instant, Option.of(new ByteArrayInputStream(serialized.get())), () -> true, HoodieCommitMetadata.class);
    
    // Verify all fields
    assertNotNull(deserialized);
    assertEquals(1, deserialized.getPartitionToWriteStats().size());
    assertEquals(true, deserialized.getCompacted());
    assertEquals(WriteOperationType.INSERT, deserialized.getOperationType());
    assertEquals("test-value", deserialized.getExtraMetadata().get("test-key"));

    // Verify write stats
    HoodieWriteStat deserializedStat = deserialized.getPartitionToWriteStats().get(TEST_PARTITION_PATH).get(0);
    assertEquals(TEST_FILE_ID, deserializedStat.getFileId());
    assertEquals(testPath, deserializedStat.getPath());
    assertEquals(TEST_PREV_COMMIT, deserializedStat.getPrevCommit());
    assertEquals(100, deserializedStat.getNumWrites());
    assertEquals(50, deserializedStat.getNumUpdateWrites());
    assertEquals(1024, deserializedStat.getTotalWriteBytes());
    assertEquals(0, deserializedStat.getTotalWriteErrors());
    assertEquals(TEST_PARTITION_PATH, deserializedStat.getPartitionPath());
    assertEquals(2048, deserializedStat.getFileSizeInBytes());
    assertEquals(TEST_BASE_FILE, deserializedStat.getPrevBaseFile());
    assertEquals(1000L, deserializedStat.getMinEventTime());
    assertEquals(2000L, deserializedStat.getMaxEventTime());

    // Verify CDC stats
    assertEquals(512L, deserializedStat.getCdcStats().get("cdc-file-1.log"));

    // Verify runtime stats
    assertNotNull(deserializedStat.getRuntimeStats());
    assertEquals(100, deserializedStat.getRuntimeStats().getTotalScanTime());
    assertEquals(200, deserializedStat.getRuntimeStats().getTotalCreateTime());
    assertEquals(300, deserializedStat.getRuntimeStats().getTotalUpsertTime());

    // Verify new fields
    assertEquals(5L, deserializedStat.getTotalLogFilesCompacted());
    assertEquals(150L, deserializedStat.getTotalLogReadTimeMs());
    assertEquals(1024L, deserializedStat.getTotalLogSizeCompacted());
    assertEquals("temp/path/file1", deserializedStat.getTempPath());
    assertEquals(0L, deserializedStat.getNumUpdates());
  }

  @Test
  public void testReplaceCommitMetadataSerDe() throws Exception {
    // Create populated replace commit metadata
    HoodieReplaceCommitMetadata metadata = new HoodieReplaceCommitMetadata();

    // Add write stats
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId(TEST_FILE_ID);
    writeStat.setPath(testPath);
    writeStat.setPrevCommit(TEST_PREV_COMMIT);
    writeStat.setNumWrites(100);
    writeStat.setNumUpdateWrites(50);
    writeStat.setTotalWriteBytes(1024);
    writeStat.setTotalWriteErrors(0);
    writeStat.setPartitionPath(TEST_PARTITION_PATH);
    writeStat.setFileSizeInBytes(2048);
    writeStat.setPrevBaseFile(TEST_BASE_FILE);
    writeStat.setMinEventTime(1000L);
    writeStat.setMaxEventTime(2000L);

    // Add new field values
    writeStat.setTotalLogFilesCompacted(5L);
    writeStat.setTotalLogReadTimeMs(150L);
    writeStat.setTotalLogSizeCompacted(1024L);
    writeStat.setTempPath("temp/path/file1");

    // Add CDC stats
    Map<String, Long> cdcStats = new HashMap<>();
    cdcStats.put("cdc-file-1.log", 512L);
    writeStat.setCdcStats(cdcStats);

    // Add runtime stats
    HoodieWriteStat.RuntimeStats runtimeStats = new HoodieWriteStat.RuntimeStats();
    runtimeStats.setTotalScanTime(100);
    runtimeStats.setTotalCreateTime(200);
    runtimeStats.setTotalUpsertTime(300);
    writeStat.setRuntimeStats(runtimeStats);

    metadata.addWriteStat(TEST_PARTITION_PATH, writeStat);

    // Add replace file IDs
    metadata.addReplaceFileId(TEST_PARTITION_PATH, "replaced-file-1");
    metadata.addReplaceFileId(TEST_PARTITION_PATH, "replaced-file-2");
    metadata.addReplaceFileId("other-partition", "replaced-file-3");

    // Set other metadata fields
    metadata.setOperationType(WriteOperationType.CLUSTER);
    metadata.setCompacted(true);
    metadata.addMetadata("test-key-1", "test-value-1");
    metadata.addMetadata("test-key-2", "test-value-2");

    // Create SerDe instance
    CommitMetadataSerDeV2 serDe = new CommitMetadataSerDeV2();

    // Create test instant
    HoodieInstant instant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, "replacecommit", "001");

    // Serialize
    Option<byte[]> serialized = serDe.serialize(metadata);
    assertTrue(serialized.isPresent());

    // Deserialize
    HoodieReplaceCommitMetadata deserialized = serDe.deserialize(instant, Option.of(new ByteArrayInputStream(serialized.get())), () -> true, HoodieReplaceCommitMetadata.class);
    
    // Verify basic fields
    assertNotNull(deserialized);
    assertEquals(true, deserialized.getCompacted());
    assertEquals(WriteOperationType.CLUSTER, deserialized.getOperationType());
    assertEquals("test-value-1", deserialized.getExtraMetadata().get("test-key-1"));
    assertEquals("test-value-2", deserialized.getExtraMetadata().get("test-key-2"));

    // Verify write stats
    assertEquals(1, deserialized.getPartitionToWriteStats().size());
    HoodieWriteStat deserializedStat = deserialized.getPartitionToWriteStats().get(TEST_PARTITION_PATH).get(0);
    assertEquals(TEST_FILE_ID, deserializedStat.getFileId());
    assertEquals(testPath, deserializedStat.getPath());
    assertEquals(TEST_PREV_COMMIT, deserializedStat.getPrevCommit());
    assertEquals(100, deserializedStat.getNumWrites());
    assertEquals(50, deserializedStat.getNumUpdateWrites());
    assertEquals(1024, deserializedStat.getTotalWriteBytes());
    assertEquals(0, deserializedStat.getTotalWriteErrors());
    assertEquals(TEST_PARTITION_PATH, deserializedStat.getPartitionPath());
    assertEquals(2048, deserializedStat.getFileSizeInBytes());
    assertEquals(TEST_BASE_FILE, deserializedStat.getPrevBaseFile());
    assertEquals(1000L, deserializedStat.getMinEventTime());
    assertEquals(2000L, deserializedStat.getMaxEventTime());

    // Verify CDC stats
    assertEquals(512L, deserializedStat.getCdcStats().get("cdc-file-1.log"));

    // Verify runtime stats
    assertNotNull(deserializedStat.getRuntimeStats());
    assertEquals(100, deserializedStat.getRuntimeStats().getTotalScanTime());
    assertEquals(200, deserializedStat.getRuntimeStats().getTotalCreateTime());
    assertEquals(300, deserializedStat.getRuntimeStats().getTotalUpsertTime());

    // Verify replace file IDs
    Map<String, List<String>> replaceFileIds = deserialized.getPartitionToReplaceFileIds();
    assertEquals(2, replaceFileIds.size());
    assertEquals(Arrays.asList("replaced-file-1", "replaced-file-2"), replaceFileIds.get(TEST_PARTITION_PATH));
    assertEquals(Collections.singletonList("replaced-file-3"), replaceFileIds.get("other-partition"));

    // Verify new fields
    assertEquals(5L, deserializedStat.getTotalLogFilesCompacted());
    assertEquals(150L, deserializedStat.getTotalLogReadTimeMs());
    assertEquals(1024L, deserializedStat.getTotalLogSizeCompacted());
    assertEquals("temp/path/file1", deserializedStat.getTempPath());
    assertEquals(0L, deserializedStat.getNumUpdates());
  }
}
