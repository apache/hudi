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

package org.apache.hudi.common.table.timeline.versioning;

import org.apache.hudi.avro.model.HoodieInstantInfo;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPartitionMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.CommitMetadataSerDe;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.v2.CommitMetadataSerDeV2;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.convertMetadataToByteArray;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class BaseTestCommitMetadataSerDe {
  protected static final String TEST_PARTITION_PATH = "2023/01/01";
  protected static final String TEST_FILE_ID = "test-file-id";
  protected static final String TEST_PREV_COMMIT = "000001";
  protected static final String TEST_BASE_FILE = "test-base-file";

  protected String testPath;

  protected abstract CommitMetadataSerDe getSerDe();

  protected abstract HoodieInstant createTestInstant(String action, String id);

  @BeforeEach
  public void setUp() {
    testPath = "file:///path/1/";
  }

  @Test
  protected void testEmptyMetadataSerDe() throws Exception {
    // Create empty commit metadata
    HoodieCommitMetadata emptyMetadata = new HoodieCommitMetadata();

    // Create SerDe instance
    CommitMetadataSerDe serDe = getSerDe();

    // Create test instant
    HoodieInstant instant = createTestInstant("commit", "001");

    // Serialize
    byte[] serialized = convertMetadataToByteArray(emptyMetadata, serDe);

    // Deserialize
    HoodieCommitMetadata deserialized = serDe.deserialize(instant, new ByteArrayInputStream(serialized), () -> false, HoodieCommitMetadata.class);

    // Verify
    assertNotNull(deserialized);
    assertEquals(0, deserialized.getPartitionToWriteStats().size());
    assertEquals(false, deserialized.getCompacted());
    assertEquals(WriteOperationType.UNKNOWN, deserialized.getOperationType());
    assertTrue(deserialized.getExtraMetadata().isEmpty());
  }

  @Test
  protected void testPopulatedMetadataSerDe() throws Exception {
    // Create populated commit metadata
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    HoodieWriteStat writeStat = createTestWriteStat();
    metadata.addWriteStat(TEST_PARTITION_PATH, writeStat);

    // Set other metadata fields
    metadata.setOperationType(WriteOperationType.INSERT);
    metadata.setCompacted(true);
    metadata.addMetadata("test-key", "test-value");

    // Create SerDe instance and test instant
    CommitMetadataSerDe serDe = getSerDe();
    HoodieInstant instant = createTestInstant("commit", "001");

    // Serialize and deserialize
    byte[] serialized = convertMetadataToByteArray(metadata, serDe);
    HoodieCommitMetadata deserialized = serDe.deserialize(instant, new ByteArrayInputStream(serialized), () -> false, HoodieCommitMetadata.class);

    // Verify
    verifyCommitMetadata(deserialized);
    verifyWriteStat(deserialized.getPartitionToWriteStats().get(TEST_PARTITION_PATH).get(0));
  }

  @Test
  protected void testReplaceCommitMetadataSerDe() throws Exception {
    // Create populated replace commit metadata
    HoodieReplaceCommitMetadata metadata = new HoodieReplaceCommitMetadata();
    HoodieWriteStat writeStat = createTestWriteStat();
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

    // Create SerDe instance and test instant
    CommitMetadataSerDe serDe = getSerDe();
    HoodieInstant instant = createTestInstant("replacecommit", "001");

    // Serialize and deserialize
    byte[] serialized = convertMetadataToByteArray(metadata, serDe);
    HoodieReplaceCommitMetadata deserialized = serDe.deserialize(instant, new ByteArrayInputStream(serialized), () -> false, HoodieReplaceCommitMetadata.class);

    // Verify
    verifyReplaceCommitMetadata(deserialized);
    verifyWriteStat(deserialized.getPartitionToWriteStats().get(TEST_PARTITION_PATH).get(0));
    verifyReplaceFileIds(deserialized.getPartitionToReplaceFileIds());
  }

  private StoragePathInfo generateFileStatus(String filePath) {
    return new StoragePathInfo(new StoragePath(filePath), 1, true, (short) 2, 1000000L, 1);
  }

  @Test
  protected void testRollbackMetadataSerDe() throws Exception {
    // Create rollback metadata
    HoodieRollbackMetadata metadata = new HoodieRollbackMetadata();

    // Create rollback stat for partition2 with log files
    Map<StoragePathInfo, Long> commandBlocksCount = new HashMap<>();
    commandBlocksCount.put(generateFileStatus("file:///path/1/partition2/.log.1"), 1L);
    HoodieRollbackPartitionMetadata rollbackPartitionMetadata = new HoodieRollbackPartitionMetadata();
    rollbackPartitionMetadata.setPartitionPath("p1");
    rollbackPartitionMetadata.setSuccessDeleteFiles(Arrays.asList("f1"));
    rollbackPartitionMetadata.setFailedDeleteFiles(new ArrayList<>());
    rollbackPartitionMetadata.setRollbackLogFiles(new HashMap<>());
    Map<String, HoodieRollbackPartitionMetadata> partitionMetadataMap = new HashMap<>();
    partitionMetadataMap.put("p1", rollbackPartitionMetadata);
    metadata.setPartitionMetadata(partitionMetadataMap);

    // Set instant to rollback
    metadata.setInstantsRollback(Collections.singletonList(new HoodieInstantInfo("001", HoodieTimeline.COMMIT_ACTION)));

    // Set other metadata fields
    metadata.setStartRollbackTime("002");
    metadata.setTimeTakenInMillis(100);
    metadata.setTotalFilesDeleted(1);
    metadata.setCommitsRollback(Arrays.asList("111", "222"));

    // Create SerDe instance and test instant
    CommitMetadataSerDe serDe = getSerDe();
    HoodieInstant instant = createTestInstant("rollback", "002");

    // Serialize and deserialize
    byte[] serialized = convertMetadataToByteArray(metadata, serDe);
    HoodieRollbackMetadata deserialized = serDe.deserialize(instant, new ByteArrayInputStream(serialized), () -> false, HoodieRollbackMetadata.class);

    // Verify
    assertNotNull(deserialized);

    // Verify other fields
    assertEquals(Collections.singletonList(new HoodieInstantInfo("001", HoodieTimeline.COMMIT_ACTION)),
        deserialized.getInstantsRollback());
    assertEquals("002", deserialized.getStartRollbackTime());
    assertEquals(100, deserialized.getTimeTakenInMillis());
    assertEquals(1, deserialized.getTotalFilesDeleted());
  }

  @Test
  protected void testEmptyFile() throws Exception {
    // Create SerDe instance and test instant
    CommitMetadataSerDe serDe = getSerDe();
    HoodieInstant instant = createTestInstant("rollback", "002");

    // Serialize and deserialize
    Option<byte[]> serialized = Option.of(new byte[]{});
    HoodieRollbackMetadata deserialized =
        serDe.deserialize(instant, new ByteArrayInputStream(serialized.get()), () -> true,
            HoodieRollbackMetadata.class);

    // Verify
    assertNotNull(deserialized);
    assertNull(deserialized.getStartRollbackTime());
    assertNull(deserialized.getCommitsRollback());
    assertNull(deserialized.getPartitionMetadata());
    assertNull(deserialized.getVersion());
  }

  @Test
  protected void testCorruptedAvroFile() {
    // Create SerDe instance and test instant
    CommitMetadataSerDe serDe = getSerDe();
    HoodieInstant instant = createTestInstant("rollback", "002");

    // Serialize and deserialize
    Option<byte[]> serialized = Option.of(new byte[]{});
    Exception ex = assertThrows(IOException.class, () -> serDe.deserialize(instant, new ByteArrayInputStream(serialized.get()), () -> false, HoodieRollbackMetadata.class));
    assertEquals("Not an Avro data file.", ex.getCause().getMessage());
  }

  @Test
  protected void testCorruptedJsonFile() {
    // Create SerDe instance and test instant
    CommitMetadataSerDe serDe = getSerDe();
    HoodieInstant instant = createTestInstant("commit", "002");

    // Serialize and deserialize
    Option<byte[]> serialized = Option.of(new byte[]{});
    Exception ex = assertThrows(IOException.class, () -> serDe.deserialize(instant, new ByteArrayInputStream(serialized.get()), () -> false, HoodieCommitMetadata.class));

    if (serDe instanceof CommitMetadataSerDeV2) {
      assertEquals("Not an Avro data file.", ex.getCause().getMessage());
    } else {
      assertTrue(ex.getCause().getMessage().startsWith("No content to map due to end-of-input"));
    }
  }

  protected HoodieWriteStat createTestWriteStat() {
    HoodieWriteStat writeStat = new HoodieWriteStat();
    // Set basic fields
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

    // Set CDC stats
    Map<String, Long> cdcStats = new HashMap<>();
    cdcStats.put("cdc-file-1.log", 512L);
    writeStat.setCdcStats(cdcStats);

    // Set runtime stats
    HoodieWriteStat.RuntimeStats runtimeStats = new HoodieWriteStat.RuntimeStats();
    runtimeStats.setTotalScanTime(100);
    runtimeStats.setTotalCreateTime(200);
    runtimeStats.setTotalUpsertTime(300);
    writeStat.setRuntimeStats(runtimeStats);

    // Set new fields
    writeStat.setTotalLogFilesCompacted(5L);
    writeStat.setTotalLogReadTimeMs(150L);
    writeStat.setTotalLogSizeCompacted(1024L);
    writeStat.setTempPath("temp/path/file1");

    return writeStat;
  }

  protected void verifyCommitMetadata(HoodieCommitMetadata metadata) {
    assertNotNull(metadata);
    assertEquals(1, metadata.getPartitionToWriteStats().size());
    assertEquals(true, metadata.getCompacted());
    assertEquals(WriteOperationType.INSERT, metadata.getOperationType());
    assertEquals("test-value", metadata.getExtraMetadata().get("test-key"));
  }

  private void verifyReplaceCommitMetadata(HoodieReplaceCommitMetadata metadata) {
    assertNotNull(metadata);
    assertEquals(true, metadata.getCompacted());
    assertEquals(WriteOperationType.CLUSTER, metadata.getOperationType());
    assertEquals("test-value-1", metadata.getExtraMetadata().get("test-key-1"));
    assertEquals("test-value-2", metadata.getExtraMetadata().get("test-key-2"));
  }

  protected void verifyWriteStat(HoodieWriteStat stat) {
    assertEquals(TEST_FILE_ID, stat.getFileId());
    assertEquals(testPath, stat.getPath());
    assertEquals(TEST_PREV_COMMIT, stat.getPrevCommit());
    assertEquals(100, stat.getNumWrites());
    assertEquals(50, stat.getNumUpdateWrites());
    assertEquals(1024, stat.getTotalWriteBytes());
    assertEquals(0, stat.getTotalWriteErrors());
    assertEquals(TEST_PARTITION_PATH, stat.getPartitionPath());
    assertEquals(2048, stat.getFileSizeInBytes());
    assertEquals(TEST_BASE_FILE, stat.getPrevBaseFile());
    assertEquals(1000L, stat.getMinEventTime());
    assertEquals(2000L, stat.getMaxEventTime());
    assertEquals(512L, stat.getCdcStats().get("cdc-file-1.log"));
    assertNotNull(stat.getRuntimeStats());
    assertEquals(100, stat.getRuntimeStats().getTotalScanTime());
    assertEquals(200, stat.getRuntimeStats().getTotalCreateTime());
    assertEquals(300, stat.getRuntimeStats().getTotalUpsertTime());
    assertEquals(5L, stat.getTotalLogFilesCompacted());
    assertEquals(150L, stat.getTotalLogReadTimeMs());
    assertEquals(1024L, stat.getTotalLogSizeCompacted());
    assertEquals("temp/path/file1", stat.getTempPath());
    assertEquals(0L, stat.getNumUpdates());
  }

  private void verifyReplaceFileIds(Map<String, List<String>> replaceFileIds) {
    assertEquals(2, replaceFileIds.size());
    assertEquals(Arrays.asList("replaced-file-1", "replaced-file-2"),
        replaceFileIds.get(TEST_PARTITION_PATH));
    assertEquals(Collections.singletonList("replaced-file-3"),
        replaceFileIds.get("other-partition"));
  }
}
