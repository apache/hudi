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

package org.apache.hudi.index.bucket;

import org.apache.hudi.common.model.ConsistentHashingNode;
import org.apache.hudi.common.model.HoodieConsistentHashingMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.TableFileSystemView.BaseFileOnlyView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.hudi.common.model.HoodieConsistentHashingMetadata.HASHING_METADATA_COMMIT_FILE_SUFFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestConsistentBucketIndexUtils {

  private HoodieTable table;
  private HoodieTableMetaClient metaClient;
  private HoodieStorage storage;
  private StoragePath hashingMetadataPath;

  @BeforeEach
  void setUp() {
    table = mock(HoodieTable.class);
    metaClient = mock(HoodieTableMetaClient.class);
    storage = mock(HoodieStorage.class);
    hashingMetadataPath = new StoragePath("/table/.hoodie/.hashing_metadata");
    when(table.getMetaClient()).thenReturn(metaClient);
    when(table.getStorage()).thenReturn(storage);
    when(metaClient.getStorage()).thenReturn(storage);
    when(metaClient.getHashingMetadataPath()).thenReturn(hashingMetadataPath.toString());
  }

  @Test
  void testLoadMetadataReturnsEmptyForMissingPartitionAndWrapsIoFailure() throws Exception {
    StoragePath partitionPath = new StoragePath(hashingMetadataPath, "p");
    doThrow(new FileNotFoundException("missing"))
        .doThrow(new IOException("storage failure"))
        .when(storage).listDirectEntries(partitionPath);
    assertFalse(ConsistentBucketIndexUtils.loadMetadata(table, "p").isPresent());

    assertThrows(HoodieIndexException.class, () -> ConsistentBucketIndexUtils.loadMetadata(table, "p"));
  }

  @Test
  void testLoadMetadataReadsInitialAndLatestCommittedFiles() throws Exception {
    HoodieConsistentHashingMetadata initial = new HoodieConsistentHashingMetadata("p", 4);
    StoragePath partitionPath = new StoragePath(hashingMetadataPath, "p");
    StoragePathInfo initialFile = file(partitionPath, initial.getFilename());
    when(storage.listDirectEntries(partitionPath)).thenReturn(Collections.singletonList(initialFile));
    when(storage.open(initialFile.getPath())).thenReturn(new ByteArrayInputStream(initial.toBytes()));

    Option<HoodieConsistentHashingMetadata> loaded = ConsistentBucketIndexUtils.loadMetadata(table, "p");
    assertTrue(loaded.isPresent());
    assertEquals(initial.getFilename(), loaded.get().getFilename());
    assertEquals(4, loaded.get().getNumBuckets());

    HoodieConsistentHashingMetadata updated = new HoodieConsistentHashingMetadata(
        (short) 0, "p", "002", 0, 0, Collections.emptyList());
    StoragePathInfo updatedFile = file(partitionPath, updated.getFilename());
    StoragePathInfo commitMarker = file(partitionPath, "002" + HASHING_METADATA_COMMIT_FILE_SUFFIX);
    when(storage.listDirectEntries(partitionPath)).thenReturn(Arrays.asList(initialFile, updatedFile, commitMarker));
    when(storage.open(updatedFile.getPath())).thenReturn(new ByteArrayInputStream(updated.toBytes()));

    loaded = ConsistentBucketIndexUtils.loadMetadata(table, "p");
    assertTrue(loaded.isPresent());
    assertEquals("002", loaded.get().getInstant());
  }

  @Test
  void testSaveMetadataHandlesExistingConcurrentAndFailedWrites() throws Exception {
    HoodieConsistentHashingMetadata metadata = new HoodieConsistentHashingMetadata("p", 4);
    StoragePath fullPath = new StoragePath(new StoragePath(hashingMetadataPath, "p"), metadata.getFilename());
    when(storage.exists(fullPath)).thenReturn(true);
    assertTrue(ConsistentBucketIndexUtils.saveMetadata(table, metadata));

    when(storage.exists(fullPath)).thenReturn(false);
    assertTrue(ConsistentBucketIndexUtils.saveMetadata(table, metadata));
    verify(storage).createImmutableFileInPath(eq(fullPath), any(Option.class), eq(true));

    doThrow(new IOException("concurrent create")).doReturn(true).when(storage).exists(fullPath);
    assertTrue(ConsistentBucketIndexUtils.saveMetadata(table, metadata));

    doThrow(new IOException("failed create")).doReturn(false).when(storage).exists(fullPath);
    assertFalse(ConsistentBucketIndexUtils.saveMetadata(table, metadata));
  }

  @Test
  void testLoadOrCreatePersistsNewMetadata() throws Exception {
    StoragePath partitionPath = new StoragePath(hashingMetadataPath, "p");
    when(storage.listDirectEntries(partitionPath)).thenThrow(new FileNotFoundException("missing"));
    when(storage.exists(any(StoragePath.class))).thenReturn(false);

    HoodieConsistentHashingMetadata metadata = ConsistentBucketIndexUtils.loadOrCreateMetadata(table, "p", 6);

    assertEquals("p", metadata.getPartitionPath());
    assertEquals(6, metadata.getNumBuckets());
    verify(storage).createImmutableFileInPath(any(StoragePath.class), any(Option.class), eq(true));
  }

  @Test
  void testLoadMetadataReturnsEmptyWhenChosenFileDisappears() throws Exception {
    HoodieConsistentHashingMetadata initial = new HoodieConsistentHashingMetadata("p", 4);
    StoragePath partitionPath = new StoragePath(hashingMetadataPath, "p");
    StoragePathInfo initialFile = file(partitionPath, initial.getFilename());
    when(storage.listDirectEntries(partitionPath)).thenReturn(Collections.singletonList(initialFile));
    when(storage.open(initialFile.getPath())).thenThrow(new FileNotFoundException("raced with cleaner"));

    assertFalse(ConsistentBucketIndexUtils.loadMetadata(table, "p").isPresent());
  }

  @Test
  void testLoadMetadataRepairsCommitMarkerForCompletedRehash() throws Exception {
    HoodieConsistentHashingMetadata updated = new HoodieConsistentHashingMetadata(
        (short) 0, "p", "002", 0, 0, Collections.emptyList());
    StoragePath partitionPath = new StoragePath(hashingMetadataPath, "p");
    StoragePathInfo updatedFile = file(partitionPath, updated.getFilename());
    StoragePath markerPath = new StoragePath(partitionPath, "002" + HASHING_METADATA_COMMIT_FILE_SUFFIX);
    when(storage.listDirectEntries(partitionPath)).thenReturn(Collections.singletonList(updatedFile));
    when(storage.open(updatedFile.getPath())).thenReturn(new ByteArrayInputStream(updated.toBytes()));
    when(storage.exists(markerPath)).thenReturn(false);
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);
    HoodieTimeline completedTimeline = mock(HoodieTimeline.class);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(activeTimeline.getCommitAndReplaceTimeline()).thenReturn(completedTimeline);
    when(completedTimeline.filterCompletedInstants()).thenReturn(completedTimeline);
    when(completedTimeline.containsInstant("002")).thenReturn(true);

    try (MockedStatic<FileIOUtils> fileIo = mockStatic(FileIOUtils.class, Mockito.CALLS_REAL_METHODS)) {
      Option<HoodieConsistentHashingMetadata> loaded = ConsistentBucketIndexUtils.loadMetadata(table, "p");

      assertTrue(loaded.isPresent());
      assertEquals("002", loaded.get().getInstant());
      fileIo.verify(() -> FileIOUtils.createFileInPath(storage, markerPath, Option.empty()));
    }
  }

  @Test
  void testLoadOrCreateReloadsMetadataAfterConcurrentCreate() throws Exception {
    HoodieConsistentHashingMetadata concurrent = new HoodieConsistentHashingMetadata("p", 3);
    StoragePath partitionPath = new StoragePath(hashingMetadataPath, "p");
    StoragePathInfo concurrentFile = file(partitionPath, concurrent.getFilename());
    when(storage.listDirectEntries(partitionPath))
        .thenThrow(new FileNotFoundException("not created yet"))
        .thenReturn(Collections.singletonList(concurrentFile));
    when(storage.open(concurrentFile.getPath())).thenReturn(new ByteArrayInputStream(concurrent.toBytes()));
    when(storage.exists(any(StoragePath.class)))
        .thenThrow(new IOException("lost create race"))
        .thenReturn(false);

    HoodieConsistentHashingMetadata loaded = ConsistentBucketIndexUtils.loadOrCreateMetadata(table, "p", 9);

    assertEquals(3, loaded.getNumBuckets());
  }

  @Test
  void testLoadMetadataRejectsUncommittedRehashWithoutMatchingBaseFile() throws Exception {
    HoodieConsistentHashingMetadata updated = new HoodieConsistentHashingMetadata(
        (short) 0, "p", "002", 0, 0,
        Collections.singletonList(new ConsistentHashingNode(100, "new-file-group")));
    StoragePath partitionPath = new StoragePath(hashingMetadataPath, "p");
    StoragePathInfo updatedFile = file(partitionPath, updated.getFilename());
    when(storage.listDirectEntries(partitionPath)).thenReturn(Collections.singletonList(updatedFile));
    when(storage.open(updatedFile.getPath())).thenReturn(new ByteArrayInputStream(updated.toBytes()));
    HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);
    HoodieTimeline completedTimeline = mock(HoodieTimeline.class);
    HoodieTimeline pendingTimeline = mock(HoodieTimeline.class);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(activeTimeline.getCommitAndReplaceTimeline()).thenReturn(completedTimeline);
    when(completedTimeline.filterCompletedInstants()).thenReturn(completedTimeline);
    when(completedTimeline.containsInstant("002")).thenReturn(false);
    when(table.getPendingCommitsTimeline()).thenReturn(pendingTimeline);
    when(pendingTimeline.containsInstant("002")).thenReturn(false);
    BaseFileOnlyView baseFileView = mock(BaseFileOnlyView.class);
    when(table.getBaseFileOnlyView()).thenReturn(baseFileView);
    when(baseFileView.getLatestBaseFiles("p")).thenAnswer(ignored -> java.util.stream.Stream.empty());

    assertFalse(ConsistentBucketIndexUtils.loadMetadata(table, "p").isPresent());
  }

  private static StoragePathInfo file(StoragePath parent, String name) {
    return new StoragePathInfo(new StoragePath(parent, name), 1L, false, (short) 1, 1L, 1L);
  }
}
