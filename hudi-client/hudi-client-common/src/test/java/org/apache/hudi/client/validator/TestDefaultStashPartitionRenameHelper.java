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

package org.apache.hudi.client.validator;

import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link DefaultStashPartitionRenameHelper}.
 */
public class TestDefaultStashPartitionRenameHelper {

  @TempDir
  private Path tempDir;

  private HoodieStorage storage;
  private DefaultStashPartitionRenameHelper helper;
  private StoragePath sourcePath;
  private StoragePath targetPath;

  @BeforeEach
  public void init() throws IOException {
    String basePath = tempDir.toAbsolutePath().toUri().toString();
    storage = HoodieTestUtils.getStorage(basePath);
    helper = new DefaultStashPartitionRenameHelper();
    sourcePath = new StoragePath(basePath, "source_partition");
    targetPath = new StoragePath(basePath, "target_partition");
    storage.createDirectory(sourcePath);
  }

  /**
   * Given: Source has files, target is empty.
   * When: stashPartitionFiles is called.
   * Then: All files are copied to target and deleted from source.
   */
  @Test
  public void testMoveAllFilesFromSourceToEmptyTarget() throws IOException {
    createTestFile(sourcePath, "file1.parquet", "data1");
    createTestFile(sourcePath, "file2.parquet", "data2");
    createTestFile(sourcePath, "file3.parquet", "data3");

    helper.stashPartitionFiles(storage, sourcePath, targetPath);

    // Target should have all 3 files
    Set<String> targetFiles = getFileNames(targetPath);
    assertEquals(3, targetFiles.size());
    assertTrue(targetFiles.contains("file1.parquet"));
    assertTrue(targetFiles.contains("file2.parquet"));
    assertTrue(targetFiles.contains("file3.parquet"));

    // Source should be empty
    List<StoragePathInfo> sourceFiles = storage.listDirectEntries(sourcePath);
    assertTrue(sourceFiles.isEmpty(), "Source should have no files after move");
  }

  /**
   * Given: Source has files, target already has some of the same files (partial prior move).
   * When: stashPartitionFiles is called.
   * Then: Only missing files are copied to target, all source files are deleted.
   */
  @Test
  public void testIdempotentMoveWithPartialPriorAttempt() throws IOException {
    // Source has 3 files
    createTestFile(sourcePath, "file1.parquet", "data1");
    createTestFile(sourcePath, "file2.parquet", "data2");
    createTestFile(sourcePath, "file3.parquet", "data3");

    // Target already has file1 from a prior partial move
    storage.createDirectory(targetPath);
    createTestFile(targetPath, "file1.parquet", "data1");

    helper.stashPartitionFiles(storage, sourcePath, targetPath);

    // Target should have all 3 files
    Set<String> targetFiles = getFileNames(targetPath);
    assertEquals(3, targetFiles.size());
    assertTrue(targetFiles.contains("file1.parquet"));
    assertTrue(targetFiles.contains("file2.parquet"));
    assertTrue(targetFiles.contains("file3.parquet"));

    // Source should be empty
    List<StoragePathInfo> sourceFiles = storage.listDirectEntries(sourcePath);
    assertTrue(sourceFiles.isEmpty(), "Source should have no files after move");
  }

  /**
   * Given: Source has files, target already has all the same files.
   * When: stashPartitionFiles is called.
   * Then: No copies happen, but source files are still deleted.
   */
  @Test
  public void testMoveWhenTargetAlreadyHasAllFiles() throws IOException {
    createTestFile(sourcePath, "file1.parquet", "data1");
    createTestFile(sourcePath, "file2.parquet", "data2");

    // Target has all files already
    storage.createDirectory(targetPath);
    createTestFile(targetPath, "file1.parquet", "data1");
    createTestFile(targetPath, "file2.parquet", "data2");

    helper.stashPartitionFiles(storage, sourcePath, targetPath);

    // Target still has 2 files
    assertEquals(2, getFileNames(targetPath).size());

    // Source should be empty
    assertTrue(storage.listDirectEntries(sourcePath).isEmpty());
  }

  /**
   * Given: Source is empty.
   * When: stashPartitionFiles is called.
   * Then: No-op, target remains unchanged.
   */
  @Test
  public void testMoveFromEmptySourceIsNoOp() throws IOException {
    // Source is empty dir, target doesn't exist

    helper.stashPartitionFiles(storage, sourcePath, targetPath);

    // Target should not have been created (or if created, should be empty)
    assertFalse(storage.exists(targetPath) && !storage.listDirectEntries(targetPath).isEmpty(),
        "Target should remain empty when source has no files");
  }

  /**
   * Given: Target directory does not exist.
   * When: stashPartitionFiles is called.
   * Then: Target directory is created and files are moved.
   */
  @Test
  public void testTargetDirectoryCreatedIfNotExists() throws IOException {
    createTestFile(sourcePath, "file1.parquet", "data1");

    assertFalse(storage.exists(targetPath), "Target should not exist before move");

    helper.stashPartitionFiles(storage, sourcePath, targetPath);

    assertTrue(storage.exists(targetPath), "Target should be created");
    assertEquals(1, getFileNames(targetPath).size());
    assertTrue(storage.listDirectEntries(sourcePath).isEmpty());
  }

  /**
   * Given: Files were previously stashed to target.
   * When: restorePartitionFiles is called.
   * Then: All files are copied back from target to source, target is emptied.
   */
  @Test
  public void testRestorePartitionFilesMovesFilesBack() throws IOException {
    // Stash files first
    createTestFile(sourcePath, "file1.parquet", "data1");
    createTestFile(sourcePath, "file2.parquet", "data2");
    helper.stashPartitionFiles(storage, sourcePath, targetPath);

    // Source should be empty, target should have files
    assertTrue(storage.listDirectEntries(sourcePath).isEmpty());
    assertEquals(2, getFileNames(targetPath).size());

    // Restore
    helper.restorePartitionFiles(storage, targetPath, sourcePath);

    // Source should have files back, target should be empty
    Set<String> restoredFiles = getFileNames(sourcePath);
    assertEquals(2, restoredFiles.size());
    assertTrue(restoredFiles.contains("file1.parquet"));
    assertTrue(restoredFiles.contains("file2.parquet"));
    assertTrue(storage.listDirectEntries(targetPath).isEmpty());
  }

  /**
   * Given: Files were partially restored (some already in source from prior attempt).
   * When: restorePartitionFiles is called.
   * Then: Only missing files are copied, all target files are deleted (idempotent).
   */
  @Test
  public void testRestoreIsIdempotentWithPartialPriorRestore() throws IOException {
    // Set up: target has 3 files, source already has 1 from prior partial restore
    storage.createDirectory(targetPath);
    createTestFile(targetPath, "file1.parquet", "data1");
    createTestFile(targetPath, "file2.parquet", "data2");
    createTestFile(targetPath, "file3.parquet", "data3");
    createTestFile(sourcePath, "file1.parquet", "data1");

    helper.restorePartitionFiles(storage, targetPath, sourcePath);

    Set<String> restoredFiles = getFileNames(sourcePath);
    assertEquals(3, restoredFiles.size());
    assertTrue(restoredFiles.contains("file1.parquet"));
    assertTrue(restoredFiles.contains("file2.parquet"));
    assertTrue(restoredFiles.contains("file3.parquet"));
    assertTrue(storage.listDirectEntries(targetPath).isEmpty());
  }

  // ---- Helper methods ----

  private void createTestFile(StoragePath dir, String fileName, String content) throws IOException {
    StoragePath filePath = new StoragePath(dir, fileName);
    try (OutputStream out = storage.create(filePath, true)) {
      out.write(content.getBytes(StandardCharsets.UTF_8));
    }
  }

  private Set<String> getFileNames(StoragePath dir) throws IOException {
    return storage.listDirectEntries(dir).stream()
        .map(info -> info.getPath().getName())
        .collect(Collectors.toSet());
  }
}
