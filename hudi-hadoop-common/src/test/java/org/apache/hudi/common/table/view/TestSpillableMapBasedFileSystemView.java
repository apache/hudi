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

package org.apache.hudi.common.table.view;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.metadata.FileSystemBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadata;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Tests spillable map based file system view {@link SpillableMapBasedFileSystemView}.
 */
public class TestSpillableMapBasedFileSystemView extends TestHoodieTableFileSystemView {

  @Override
  protected SyncableFileSystemView getFileSystemView(HoodieTimeline timeline) {
    HoodieTableMetadata tableMetadata = new FileSystemBackedTableMetadata(getEngineContext(), metaClient.getTableConfig(), metaClient.getStorage(),
        metaClient.getBasePath().toString());
    return new SpillableMapBasedFileSystemView(tableMetadata, metaClient, timeline, FileSystemViewStorageConfig.newBuilder()
        // pure disk base View
        .withStorageType(FileSystemViewStorageType.SPILLABLE_DISK).withMaxMemoryForView(0L).build(),
        HoodieCommonConfig.newBuilder().build());
  }

  /**
   * That closing the view actually releases what it spilled.
   *
   * <p>The view's six state maps are {@code ExternalSpillableMap}s whose disk half owns a file, a
   * write handle and a per-thread read handle. Only {@code close()} gives those up: a
   * {@code BitCaskDiskMap}'s file is otherwise left to {@code deleteOnExit}, which does nothing at
   * all for a JVM that is killed rather than exiting.
   *
   * <p>This exists because that close was once ordered so that it could never run:
   * {@code closeResources()} called {@code super.closeResources()} first, and
   * {@code HoodieTableFileSystemView.closeResources()} sets all six fields to null, so every
   * {@code close*IfPresent()} that followed saw a null field and returned. Nothing was closed and
   * nothing failed — which is why the residue on disk is the only thing that can catch it, and why
   * this test asserts on a spill directory of its own rather than on the shared default.
   */
  @Test
  public void testCloseReleasesSpilledFiles() throws Exception {
    Path spillDir = tempDir.resolve("view-spill-" + UUID.randomUUID());
    Files.createDirectories(spillDir);

    String instantTime = "1";
    String partitionPath = "2016/05/01";
    FileCreateUtils.createBaseFile(metaClient, partitionPath, instantTime, UUID.randomUUID().toString());
    FileCreateUtils.createBaseFile(metaClient, partitionPath, instantTime, UUID.randomUUID().toString());
    FileCreateUtils.createCommit(metaClient, instantTime);
    metaClient = HoodieTableMetaClient.reload(metaClient);

    HoodieTableMetadata tableMetadata = new FileSystemBackedTableMetadata(getEngineContext(),
        metaClient.getTableConfig(), metaClient.getStorage(), metaClient.getBasePath().toString());
    SpillableMapBasedFileSystemView view = new SpillableMapBasedFileSystemView(tableMetadata,
        metaClient, metaClient.getActiveTimeline().filterCompletedInstants(),
        FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.SPILLABLE_DISK)
            // Zero, so the first entry spills instead of being held in memory: every per-map budget
            // is a fraction of this one number, so this is what makes the disk half exist at all.
            .withMaxMemoryForView(0L)
            .withBaseStoreDir(spillDir.toString())
            .build(),
        HoodieCommonConfig.newBuilder().build());

    // Lists the partition, which is what puts the file groups into the spillable map. Asserted
    // rather than assumed: with no file group there is nothing to spill and the check below would
    // hold vacuously.
    assertFalse(view.getAllFileGroups(partitionPath).collect(Collectors.toList()).isEmpty(),
        "the view found no file group, so nothing was spilled and this test proves nothing");
    assertFalse(filesUnder(spillDir).isEmpty(),
        "the view did not spill to " + spillDir + ", so this test proves nothing about closing it");

    view.close();

    List<String> leftOver = filesUnder(spillDir);
    assertTrue(leftOver.isEmpty(),
        "closing the view left its spilled state on disk: " + leftOver + ". The "
            + "ExternalSpillableMaps were not closed -- closeResources() must close them BEFORE "
            + "calling super.closeResources(), which nulls the fields holding them");
  }

  /**
   * A spilled map whose close throws still lets the rest of the teardown run: the view ends up
   * closed, rather than half-closed with its table metadata still open.
   */
  @Test
  public void testCloseCompletesWhenASpilledMapFailsToClose() throws Exception {
    Path spillDir = tempDir.resolve("view-spill-" + UUID.randomUUID());
    Files.createDirectories(spillDir);

    String instantTime = "1";
    String partitionPath = "2016/05/01";
    FileCreateUtils.createBaseFile(metaClient, partitionPath, instantTime, UUID.randomUUID().toString());
    FileCreateUtils.createCommit(metaClient, instantTime);
    metaClient = HoodieTableMetaClient.reload(metaClient);

    HoodieTableMetadata tableMetadata = new FileSystemBackedTableMetadata(getEngineContext(),
        metaClient.getTableConfig(), metaClient.getStorage(), metaClient.getBasePath().toString());
    SpillableMapBasedFileSystemView view = new SpillableMapBasedFileSystemView(tableMetadata,
        metaClient, metaClient.getActiveTimeline().filterCompletedInstants(),
        FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.SPILLABLE_DISK)
            .withMaxMemoryForView(0L)
            .withBaseStoreDir(spillDir.toString())
            .build(),
        HoodieCommonConfig.newBuilder()
            .withSpillableDiskMapType(ExternalSpillableMap.DiskMapType.ROCKS_DB)
            .build());
    assertFalse(view.getAllFileGroups(partitionPath).collect(Collectors.toList()).isEmpty(),
        "the view found no file group, so nothing was spilled");
    assertFalse(filesUnder(spillDir).isEmpty(), "the view did not spill to " + spillDir);

    // Read-only directories make RocksDBDAO.close() fail to delete its store and throw. That failure
    // is logged, not rethrown: closing a view never threw before the maps were actually closed, and
    // a caller such as FileSystemViewManager.close() would otherwise stop at the first such view.
    List<File> dirs = directoriesUnder(spillDir);
    dirs.forEach(dir -> assertTrue(dir.setWritable(false), "could not make " + dir + " read-only"));
    try {
      // A read-only directory does not stop a root user from deleting it.
      assumeFalse(Files.isWritable(spillDir), "running as a user that ignores directory permissions");
      assertDoesNotThrow(view::close);
    } finally {
      dirs.forEach(dir -> dir.setWritable(true));
    }
    assertTrue(view.isClosed(), "a failing map close must not skip the rest of the teardown");
  }

  private static List<File> directoriesUnder(Path dir) throws IOException {
    try (Stream<Path> walk = Files.walk(dir)) {
      return walk.map(Path::toFile).filter(File::isDirectory).collect(Collectors.toList());
    }
  }

  private static List<String> filesUnder(Path dir) throws IOException {
    try (Stream<Path> walk = Files.walk(dir)) {
      return walk.filter(path -> path.toFile().isFile()).map(Path::toString).collect(Collectors.toList());
    }
  }
}
