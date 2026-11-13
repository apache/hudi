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
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.FileSystemBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadata;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A read over every partition of a view, traversed while an incremental sync of the view runs, shows the view of one
 * state.
 */
public class TestIncrementalFSViewSyncSnapshotReads extends HoodieCommonTestHarness {

  private static final String PARTITION = "2018/01/01";
  private static final String OTHER_PARTITION = "2018/01/02";
  private static final String WRITE_TOKEN = "1-0-1";

  @BeforeEach
  public void setUp() throws IOException {
    initMetaClient();
    Files.createDirectories(Paths.get(basePath, PARTITION));
    Files.createDirectories(Paths.get(basePath, OTHER_PARTITION));
  }

  @AfterEach
  public void tearDown() {
    cleanMetaClient();
  }

  private void addCommit(String instant, List<String> fileIds) throws IOException {
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    for (String partition : Arrays.asList(PARTITION, OTHER_PARTITION)) {
      for (String fileId : fileIds) {
        java.nio.file.Path file = Paths.get(basePath, partition, baseFileName(instant, fileId));
        Files.createFile(file);
        HoodieWriteStat stat = new HoodieWriteStat();
        stat.setFileId(fileId);
        stat.setPath(partition + "/" + file.getFileName());
        metadata.addWriteStat(partition, stat);
      }
    }
    HoodieInstant inflight = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, instant);
    metaClient.getActiveTimeline().createNewInstant(inflight);
    metaClient.getActiveTimeline().saveAsComplete(inflight, Option.of(metadata));
    metaClient.reloadActiveTimeline();
  }

  private static String baseFileName(String instant, String fileId) {
    return FSUtils.makeBaseFileName(instant, WRITE_TOKEN, fileId, BASE_FILE_EXTENSION);
  }

  private HoodieTableFileSystemView createView(boolean spillable) {
    HoodieTableMetadata tableMetadata = new FileSystemBackedTableMetadata(new HoodieLocalEngineContext(metaClient.getStorageConf()),
        metaClient.getTableConfig(), metaClient.getStorage(), metaClient.getBasePath().toString());
    HoodieTimeline timeline = metaClient.getActiveTimeline().filterCompletedOrMajorOrMinorCompactionInstants();
    if (!spillable) {
      return new HoodieTableFileSystemView(tableMetadata, metaClient, timeline, true);
    }
    FileSystemViewStorageConfig config = FileSystemViewStorageConfig.newBuilder().withStorageType(FileSystemViewStorageType.SPILLABLE_DISK)
        .withIncrementalTimelineSync(true).withBaseStoreDir(tempDir.resolve("spill").toString()).build();
    return new SpillableMapBasedFileSystemView(tableMetadata, metaClient, timeline, config, HoodieCommonConfig.newBuilder().build());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void readsOfAllPartitionsTraversedDuringASyncShowOneState(boolean spillable) throws IOException {
    List<String> fileIds = Arrays.asList(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    addCommit("001", fileIds);
    HoodieTableFileSystemView view = createView(spillable);
    assertEquals(2, view.getLatestBaseFiles(PARTITION).count());
    assertEquals(2, view.getLatestBaseFiles(OTHER_PARTITION).count());
    assertTraversalDuringASyncShowsOneState(view, fileIds, "002", v -> v.getLatestBaseFiles().map(HoodieBaseFile::getFileName));
    assertTraversalDuringASyncShowsOneState(view, fileIds, "003",
        v -> v.getAllFileGroups().map(fileGroup -> fileGroup.getAllBaseFiles().findFirst().get().getFileName()));
    view.close();
  }

  /**
   * Takes the first element of the read, syncs the view to a commit that rewrites both partitions, then takes the rest.
   */
  private void assertTraversalDuringASyncShowsOneState(HoodieTableFileSystemView view, List<String> fileIds, String instant,
                                                       Function<HoodieTableFileSystemView, Stream<String>> read) throws IOException {
    Set<String> before = read.apply(view).collect(Collectors.toCollection(TreeSet::new));
    Iterator<String> traversal = read.apply(view).iterator();
    Set<String> traversed = new TreeSet<>();
    traversed.add(traversal.next());
    addCommit(instant, fileIds);
    view.sync();
    traversal.forEachRemaining(traversed::add);

    Set<String> after = fileIds.stream().map(fileId -> baseFileName(instant, fileId)).collect(Collectors.toCollection(TreeSet::new));
    assertEquals(after, read.apply(view).collect(Collectors.toCollection(TreeSet::new)), "the sync applied the commit to both partitions");
    assertTrue(traversed.equals(before) || traversed.equals(after),
        "a traversal during the sync shows one state, but showed " + traversed + " between " + before + " and " + after);
  }
}
