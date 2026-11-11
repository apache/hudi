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

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metadata.FileSystemBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadata;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Incremental timeline sync across the state transitions of pending compactions and log compactions.
 */
public class TestIncrementalFSViewSyncTransitions extends HoodieCommonTestHarness {

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

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

  /**
   * A view that counts its complete initialisations: one from the constructor, one more for every revert to a complete
   * sync.
   */
  interface CountingView extends SyncableFileSystemView {
    int initCalls();

    void addPending(String instant, CompactionOperation operation);
  }

  static final class CountingMemoryView extends HoodieTableFileSystemView implements CountingView {
    private int initCalls;

    CountingMemoryView(HoodieTableMetaClient metaClient) {
      super(tableMetadata(metaClient), metaClient, managerTimeline(metaClient), true);
    }

    @Override
    public void init(HoodieTableMetaClient metaClient, HoodieTimeline visibleActiveTimeline) {
      initCalls++;
      super.init(metaClient, visibleActiveTimeline);
    }

    @Override
    public int initCalls() {
      return initCalls;
    }

    @Override
    public void addPending(String instant, CompactionOperation operation) {
      addPendingCompactionOperations(Stream.of(Pair.of(instant, operation)));
    }
  }

  static final class CountingSpillableView extends SpillableMapBasedFileSystemView implements CountingView {
    private int initCalls;

    CountingSpillableView(HoodieTableMetaClient metaClient, FileSystemViewStorageConfig config) {
      super(tableMetadata(metaClient), metaClient, managerTimeline(metaClient), config, HoodieCommonConfig.newBuilder().build());
    }

    @Override
    public void init(HoodieTableMetaClient metaClient, HoodieTimeline visibleActiveTimeline) {
      initCalls++;
      super.init(metaClient, visibleActiveTimeline);
    }

    @Override
    public int initCalls() {
      return initCalls;
    }

    @Override
    public void addPending(String instant, CompactionOperation operation) {
      addPendingCompactionOperations(Stream.of(Pair.of(instant, operation)));
    }
  }

  static final class CountingRocksDbView extends RocksDbBasedFileSystemView implements CountingView {
    private int initCalls;

    CountingRocksDbView(HoodieTableMetaClient metaClient, FileSystemViewStorageConfig config) {
      super(tableMetadata(metaClient), metaClient, managerTimeline(metaClient), config);
    }

    @Override
    protected void init(HoodieTableMetaClient metaClient, HoodieTimeline visibleActiveTimeline) {
      initCalls++;
      super.init(metaClient, visibleActiveTimeline);
    }

    @Override
    public int initCalls() {
      return initCalls;
    }

    @Override
    public void addPending(String instant, CompactionOperation operation) {
      addPendingCompactionOperations(Stream.of(Pair.of(instant, operation)));
    }
  }

  /**
   * The timeline the view manager builds its views with.
   */
  private static HoodieTimeline managerTimeline(HoodieTableMetaClient metaClient) {
    return metaClient.getActiveTimeline().filterCompletedAndCompactionInstants();
  }

  private CountingView countingView(FileSystemViewStorageType type) {
    FileSystemViewStorageConfig config = FileSystemViewStorageConfig.newBuilder().withStorageType(type).withIncrementalTimelineSync(true)
        .withBaseStoreDir(tempDir.resolve("spill").toString()).withRocksDBPath(tempDir.resolve("rocksdb").toString()).build();
    switch (type) {
      case SPILLABLE_DISK:
        return new CountingSpillableView(metaClient, config);
      case EMBEDDED_KV_STORE:
        return new CountingRocksDbView(metaClient, config);
      default:
        return new CountingMemoryView(metaClient);
    }
  }

  static HoodieTableMetadata tableMetadata(HoodieTableMetaClient metaClient) {
    return new FileSystemBackedTableMetadata(new HoodieLocalEngineContext(metaClient.getStorageConf()), metaClient.getTableConfig(),
        metaClient.getStorage(), metaClient.getBasePath().toString());
  }

  private static List<String> fileIds() {
    return Arrays.asList(UUID.randomUUID().toString(), UUID.randomUUID().toString());
  }

  private void addDeltaCommit(String instant, String partition, List<String> fileIds) throws IOException {
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    for (String fileId : fileIds) {
      Path file = Paths.get(basePath, partition, FSUtils.makeInlineLogFileName(fileId, ".log", instant, 1, WRITE_TOKEN));
      Files.createFile(file);
      HoodieWriteStat stat = new HoodieWriteStat();
      stat.setFileId(fileId);
      stat.setPath(partition + "/" + file.getFileName());
      metadata.addWriteStat(partition, stat);
    }
    HoodieInstant inflight = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, instant);
    metaClient.getActiveTimeline().createNewInstant(inflight);
    metaClient.getActiveTimeline().saveAsComplete(inflight, Option.of(metadata));
    metaClient.reloadActiveTimeline();
  }

  private HoodieInstant requestCompaction(String action, String instant, List<FileSlice> slices) {
    HoodieInstant requested = INSTANT_GENERATOR.createNewInstant(State.REQUESTED, action, instant);
    HoodieCompactionPlan plan = CompactionUtils.buildFromFileSlices(
        slices.stream().map(s -> Pair.of(s.getPartitionPath(), s)).collect(Collectors.toList()), Option.empty(), Option.empty());
    if (HoodieTimeline.COMPACTION_ACTION.equals(action)) {
      metaClient.getActiveTimeline().saveToCompactionRequested(requested, plan);
    } else {
      metaClient.getActiveTimeline().saveToLogCompactionRequested(requested, plan);
    }
    metaClient.reloadActiveTimeline();
    return requested;
  }

  private void completeCompaction(String instant, String partition, List<String> fileIds) throws IOException {
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.setCompacted(true);
    for (String fileId : fileIds) {
      Path file = Paths.get(basePath, partition, FSUtils.makeBaseFileName(instant, WRITE_TOKEN, fileId, BASE_FILE_EXTENSION));
      Files.createFile(file);
      HoodieWriteStat stat = new HoodieWriteStat();
      stat.setFileId(fileId);
      stat.setPath(partition + "/" + file.getFileName());
      metadata.addWriteStat(partition, stat);
    }
    HoodieInstant inflight = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, instant);
    metaClient.getActiveTimeline().transitionCompactionInflightToComplete(false, inflight, metadata);
    metaClient.reloadActiveTimeline();
  }

  private static Set<String> slices(SyncableFileSystemView view, String partition) {
    return view.getLatestFileSlices(partition)
        .map(s -> s.getFileId() + "@" + s.getBaseInstantTime() + ":" + s.getBaseFile().map(b -> b.getFileName()).orElse("-")
            + ":" + s.getLogFiles().map(HoodieLogFile::getFileName).sorted().collect(Collectors.joining(",")))
        .collect(Collectors.toSet());
  }

  @ParameterizedTest
  @EnumSource(value = FileSystemViewStorageType.class, names = {"MEMORY", "SPILLABLE_DISK", "EMBEDDED_KV_STORE"})
  public void testCompactionStateTransitionsSyncIncrementally(FileSystemViewStorageType type) throws IOException {
    List<String> fileIds = fileIds();
    addDeltaCommit("001", PARTITION, fileIds);
    CountingView view = countingView(type);
    List<FileSlice> slices = view.getLatestFileSlices(PARTITION).collect(Collectors.toList());
    assertEquals(2, slices.size());

    HoodieInstant requested = requestCompaction(HoodieTimeline.COMPACTION_ACTION, "002", slices);
    view.sync();
    assertEquals(1, view.initCalls(), "a requested compaction syncs incrementally");
    assertEquals(2, view.getPendingCompactionOperations().count());

    metaClient.getActiveTimeline().transitionCompactionRequestedToInflight(requested);
    metaClient.reloadActiveTimeline();
    view.sync();
    assertEquals(1, view.initCalls(), "the transition to inflight syncs incrementally");
    assertEquals(2, view.getPendingCompactionOperations().count());
    view.getLatestFileSlices(PARTITION).forEach(s -> {
      assertEquals("002", s.getBaseInstantTime());
      assertFalse(s.getBaseFile().isPresent());
    });

    completeCompaction("002", PARTITION, fileIds);
    view.sync();
    assertEquals(1, view.initCalls(), "the completed compaction syncs incrementally");
    assertEquals(0, view.getPendingCompactionOperations().count());
    assertEquals(2, view.getLatestBaseFiles(PARTITION).filter(f -> f.getCommitTime().equals("002")).count());

    addDeltaCommit("003", PARTITION, fileIds);
    view.sync();
    assertEquals(1, view.initCalls());
    SyncableFileSystemView fresh = getFileSystemView(metaClient);
    assertEquals(slices(fresh, PARTITION), slices(view, PARTITION));
    fresh.close();
    view.close();
  }

  @ParameterizedTest
  @EnumSource(value = FileSystemViewStorageType.class, names = {"MEMORY", "SPILLABLE_DISK", "EMBEDDED_KV_STORE"})
  public void testLogCompactionStateTransitionSyncsIncrementally(FileSystemViewStorageType type) throws IOException {
    List<String> fileIds = fileIds();
    addDeltaCommit("001", PARTITION, fileIds);
    addDeltaCommit("002", PARTITION, fileIds);
    CountingView view = countingView(type);
    List<FileSlice> slices = view.getLatestFileSlices(PARTITION).collect(Collectors.toList());

    HoodieInstant requested = requestCompaction(HoodieTimeline.LOG_COMPACTION_ACTION, "003", slices);
    view.sync();
    assertEquals(1, view.initCalls());
    assertEquals(2, view.getPendingLogCompactionOperations().count());

    metaClient.getActiveTimeline().transitionLogCompactionRequestedToInflight(requested);
    metaClient.reloadActiveTimeline();
    view.sync();
    assertEquals(1, view.initCalls(), "the transition to inflight syncs incrementally");
    assertEquals(2, view.getPendingLogCompactionOperations().count());
    view.close();
  }

  @ParameterizedTest
  @EnumSource(value = FileSystemViewStorageType.class, names = {"MEMORY", "SPILLABLE_DISK", "EMBEDDED_KV_STORE"})
  public void testPendingLogCompactionStaysOutOfLaterSyncs(FileSystemViewStorageType type) throws IOException {
    List<String> fileIds = fileIds();
    addDeltaCommit("001", PARTITION, fileIds);
    addDeltaCommit("002", PARTITION, fileIds);
    CountingView view = countingView(type);
    requestCompaction(HoodieTimeline.LOG_COMPACTION_ACTION, "003", view.getLatestFileSlices(PARTITION).collect(Collectors.toList()));
    view.sync();
    assertEquals(1, view.initCalls());
    assertEquals(0, view.getLatestFileSlices(OTHER_PARTITION).count());

    addDeltaCommit("004", OTHER_PARTITION, fileIds());
    view.sync();
    assertEquals(1, view.initCalls(), "a delta commit while a log compaction is pending syncs incrementally");
    assertEquals(2, view.getPendingLogCompactionOperations().count());
    SyncableFileSystemView fresh = getFileSystemView(metaClient);
    assertEquals(slices(fresh, OTHER_PARTITION), slices(view, OTHER_PARTITION));
    fresh.close();
    view.close();
  }

  @ParameterizedTest
  @EnumSource(value = FileSystemViewStorageType.class, names = {"MEMORY", "SPILLABLE_DISK", "EMBEDDED_KV_STORE"})
  public void testRemovedLogCompactionLeavesNoPendingOperations(FileSystemViewStorageType type) throws IOException {
    List<String> fileIds = fileIds();
    addDeltaCommit("001", PARTITION, fileIds);
    addDeltaCommit("002", PARTITION, fileIds);
    CountingView view = countingView(type);
    HoodieInstant requested = requestCompaction(HoodieTimeline.LOG_COMPACTION_ACTION, "003",
        view.getLatestFileSlices(PARTITION).collect(Collectors.toList()));
    view.sync();
    assertEquals(2, view.getPendingLogCompactionOperations().count());

    metaClient.getActiveTimeline().deletePending(requested);
    metaClient.reloadActiveTimeline();
    view.sync();
    assertEquals(0, view.getPendingLogCompactionOperations().count());
    SyncableFileSystemView fresh = getFileSystemView(metaClient);
    assertEquals(slices(fresh, PARTITION), slices(view, PARTITION));
    fresh.close();
    view.close();
  }

  @ParameterizedTest
  @EnumSource(value = FileSystemViewStorageType.class, names = {"MEMORY", "SPILLABLE_DISK", "EMBEDDED_KV_STORE"})
  public void testViewCreatedWhileALogCompactionIsPendingRebuildsOnce(FileSystemViewStorageType type) throws IOException {
    List<String> fileIds = fileIds();
    addDeltaCommit("001", PARTITION, fileIds);
    addDeltaCommit("002", PARTITION, fileIds);
    SyncableFileSystemView probe = getFileSystemView(metaClient);
    requestCompaction(HoodieTimeline.LOG_COMPACTION_ACTION, "003", probe.getLatestFileSlices(PARTITION).collect(Collectors.toList()));
    probe.close();
    CountingView view = countingView(type);

    addDeltaCommit("004", OTHER_PARTITION, fileIds());
    view.sync();
    assertEquals(2, view.initCalls(), "the first sync does not find the pending log compaction in the view's timeline");

    addDeltaCommit("005", OTHER_PARTITION, fileIds());
    view.sync();
    assertEquals(2, view.initCalls(), "later syncs are incremental");
    assertEquals(2, view.getPendingLogCompactionOperations().count());
    view.close();
  }

  @ParameterizedTest
  @EnumSource(value = FileSystemViewStorageType.class, names = {"MEMORY", "SPILLABLE_DISK", "EMBEDDED_KV_STORE"})
  public void testCompactionAndLogCompactionPendingTogetherSyncIncrementally(FileSystemViewStorageType type) throws IOException {
    List<String> fileIds = fileIds();
    addDeltaCommit("001", PARTITION, fileIds);
    addDeltaCommit("002", PARTITION, fileIds);
    CountingView view = countingView(type);
    List<FileSlice> slices = view.getLatestFileSlices(PARTITION).sorted(Comparator.comparing(FileSlice::getFileId)).collect(Collectors.toList());
    String logCompacted = slices.get(0).getFileId();
    String compacted = slices.get(1).getFileId();

    HoodieInstant logCompaction = requestCompaction(HoodieTimeline.LOG_COMPACTION_ACTION, "003", Collections.singletonList(slices.get(0)));
    HoodieInstant compaction = requestCompaction(HoodieTimeline.COMPACTION_ACTION, "004", Collections.singletonList(slices.get(1)));
    view.sync();
    metaClient.getActiveTimeline().transitionLogCompactionRequestedToInflight(logCompaction);
    metaClient.getActiveTimeline().transitionCompactionRequestedToInflight(compaction);
    metaClient.reloadActiveTimeline();
    view.sync();
    assertEquals(1, view.initCalls(), "both transitions to inflight sync incrementally");
    assertEquals(1, view.getPendingLogCompactionOperations().count());
    assertEquals(1, view.getPendingCompactionOperations().count());

    addDeltaCommit("005", PARTITION, Collections.singletonList(logCompacted));
    view.sync();
    completeCompaction("004", PARTITION, Collections.singletonList(compacted));
    view.sync();
    assertEquals(1, view.initCalls(), "a delta commit and the completed compaction sync incrementally");
    assertEquals(1, view.getPendingLogCompactionOperations().count());
    assertEquals(0, view.getPendingCompactionOperations().count());
    SyncableFileSystemView fresh = getFileSystemView(metaClient);
    assertEquals(slices(fresh, PARTITION), slices(view, PARTITION));
    fresh.close();
    view.close();
  }

  @ParameterizedTest
  @EnumSource(value = FileSystemViewStorageType.class, names = {"MEMORY", "SPILLABLE_DISK", "EMBEDDED_KV_STORE"})
  public void testUnschedulingAPendingCompactionRebuildsTheView(FileSystemViewStorageType type) throws IOException {
    List<String> fileIds = fileIds();
    addDeltaCommit("001", PARTITION, fileIds);
    CountingView view = countingView(type);
    HoodieInstant requested = requestCompaction(HoodieTimeline.COMPACTION_ACTION, "002", view.getLatestFileSlices(PARTITION).collect(Collectors.toList()));
    view.sync();
    assertEquals(2, view.getPendingCompactionOperations().count());

    metaClient.getActiveTimeline().deleteCompactionRequested(requested);
    metaClient.reloadActiveTimeline();
    view.sync();
    assertEquals(2, view.initCalls(), "an unscheduled compaction reverts to a complete sync");
    assertEquals(0, view.getPendingCompactionOperations().count());
    view.getLatestFileSlices(PARTITION).forEach(s -> assertEquals("001", s.getBaseInstantTime()));
    view.close();
  }

  @ParameterizedTest
  @EnumSource(value = FileSystemViewStorageType.class, names = {"MEMORY", "SPILLABLE_DISK", "EMBEDDED_KV_STORE"})
  public void testDifferentCompactionInstantForAPendingFileGroupIsRefused(FileSystemViewStorageType type) throws IOException {
    addDeltaCommit("001", PARTITION, fileIds());
    CountingView view = countingView(type);
    requestCompaction(HoodieTimeline.COMPACTION_ACTION, "002", view.getLatestFileSlices(PARTITION).collect(Collectors.toList()));
    view.sync();
    CompactionOperation operation = view.getPendingCompactionOperations().findFirst().get().getValue();
    assertThrows(IllegalArgumentException.class, () -> view.addPending("004", operation));
    view.close();
  }
}
