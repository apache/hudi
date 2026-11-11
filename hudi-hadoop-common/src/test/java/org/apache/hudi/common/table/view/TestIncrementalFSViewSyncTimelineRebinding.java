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
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.serialization.CustomSerializer;
import org.apache.hudi.common.serialization.HoodieFileGroupSerializer;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.metadata.FileSystemBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadata;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * After an incremental sync every stored file group, in partitions the synced instants touched or not, refers to the
 * view's current timeline and to no earlier one.
 */
public class TestIncrementalFSViewSyncTimelineRebinding extends HoodieCommonTestHarness {

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

  private static HoodieTableMetadata tableMetadata(HoodieTableMetaClient metaClient) {
    return new FileSystemBackedTableMetadata(new HoodieLocalEngineContext(metaClient.getStorageConf()), metaClient.getTableConfig(),
        metaClient.getStorage(), metaClient.getBasePath().toString());
  }

  private static List<String> fileIds() {
    return Arrays.asList(UUID.randomUUID().toString(), UUID.randomUUID().toString());
  }

  private void addDeltaCommit(String instant, String partition, List<String> fileIds) throws IOException {
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    for (String fileId : fileIds) {
      java.nio.file.Path file = Paths.get(basePath, partition, FSUtils.makeInlineLogFileName(fileId, ".log", instant, 1, WRITE_TOKEN));
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

  private static Set<String> slices(SyncableFileSystemView view, String partition) {
    return view.getLatestFileSlices(partition)
        .map(s -> s.getFileId() + "@" + s.getBaseInstantTime() + ":" + s.getLogFiles().map(HoodieLogFile::getFileName).sorted().collect(Collectors.joining(",")))
        .collect(Collectors.toSet());
  }

  @Test
  public void untouchedPartitionsFollowTheTimelineAfterIncrementalSync() throws IOException {
    List<String> touched = fileIds();
    List<String> untouched = fileIds();
    addDeltaCommit("001", PARTITION, touched);
    addDeltaCommit("002", OTHER_PARTITION, untouched);
    HoodieTableFileSystemView view = new HoodieTableFileSystemView(tableMetadata(metaClient), metaClient,
        metaClient.getActiveTimeline().filterCompletedOrMajorOrMinorCompactionInstants(), true);
    assertUntouchedPartitionFollowsTheTimeline(view, touched, untouched);
    view.close();
  }

  @Test
  public void untouchedPartitionsFollowTheTimelineInTheSpillableView() throws IOException {
    List<String> touched = fileIds();
    List<String> untouched = fileIds();
    addDeltaCommit("001", PARTITION, touched);
    addDeltaCommit("002", OTHER_PARTITION, untouched);
    FileSystemViewStorageConfig config = FileSystemViewStorageConfig.newBuilder().withStorageType(FileSystemViewStorageType.SPILLABLE_DISK)
        .withIncrementalTimelineSync(true).withBaseStoreDir(tempDir.resolve("spill").toString()).build();
    SpillableMapBasedFileSystemView view = new SpillableMapBasedFileSystemView(tableMetadata(metaClient), metaClient,
        metaClient.getActiveTimeline().filterCompletedOrMajorOrMinorCompactionInstants(), config, HoodieCommonConfig.newBuilder().build());
    assertUntouchedPartitionFollowsTheTimeline(view, touched, untouched);
    view.close();
  }

  @Test
  public void spillableViewIsJavaSerializable() throws Exception {
    List<String> resident = fileIds();
    addDeltaCommit("001", PARTITION, resident);
    addDeltaCommit("002", OTHER_PARTITION, fileIds());
    FileSystemViewStorageConfig config = FileSystemViewStorageConfig.newBuilder().withStorageType(FileSystemViewStorageType.SPILLABLE_DISK)
        .withIncrementalTimelineSync(true).withBaseStoreDir(tempDir.resolve("spill").toString()).build();
    OnePartitionInMemoryView.spillDir = tempDir.resolve("spill-maps").toString();
    OnePartitionInMemoryView view = new OnePartitionInMemoryView(metaClient, config);
    Set<String> residentSlices = slices(view, PARTITION);
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(view);
    }
    OnePartitionInMemoryView copy;
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      copy = (OnePartitionInMemoryView) in.readObject();
    }
    assertEquals(residentSlices, slices(copy, PARTITION));
    assertEquals(2, copy.getLatestFileSlices(OTHER_PARTITION).count());
    assertEquals(1, copy.diskEntries(), "the partition loaded by the copy spilled to disk");
    assertNotSame(view.getVisibleCommitsAndCompactionTimeline(), copy.getVisibleCommitsAndCompactionTimeline());
    assertTrue(copy.getAllFileGroups(OTHER_PARTITION).allMatch(g -> g.getTimeline() == copy.getVisibleCommitsAndCompactionTimeline()),
        "file groups read back by the copy are bound to the copy's timeline");

    addDeltaCommit("003", PARTITION, resident);
    copy.sync();
    assertEquals("003", copy.getVisibleCommitsAndCompactionTimeline().lastInstant().get().requestedTime());
    assertTrue(copy.getAllFileGroups(OTHER_PARTITION).allMatch(g -> g.getTimeline() == copy.getVisibleCommitsAndCompactionTimeline()),
        "after a sync the copy reads its spilled partition back bound to its new timeline");
    copy.close();
    view.close();
  }

  /**
   * A spillable map that counts the reads that include its disk entries.
   */
  static final class CountingSpillableMap<T extends Serializable, R> extends ExternalSpillableMap<T, R> {
    // static: the view creates this map from its super constructor, before instance fields are set
    static final AtomicInteger DISK_INCLUDING_READS = new AtomicInteger();

    CountingSpillableMap(long maxInMemorySizeInBytes, String baseFilePath, SizeEstimator<T> keySizeEstimator, SizeEstimator<R> valueSizeEstimator,
                         CustomSerializer<R> valueSerializer) throws IOException {
      super(maxInMemorySizeInBytes, baseFilePath, keySizeEstimator, valueSizeEstimator, DiskMapType.BITCASK, valueSerializer, false, "counting");
    }

    @Override
    public Iterator<R> iterator() {
      DISK_INCLUDING_READS.incrementAndGet();
      return super.iterator();
    }

    @Override
    public Iterator<R> iterator(Predicate<T> filter) {
      DISK_INCLUDING_READS.incrementAndGet();
      return super.iterator(filter);
    }

    @Override
    public Stream<R> valueStream() {
      DISK_INCLUDING_READS.incrementAndGet();
      return super.valueStream();
    }

    @Override
    public Collection<R> values() {
      DISK_INCLUDING_READS.incrementAndGet();
      return super.values();
    }

    @Override
    public Set<Entry<T, R>> entrySet() {
      DISK_INCLUDING_READS.incrementAndGet();
      return super.entrySet();
    }
  }

  /**
   * A spillable view whose file group map has a budget of one byte after the sizing factor, so the first loaded
   * partition stays in memory and every other one spills.
   */
  static final class OnePartitionInMemoryView extends SpillableMapBasedFileSystemView {
    // static: createPartitionToFileGroups runs from the super constructor, before instance fields are set
    private static String spillDir;

    OnePartitionInMemoryView(HoodieTableMetaClient metaClient, FileSystemViewStorageConfig config) {
      super(tableMetadata(metaClient), metaClient,
          metaClient.getActiveTimeline().filterCompletedOrMajorOrMinorCompactionInstants(), config, HoodieCommonConfig.newBuilder().build());
    }

    @Override
    protected Map<String, List<HoodieFileGroup>> createPartitionToFileGroups() {
      try {
        return new CountingSpillableMap<String, List<HoodieFileGroup>>(2L, spillDir + "/" + UUID.randomUUID(), new DefaultSizeEstimator<String>(),
            new HoodieFileGroupSizeEstimator(), new HoodieFileGroupSerializer(this::getVisibleCommitsAndCompactionTimeline));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    int diskEntries() {
      return ((ExternalSpillableMap<?, ?>) partitionToFileGroupsMap).getDiskBasedMapNumEntries();
    }
  }

  @Test
  public void spilledPartitionsAreLeftOnDiskByTheRebinding() throws IOException {
    List<String> resident = fileIds();
    List<String> spilled = fileIds();
    addDeltaCommit("001", PARTITION, resident);
    addDeltaCommit("002", OTHER_PARTITION, spilled);
    FileSystemViewStorageConfig config = FileSystemViewStorageConfig.newBuilder().withStorageType(FileSystemViewStorageType.SPILLABLE_DISK)
        .withIncrementalTimelineSync(true).withBaseStoreDir(tempDir.resolve("spill").toString()).build();
    OnePartitionInMemoryView.spillDir = tempDir.resolve("spill-maps").toString();
    OnePartitionInMemoryView view = new OnePartitionInMemoryView(metaClient, config);
    assertEquals(2, view.getLatestFileSlices(PARTITION).count());
    assertEquals(2, view.getLatestFileSlices(OTHER_PARTITION).count());
    assertEquals(1, view.diskEntries(), "the second loaded partition spilled to disk");
    Set<String> spilledSlices = slices(view, OTHER_PARTITION);
    HoodieTimeline before = view.getAllFileGroups(PARTITION).findFirst().get().getTimeline();
    HoodieFileGroup spilledBefore = view.getAllFileGroups(OTHER_PARTITION).findFirst().get();
    assertEquals("002", spilledBefore.getTimeline().lastInstant().get().requestedTime());

    addDeltaCommit("003", PARTITION, resident);
    int readsBefore = CountingSpillableMap.DISK_INCLUDING_READS.get();
    view.sync();
    assertEquals(readsBefore, CountingSpillableMap.DISK_INCLUDING_READS.get(), "the sync reads no spilled partition");
    assertEquals(1, view.diskEntries());
    HoodieTimeline current = view.getAllFileGroups(PARTITION).findFirst().get().getTimeline();
    assertNotSame(before, current);
    assertEquals("003", current.lastInstant().get().requestedTime());
    assertTrue(view.getAllFileGroups(PARTITION).allMatch(g -> g.getTimeline() == current), "resident file groups refer to the current timeline");
    assertEquals(spilledSlices, slices(view, OTHER_PARTITION), "the spilled partition reads back with the same slices");
    assertTrue(view.getAllFileGroups(OTHER_PARTITION).allMatch(g -> g.getTimeline() == current),
        "file groups read back from disk are bound to the current timeline");
    assertEquals("003", view.getAllFileGroups(OTHER_PARTITION).findFirst().get().getTimeline().lastInstant().get().requestedTime());
    assertEquals("002", spilledBefore.getTimeline().lastInstant().get().requestedTime(), "a file group returned earlier keeps its snapshot");
    assertEquals(2, view.getLatestFileSlices(PARTITION).filter(s -> s.getLogFiles().count() == 2).count());
    view.close();
  }

  private void assertUntouchedPartitionFollowsTheTimeline(HoodieTableFileSystemView view, List<String> touched, List<String> untouched) throws IOException {
    assertEquals(2, view.getLatestFileSlices(PARTITION).count());
    assertEquals(2, view.getLatestFileSlices(OTHER_PARTITION).count());
    Set<String> untouchedSlices = slices(view, OTHER_PARTITION);
    HoodieTimeline before = view.getAllFileGroups(OTHER_PARTITION).findFirst().get().getTimeline();

    addDeltaCommit("003", PARTITION, touched);
    view.sync();

    Set<HoodieTimeline> timelines = Collections.newSetFromMap(new IdentityHashMap<>());
    view.getAllFileGroups().map(HoodieFileGroup::getTimeline).forEach(timelines::add);
    assertEquals(1, timelines.size(), "every stored file group refers to one timeline object");
    assertFalse(timelines.contains(before), "the timeline of the earlier sync is not referenced");
    HoodieFileGroup untouchedGroup = view.getAllFileGroups(OTHER_PARTITION).findFirst().get();
    assertNotSame(before, untouchedGroup.getTimeline());
    assertEquals("003", untouchedGroup.getTimeline().lastInstant().get().requestedTime());
    assertEquals(untouchedSlices, slices(view, OTHER_PARTITION), "the untouched partition keeps its slices");
    assertEquals(2, view.getLatestFileSlices(PARTITION).filter(s -> s.getLogFiles().count() == 2).count(), "the touched partition gained its log files");
    assertTrue(view.getAllFileGroups(PARTITION).allMatch(g -> g.getTimeline() == untouchedGroup.getTimeline()));
  }
}
