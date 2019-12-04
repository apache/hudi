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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.HoodieCommonTestHarness;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDataFile;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.SyncableFileSystemView;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.util.AvroUtils;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.table.HoodieTimeline.COMPACTION_ACTION;

/**
 * Tests incremental file system view sync.
 */
public class TestIncrementalFSViewSync extends HoodieCommonTestHarness {

  private static final transient Logger LOG = LogManager.getLogger(TestIncrementalFSViewSync.class);

  private static String TEST_WRITE_TOKEN = "1-0-1";

  private final List<String> partitions = Arrays.asList("2018/01/01", "2018/01/02", "2019/03/01");
  private final List<String> fileIdsPerPartition =
      IntStream.range(0, 10).mapToObj(x -> UUID.randomUUID().toString()).collect(Collectors.toList());

  @Before
  public void init() throws IOException {
    initMetaClient();
    partitions.forEach(p -> new File(basePath + "/" + p).mkdirs());
    refreshFsView();
  }

  @Test
  public void testEmptyPartitionsAndTimeline() throws IOException {
    SyncableFileSystemView view = getFileSystemView(metaClient);
    Assert.assertFalse(view.getLastInstant().isPresent());
    partitions.forEach(p -> Assert.assertEquals(0, view.getLatestFileSlices(p).count()));
  }

  @Test
  public void testAsyncCompaction() throws IOException {
    SyncableFileSystemView view = getFileSystemView(metaClient);
    view.sync();

    // Run 3 ingestion on MOR table (3 delta commits)
    Map<String, List<String>> instantsToFiles =
        testMultipleWriteSteps(view, Arrays.asList("11", "12", "13"), true, "11");

    // Schedule Compaction
    scheduleCompaction(view, "14");

    // Restore pending compaction
    unscheduleCompaction(view, "14", "13", "11");

    // Add one more delta instant
    instantsToFiles.putAll(testMultipleWriteSteps(view, Arrays.asList("15"), true, "11"));

    // Schedule Compaction again
    scheduleCompaction(view, "16");

    // Run Compaction - This will be the second file-slice
    testMultipleWriteSteps(view, Arrays.asList("16"), false, "16", 2);

    // Run 2 more ingest
    instantsToFiles.putAll(testMultipleWriteSteps(view, Arrays.asList("17", "18"), true, "16", 2));

    // Schedule Compaction again
    scheduleCompaction(view, "19");

    // Run one more ingestion after pending compaction. THis will be 3rd slice
    instantsToFiles.putAll(testMultipleWriteSteps(view, Arrays.asList("20"), true, "19", 3));

    // Clean first slice
    testCleans(view, Arrays.asList("21"),
        new ImmutableMap.Builder<String, List<String>>().put("11", Arrays.asList("12", "13", "15")).build(),
        instantsToFiles, Arrays.asList("11"));

    // Add one more ingestion instant. This should be 2nd slice now
    instantsToFiles.putAll(testMultipleWriteSteps(view, Arrays.asList("22"), true, "19", 2));

    // Restore last ingestion
    testRestore(view, Arrays.asList("23"), true, new HashMap<>(), Arrays.asList("22"), "24", false);

    // Run one more ingestion. THis is still 2nd slice
    instantsToFiles.putAll(testMultipleWriteSteps(view, Arrays.asList("24"), true, "19", 2));

    // Finish Compaction
    instantsToFiles.putAll(testMultipleWriteSteps(view, Arrays.asList("19"), false, "19", 2,
        Arrays.asList(new HoodieInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "24"))));
  }

  @Test
  public void testIngestion() throws IOException {
    SyncableFileSystemView view = getFileSystemView(metaClient);

    // Add an empty ingestion
    String firstEmptyInstantTs = "11";
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metaClient.getActiveTimeline().saveAsComplete(
        new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, firstEmptyInstantTs),
        Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));

    view.sync();
    Assert.assertTrue(view.getLastInstant().isPresent());
    Assert.assertEquals("11", view.getLastInstant().get().getTimestamp());
    Assert.assertEquals(State.COMPLETED, view.getLastInstant().get().getState());
    Assert.assertEquals(HoodieTimeline.COMMIT_ACTION, view.getLastInstant().get().getAction());
    partitions.forEach(p -> Assert.assertEquals(0, view.getLatestFileSlices(p).count()));

    metaClient.reloadActiveTimeline();
    SyncableFileSystemView newView = getFileSystemView(metaClient);
    for (String partition : partitions) {
      newView.getAllFileGroups(partition).count();
    }

    areViewsConsistent(view, newView, 0L);

    // Add 3 non-empty ingestions to COW table
    Map<String, List<String>> instantsToFiles = testMultipleWriteSteps(view, Arrays.asList("12", "13", "14"));

    // restore instants in reverse order till we rollback all
    testRestore(view, Arrays.asList("15", "16", "17"), false, instantsToFiles, Arrays.asList("14", "13", "12"), "17",
        true);

    // Add 5 non-empty ingestions back-to-back
    instantsToFiles = testMultipleWriteSteps(view, Arrays.asList("18", "19", "20"));

    // Clean instants.
    testCleans(view, Arrays.asList("21", "22"), instantsToFiles, Arrays.asList("18", "19"));
  }

  /**
   * Tests FS View incremental syncing behavior when multiple instants gets committed.
   */
  @Test
  public void testMultipleTransitions() throws IOException {

    SyncableFileSystemView view1 = getFileSystemView(metaClient);
    view1.sync();
    Map<String, List<String>> instantsToFiles = null;

    /**
     * Case where incremental syncing is catching up on more than one ingestion at a time
     */
    // Run 1 ingestion on MOR table (1 delta commits). View1 is now sync up to this point
    instantsToFiles = testMultipleWriteSteps(view1, Arrays.asList("11"), true, "11");

    SyncableFileSystemView view2 =
        getFileSystemView(new HoodieTableMetaClient(metaClient.getHadoopConf(), metaClient.getBasePath()));

    // Run 2 more ingestion on MOR table. View1 is not yet synced but View2 is
    instantsToFiles.putAll(testMultipleWriteSteps(view2, Arrays.asList("12", "13"), true, "11"));

    // Now Sync view1 and add 1 more ingestion. Check if view1 is able to catchup correctly
    instantsToFiles.putAll(testMultipleWriteSteps(view1, Arrays.asList("14"), true, "11"));

    view2.sync();
    SyncableFileSystemView view3 =
        getFileSystemView(new HoodieTableMetaClient(metaClient.getHadoopConf(), metaClient.getBasePath()));
    partitions.stream().forEach(p -> view3.getLatestFileSlices(p).count());
    view3.sync();
    areViewsConsistent(view1, view2, partitions.size() * fileIdsPerPartition.size());

    /**
     * Case where a compaction is scheduled and then unscheduled
     */
    scheduleCompaction(view2, "15");
    unscheduleCompaction(view2, "15", "14", "11");
    view1.sync();
    areViewsConsistent(view1, view2, partitions.size() * fileIdsPerPartition.size());
    SyncableFileSystemView view4 =
        getFileSystemView(new HoodieTableMetaClient(metaClient.getHadoopConf(), metaClient.getBasePath()));
    partitions.stream().forEach(p -> view4.getLatestFileSlices(p).count());
    view4.sync();

    /**
     * Case where a compaction is scheduled, 2 ingestion happens and then a compaction happens
     */
    scheduleCompaction(view2, "16");
    instantsToFiles.putAll(testMultipleWriteSteps(view2, Arrays.asList("17", "18"), true, "16", 2));
    // Compaction
    testMultipleWriteSteps(view2, Arrays.asList("16"), false, "16", 2,
        Arrays.asList(new HoodieInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "18")));
    view1.sync();
    areViewsConsistent(view1, view2, partitions.size() * fileIdsPerPartition.size() * 2);
    SyncableFileSystemView view5 =
        getFileSystemView(new HoodieTableMetaClient(metaClient.getHadoopConf(), metaClient.getBasePath()));
    partitions.stream().forEach(p -> view5.getLatestFileSlices(p).count());
    view5.sync();

    /**
     * Case where a clean happened and then rounds of ingestion and compaction happened
     */
    testCleans(view2, Arrays.asList("19"),
        new ImmutableMap.Builder<String, List<String>>().put("11", Arrays.asList("12", "13", "14")).build(),
        instantsToFiles, Arrays.asList("11"));
    scheduleCompaction(view2, "20");
    instantsToFiles.putAll(testMultipleWriteSteps(view2, Arrays.asList("21", "22"), true, "20", 2));
    // Compaction
    testMultipleWriteSteps(view2, Arrays.asList("20"), false, "20", 2,
        Arrays.asList(new HoodieInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "22")));
    // Run one more round of ingestion
    instantsToFiles.putAll(testMultipleWriteSteps(view2, Arrays.asList("23", "24"), true, "20", 2));
    view1.sync();
    areViewsConsistent(view1, view2, partitions.size() * fileIdsPerPartition.size() * 2);
    SyncableFileSystemView view6 =
        getFileSystemView(new HoodieTableMetaClient(metaClient.getHadoopConf(), metaClient.getBasePath()));
    partitions.stream().forEach(p -> view5.getLatestFileSlices(p).count());
    view6.sync();

    /**
     * Case where multiple restores and ingestions happened
     */
    testRestore(view2, Arrays.asList("25"), true, new HashMap<>(), Arrays.asList("24"), "29", true);
    testRestore(view2, Arrays.asList("26"), true, new HashMap<>(), Arrays.asList("23"), "29", false);
    instantsToFiles.putAll(testMultipleWriteSteps(view2, Arrays.asList("27"), true, "20", 2));
    scheduleCompaction(view2, "28");
    instantsToFiles.putAll(testMultipleWriteSteps(view2, Arrays.asList("29"), true, "28", 3));
    // Compaction
    testMultipleWriteSteps(view2, Arrays.asList("28"), false, "28", 3,
        Arrays.asList(new HoodieInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, "29")));

    Arrays.asList(view1, view2, view3, view4, view5, view6).stream().forEach(v -> {
      v.sync();
      areViewsConsistent(v, view1, partitions.size() * fileIdsPerPartition.size() * 3);
    });
  }

  /*
   ********************************************************************************************************
   * HELPER METHODS
   *********************************************************************************************************
   */
  /**
   * Helper to run one or more rounds of cleaning, incrementally syncing the view and then validate.
   */
  private void testCleans(SyncableFileSystemView view, List<String> newCleanerInstants,
      Map<String, List<String>> instantsToFiles, List<String> cleanedInstants) {
    Map<String, List<String>> deltaInstantMap = cleanedInstants.stream().map(e -> Pair.of(e, new ArrayList()))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    testCleans(view, newCleanerInstants, deltaInstantMap, instantsToFiles, cleanedInstants);
  }

  /**
   * Simulates one of more cleaning, incrementally sync the view and validate the view.
   *
   * @param view Hoodie View
   * @param newCleanerInstants Cleaner Instants
   * @param deltaInstantMap File-Slice Base Instants to Delta Instants
   * @param instantsToFiles List of files associated with each instant
   * @param cleanedInstants List of cleaned instants
   */
  private void testCleans(SyncableFileSystemView view, List<String> newCleanerInstants,
      Map<String, List<String>> deltaInstantMap, Map<String, List<String>> instantsToFiles,
      List<String> cleanedInstants) {
    Assert.assertEquals(newCleanerInstants.size(), cleanedInstants.size());
    long initialFileSlices = partitions.stream().mapToLong(p -> view.getAllFileSlices(p).count()).findAny().getAsLong();
    long exp = initialFileSlices;
    LOG.info("Initial File Slices :" + exp);
    for (int idx = 0; idx < newCleanerInstants.size(); idx++) {
      String instant = cleanedInstants.get(idx);
      try {
        List<String> filesToDelete = new ArrayList<>(instantsToFiles.get(instant));
        deltaInstantMap.get(instant).stream().forEach(n -> filesToDelete.addAll(instantsToFiles.get(n)));

        performClean(view, instant, filesToDelete, newCleanerInstants.get(idx));

        exp -= fileIdsPerPartition.size();
        final long expTotalFileSlicesPerPartition = exp;
        view.sync();
        Assert.assertTrue(view.getLastInstant().isPresent());
        Assert.assertEquals(newCleanerInstants.get(idx), view.getLastInstant().get().getTimestamp());
        Assert.assertEquals(State.COMPLETED, view.getLastInstant().get().getState());
        Assert.assertEquals(HoodieTimeline.CLEAN_ACTION, view.getLastInstant().get().getAction());
        partitions.forEach(p -> {
          LOG.info("PARTTITION : " + p);
          LOG.info("\tFileSlices :" + view.getAllFileSlices(p).collect(Collectors.toList()));
        });

        partitions.forEach(p -> Assert.assertEquals(fileIdsPerPartition.size(), view.getLatestFileSlices(p).count()));
        partitions.forEach(p -> Assert.assertEquals(expTotalFileSlicesPerPartition, view.getAllFileSlices(p).count()));

        metaClient.reloadActiveTimeline();
        SyncableFileSystemView newView = getFileSystemView(metaClient);
        for (String partition : partitions) {
          newView.getAllFileGroups(partition).count();
        }
        areViewsConsistent(view, newView, expTotalFileSlicesPerPartition * partitions.size());
      } catch (IOException e) {
        throw new HoodieException(e);
      }
    }
  }

  /**
   * Simulates one of more restores, incrementally sync the view and validate the view.
   *
   * @param view Hoodie View
   * @param newRestoreInstants Restore Instants
   * @param isDeltaCommit is Delta Commit ?
   * @param instantsToFiles List of files associated with each instant
   * @param rolledBackInstants List of rolled-back instants
   * @param emptyRestoreInstant Restore instant at which dataset becomes empty
   */
  private void testRestore(SyncableFileSystemView view, List<String> newRestoreInstants, boolean isDeltaCommit,
      Map<String, List<String>> instantsToFiles, List<String> rolledBackInstants, String emptyRestoreInstant,
      boolean isRestore) throws IOException {
    Assert.assertEquals(newRestoreInstants.size(), rolledBackInstants.size());
    long initialFileSlices = partitions.stream().mapToLong(p -> view.getAllFileSlices(p).count()).findAny().getAsLong();
    IntStream.range(0, newRestoreInstants.size()).forEach(idx -> {
      String instant = rolledBackInstants.get(idx);
      try {
        performRestore(view, instant, instantsToFiles.get(instant), newRestoreInstants.get(idx), isRestore);
        final long expTotalFileSlicesPerPartition =
            isDeltaCommit ? initialFileSlices : initialFileSlices - ((idx + 1) * fileIdsPerPartition.size());
        view.sync();
        Assert.assertTrue(view.getLastInstant().isPresent());
        LOG.info("Last Instant is :" + view.getLastInstant().get());
        if (isRestore) {
          Assert.assertEquals(newRestoreInstants.get(idx), view.getLastInstant().get().getTimestamp());
          Assert.assertEquals(isRestore ? HoodieTimeline.RESTORE_ACTION : HoodieTimeline.ROLLBACK_ACTION,
              view.getLastInstant().get().getAction());
        }
        Assert.assertEquals(State.COMPLETED, view.getLastInstant().get().getState());

        if (HoodieTimeline.compareTimestamps(newRestoreInstants.get(idx), emptyRestoreInstant,
            HoodieTimeline.GREATER_OR_EQUAL)) {
          partitions.forEach(p -> Assert.assertEquals(0, view.getLatestFileSlices(p).count()));
        } else {
          partitions.forEach(p -> Assert.assertEquals(fileIdsPerPartition.size(), view.getLatestFileSlices(p).count()));
        }
        partitions.forEach(p -> Assert.assertEquals(expTotalFileSlicesPerPartition, view.getAllFileSlices(p).count()));

        metaClient.reloadActiveTimeline();
        SyncableFileSystemView newView = getFileSystemView(metaClient);
        for (String partition : partitions) {
          newView.getAllFileGroups(partition).count();
        }
        areViewsConsistent(view, newView, expTotalFileSlicesPerPartition * partitions.size());
      } catch (IOException e) {
        throw new HoodieException(e);
      }
    });
  }

  /**
   * Simulate a Cleaner operation cleaning up an instant.
   *
   * @param view Hoodie View
   * @param instant Instant to be cleaner
   * @param files List of files to be deleted
   * @param cleanInstant Cleaner Instant
   */
  private void performClean(SyncableFileSystemView view, String instant, List<String> files, String cleanInstant)
      throws IOException {
    Map<String, List<String>> partititonToFiles = deleteFiles(files);
    List<HoodieCleanStat> cleanStats = partititonToFiles.entrySet().stream().map(e -> {
      return new HoodieCleanStat(HoodieCleaningPolicy.KEEP_LATEST_COMMITS, e.getKey(), e.getValue(), e.getValue(),
          new ArrayList<>(), Integer.toString(Integer.parseInt(instant) + 1));
    }).collect(Collectors.toList());

    HoodieCleanMetadata cleanMetadata = CleanerUtils
        .convertCleanMetadata(metaClient, cleanInstant, Option.empty(), cleanStats);
    metaClient.getActiveTimeline().saveAsComplete(new HoodieInstant(true, HoodieTimeline.CLEAN_ACTION, cleanInstant),
        AvroUtils.serializeCleanMetadata(cleanMetadata));
  }

  /**
   * Simulate Restore of an instant in timeline and fsview.
   *
   * @param view Hoodie View
   * @param instant Instant to be rolled-back
   * @param files List of files to be deleted as part of rollback
   * @param rollbackInstant Restore Instant
   */
  private void performRestore(SyncableFileSystemView view, String instant, List<String> files, String rollbackInstant,
      boolean isRestore) throws IOException {
    Map<String, List<String>> partititonToFiles = deleteFiles(files);
    List<HoodieRollbackStat> rollbackStats = partititonToFiles.entrySet().stream().map(e -> {
      return new HoodieRollbackStat(e.getKey(), e.getValue(), new ArrayList<>(), new HashMap<>());
    }).collect(Collectors.toList());

    List<String> rollbacks = new ArrayList<>();
    rollbacks.add(instant);

    HoodieRollbackMetadata rollbackMetadata =
        AvroUtils.convertRollbackMetadata(rollbackInstant, Option.empty(), rollbacks, rollbackStats);
    if (isRestore) {
      HoodieRestoreMetadata metadata = new HoodieRestoreMetadata();

      List<HoodieRollbackMetadata> rollbackM = new ArrayList<>();
      rollbackM.add(rollbackMetadata);
      metadata.setHoodieRestoreMetadata(new ImmutableMap.Builder().put(rollbackInstant, rollbackM).build());
      List<String> rollbackInstants = new ArrayList<>();
      rollbackInstants.add(rollbackInstant);
      metadata.setInstantsToRollback(rollbackInstants);
      metadata.setStartRestoreTime(rollbackInstant);

      metaClient.getActiveTimeline().saveAsComplete(
          new HoodieInstant(true, HoodieTimeline.RESTORE_ACTION, rollbackInstant),
          AvroUtils.serializeRestoreMetadata(metadata));
    } else {
      metaClient.getActiveTimeline().saveAsComplete(
          new HoodieInstant(true, HoodieTimeline.ROLLBACK_ACTION, rollbackInstant),
          AvroUtils.serializeRollbackMetadata(rollbackMetadata));
    }
  }

  /**
   * Utility to delete a list of files and group the deleted files by partitions.
   *
   * @param files List of files to be deleted
   */
  private Map<String, List<String>> deleteFiles(List<String> files) {

    if (null == files) {
      return new HashMap<>();
    }

    Map<String, List<String>> partititonToFiles = new HashMap<>();
    partitions.forEach(p -> partititonToFiles.put(p, new ArrayList<>()));

    for (String f : files) {
      String fullPath = String.format("%s/%s", metaClient.getBasePath(), f);
      new File(fullPath).delete();
      String partition = partitions.stream().filter(p -> f.startsWith(p)).findAny().get();
      partititonToFiles.get(partition).add(fullPath);
    }
    return partititonToFiles;
  }

  /**
   * Schedule a pending compaction and validate.
   *
   * @param view Hoodie View
   * @param instantTime COmpaction Instant Time
   */
  private void scheduleCompaction(SyncableFileSystemView view, String instantTime) throws IOException {
    List<Pair<String, FileSlice>> slices = partitions.stream()
        .flatMap(p -> view.getLatestFileSlices(p).map(s -> Pair.of(p, s))).collect(Collectors.toList());

    long initialExpTotalFileSlices = partitions.stream().mapToLong(p -> view.getAllFileSlices(p).count()).sum();

    HoodieCompactionPlan plan = CompactionUtils.buildFromFileSlices(slices, Option.empty(), Option.empty());
    HoodieInstant compactionInstant = new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, instantTime);
    metaClient.getActiveTimeline().saveToCompactionRequested(compactionInstant,
        AvroUtils.serializeCompactionPlan(plan));

    view.sync();
    partitions.stream().forEach(p -> {
      view.getLatestFileSlices(p).forEach(fs -> {
        Assert.assertEquals(instantTime, fs.getBaseInstantTime());
        Assert.assertEquals(p, fs.getPartitionPath());
        Assert.assertFalse(fs.getDataFile().isPresent());
      });
      view.getLatestMergedFileSlicesBeforeOrOn(p, instantTime).forEach(fs -> {
        Assert
            .assertTrue(HoodieTimeline.compareTimestamps(instantTime, fs.getBaseInstantTime(), HoodieTimeline.GREATER));
        Assert.assertEquals(p, fs.getPartitionPath());
      });
    });

    metaClient.reloadActiveTimeline();
    SyncableFileSystemView newView = getFileSystemView(metaClient);
    partitions.forEach(p -> newView.getLatestFileSlices(p).count());
    areViewsConsistent(view, newView, initialExpTotalFileSlices + partitions.size() * fileIdsPerPartition.size());
  }

  /**
   * Unschedule a compaction instant and validate incremental fs view.
   *
   * @param view Hoodie View
   * @param compactionInstantTime Compaction Instant to be removed
   * @param newLastInstant New Last instant
   * @param newBaseInstant New Base instant of last file-slice
   */
  private void unscheduleCompaction(SyncableFileSystemView view, String compactionInstantTime, String newLastInstant,
      String newBaseInstant) throws IOException {
    HoodieInstant instant = new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, compactionInstantTime);
    boolean deleted = metaClient.getFs().delete(new Path(metaClient.getMetaPath(), instant.getFileName()), false);
    Preconditions.checkArgument(deleted, "Unable to delete compaction instant.");

    view.sync();
    Assert.assertEquals(newLastInstant, view.getLastInstant().get().getTimestamp());
    partitions.stream().forEach(p -> {
      view.getLatestFileSlices(p).forEach(fs -> {
        Assert.assertEquals(newBaseInstant, fs.getBaseInstantTime());
      });
    });
  }

  /**
   * Perform one or more rounds of ingestion/compaction and validate incremental timeline syncing.
   *
   * @param view Hoodie View
   * @param instants Ingestion/Commit INstants
   * @param deltaCommit Delta COmmit ?
   * @param baseInstantForDeltaCommit Base Instant to be used in case of delta-commit
   * @return List of new file created
   */
  private Map<String, List<String>> testMultipleWriteSteps(SyncableFileSystemView view, List<String> instants,
      boolean deltaCommit, String baseInstantForDeltaCommit) throws IOException {
    return testMultipleWriteSteps(view, instants, deltaCommit, baseInstantForDeltaCommit, 1);
  }

  /**
   * Perform one or more rounds of ingestion/compaction and validate incremental timeline syncing.
   *
   * @param view Hoodie View
   * @param instants Ingestion/Commit INstants
   * @param deltaCommit Delta COmmit ?
   * @param baseInstantForDeltaCommit Base Instant to be used in case of delta-commit
   * @param begin initial file-slice offset
   * @return List of new file created
   */
  private Map<String, List<String>> testMultipleWriteSteps(SyncableFileSystemView view, List<String> instants,
      boolean deltaCommit, String baseInstantForDeltaCommit, int begin) throws IOException {
    return testMultipleWriteSteps(view, instants, deltaCommit, baseInstantForDeltaCommit, begin,
        instants.stream()
            .map(i -> new HoodieInstant(State.COMPLETED,
                deltaCommit ? HoodieTimeline.DELTA_COMMIT_ACTION : HoodieTimeline.COMMIT_ACTION, i))
            .collect(Collectors.toList()));
  }

  /**
   * Perform one or more rounds of ingestion/compaction and validate incremental timeline syncing.
   *
   * @param view Hoodie View
   * @param instants Ingestion/Commit INstants
   * @return List of new file created
   */
  private Map<String, List<String>> testMultipleWriteSteps(SyncableFileSystemView view, List<String> instants)
      throws IOException {
    return testMultipleWriteSteps(view, instants, false, null, 1);
  }

  /**
   * Perform one or more rounds of ingestion/compaction and validate incremental timeline syncing.
   *
   * @param view Hoodie View
   * @param instants Ingestion/Commit INstants
   * @param deltaCommit Delta COmmit ?
   * @param baseInstantForDeltaCommit Base Instant to be used in case of delta-commit
   * @param begin initial file-slice offset
   * @param lastInstants List of Last Instants at each time we ingest/compact
   * @return List of new file created
   */
  private Map<String, List<String>> testMultipleWriteSteps(SyncableFileSystemView view, List<String> instants,
      boolean deltaCommit, String baseInstantForDeltaCommit, int begin, List<HoodieInstant> lastInstants)
      throws IOException {
    Map<String, List<String>> instantToFiles = new HashMap<>();

    int multiple = begin;
    for (int idx = 0; idx < instants.size(); idx++) {
      String instant = instants.get(idx);
      LOG.info("Adding instant=" + instant);
      HoodieInstant lastInstant = lastInstants.get(idx);
      // Add a non-empty ingestion to COW table
      List<String> filePaths =
          addInstant(metaClient, instant, deltaCommit, deltaCommit ? baseInstantForDeltaCommit : instant);
      view.sync();
      Assert.assertTrue(view.getLastInstant().isPresent());
      Assert.assertEquals(lastInstant.getTimestamp(), view.getLastInstant().get().getTimestamp());
      Assert.assertEquals(State.COMPLETED, view.getLastInstant().get().getState());
      Assert.assertEquals(
          "Expected Last=" + lastInstant + ", Found Instants="
              + view.getTimeline().getInstants().collect(Collectors.toList()),
          lastInstant.getAction(), view.getLastInstant().get().getAction());
      partitions.forEach(p -> Assert.assertEquals(fileIdsPerPartition.size(), view.getLatestFileSlices(p).count()));
      final long expTotalFileSlicesPerPartition = fileIdsPerPartition.size() * multiple;
      partitions.forEach(p -> Assert.assertEquals(expTotalFileSlicesPerPartition, view.getAllFileSlices(p).count()));
      if (deltaCommit) {
        partitions.forEach(p -> {
          view.getLatestFileSlices(p).forEach(f -> {
            Assert.assertEquals(baseInstantForDeltaCommit, f.getBaseInstantTime());
          });
        });
      } else {
        partitions.forEach(p -> {
          view.getLatestDataFiles(p).forEach(f -> {
            Assert.assertEquals(instant, f.getCommitTime());
          });
        });
      }

      metaClient.reloadActiveTimeline();
      SyncableFileSystemView newView = getFileSystemView(metaClient);
      for (String partition : partitions) {
        newView.getAllFileGroups(partition).count();
      }
      areViewsConsistent(view, newView, fileIdsPerPartition.size() * partitions.size() * multiple);
      instantToFiles.put(instant, filePaths);
      if (!deltaCommit) {
        multiple++;
      }
    }
    return instantToFiles;
  }

  /**
   * Check for equality of views.
   *
   * @param view1 View1
   * @param view2 View2
   */
  private void areViewsConsistent(SyncableFileSystemView view1, SyncableFileSystemView view2,
      long expectedTotalFileSlices) {
    // Timeline check
    HoodieTimeline timeline1 = view1.getTimeline();
    HoodieTimeline timeline2 = view2.getTimeline();
    Assert.assertEquals(view1.getLastInstant(), view2.getLastInstant());
    Iterators.elementsEqual(timeline1.getInstants().iterator(), timeline2.getInstants().iterator());

    // View Checks
    Map<HoodieFileGroupId, HoodieFileGroup> fileGroupsMap1 = partitions.stream().flatMap(p -> view1.getAllFileGroups(p))
        .collect(Collectors.toMap(fg -> fg.getFileGroupId(), fg -> fg));
    Map<HoodieFileGroupId, HoodieFileGroup> fileGroupsMap2 = partitions.stream().flatMap(p -> view2.getAllFileGroups(p))
        .collect(Collectors.toMap(fg -> fg.getFileGroupId(), fg -> fg));
    Assert.assertEquals(fileGroupsMap1.keySet(), fileGroupsMap2.keySet());
    long gotSlicesCount = fileGroupsMap1.keySet().stream()
        .map(k -> Pair.of(fileGroupsMap1.get(k), fileGroupsMap2.get(k))).mapToLong(e -> {
          HoodieFileGroup fg1 = e.getKey();
          HoodieFileGroup fg2 = e.getValue();
          Assert.assertEquals(fg1.getFileGroupId(), fg2.getFileGroupId());
          List<FileSlice> slices1 = fg1.getAllRawFileSlices().collect(Collectors.toList());
          List<FileSlice> slices2 = fg2.getAllRawFileSlices().collect(Collectors.toList());
          Assert.assertEquals(slices1.size(), slices2.size());
          IntStream.range(0, slices1.size()).mapToObj(idx -> Pair.of(slices1.get(idx), slices2.get(idx)))
              .forEach(e2 -> {
                FileSlice slice1 = e2.getKey();
                FileSlice slice2 = e2.getValue();
                Assert.assertEquals(slice1.getBaseInstantTime(), slice2.getBaseInstantTime());
                Assert.assertEquals(slice1.getFileId(), slice2.getFileId());
                Assert.assertEquals(slice1.getDataFile().isPresent(), slice2.getDataFile().isPresent());
                if (slice1.getDataFile().isPresent()) {
                  HoodieDataFile df1 = slice1.getDataFile().get();
                  HoodieDataFile df2 = slice2.getDataFile().get();
                  Assert.assertEquals(df1.getCommitTime(), df2.getCommitTime());
                  Assert.assertEquals(df1.getFileId(), df2.getFileId());
                  Assert.assertEquals(df1.getFileName(), df2.getFileName());
                  Assert.assertEquals(Path.getPathWithoutSchemeAndAuthority(new Path(df1.getPath())),
                      Path.getPathWithoutSchemeAndAuthority(new Path(df2.getPath())));
                }
                List<Path> logPaths1 = slice1.getLogFiles()
                    .map(lf -> Path.getPathWithoutSchemeAndAuthority(lf.getPath())).collect(Collectors.toList());
                List<Path> logPaths2 = slice2.getLogFiles()
                    .map(lf -> Path.getPathWithoutSchemeAndAuthority(lf.getPath())).collect(Collectors.toList());
                Assert.assertEquals(logPaths1, logPaths2);
              });
          return slices1.size();
        }).sum();
    Assert.assertEquals(expectedTotalFileSlices, gotSlicesCount);

    // Pending Compaction Operations Check
    Set<Pair<String, CompactionOperation>> ops1 = view1.getPendingCompactionOperations().collect(Collectors.toSet());
    Set<Pair<String, CompactionOperation>> ops2 = view2.getPendingCompactionOperations().collect(Collectors.toSet());
    Assert.assertEquals(ops1, ops2);
  }

  private List<String> addInstant(HoodieTableMetaClient metaClient, String instant, boolean deltaCommit,
      String baseInstant) throws IOException {
    List<Pair<String, HoodieWriteStat>> writeStats = partitions.stream().flatMap(p -> {
      return fileIdsPerPartition.stream().map(f -> {
        try {
          File file = new File(basePath + "/" + p + "/"
              + (deltaCommit
                  ? FSUtils.makeLogFileName(f, ".log", baseInstant, Integer.parseInt(instant), TEST_WRITE_TOKEN)
                  : FSUtils.makeDataFileName(instant, TEST_WRITE_TOKEN, f)));
          file.createNewFile();
          HoodieWriteStat w = new HoodieWriteStat();
          w.setFileId(f);
          w.setPath(String.format("%s/%s", p, file.getName()));
          return Pair.of(p, w);
        } catch (IOException e) {
          throw new HoodieException(e);
        }
      });
    }).collect(Collectors.toList());

    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    writeStats.forEach(e -> metadata.addWriteStat(e.getKey(), e.getValue()));
    metaClient.getActiveTimeline()
        .saveAsComplete(new HoodieInstant(true,
            deltaCommit ? HoodieTimeline.DELTA_COMMIT_ACTION : HoodieTimeline.COMMIT_ACTION, instant),
            Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    // Delete pending compaction if present
    metaClient.getFs().delete(new Path(metaClient.getMetaPath(),
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, instant).getFileName()));
    return writeStats.stream().map(e -> e.getValue().getPath()).collect(Collectors.toList());
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

}
