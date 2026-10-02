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

package org.apache.hudi.utilities.health;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.FileSystemBackedTableMetadata;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StoragePath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Checks whether the metadata table is caught up with the data table, and whether the file listing
 * it serves agrees with what is actually on storage.
 *
 * <p>Two signals. The first is sync lag: every completed data-table write is mirrored by a delta
 * commit on the metadata table with the same instant time, so the latest completed metadata-table
 * delta commit says how far the metadata table has been brought up to date. A data-table write
 * newer than that is one the metadata table does not know about, and readers relying on the
 * metadata table for listings will not see its files.
 *
 * <p>The second is listing consistency: for a bounded sample of partitions, the latest base files
 * and log files are listed twice -- once through the metadata table and once straight from the
 * file system -- and compared. A disagreement means the metadata table has drifted from storage,
 * which is the one condition that justifies rebuilding it. The sample is bounded because listing a
 * partition from the file system is precisely the expensive operation the metadata table exists to
 * avoid; a daily health check must not turn into a full table scan. It is weighted towards the
 * most recently written partitions, where drift actually originates, with a couple of the oldest
 * added for coverage. For a complete comparison the finding points at
 * {@code HoodieMetadataTableValidator}.
 *
 * <p>Engine-agnostic: both listing backends and the file-system views are built on
 * {@link HoodieLocalEngineContext}, so no Spark session is needed.
 */
public class MetadataTableSyncCheck implements HealthCheck {

  static final String NAME = "mdt-sync";

  /**
   * Upper bound on partitions whose listings are compared. Each sampled partition costs one
   * file-system listing, which is the operation the metadata table exists to avoid, so the sample
   * is kept small enough to be cheap on a schedule while still catching drift that affects the
   * table broadly rather than a single partition.
   *
   * <p>The sample is taken from the <em>end</em> of the sorted partition list. Drift comes from
   * recent writes, cleans and rollbacks, so the partitions most likely to disagree are the ones
   * most recently written -- and on the common date-partitioned table, lexical order is time
   * order, so the last partitions are the newest. Sampling the first ones would compare the
   * partitions least likely to have changed.
   */
  static final int MAX_PARTITIONS_TO_COMPARE = 20;

  /**
   * How many of the oldest partitions are added to the sample when the table has more partitions
   * than {@link #MAX_PARTITIONS_TO_COMPARE}, so that drift confined to old data (a bad clean, a
   * partition dropped by hand) is not invisible to a check that otherwise looks only at new data.
   */
  static final int OLDEST_PARTITIONS_TO_INCLUDE = 2;

  /**
   * How many differing file names to spell out per partition before truncating. Enough to identify
   * the pattern (one file, one file group, everything) without flooding the report.
   */
  static final int MAX_DIFFERING_FILES_TO_NAME = 5;

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String getDescription() {
    return "Whether the metadata table is caught up with the data table and its file listing matches storage";
  }

  @Override
  public HealthCheckResult check(HealthCheckContext context) throws Exception {
    Option<String> missing = context.firstMissingConfig(HoodieMetadataConfig.ENABLE.key());
    if (missing.isPresent()) {
      return context.skipForMissingConfig(NAME, missing.get());
    }

    boolean metadataEnabled = Boolean.parseBoolean(context.getProps().getString(
        HoodieMetadataConfig.ENABLE.key(), String.valueOf(HoodieMetadataConfig.ENABLE.defaultValue())));
    if (!metadataEnabled) {
      return HealthCheckResult.skipped(NAME, "Metadata table is disabled for this writer ("
          + HoodieMetadataConfig.ENABLE.key() + "=false), so there is nothing to compare against");
    }

    HoodieTableMetaClient metaClient = context.getMetaClient();
    Option<String> noMetadataTable = MetadataTablePresence.reasonAbsent(metaClient);
    if (noMetadataTable.isPresent()) {
      return HealthCheckResult.skipped(NAME, noMetadataTable.get());
    }

    HealthCheckResult.Builder result = HealthCheckResult.builder(NAME)
        .config(HoodieMetadataConfig.ENABLE.key(), true)
        .config("effective.partitions.sampled.max", MAX_PARTITIONS_TO_COMPARE);

    HoodieEngineContext engineContext = new HoodieLocalEngineContext(metaClient.getStorageConf());
    String basePath = metaClient.getBasePath().toString();
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build();

    try (HoodieTableMetadata metadataBacked =
             new HoodieBackedTableMetadata(engineContext, metaClient.getStorage(), metadataConfig, basePath);
         HoodieTableMetadata fileSystemBacked =
             new FileSystemBackedTableMetadata(engineContext, metaClient.getTableConfig(), metaClient.getStorage(), basePath)) {

      SyncLag lag = measureSyncLag(metaClient, metadataBacked);
      lag.report(result);

      ListingComparison listing = compareSampledPartitions(metaClient, metadataBacked, fileSystemBacked);
      listing.report(result);

      return verdict(result, lag, listing);
    }
  }

  private HealthCheckResult verdict(HealthCheckResult.Builder result, SyncLag lag, ListingComparison listing) {
    boolean behind = lag.commitsBehind > 0;
    boolean inconsistent = !listing.differences.isEmpty();

    if (!behind && !inconsistent) {
      return result.status(HealthStatus.HEALTHY)
          .summary(String.format("Metadata table is synced to %s and its listing matches storage in all %d sampled partition(s)",
              lag.syncedInstant.orElse("(no data-table writes yet)"), listing.partitionsSampled))
          .build();
    }

    if (behind) {
      result.finding(String.format(
          "The metadata table is synced to %s but the data table's latest completed write is %s; %d completed "
              + "data-table write(s) are missing from the metadata table. Readers that list through the "
              + "metadata table will not see files from those writes.",
          lag.syncedInstant.orElse("nothing"), lag.latestDataInstant.orElse("nothing"), lag.commitsBehind));
      result.finding("If a writer is running right now, this can be a write that is still being finalized: "
          + "rerun once it completes. If the metadata table stays behind, look for failed or incomplete "
          + "metadata-table delta commits ('metadata timeline show incomplete' in hudi-cli) and for "
          + "metadata-table errors in the writer logs. Do not delete or rebuild the metadata table for "
          + "lag alone; the next successful write brings it up to date.");
    }

    for (PartitionDifference difference : listing.differences) {
      result.finding(difference.describe());
    }
    if (inconsistent) {
      result.finding("The metadata table's listing has drifted from storage. Confirm the extent with the full "
          + "comparison before acting: run HoodieMetadataTableValidator (spark-submit, hudi-utilities) with "
          + "--validate-latest-file-slices --validate-latest-base-files. If it confirms the drift, the remedy "
          + "is to rebuild the metadata table by disabling it for one write and re-enabling it; that is "
          + "disruptive (a full listing and re-indexing of the table) and should be scheduled deliberately.");
    }

    String summary = inconsistent
        ? String.format("Metadata table listing differs from storage in %d of %d sampled partition(s)%s",
            listing.differences.size(), listing.partitionsSampled,
            behind ? ", and it is " + lag.commitsBehind + " write(s) behind" : "")
        : String.format("Metadata table is %d completed data-table write(s) behind (synced to %s, latest write %s)",
            lag.commitsBehind, lag.syncedInstant.orElse("nothing"), lag.latestDataInstant.orElse("nothing"));
    return result.status(HealthStatus.UNHEALTHY).summary(summary).build();
  }

  private SyncLag measureSyncLag(HoodieTableMetaClient metaClient, HoodieTableMetadata metadataBacked) {
    HoodieTimeline completedDataWrites = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    Option<String> latestDataInstant = completedDataWrites.lastInstant().map(HoodieInstant::requestedTime);
    Option<String> syncedInstant = metadataBacked.getSyncedInstantTime();

    int commitsBehind;
    if (!latestDataInstant.isPresent()) {
      commitsBehind = 0;
    } else if (!syncedInstant.isPresent()) {
      commitsBehind = completedDataWrites.countInstants();
    } else {
      commitsBehind = completedDataWrites.findInstantsAfter(syncedInstant.get()).countInstants();
    }
    return new SyncLag(latestDataInstant, syncedInstant, commitsBehind);
  }

  /**
   * Compares the latest base-file and log-file names per sampled partition between the two listing
   * backends. Both views are opened over the same completed-commits timeline so that a write landing
   * mid-check cannot show up as a difference.
   */
  private ListingComparison compareSampledPartitions(HoodieTableMetaClient metaClient,
                                                    HoodieTableMetadata metadataBacked,
                                                    HoodieTableMetadata fileSystemBacked) throws IOException {
    List<String> sampled = samplePartitions(metadataBacked.getAllPartitionPaths());

    HoodieTimeline visibleTimeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    List<PartitionDifference> differences = new ArrayList<>();
    try (HoodieTableFileSystemView metadataView = new HoodieTableFileSystemView(metadataBacked, metaClient, visibleTimeline);
         HoodieTableFileSystemView fileSystemView = new HoodieTableFileSystemView(fileSystemBacked, metaClient, visibleTimeline)) {
      for (String partition : sampled) {
        PartitionDifference difference = new PartitionDifference(partition,
            latestBaseFileNames(metadataView, partition), latestBaseFileNames(fileSystemView, partition),
            latestLogFileNames(metadataView, partition), latestLogFileNames(fileSystemView, partition));
        if (difference.isPresent()) {
          differences.add(difference);
        }
      }
    }
    return new ListingComparison(sampled.size(), differences);
  }

  /**
   * The partitions to compare: all of them when there are few, otherwise the newest
   * {@link #MAX_PARTITIONS_TO_COMPARE} in sorted order plus the {@link #OLDEST_PARTITIONS_TO_INCLUDE}
   * oldest. Sorted, so the result is stable from one run to the next.
   */
  static List<String> samplePartitions(List<String> allPartitions) {
    List<String> sorted = new ArrayList<>(allPartitions);
    Collections.sort(sorted);
    if (sorted.size() <= MAX_PARTITIONS_TO_COMPARE) {
      return sorted;
    }
    List<String> sampled = new ArrayList<>(sorted.subList(0, OLDEST_PARTITIONS_TO_INCLUDE));
    sampled.addAll(sorted.subList(sorted.size() - MAX_PARTITIONS_TO_COMPARE, sorted.size()));
    return sampled;
  }

  private static Set<String> latestBaseFileNames(HoodieTableFileSystemView view, String partition) {
    return view.getLatestBaseFiles(partition).map(HoodieBaseFile::getFileName).collect(Collectors.toCollection(TreeSet::new));
  }

  private static Set<String> latestLogFileNames(HoodieTableFileSystemView view, String partition) {
    return view.getLatestFileSlices(partition).flatMap(FileSlice::getLogFiles)
        .map(HoodieLogFile::getFileName).collect(Collectors.toCollection(TreeSet::new));
  }

  private static final class SyncLag {
    private final Option<String> latestDataInstant;
    private final Option<String> syncedInstant;
    private final int commitsBehind;

    private SyncLag(Option<String> latestDataInstant, Option<String> syncedInstant, int commitsBehind) {
      this.latestDataInstant = latestDataInstant;
      this.syncedInstant = syncedInstant;
      this.commitsBehind = commitsBehind;
    }

    private void report(HealthCheckResult.Builder result) {
      result.config("observed.data.table.latest.completed.instant", latestDataInstant.orElse("none"))
          .config("observed.mdt.synced.instant", syncedInstant.orElse("none"))
          .config("observed.data.table.writes.behind", commitsBehind);
    }
  }

  private static final class ListingComparison {
    private final int partitionsSampled;
    private final List<PartitionDifference> differences;

    private ListingComparison(int partitionsSampled, List<PartitionDifference> differences) {
      this.partitionsSampled = partitionsSampled;
      this.differences = differences;
    }

    private void report(HealthCheckResult.Builder result) {
      result.config("observed.partitions.sampled", partitionsSampled)
          .config("observed.partitions.inconsistent", differences.size());
    }
  }

  /** What one partition's two listings disagree on, if anything. */
  private static final class PartitionDifference {
    private final String partition;
    private final Set<String> baseFilesOnlyInMetadata;
    private final Set<String> baseFilesOnlyOnStorage;
    private final Set<String> logFilesOnlyInMetadata;
    private final Set<String> logFilesOnlyOnStorage;

    private PartitionDifference(String partition, Set<String> metadataBaseFiles, Set<String> storageBaseFiles,
                                Set<String> metadataLogFiles, Set<String> storageLogFiles) {
      this.partition = partition;
      this.baseFilesOnlyInMetadata = difference(metadataBaseFiles, storageBaseFiles);
      this.baseFilesOnlyOnStorage = difference(storageBaseFiles, metadataBaseFiles);
      this.logFilesOnlyInMetadata = difference(metadataLogFiles, storageLogFiles);
      this.logFilesOnlyOnStorage = difference(storageLogFiles, metadataLogFiles);
    }

    private static Set<String> difference(Set<String> left, Set<String> right) {
      Set<String> onlyInLeft = new TreeSet<>(left);
      onlyInLeft.removeAll(right);
      return onlyInLeft;
    }

    private boolean isPresent() {
      return !baseFilesOnlyInMetadata.isEmpty() || !baseFilesOnlyOnStorage.isEmpty()
          || !logFilesOnlyInMetadata.isEmpty() || !logFilesOnlyOnStorage.isEmpty();
    }

    private String describe() {
      StringBuilder sb = new StringBuilder(String.format("Partition '%s': ", partition.isEmpty() ? "(non-partitioned)" : partition));
      appendIfAny(sb, "base files listed by the metadata table but absent on storage", baseFilesOnlyInMetadata);
      appendIfAny(sb, "base files on storage but missing from the metadata table", baseFilesOnlyOnStorage);
      appendIfAny(sb, "log files listed by the metadata table but absent on storage", logFilesOnlyInMetadata);
      appendIfAny(sb, "log files on storage but missing from the metadata table", logFilesOnlyOnStorage);
      return sb.toString();
    }

    private static void appendIfAny(StringBuilder sb, String label, Set<String> files) {
      if (files.isEmpty()) {
        return;
      }
      List<String> named = files.stream().limit(MAX_DIFFERING_FILES_TO_NAME).collect(Collectors.toList());
      String more = files.size() > named.size() ? String.format(" (+%d more)", files.size() - named.size()) : "";
      sb.append(String.format("%d %s %s%s. ", files.size(), label, named, more));
    }
  }

  /**
   * Whether a metadata table exists for the data table. Shared by the metadata-table checks so
   * they skip for the same reasons in the same words.
   */
  static final class MetadataTablePresence {

    private MetadataTablePresence() {
    }

    /** Empty when the metadata table is present; otherwise the reason to skip, for the operator. */
    static Option<String> reasonAbsent(HoodieTableMetaClient metaClient) throws IOException {
      if (!metaClient.getTableConfig().isMetadataTableAvailable()) {
        return Option.of("Table has no metadata table (its FILES partition has not been initialized)");
      }
      StoragePath metadataTablePath = HoodieTableMetadata.getMetadataTableBasePath(metaClient.getBasePath());
      if (!metaClient.getStorage().exists(metadataTablePath)) {
        return Option.of("Metadata table is registered in hoodie.properties but " + metadataTablePath
            + " does not exist on storage; the next write will re-initialize it");
      }
      return Option.empty();
    }
  }
}
