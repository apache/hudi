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

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.RECORD_INDEX_AVERAGE_RECORD_SIZE;

/**
 * Checks whether the record-level index is sized appropriately for the table it serves.
 *
 * <p>The number of file groups in the record index is decided once, when the index is initialized,
 * from the record count at that moment and the writer's sizing configuration. It never changes
 * afterwards: every subsequent index write lands in one of those same file groups. A table that has
 * grown well past what its index was sized for therefore has file groups that are each far larger
 * than the writer's configuration intended, which slows every index lookup and concentrates every
 * index write on the same few file groups.
 *
 * <p>So the check re-runs the writer's own sizing estimate against the table as it is now and
 * compares that ideal against the file-group count the index actually has. It also looks at the
 * largest file group directly, since a group past the configured maximum size is the symptom the
 * operator will feel regardless of how the count came out. Both signals share one remedy: the index
 * has to be re-bootstrapped with corrected sizing, which is disruptive and is spelled out in the
 * findings.
 *
 * <p>A partitioned record index is sized per data partition, not as a whole, so for that layout
 * the count comparison is made per data partition and the worst one is reported. Which layout the
 * writer sizes for is read from its configuration and which layout is on storage is read from the
 * index's file-group names; if the two disagree, the count comparison is skipped and the report says
 * so, while the file-group size rule still runs.
 *
 * <p>The record count is estimated from the index's own footprint on storage divided by the same
 * average record size the writer sizes with, so that no data file is scanned. The estimate is
 * conservative -- index files are compressed, so the true count is higher -- and it is labelled as
 * an estimate in the report.
 */
public class RecordIndexSizingCheck implements HealthCheck {

  static final String NAME = "record-index";

  /**
   * Actual-to-ideal file-group ratio below which the index is called undersized. The ideal count
   * already carries the writer's growth factor, so an index initialized on a smaller table sits
   * somewhat below 1.0 during ordinary growth. The slack keeps that from alerting while a table
   * that has grown past what its index was sized for still does.
   */
  static final double MIN_ACCEPTABLE_FILE_GROUP_RATIO = 0.5;

  /**
   * How far past the configured maximum a single file group may grow before the index is called
   * unhealthy. File groups are bounded only at initialization; between metadata compactions, log
   * files push a group past the configured size transiently, so alerting at exactly the maximum
   * would fire on healthy tables.
   */
  static final double MAX_FILE_GROUP_SIZE_SLACK_FACTOR = 1.5;

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String getDescription() {
    return "Whether the record-level index has enough file groups for the table it serves";
  }

  @Override
  public boolean appliesTo(HealthCheckContext context) {
    return context.getMetaClient().getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX);
  }

  @Override
  public HealthCheckResult check(HealthCheckContext context) throws Exception {
    if (!appliesTo(context)) {
      return HealthCheckResult.skipped(NAME, String.format(
          "Table has no record-level index: '%s' in the table configuration does not list '%s'",
          HoodieTableConfig.TABLE_METADATA_PARTITIONS.key(), MetadataPartitionType.RECORD_INDEX.getPartitionPath()));
    }

    TypedProperties props = context.getProps();
    boolean partitionedIndex = ConfigUtils.getBooleanWithAltKeys(props, HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP);
    ConfigProperty<Integer> minCountProp = partitionedIndex
        ? HoodieMetadataConfig.RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP
        : HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP;
    ConfigProperty<Integer> maxCountProp = partitionedIndex
        ? HoodieMetadataConfig.RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP
        : HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP;

    Option<String> missing = firstMissingConfig(context, minCountProp, maxCountProp,
        HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP,
        HoodieMetadataConfig.RECORD_INDEX_GROWTH_FACTOR_PROP);
    if (missing.isPresent()) {
      return context.skipForMissingConfig(NAME, missing.get());
    }

    SizingConfig sizing = new SizingConfig(
        Integer.parseInt(configValue(props, minCountProp)),
        Integer.parseInt(configValue(props, maxCountProp)),
        Float.parseFloat(configValue(props, HoodieMetadataConfig.RECORD_INDEX_GROWTH_FACTOR_PROP)),
        Long.parseLong(configValue(props, HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP)));

    HealthCheckResult.Builder result = HealthCheckResult.builder(NAME)
        .config(HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key(), partitionedIndex)
        .config(minCountProp.key(), sizing.minFileGroupCount)
        .config(maxCountProp.key(), sizing.maxFileGroupCount)
        .config(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP.key(), sizing.maxFileGroupSizeBytes)
        .config(HoodieMetadataConfig.RECORD_INDEX_GROWTH_FACTOR_PROP.key(), sizing.growthFactor)
        .config("effective.average.record.size.bytes", RECORD_INDEX_AVERAGE_RECORD_SIZE);

    if (sizing.maxFileGroupSizeBytes < RECORD_INDEX_AVERAGE_RECORD_SIZE) {
      return result.status(HealthStatus.SKIPPED)
          .summary(String.format(
              "'%s' is %d bytes, smaller than the %d-byte average index record the writer sizes with, so no "
                  + "file-group count can be estimated. The writer cannot size an index with this value either.",
              HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP.key(), sizing.maxFileGroupSizeBytes,
              RECORD_INDEX_AVERAGE_RECORD_SIZE))
          .build();
    }

    RecordIndexFootprint footprint = measureRecordIndex(context.getMetaClient());
    long maxHealthyFileGroupBytes = (long) (MAX_FILE_GROUP_SIZE_SLACK_FACTOR * sizing.maxFileGroupSizeBytes);
    result.config("effective.max.healthy.file.group.bytes", maxHealthyFileGroupBytes)
        .config("observed.file.group.count", footprint.fileGroupCount)
        .config("observed.base.file.count", footprint.baseFileCount)
        .config("observed.log.file.count", footprint.logFileCount)
        .config("observed.total.size.bytes", footprint.totalSizeBytes)
        .config("observed.largest.file.group.bytes", footprint.largestFileGroupBytes)
        .config("observed.largest.file.group.id", footprint.largestFileGroupId);

    Option<CountComparison> comparison = compareFileGroupCount(footprint, partitionedIndex, sizing, result);
    boolean undersized = comparison.isPresent() && comparison.get().isUndersized();
    boolean oversizedFileGroup = footprint.largestFileGroupBytes > maxHealthyFileGroupBytes;

    if (!undersized && !oversizedFileGroup) {
      return result.status(HealthStatus.HEALTHY)
          .summary(comparison.isPresent()
              ? String.format("%s has %d file group(s) against an ideal of %d for an estimated %d records; largest "
                      + "file group is %d bytes", comparison.get().subject(), comparison.get().observedFileGroupCount,
                  comparison.get().idealFileGroupCount, comparison.get().estimatedRecordCount, footprint.largestFileGroupBytes)
              : String.format("Record index has %d file group(s); largest file group is %d bytes, within the "
                      + "configured maximum. File-group count was not compared against the writer's sizing.",
                  footprint.fileGroupCount, footprint.largestFileGroupBytes))
          .build();
    }

    result.status(HealthStatus.UNHEALTHY);
    if (undersized) {
      CountComparison shortfall = comparison.get();
      result.summary(String.format(
              "%s has %d file group(s) but the writer's sizing calls for %d (at least %d to be healthy)",
              shortfall.subject(), shortfall.observedFileGroupCount, shortfall.idealFileGroupCount,
              shortfall.minHealthyFileGroupCount()))
          .finding(String.format(
              "%s was initialized with %d file group(s). Its %d bytes on storage put it at an estimated %d records "
                  + "(an estimate: index bytes divided by the %d-byte average record the writer sizes with), which "
                  + "with the configured growth factor and maximum file-group size calls for %d. Each file group "
                  + "holds roughly %.1fx more keys than intended, so index lookups read larger files and every "
                  + "index write concentrates on the same few file groups.",
              shortfall.subject(), shortfall.observedFileGroupCount, shortfall.observedSizeBytes,
              shortfall.estimatedRecordCount, RECORD_INDEX_AVERAGE_RECORD_SIZE, shortfall.idealFileGroupCount,
              (double) shortfall.idealFileGroupCount / Math.max(shortfall.observedFileGroupCount, 1)));
    }
    if (oversizedFileGroup) {
      if (!undersized) {
        result.summary(String.format(
            "Record index file group %s is %d bytes, past the configured maximum of %d bytes",
            footprint.largestFileGroupId, footprint.largestFileGroupBytes, sizing.maxFileGroupSizeBytes));
      }
      result.finding(String.format(
          "File group %s is %d bytes, past the configured maximum of %d bytes even with %.1fx slack. Compacting "
              + "it takes proportionally longer and every lookup routed to it reads that much more. A file group "
              + "that keeps growing between metadata compactions means the index has too few file groups for "
              + "the table.",
          footprint.largestFileGroupId, footprint.largestFileGroupBytes, sizing.maxFileGroupSizeBytes,
          MAX_FILE_GROUP_SIZE_SLACK_FACTOR));
    }
    result.finding(rebootstrapRemedy());
    return result.build();
  }

  /**
   * Compares the file-group count against the writer's sizing estimate, at the granularity the
   * writer sizes at: the whole index for a global record index, each data partition for a
   * partitioned one (the worst data partition is returned). Echoes what it compared, or why it
   * did not, into the result. Empty when the writer's configured layout disagrees with the layout
   * on storage, since neither estimate would be comparable to the observed count then.
   */
  private static Option<CountComparison> compareFileGroupCount(RecordIndexFootprint footprint, boolean partitionedIndex,
                                                               SizingConfig sizing, HealthCheckResult.Builder result) {
    boolean layoutKnown = footprint.fileGroupCount > 0;
    if (layoutKnown && footprint.partitionedLayout != partitionedIndex) {
      result.config("effective.count.comparison", String.format(
          "skipped: writer configuration says a %s record index but the index on storage is %s",
          partitionedIndex ? "partitioned" : "global", footprint.partitionedLayout ? "partitioned" : "global"));
      return Option.empty();
    }

    CountComparison comparison;
    if (!partitionedIndex) {
      result.config("effective.count.comparison", "whole index (global record index)");
      comparison = CountComparison.of(Option.empty(), footprint, sizing);
    } else {
      result.config("effective.count.comparison", "per data partition (partitioned record index); worst data partition reported")
          .config("observed.data.partition.count", footprint.byDataPartition.size());
      comparison = CountComparison.of(Option.empty(), footprint, sizing);
      for (Map.Entry<String, RecordIndexFootprint> entry : footprint.byDataPartition.entrySet()) {
        CountComparison candidate = CountComparison.of(Option.of(entry.getKey()), entry.getValue(), sizing);
        if (candidate.ratio() < comparison.ratio() || comparison.dataPartition.isEmpty()) {
          comparison = candidate;
        }
      }
      if (comparison.dataPartition.isPresent()) {
        result.config("observed.worst.data.partition", comparison.dataPartition.get())
            .config("observed.worst.data.partition.file.group.count", comparison.observedFileGroupCount);
      }
    }

    result.config("effective.ideal.file.group.count", comparison.idealFileGroupCount)
        .config("effective.min.healthy.file.group.count", comparison.minHealthyFileGroupCount())
        .config("observed.estimated.record.count", comparison.estimatedRecordCount);
    return Option.of(comparison);
  }

  /**
   * The one remedy for a mis-sized record index. The file-group count is fixed at initialization,
   * so there is no in-place resize; the operator has to drop the index and build it again.
   */
  private static String rebootstrapRemedy() {
    return "The file-group count of the record index is fixed when the index is initialized and cannot be "
        + "changed in place. To resize it, re-bootstrap the index: drop it with 'metadata delete-record-index' in "
        + "hudi-cli (or HoodieIndexer --mode dropindex --index-types RECORD_INDEX), correct the sizing "
        + "configuration, then rebuild it with HoodieIndexer --mode scheduleAndExecute --index-types RECORD_INDEX. "
        + "This is disruptive: the record index is unavailable until the rebuild completes, and the drop cannot be "
        + "undone in place. Plan it as a maintenance window.";
  }

  /**
   * Like {@link HealthCheckContext#firstMissingConfig(String...)}, but aware of each property's
   * alternative keys, since the record-index sizing keys were renamed and a writer may still supply
   * the old spelling.
   */
  private static Option<String> firstMissingConfig(HealthCheckContext context, ConfigProperty<?>... properties) {
    if (context.isApplyAllDefaults()) {
      return Option.empty();
    }
    for (ConfigProperty<?> property : properties) {
      if (!ConfigUtils.containsConfigProperty(context.getProps(), property)) {
        return Option.of(property.key());
      }
    }
    return Option.empty();
  }

  private static String configValue(TypedProperties props, ConfigProperty<?> property) {
    return ConfigUtils.getStringWithAltKeys(props, property, String.valueOf(property.defaultValue()));
  }

  /**
   * Sizes the record index from the latest merged file slice of each of its file groups in the
   * metadata table, and per data partition as well when the file-group names show a partitioned
   * layout. The metadata reader is engine-agnostic and closed once the slices are read.
   */
  private static RecordIndexFootprint measureRecordIndex(HoodieTableMetaClient metaClient) throws Exception {
    HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(metaClient.getStorageConf());
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build();
    try (HoodieBackedTableMetadata metadata = new HoodieBackedTableMetadata(
        engineContext, metaClient.getStorage(), metadataConfig, metaClient.getBasePath().toString())) {
      List<FileSlice> fileSlices = metadata.getFilegroupsForPartition(MetadataPartitionType.RECORD_INDEX);
      boolean partitionedLayout = !fileSlices.isEmpty()
          && HoodieTableMetadataUtil.verifyRLIFile(fileSlices.get(0).getFileId(), true);

      Map<String, RecordIndexFootprint> byDataPartition = new TreeMap<>();
      if (partitionedLayout) {
        metadata.getBucketizedFileGroupsForPartitionedRLI(MetadataPartitionType.RECORD_INDEX)
            .forEach((dataPartition, slices) -> byDataPartition.put(dataPartition, RecordIndexFootprint.of(slices)));
      }
      return RecordIndexFootprint.of(fileSlices).withLayout(partitionedLayout, byDataPartition);
    }
  }

  /** The writer's sizing configuration, as supplied or defaulted. */
  private static final class SizingConfig {
    final int minFileGroupCount;
    final int maxFileGroupCount;
    final float growthFactor;
    final long maxFileGroupSizeBytes;

    SizingConfig(int minFileGroupCount, int maxFileGroupCount, float growthFactor, long maxFileGroupSizeBytes) {
      this.minFileGroupCount = minFileGroupCount;
      this.maxFileGroupCount = maxFileGroupCount;
      this.growthFactor = growthFactor;
      this.maxFileGroupSizeBytes = maxFileGroupSizeBytes;
    }
  }

  /** One observed-versus-ideal file-group comparison, for the whole index or one data partition. */
  private static final class CountComparison {
    final Option<String> dataPartition;
    final int observedFileGroupCount;
    final long observedSizeBytes;
    final long estimatedRecordCount;
    final int idealFileGroupCount;

    private CountComparison(Option<String> dataPartition, int observedFileGroupCount, long observedSizeBytes,
                            long estimatedRecordCount, int idealFileGroupCount) {
      this.dataPartition = dataPartition;
      this.observedFileGroupCount = observedFileGroupCount;
      this.observedSizeBytes = observedSizeBytes;
      this.estimatedRecordCount = estimatedRecordCount;
      this.idealFileGroupCount = idealFileGroupCount;
    }

    static CountComparison of(Option<String> dataPartition, RecordIndexFootprint footprint, SizingConfig sizing) {
      long estimatedRecordCount = footprint.totalSizeBytes / RECORD_INDEX_AVERAGE_RECORD_SIZE;
      int idealFileGroupCount = HoodieTableMetadataUtil.estimateFileGroupCount(
          MetadataPartitionType.RECORD_INDEX, () -> estimatedRecordCount, RECORD_INDEX_AVERAGE_RECORD_SIZE,
          sizing.minFileGroupCount, sizing.maxFileGroupCount, sizing.growthFactor, sizing.maxFileGroupSizeBytes);
      return new CountComparison(dataPartition, footprint.fileGroupCount, footprint.totalSizeBytes,
          estimatedRecordCount, idealFileGroupCount);
    }

    int minHealthyFileGroupCount() {
      return (int) Math.ceil(MIN_ACCEPTABLE_FILE_GROUP_RATIO * idealFileGroupCount);
    }

    boolean isUndersized() {
      return observedFileGroupCount < minHealthyFileGroupCount();
    }

    double ratio() {
      return (double) observedFileGroupCount / Math.max(idealFileGroupCount, 1);
    }

    String subject() {
      return dataPartition.map(p -> String.format("Data partition '%s' of the record index", p))
          .orElse("Record index");
    }
  }

  /** What the record index occupies on storage, taken from its latest file slices. */
  private static final class RecordIndexFootprint {
    final int fileGroupCount;
    final int baseFileCount;
    final int logFileCount;
    final long totalSizeBytes;
    final long largestFileGroupBytes;
    final String largestFileGroupId;
    final boolean partitionedLayout;
    final Map<String, RecordIndexFootprint> byDataPartition;

    private RecordIndexFootprint(int fileGroupCount, int baseFileCount, int logFileCount, long totalSizeBytes,
                                 long largestFileGroupBytes, String largestFileGroupId, boolean partitionedLayout,
                                 Map<String, RecordIndexFootprint> byDataPartition) {
      this.fileGroupCount = fileGroupCount;
      this.baseFileCount = baseFileCount;
      this.logFileCount = logFileCount;
      this.totalSizeBytes = totalSizeBytes;
      this.largestFileGroupBytes = largestFileGroupBytes;
      this.largestFileGroupId = largestFileGroupId;
      this.partitionedLayout = partitionedLayout;
      this.byDataPartition = byDataPartition;
    }

    static RecordIndexFootprint of(List<FileSlice> fileSlices) {
      int baseFiles = 0;
      int logFiles = 0;
      long totalBytes = 0;
      long largestBytes = 0;
      String largestId = "";
      for (FileSlice slice : fileSlices) {
        baseFiles += slice.getBaseFile().isPresent() ? 1 : 0;
        logFiles += slice.getLogFileCnt();
        long sliceBytes = slice.getTotalFileSize();
        totalBytes += sliceBytes;
        if (sliceBytes > largestBytes) {
          largestBytes = sliceBytes;
          largestId = slice.getFileId();
        }
      }
      return new RecordIndexFootprint(fileSlices.size(), baseFiles, logFiles, totalBytes, largestBytes, largestId,
          false, new TreeMap<>());
    }

    RecordIndexFootprint withLayout(boolean partitioned, Map<String, RecordIndexFootprint> perDataPartition) {
      return new RecordIndexFootprint(fileGroupCount, baseFileCount, logFileCount, totalSizeBytes,
          largestFileGroupBytes, largestFileGroupId, partitioned, perDataPartition);
    }
  }
}
