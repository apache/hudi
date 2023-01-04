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

package org.apache.hudi.metadata;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;

import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalTypes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.metadata.HoodieTableMetadata.EMPTY_PARTITION_NAME;
import static org.apache.hudi.metadata.HoodieTableMetadata.NON_PARTITIONED_NAME;

/**
 * Metadata common utils.
 */
public class HoodieMetadataCommonUtils {
  private static final Logger LOG = LogManager.getLogger(HoodieMetadataCommonUtils.class);

  public static final String PARTITION_NAME_FILES = "files";
  public static final String PARTITION_NAME_COLUMN_STATS = "column_stats";
  public static final String PARTITION_NAME_BLOOM_FILTERS = "bloom_filters";
  public static final String PARTITION_NAME_RECORD_INDEX = "record_index";

  // Virtual keys support for metadata table. This Field is
  // from the metadata payload schema.
  public static final String RECORD_KEY_FIELD_NAME = HoodieMetadataPayload.KEY_FIELD_NAME;

  /**
   * Returns whether the files partition of metadata table is ready for read.
   *
   * @param metaClient {@link HoodieTableMetaClient} instance.
   * @return true if the files partition of metadata table is ready for read,
   * based on the table config; false otherwise.
   */
  public static boolean isFilesPartitionAvailable(HoodieTableMetaClient metaClient) {
    return metaClient.getTableConfig().getMetadataPartitions()
        .contains(HoodieMetadataCommonUtils.PARTITION_NAME_FILES);
  }

  /**
   * Returns a {@code HoodieTableMetaClient} for the metadata table.
   *
   * @param datasetMetaClient {@code HoodieTableMetaClient} for the dataset.
   * @return {@code HoodieTableMetaClient} for the metadata table.
   */
  public static HoodieTableMetaClient getMetadataTableMetaClient(HoodieTableMetaClient datasetMetaClient) {
    final String metadataBasePath = HoodieTableMetadata.getMetadataTableBasePath(datasetMetaClient.getBasePath());
    return HoodieTableMetaClient.builder().setBasePath(metadataBasePath).setConf(datasetMetaClient.getHadoopConf())
        .build();
  }

  /**
   * Get the latest file slices for a Metadata Table partition. If the file slice is
   * because of pending compaction instant, then merge the file slice with the one
   * just before the compaction instant time. The list of file slices returned is
   * sorted in the correct order of file group name.
   *
   * @param metaClient - Instance of {@link HoodieTableMetaClient}.
   * @param fsView instance of {@link HoodieTableFileSystemView}.
   * @param partition  - The name of the partition whose file groups are to be loaded.
   * @return List of latest file slices for all file groups in a given partition.
   */
  public static List<FileSlice> getPartitionLatestMergedFileSlices(HoodieTableMetaClient metaClient, HoodieTableFileSystemView fsView, String partition) {
    LOG.info("Loading latest merged file slices for metadata table partition " + partition);
    return getPartitionFileSlices(metaClient, Option.of(fsView), partition, true);
  }

  /**
   * Get the latest file slices for a Metadata Table partition. The list of file slices
   * returned is sorted in the correct order of file group name.
   *
   * @param metaClient - Instance of {@link HoodieTableMetaClient}.
   * @param fsView     - Metadata table filesystem view
   * @param partition  - The name of the partition whose file groups are to be loaded.
   * @return List of latest file slices for all file groups in a given partition.
   */
  public static List<FileSlice> getPartitionLatestFileSlices(HoodieTableMetaClient metaClient,
                                                             Option<HoodieTableFileSystemView> fsView, String partition) {
    LOG.info("Loading latest file slices for metadata table partition " + partition);
    return getPartitionFileSlices(metaClient, fsView, partition, false);
  }

  /**
   * Get metadata table file system view.
   *
   * @param metaClient - Metadata table meta client
   * @return Filesystem view for the metadata table
   */
  public static HoodieTableFileSystemView getFileSystemView(HoodieTableMetaClient metaClient) {
    // If there are no commits on the metadata table then the table's
    // default FileSystemView will not return any file slices even
    // though we may have initialized them.
    HoodieTimeline timeline = metaClient.getActiveTimeline();
    if (timeline.empty()) {
      final HoodieInstant instant = new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION,
          HoodieActiveTimeline.createNewInstantTime());
      timeline = new HoodieDefaultTimeline(Stream.of(instant), metaClient.getActiveTimeline()::getInstantDetails);
    }
    return new HoodieTableFileSystemView(metaClient, timeline);
  }

  /**
   * Get the latest file slices for a given partition.
   *
   * @param metaClient      - Instance of {@link HoodieTableMetaClient}.
   * @param partition       - The name of the partition whose file groups are to be loaded.
   * @param mergeFileSlices - When enabled, will merge the latest file slices with the last known
   *                        completed instant. This is useful for readers when there are pending
   *                        compactions. MergeFileSlices when disabled, will return the latest file
   *                        slices without any merging, and this is needed for the writers.
   * @return List of latest file slices for all file groups in a given partition.
   */
  private static List<FileSlice> getPartitionFileSlices(HoodieTableMetaClient metaClient,
                                                        Option<HoodieTableFileSystemView> fileSystemView,
                                                        String partition,
                                                        boolean mergeFileSlices) {
    HoodieTableFileSystemView fsView = fileSystemView.orElse(getFileSystemView(metaClient));
    Stream<FileSlice> fileSliceStream;
    if (mergeFileSlices) {
      if (metaClient.getActiveTimeline().filterCompletedInstants().lastInstant().isPresent()) {
        fileSliceStream = fsView.getLatestMergedFileSlicesBeforeOrOn(
            partition, metaClient.getActiveTimeline().filterCompletedInstants().lastInstant().get().getTimestamp());
      } else {
        return Collections.EMPTY_LIST;
      }
    } else {
      fileSliceStream = fsView.getLatestFileSlices(partition);
    }
    return fileSliceStream.sorted(Comparator.comparing(FileSlice::getFileId)).collect(Collectors.toList());
  }

  /**
   * Get the latest file slices for a given partition including the inflight ones.
   *
   * @param metaClient     - instance of {@link HoodieTableMetaClient}
   * @param fileSystemView - hoodie table file system view, which will be fetched from meta client if not already present
   * @param partition      - name of the partition whose file groups are to be loaded
   * @return
   */
  public static List<FileSlice> getPartitionLatestFileSlicesIncludingInflight(HoodieTableMetaClient metaClient,
                                                                              Option<HoodieTableFileSystemView> fileSystemView,
                                                                              String partition) {
    HoodieTableFileSystemView fsView = fileSystemView.orElse(getFileSystemView(metaClient));
    Stream<FileSlice> fileSliceStream = fsView.fetchLatestFileSlicesIncludingInflight(partition);
    return fileSliceStream
        .sorted(Comparator.comparing(FileSlice::getFileId))
        .collect(Collectors.toList());
  }

  /**
   * Map a record key to a file group in partition of interest.
   * <p>
   * Note: For hashing, the algorithm is same as String.hashCode() but is being defined here as hashCode()
   * implementation is not guaranteed by the JVM to be consistent across JVM versions and implementations.
   *
   * @param recordKey record key for which the file group index is looked up for.
   * @return An integer hash of the given string
   */
  public static int mapRecordKeyToFileGroupIndex(String recordKey, int numFileGroups) {
    int h = 0;
    for (int i = 0; i < recordKey.length(); ++i) {
      h = 31 * h + recordKey.charAt(i);
    }

    return Math.abs(Math.abs(h) % numFileGroups);
  }

  /**
   * Maps record key to spark partition.
   * if numFileGroups > shuffleParallelism, total spark partitions = numFileGroups.
   * else total spark partitions = shuffleParallelism.
   * In latter case, we first get the hash of record key and mod w/ numFileGroups and add a random value so as to match total spark partitions.
   * for eg, if numFileGroups = 10 and shuffle parallelism = 200,
   * records mapped to FG_1 will be spread across spark partitions 0 to 19.
   * records mapped to FG_2 will be spread across spark partitions 20 to 39, and so on.
   * @param recordKey record key for which spark partition is expected.
   * @param numFileGroups num of file groups in record index.
   * @param shuffleParallelism shuffle parallelism to use.
   * @param random instance of {@link Random} to use to generate random spark partition.
   * @return the integer representing spark partition.
   */
  public static int mapRecordKeyToSparkPartition(String recordKey, int numFileGroups, int shuffleParallelism, Random random) {
    int h = 0;
    for (int i = 0; i < recordKey.length(); ++i) {
      h = 31 * h + recordKey.charAt(i);
    }

    int recordKeyHash = Math.abs(Math.abs(h) % numFileGroups);
    if (shuffleParallelism <= numFileGroups) {
      return recordKeyHash;
    }
    int numSparkPartitionsPerFileGroup = shuffleParallelism / numFileGroups;
    int randomDelta = random.nextInt(numSparkPartitionsPerFileGroup);
    return randomDelta + (recordKeyHash * numSparkPartitionsPerFileGroup);
  }

  /**
   * Returns partition name for the given path.
   */
  public static String getPartitionIdentifier(@Nonnull String relativePartitionPath) {
    return EMPTY_PARTITION_NAME.equals(relativePartitionPath) ? NON_PARTITIONED_NAME : relativePartitionPath;
  }

  /**
   * Does an upcast for {@link BigDecimal} instance to align it with scale/precision expected by
   * the {@link org.apache.avro.LogicalTypes.Decimal} Avro logical type
   */
  public static BigDecimal tryUpcastDecimal(BigDecimal value, final LogicalTypes.Decimal decimal) {
    final int scale = decimal.getScale();
    final int valueScale = value.scale();

    boolean scaleAdjusted = false;
    if (valueScale != scale) {
      try {
        value = value.setScale(scale, RoundingMode.UNNECESSARY);
        scaleAdjusted = true;
      } catch (ArithmeticException aex) {
        throw new AvroTypeException(
            "Cannot encode decimal with scale " + valueScale + " as scale " + scale + " without rounding");
      }
    }

    int precision = decimal.getPrecision();
    int valuePrecision = value.precision();
    if (valuePrecision > precision) {
      if (scaleAdjusted) {
        throw new AvroTypeException("Cannot encode decimal with precision " + valuePrecision + " as max precision "
            + precision + ". This is after safely adjusting scale from " + valueScale + " to required " + scale);
      } else {
        throw new AvroTypeException(
            "Cannot encode decimal with precision " + valuePrecision + " as max precision " + precision);
      }
    }

    return value;
  }
}
