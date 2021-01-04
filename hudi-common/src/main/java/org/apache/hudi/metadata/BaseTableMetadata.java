
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

package org.apache.hudi.metadata;

import org.apache.avro.Schema;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public abstract class BaseTableMetadata implements HoodieTableMetadata {

  private static final Logger LOG = LogManager.getLogger(BaseTableMetadata.class);

  static final long MAX_MEMORY_SIZE_IN_BYTES = 1024 * 1024 * 1024;
  static final int BUFFER_SIZE = 10 * 1024 * 1024;

  protected final SerializableConfiguration hadoopConf;
  protected final String datasetBasePath;
  protected boolean enabled;
  protected final Option<HoodieMetadataMetrics> metrics;

  private final boolean validateLookups;
  private final boolean assumeDatePartitioning;

  // Directory used for Spillable Map when merging records
  protected final String spillableMapDirectory;
  private transient HoodieMetadataMergedInstantRecordScanner timelineRecordScanner;

  protected BaseTableMetadata(Configuration hadoopConf, String datasetBasePath, String spillableMapDirectory,
                              boolean enabled, boolean validateLookups, boolean enableMetrics,
                              boolean assumeDatePartitioning) {
    this.hadoopConf = new SerializableConfiguration(hadoopConf);
    this.datasetBasePath = datasetBasePath;
    this.spillableMapDirectory = spillableMapDirectory;

    this.enabled = enabled;
    this.validateLookups = validateLookups;
    this.assumeDatePartitioning = assumeDatePartitioning;

    if (enableMetrics) {
      this.metrics = Option.of(new HoodieMetadataMetrics(Registry.getRegistry("HoodieMetadata")));
    } else {
      this.metrics = Option.empty();
    }
  }

  /**
   * Return the list of partitions in the dataset.
   *
   * If the Metadata Table is enabled, the listing is retrieved from the stored metadata. Otherwise, the list of
   * partitions is retrieved directly from the underlying {@code FileSystem}.
   *
   * On any errors retrieving the listing from the metadata, defaults to using the file system listings.
   *
   */
  @Override
  public List<String> getAllPartitionPaths() throws IOException {
    if (enabled) {
      try {
        return fetchAllPartitionPaths();
      } catch (Exception e) {
        LOG.error("Failed to retrieve list of partition from metadata", e);
      }
    }
    return new FileSystemBackedTableMetadata(hadoopConf, datasetBasePath, assumeDatePartitioning).getAllPartitionPaths();
  }

  /**
   * Return the list of files in a partition.
   *
   * If the Metadata Table is enabled, the listing is retrieved from the stored metadata. Otherwise, the list of
   * partitions is retrieved directly from the underlying {@code FileSystem}.
   *
   * On any errors retrieving the listing from the metadata, defaults to using the file system listings.
   *
   * @param partitionPath The absolute path of the partition to list
   */
  @Override
  public FileStatus[] getAllFilesInPartition(Path partitionPath)
      throws IOException {
    if (enabled) {
      try {
        return fetchAllFilesInPartition(partitionPath);
      } catch (Exception e) {
        LOG.error("Failed to retrieve files in partition " + partitionPath + " from metadata", e);
      }
    }

    return FSUtils.getFs(partitionPath.toString(), hadoopConf.get()).listStatus(partitionPath);
  }

  /**
   * Returns a list of all partitions.
   */
  protected List<String> fetchAllPartitionPaths() throws IOException {
    HoodieTimer timer = new HoodieTimer().startTimer();
    Option<HoodieRecord<HoodieMetadataPayload>> hoodieRecord = getMergedRecordByKey(RECORDKEY_PARTITION_LIST);
    metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.LOOKUP_PARTITIONS_STR, timer.endTimer()));

    List<String> partitions = Collections.emptyList();
    if (hoodieRecord.isPresent()) {
      if (!hoodieRecord.get().getData().getDeletions().isEmpty()) {
        throw new HoodieMetadataException("Metadata partition list record is inconsistent: "
            + hoodieRecord.get().getData());
      }

      partitions = hoodieRecord.get().getData().getFilenames();
      // Partition-less tables have a single empty partition
      if (partitions.contains(NON_PARTITIONED_NAME)) {
        partitions.remove(NON_PARTITIONED_NAME);
        partitions.add("");
      }
    }

    if (validateLookups) {
      // Validate the Metadata Table data by listing the partitions from the file system
      timer.startTimer();
      FileSystemBackedTableMetadata fileSystemBackedTableMetadata = new FileSystemBackedTableMetadata(hadoopConf, datasetBasePath, assumeDatePartitioning);
      List<String> actualPartitions = fileSystemBackedTableMetadata.getAllPartitionPaths();
      metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.VALIDATE_PARTITIONS_STR, timer.endTimer()));

      Collections.sort(actualPartitions);
      Collections.sort(partitions);
      if (!actualPartitions.equals(partitions)) {
        LOG.error("Validation of metadata partition list failed. Lists do not match.");
        LOG.error("Partitions from metadata: " + Arrays.toString(partitions.toArray()));
        LOG.error("Partitions from file system: " + Arrays.toString(actualPartitions.toArray()));

        metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.VALIDATE_ERRORS_STR, 0));
      }

      // Return the direct listing as it should be correct
      partitions = actualPartitions;
    }

    LOG.info("Listed partitions from metadata: #partitions=" + partitions.size());
    return partitions;
  }

  /**
   * Return all the files from the partition.
   *
   * @param partitionPath The absolute path of the partition
   */
  FileStatus[] fetchAllFilesInPartition(Path partitionPath) throws IOException {
    String partitionName = FSUtils.getRelativePartitionPath(new Path(datasetBasePath), partitionPath);
    if (partitionName.isEmpty()) {
      partitionName = NON_PARTITIONED_NAME;
    }

    HoodieTimer timer = new HoodieTimer().startTimer();
    Option<HoodieRecord<HoodieMetadataPayload>> hoodieRecord = getMergedRecordByKey(partitionName);
    metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.LOOKUP_FILES_STR, timer.endTimer()));

    FileStatus[] statuses = {};
    if (hoodieRecord.isPresent()) {
      if (!hoodieRecord.get().getData().getDeletions().isEmpty()) {
        throw new HoodieMetadataException("Metadata record for partition " + partitionName + " is inconsistent: "
            + hoodieRecord.get().getData());
      }
      statuses = hoodieRecord.get().getData().getFileStatuses(partitionPath);
    }

    if (validateLookups) {
      // Validate the Metadata Table data by listing the partitions from the file system
      timer.startTimer();

      // Ignore partition metadata file
      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(hadoopConf.get(), datasetBasePath);
      FileStatus[] directStatuses = metaClient.getFs().listStatus(partitionPath,
          p -> !p.getName().equals(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE));
      metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.VALIDATE_FILES_STR, timer.endTimer()));

      List<String> directFilenames = Arrays.stream(directStatuses)
          .map(s -> s.getPath().getName()).sorted()
          .collect(Collectors.toList());

      List<String> metadataFilenames = Arrays.stream(statuses)
          .map(s -> s.getPath().getName()).sorted()
          .collect(Collectors.toList());

      if (!metadataFilenames.equals(directFilenames)) {
        LOG.error("Validation of metadata file listing for partition " + partitionName + " failed.");
        LOG.error("File list from metadata: " + Arrays.toString(metadataFilenames.toArray()));
        LOG.error("File list from direct listing: " + Arrays.toString(directFilenames.toArray()));

        metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.VALIDATE_ERRORS_STR, 0));
      }

      // Return the direct listing as it should be correct
      statuses = directStatuses;
    }

    LOG.info("Listed file in partition from metadata: partition=" + partitionName + ", #files=" + statuses.length);
    return statuses;
  }

  /**
   * Retrieve the merged {@code HoodieRecord} mapped to the given key.
   *
   * @param key The key of the record
   */
  private Option<HoodieRecord<HoodieMetadataPayload>> getMergedRecordByKey(String key) throws IOException {

    Option<HoodieRecord<HoodieMetadataPayload>> mergedRecord;
    openTimelineScanner();

    Option<HoodieRecord<HoodieMetadataPayload>> metadataHoodieRecord = getRecordByKeyFromMetadata(key);
    // Retrieve record from unsynced timeline instants
    Option<HoodieRecord<HoodieMetadataPayload>> timelineHoodieRecord = timelineRecordScanner.getRecordByKey(key);
    if (timelineHoodieRecord.isPresent()) {
      if (metadataHoodieRecord.isPresent()) {
        HoodieRecordPayload mergedPayload = timelineHoodieRecord.get().getData().preCombine(metadataHoodieRecord.get().getData());
        mergedRecord = Option.of(new HoodieRecord(metadataHoodieRecord.get().getKey(), mergedPayload));
      } else {
        mergedRecord = timelineHoodieRecord;
      }
    } else {
      mergedRecord = metadataHoodieRecord;
    }
    return mergedRecord;
  }

  protected abstract Option<HoodieRecord<HoodieMetadataPayload>> getRecordByKeyFromMetadata(String key) throws IOException;

  private void openTimelineScanner() throws IOException {
    if (timelineRecordScanner != null) {
      // Already opened
      return;
    }

    HoodieTableMetaClient datasetMetaClient = new HoodieTableMetaClient(hadoopConf.get(), datasetBasePath);
    List<HoodieInstant> unSyncedInstants = findInstantsToSync(datasetMetaClient);
    Schema schema = HoodieAvroUtils.addMetadataFields(HoodieMetadataRecord.getClassSchema());
    timelineRecordScanner =
        new HoodieMetadataMergedInstantRecordScanner(datasetMetaClient, unSyncedInstants, getSyncedInstantTime(), schema, MAX_MEMORY_SIZE_IN_BYTES, spillableMapDirectory, null);
  }

  protected List<HoodieInstant> findInstantsToSync() {
    HoodieTableMetaClient datasetMetaClient = new HoodieTableMetaClient(hadoopConf.get(), datasetBasePath);
    return findInstantsToSync(datasetMetaClient);
  }

  protected abstract List<HoodieInstant> findInstantsToSync(HoodieTableMetaClient datasetMetaClient);

  public boolean isInSync() {
    return enabled && findInstantsToSync().isEmpty();
  }

  protected void closeReaders() {
    timelineRecordScanner = null;
  }
}
