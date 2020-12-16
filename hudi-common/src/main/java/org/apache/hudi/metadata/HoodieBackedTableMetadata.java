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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Table metadata provided by an internal DFS backed Hudi metadata table.
 *
 * If the metadata table does not exist, RPC calls are used to retrieve file listings from the file system.
 * No updates are applied to the table and it is not synced.
 */
public class HoodieBackedTableMetadata implements HoodieTableMetadata {

  private static final Logger LOG = LogManager.getLogger(HoodieBackedTableMetadata.class);
  private static final long MAX_MEMORY_SIZE_IN_BYTES = 1024 * 1024 * 1024;
  private static final int BUFFER_SIZE = 10 * 1024 * 1024;

  private final SerializableConfiguration hadoopConf;
  private final String datasetBasePath;
  private final String metadataBasePath;
  private final Option<HoodieMetadataMetrics> metrics;
  private HoodieTableMetaClient metaClient;

  private boolean enabled;
  private final boolean validateLookups;
  private final boolean assumeDatePartitioning;
  // Directory used for Spillable Map when merging records
  private final String spillableMapDirectory;

  // Readers for the base and log file which store the metadata
  private transient HoodieFileReader<GenericRecord> basefileReader;
  private transient HoodieMetadataMergedLogRecordScanner logRecordScanner;

  public HoodieBackedTableMetadata(Configuration conf, String datasetBasePath, String spillableMapDirectory,
                                   boolean enabled, boolean validateLookups, boolean assumeDatePartitioning) {
    this(conf, datasetBasePath, spillableMapDirectory, enabled, validateLookups, false, assumeDatePartitioning);
  }

  public HoodieBackedTableMetadata(Configuration conf, String datasetBasePath, String spillableMapDirectory,
                                   boolean enabled, boolean validateLookups, boolean enableMetrics,
                                   boolean assumeDatePartitioning) {
    this.hadoopConf = new SerializableConfiguration(conf);
    this.datasetBasePath = datasetBasePath;
    this.metadataBasePath = HoodieTableMetadata.getMetadataTableBasePath(datasetBasePath);
    this.validateLookups = validateLookups;
    this.spillableMapDirectory = spillableMapDirectory;
    this.enabled = enabled;
    this.assumeDatePartitioning = assumeDatePartitioning;

    if (enabled) {
      try {
        this.metaClient = new HoodieTableMetaClient(hadoopConf.get(), metadataBasePath);
      } catch (TableNotFoundException e) {
        LOG.error("Metadata table was not found at path " + metadataBasePath);
        this.enabled = false;
      } catch (Exception e) {
        LOG.error("Failed to initialize metadata table at path " + metadataBasePath, e);
        this.enabled = false;
      }
    } else {
      LOG.info("Metadata table is disabled.");
    }

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
  public List<String> getAllPartitionPaths()
      throws IOException {
    if (enabled) {
      try {
        return fetchAllPartitionPaths();
      } catch (Exception e) {
        LOG.error("Failed to retrieve list of partition from metadata", e);
      }
    }

    FileSystem fs = FSUtils.getFs(datasetBasePath, hadoopConf.get());
    return FSUtils.getAllPartitionPaths(fs, datasetBasePath, assumeDatePartitioning);
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
        LOG.error("Failed to retrive files in partition " + partitionPath + " from metadata", e);
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
      List<String> actualPartitions  = FSUtils.getAllPartitionPaths(metaClient.getFs(), datasetBasePath, false);
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
    openBaseAndLogFiles();

    // Retrieve record from base file
    HoodieRecord<HoodieMetadataPayload> hoodieRecord = null;
    if (basefileReader != null) {
      HoodieTimer timer = new HoodieTimer().startTimer();
      Option<GenericRecord> baseRecord = basefileReader.getRecordByKey(key);
      if (baseRecord.isPresent()) {
        hoodieRecord = SpillableMapUtils.convertToHoodieRecordPayload(baseRecord.get(),
            metaClient.getTableConfig().getPayloadClass());
        metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.BASEFILE_READ_STR, timer.endTimer()));
      }
    }

    // Retrieve record from log file
    Option<HoodieRecord<HoodieMetadataPayload>> logHoodieRecord = logRecordScanner.getRecordByKey(key);
    if (logHoodieRecord.isPresent()) {
      if (hoodieRecord != null) {
        // Merge the payloads
        HoodieRecordPayload mergedPayload = logHoodieRecord.get().getData().preCombine(hoodieRecord.getData());
        hoodieRecord = new HoodieRecord(hoodieRecord.getKey(), mergedPayload);
      } else {
        hoodieRecord = logHoodieRecord.get();
      }
    }

    return Option.ofNullable(hoodieRecord);
  }

  /**
   * Open readers to the base and log files.
   */
  private synchronized void openBaseAndLogFiles() throws IOException {
    if (logRecordScanner != null) {
      // Already opened
      return;
    }

    HoodieTimer timer = new HoodieTimer().startTimer();

    // Metadata is in sync till the latest completed instant on the dataset
    HoodieTableMetaClient datasetMetaClient = new HoodieTableMetaClient(hadoopConf.get(), datasetBasePath);
    String latestInstantTime = datasetMetaClient.getActiveTimeline().filterCompletedInstants().lastInstant()
        .map(HoodieInstant::getTimestamp).orElse(SOLO_COMMIT_TIMESTAMP);

    // Find the latest file slice
    HoodieTimeline timeline = metaClient.reloadActiveTimeline();
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline());
    List<FileSlice> latestSlices = fsView.getLatestFileSlices(MetadataPartitionType.FILES.partitionPath()).collect(Collectors.toList());
    ValidationUtils.checkArgument(latestSlices.size() == 1);

    // If the base file is present then create a reader
    Option<HoodieBaseFile> basefile = latestSlices.get(0).getBaseFile();
    if (basefile.isPresent()) {
      String basefilePath = basefile.get().getPath();
      basefileReader = HoodieFileReaderFactory.getFileReader(hadoopConf.get(), new Path(basefilePath));
      LOG.info("Opened metadata base file from " + basefilePath + " at instant " + basefile.get().getCommitTime());
    }

    // Open the log record scanner using the log files from the latest file slice
    List<String> logFilePaths = latestSlices.get(0).getLogFiles().sorted(HoodieLogFile.getLogFileComparator())
        .map(o -> o.getPath().toString())
        .collect(Collectors.toList());

    Option<HoodieInstant> lastInstant = timeline.filterCompletedInstants().lastInstant();
    String latestMetaInstantTimestamp = lastInstant.map(HoodieInstant::getTimestamp).orElse(SOLO_COMMIT_TIMESTAMP);

    // Load the schema
    Schema schema = HoodieAvroUtils.addMetadataFields(HoodieMetadataRecord.getClassSchema());

    logRecordScanner =
        new HoodieMetadataMergedLogRecordScanner(metaClient.getFs(), metadataBasePath,
            logFilePaths, schema, latestMetaInstantTimestamp, MAX_MEMORY_SIZE_IN_BYTES, BUFFER_SIZE,
            spillableMapDirectory, null);

    LOG.info("Opened metadata log files from " + logFilePaths + " at instant " + latestInstantTime
        + "(dataset instant=" + latestInstantTime + ", metadata instant=" + latestMetaInstantTimestamp + ")");

    metrics.ifPresent(metrics -> metrics.updateMetrics(HoodieMetadataMetrics.SCAN_STR, timer.endTimer()));
  }

  protected void closeReaders() {
    if (basefileReader != null) {
      basefileReader.close();
      basefileReader = null;
    }
    logRecordScanner = null;
  }

  /**
   * Return {@code True} if all Instants from the dataset have been synced with the Metadata Table.
   */
  @Override
  public boolean isInSync() {
    return enabled && findInstantsToSync().isEmpty();
  }

  private List<HoodieInstant> findInstantsToSync() {
    HoodieTableMetaClient datasetMetaClient = new HoodieTableMetaClient(hadoopConf.get(), datasetBasePath);
    return findInstantsToSync(datasetMetaClient);
  }

  /**
   * Return an ordered list of instants which have not been synced to the Metadata Table.

   * @param datasetMetaClient {@code HoodieTableMetaClient} for the dataset
   */
  protected List<HoodieInstant> findInstantsToSync(HoodieTableMetaClient datasetMetaClient) {
    HoodieActiveTimeline metaTimeline = metaClient.reloadActiveTimeline();

    // All instants on the data timeline, which are greater than the last instant on metadata timeline
    // are candidates for sync.
    Option<HoodieInstant> latestMetadataInstant = metaTimeline.filterCompletedInstants().lastInstant();
    ValidationUtils.checkArgument(latestMetadataInstant.isPresent(),
        "At least one completed instant should exist on the metadata table, before syncing.");
    String latestMetadataInstantTime = latestMetadataInstant.get().getTimestamp();
    HoodieDefaultTimeline candidateTimeline = datasetMetaClient.getActiveTimeline().findInstantsAfter(latestMetadataInstantTime, Integer.MAX_VALUE);
    Option<HoodieInstant> earliestIncompleteInstant = candidateTimeline.filterInflightsAndRequested().firstInstant();

    if (earliestIncompleteInstant.isPresent()) {
      return candidateTimeline.filterCompletedInstants()
          .findInstantsBefore(earliestIncompleteInstant.get().getTimestamp())
          .getInstants().collect(Collectors.toList());
    } else {
      return candidateTimeline.filterCompletedInstants()
          .getInstants().collect(Collectors.toList());
    }
  }

  /**
   * Return the timestamp of the latest compaction instant.
   */
  @Override
  public Option<String> getSyncedInstantTime() {
    if (!enabled) {
      return Option.empty();
    }

    HoodieActiveTimeline timeline = metaClient.reloadActiveTimeline();
    return timeline.getDeltaCommitTimeline().filterCompletedInstants()
        .lastInstant().map(HoodieInstant::getTimestamp);
  }

  public boolean enabled() {
    return enabled;
  }

  public SerializableConfiguration getHadoopConf() {
    return hadoopConf;
  }

  public String getDatasetBasePath() {
    return datasetBasePath;
  }

  public HoodieTableMetaClient getMetaClient() {
    return metaClient;
  }

  public Map<String, String> stats() {
    return metrics.map(m -> m.getStats(true, metaClient, this)).orElse(new HashMap<>());
  }
}
