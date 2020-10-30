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
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
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
 * Reader for Metadata Table.
 *
 * If the metadata table does not exist, RPC calls are used to retrieve file listings from the file system.
 * No updates are applied to the table and it is not synced.
 */
public class HoodieMetadataReader implements Serializable {
  private static final Logger LOG = LogManager.getLogger(HoodieMetadataReader.class);

  // Base path of the Metadata Table relative to the dataset (.hoodie/metadata)
  private static final String METADATA_TABLE_REL_PATH = HoodieTableMetaClient.METAFOLDER_NAME + Path.SEPARATOR
      + "metadata";

  // Table name suffix
  protected static final String METADATA_TABLE_NAME_SUFFIX = "_metadata";

  // Timestamp for a commit when the base dataset had not had any commits yet.
  protected static final String SOLO_COMMIT_TIMESTAMP = "00000000000000";


  // Name of partition which saves file listings
  public static final String METADATA_PARTITION_NAME = "metadata_partition";
  // List of all partitions
  public static final String[] METADATA_ALL_PARTITIONS = {METADATA_PARTITION_NAME};
  // Key for the record which saves list of all partitions
  protected static final String RECORDKEY_PARTITION_LIST = "__all_partitions__";

  // The partition name used for non-partitioned tables
  protected static final String NON_PARTITIONED_NAME = ".";

  // Metric names
  public static final String LOOKUP_PARTITIONS_STR = "lookup_partitions";
  public static final String LOOKUP_FILES_STR = "lookup_files";
  public static final String VALIDATE_PARTITIONS_STR = "validate_partitions";
  public static final String VALIDATE_FILES_STR = "validate_files";
  public static final String VALIDATE_ERRORS_STR = "validate_errors";
  public static final String SCAN_STR = "scan";
  public static final String BASEFILE_READ_STR = "basefile_read";

  // Stats names
  public static final String STAT_TOTAL_BASE_FILE_SIZE = "totalBaseFileSizeInBytes";
  public static final String STAT_TOTAL_LOG_FILE_SIZE = "totalLogFileSizeInBytes";
  public static final String STAT_COUNT_BASE_FILES = "baseFileCount";
  public static final String STAT_COUNT_LOG_FILES = "logFileCount";
  public static final String STAT_COUNT_PARTITION = "partitionCount";
  public static final String STAT_IN_SYNC = "isInSync";
  public static final String STAT_LAST_COMPACTION_TIMESTAMP = "lastCompactionTimestamp";

  // A base directory where the metadata tables should be saved outside the dataset directory.
  // This is used in tests on existing datasets.
  private static String metadataBaseDirectory;

  protected final SerializableConfiguration hadoopConf;
  protected final String datasetBasePath;
  protected final String metadataBasePath;
  protected Registry metricsRegistry;
  protected HoodieTableMetaClient metaClient;
  protected boolean enabled;
  private final boolean validateLookups;
  private long maxMemorySizeInBytes = 1024 * 1024 * 1024; // TODO
  private int bufferSize = 10 * 1024 * 1024; // TODO

  // Directory used for Splillable Map when merging records
  private String spillableMapDirectory;

  // Readers for the base and log file which store the metadata
  private transient HoodieFileReader<GenericRecord> basefileReader;
  private transient HoodieMetadataMergedLogRecordScanner logRecordScanner;

  /**
   * Create a the Metadata Table in read-only mode.
   */
  public HoodieMetadataReader(Configuration conf, String datasetBasePath, String spillableMapDirectory,
                              boolean enabled, boolean validateLookups) {
    this(conf, datasetBasePath, spillableMapDirectory, enabled, validateLookups, false);
  }

  /**
   * Create a the Metadata Table in read-only mode.
   */
  public HoodieMetadataReader(Configuration conf, String datasetBasePath, String spillableMapDirectory,
                              boolean enabled, boolean validateLookups, boolean enableMetrics) {
    this.hadoopConf = new SerializableConfiguration(conf);
    this.datasetBasePath = datasetBasePath;
    this.metadataBasePath = getMetadataTableBasePath(datasetBasePath);
    this.validateLookups = validateLookups;
    this.spillableMapDirectory = spillableMapDirectory;

    if (enabled) {
      try {
        metaClient = new HoodieTableMetaClient(hadoopConf.get(), metadataBasePath);
      } catch (TableNotFoundException e) {
        LOG.error("Metadata table was not found at path " + metadataBasePath);
        enabled = false;
      } catch (Exception e) {
        LOG.error("Failed to initialize metadata table at path " + metadataBasePath, e);
        enabled = false;
      }
    } else {
      LOG.info("Metadata table is disabled.");
    }

    if (enableMetrics) {
      metricsRegistry = Registry.getRegistry("HoodieMetadata");
    }

    this.enabled = enabled;
  }

  /**
   * Return the list of partitions in the dataset.
   *
   * If the Metadata Table is enabled, the listing is retrieved from the stored metadata. Otherwise, the list of
   * partitions is retrieved directly from the underlying {@code FileSystem}.
   *
   * On any errors retrieving the listing from the metadata, defaults to using the file system listings.
   *
   * @param fs The {@code FileSystem}
   * @param basePath Base path of the dataset
   * @param assumeDatePartitioning True if the dataset uses date based partitioning
   */
  public List<String> getAllPartitionPaths(FileSystem fs, String basePath, boolean assumeDatePartitioning)
      throws IOException {
    if (enabled) {
      try {
        return getAllPartitionPaths();
      } catch (Exception e) {
        LOG.error("Failed to retrive list of partition from metadata", e);
      }
    }

    return getAllPartitionPathsByListing(fs, basePath, assumeDatePartitioning);
  }

  /**
   * Return the list of files in a partition.
   *
   * If the Metadata Table is enabled, the listing is retrieved from the stored metadata. Otherwise, the list of
   * partitions is retrieved directly from the underlying {@code FileSystem}.
   *
   * On any errors retrieving the listing from the metadata, defaults to using the file system listings.
   *
   * @param hadoopConf {@code Configuration}
   * @param partitionPath The absolute path of the partition to list
   */
  public FileStatus[] getAllFilesInPartition(Configuration hadoopConf, String basePath, Path partitionPath)
      throws IOException {
    if (enabled) {
      try {
        return getAllFilesInPartition(partitionPath);
      } catch (Exception e) {
        LOG.error("Failed to retrive files in partition " + partitionPath + " from metadata", e);
      }
    }

    return getAllFilesInPartitionByListing(hadoopConf, basePath, partitionPath);
  }

  /**
   * Returns a list of all partitions.
   */
  protected List<String> getAllPartitionPaths() throws IOException {
    HoodieTimer timer = new HoodieTimer().startTimer();
    Option<HoodieRecord<HoodieMetadataPayload>> hoodieRecord = getMergedRecordByKey(RECORDKEY_PARTITION_LIST);
    updateMetrics(LOOKUP_PARTITIONS_STR, timer.endTimer());

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
      List<String> actualPartitions  = getAllPartitionPathsByListing(metaClient.getFs(), datasetBasePath, false);
      updateMetrics(VALIDATE_PARTITIONS_STR, timer.endTimer());

      Collections.sort(actualPartitions);
      Collections.sort(partitions);
      if (!actualPartitions.equals(partitions)) {
        LOG.error("Validation of metadata partition list failed. Lists do not match.");
        LOG.error("Partitions from metadata: " + Arrays.toString(partitions.toArray()));
        LOG.error("Partitions from file system: " + Arrays.toString(actualPartitions.toArray()));

        updateMetrics(VALIDATE_ERRORS_STR, 0);
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
  FileStatus[] getAllFilesInPartition(Path partitionPath) throws IOException {
    String partitionName = FSUtils.getRelativePartitionPath(new Path(datasetBasePath), partitionPath);
    if (partitionName.equals("")) {
      partitionName = NON_PARTITIONED_NAME;
    }

    HoodieTimer timer = new HoodieTimer().startTimer();
    Option<HoodieRecord<HoodieMetadataPayload>> hoodieRecord = getMergedRecordByKey(partitionName);
    updateMetrics(LOOKUP_FILES_STR, timer.endTimer());

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
      updateMetrics(VALIDATE_FILES_STR, timer.endTimer());

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

        updateMetrics(VALIDATE_ERRORS_STR, 0);
      }

      // Return the direct listing as it should be correct
      statuses = directStatuses;
    }

    LOG.info("Listed file in partition from metadata: partition=" + partitionName + ", #files=" + statuses.length);
    return statuses;
  }

  private FileStatus[] getAllFilesInPartitionByListing(Configuration hadoopConf, String basePath, Path partitionPath)
      throws IOException {
    return FSUtils.getFs(partitionPath.toString(), hadoopConf).listStatus(partitionPath);
  }

  private List<String> getAllPartitionPathsByListing(FileSystem fs, String basePath, boolean assumeDatePartitioning)
      throws IOException {
    return FSUtils.getAllPartitionPaths(fs, basePath, assumeDatePartitioning);
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
        hoodieRecord = SpillableMapUtils.convertToHoodieRecordPayload((GenericRecord) baseRecord.get(),
            metaClient.getTableConfig().getPayloadClass());
        updateMetrics(BASEFILE_READ_STR, timer.endTimer());
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

    long t1 = System.currentTimeMillis();

    // Metadata is in sync till the latest completed instant on the dataset
    HoodieTableMetaClient datasetMetaClient = new HoodieTableMetaClient(hadoopConf.get(), datasetBasePath);
    Option<HoodieInstant> datasetLatestInstant = datasetMetaClient.getActiveTimeline().filterCompletedInstants()
        .lastInstant();
    String latestInstantTime = datasetLatestInstant.isPresent() ? datasetLatestInstant.get().getTimestamp()
        : SOLO_COMMIT_TIMESTAMP;

    // Find the latest file slice
    HoodieTimeline timeline = metaClient.reloadActiveTimeline();
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline());
    List<FileSlice> latestSlices = fsView.getLatestFileSlices(METADATA_PARTITION_NAME).collect(Collectors.toList());
    ValidationUtils.checkArgument(latestSlices.size() == 1);

    // If the base file is present then create a reader
    Option<HoodieBaseFile> basefile = latestSlices.get(0).getBaseFile();
    if (basefile.isPresent()) {
      String basefilePath = basefile.get().getPath();
      basefileReader = HoodieFileReaderFactory.getFileReader(hadoopConf.get(), new Path(basefilePath));

      LOG.info("Opened metadata base file from " + basefilePath + " at instant " + basefile.get().getCommitTime());
    }

    // Open the log record scanner using the log files from the latest file slice
    List<String> logFilePaths = latestSlices.get(0).getLogFiles().map(o -> o.getPath().toString())
        .collect(Collectors.toList());

    Option<HoodieInstant> lastInstant = timeline.filterCompletedInstants().lastInstant();
    String latestMetaInstantTimestamp = lastInstant.isPresent() ? lastInstant.get().getTimestamp()
        : SOLO_COMMIT_TIMESTAMP;
    if (!HoodieTimeline.compareTimestamps(latestInstantTime, HoodieTimeline.EQUALS, latestMetaInstantTimestamp)) {
      // TODO: This can be false positive if the metadata table had a compaction or clean
      LOG.warn("Metadata has more recent instant " + latestMetaInstantTimestamp + " than dataset " + latestInstantTime);
    }

    // Load the schema
    Schema schema = HoodieAvroUtils.addMetadataFields(HoodieMetadataRecord.getClassSchema());

    // TODO: The below code may open the metadata to include incomplete instants on the dataset
    logRecordScanner =
        new HoodieMetadataMergedLogRecordScanner(metaClient.getFs(), metadataBasePath,
            logFilePaths, schema, latestMetaInstantTimestamp, maxMemorySizeInBytes, bufferSize,
            spillableMapDirectory, null);

    LOG.info("Opened metadata log files from " + logFilePaths + " at instant " + latestInstantTime
        + "(dataset instant=" + latestInstantTime + ", metadata instant=" + latestMetaInstantTimestamp + ")");

    updateMetrics(SCAN_STR, System.currentTimeMillis() - t1);
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
  public boolean isInSync() {
    // There should not be any instants to sync
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

    // All instants since the last time metadata table was compacted are candidates for sync
    Option<String> compactionTimestamp = getLatestCompactionTimestamp();

    // If there has not been any compaction then the first delta commit instant should be the one at which
    // the metadata table was created. We should not sync any instants before that creation time.
    Option<HoodieInstant> oldestMetaInstant = Option.empty();
    if (!compactionTimestamp.isPresent()) {
      oldestMetaInstant = metaTimeline.getDeltaCommitTimeline().filterCompletedInstants().firstInstant();
      if (oldestMetaInstant.isPresent()) {
        // TODO: Ensure this is the instant at which we created the metadata table
      }
    }

    String metaSyncTimestamp = compactionTimestamp.isPresent() ? compactionTimestamp.get()
        : oldestMetaInstant.isPresent() ? oldestMetaInstant.get().getTimestamp() : SOLO_COMMIT_TIMESTAMP;

    // Metadata table is updated when an instant is completed except for the following:
    //  CLEAN: metadata table is updated during inflight. So for CLEAN we accept inflight actions.
    List<HoodieInstant> datasetInstants = datasetMetaClient.getActiveTimeline().getInstants()
        .filter(i -> i.isCompleted() || (i.getAction().equals(HoodieTimeline.CLEAN_ACTION) && i.isInflight()))
        .filter(i -> metaSyncTimestamp.isEmpty()
            || HoodieTimeline.compareTimestamps(i.getTimestamp(), HoodieTimeline.GREATER_THAN_OR_EQUALS,
                metaSyncTimestamp))
        .collect(Collectors.toList());

    // Each operation on dataset leads to a delta-commit on the metadata MOR table. So find only delta-commit
    // instants in metadata table which are after the last compaction.
    Map<String, HoodieInstant> metadataInstantMap = metaTimeline.getDeltaCommitTimeline().filterCompletedInstants()
        .findInstantsAfterOrEquals(metaSyncTimestamp, Integer.MAX_VALUE).getInstants()
        .collect(Collectors.toMap(HoodieInstant::getTimestamp, Function.identity()));

    List<HoodieInstant> instantsToSync = new LinkedList<>();
    datasetInstants.forEach(instant -> {
      if (metadataInstantMap.containsKey(instant.getTimestamp())) {
        // instant already synced to metadata table
        if (!instantsToSync.isEmpty()) {
          // TODO: async clean and async compaction are not yet handled. They have a timestamp which is in the past
          // (when the operation was scheduled) and even on completion they retain their old timestamp.
          LOG.warn("Found out-of-order already synced instant " + instant + ". Instants to sync=" + instantsToSync);
        }
      } else {
        instantsToSync.add(instant);
      }
    });
    return instantsToSync;
  }

  /**
   * Return the timestamp of the latest compaction instant.
   */
  public Option<String> getLatestCompactionTimestamp() {
    if (!enabled) {
      return Option.empty();
    }

    HoodieTimeline timeline = metaClient.reloadActiveTimeline();
    Option<HoodieInstant> lastCompactionInstant = timeline.filterCompletedInstants()
        .filter(i -> i.getAction().equals(HoodieTimeline.COMMIT_ACTION)).lastInstant();

    if (lastCompactionInstant.isPresent()) {
      return Option.of(lastCompactionInstant.get().getTimestamp());
    } else {
      return Option.empty();
    }
  }

  public Map<String, String> getStats(boolean detailed) throws IOException {
    metaClient.reloadActiveTimeline();
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline());
    return getStats(fsView, detailed);
  }

  private Map<String, String> getStats(HoodieTableFileSystemView fsView, boolean detailed) throws IOException {
    Map<String, String> stats = new HashMap<>();

    // Total size of the metadata and count of base/log files
    long totalBaseFileSizeInBytes = 0;
    long totalLogFileSizeInBytes = 0;
    int baseFileCount = 0;
    int logFileCount = 0;
    List<FileSlice> latestSlices = fsView.getLatestFileSlices(METADATA_PARTITION_NAME).collect(Collectors.toList());

    for (FileSlice slice : latestSlices) {
      if (slice.getBaseFile().isPresent()) {
        totalBaseFileSizeInBytes += slice.getBaseFile().get().getFileStatus().getLen();
        ++baseFileCount;
      }
      Iterator<HoodieLogFile> it = slice.getLogFiles().iterator();
      while (it.hasNext()) {
        totalLogFileSizeInBytes += it.next().getFileStatus().getLen();
        ++logFileCount;
      }
    }

    stats.put(STAT_TOTAL_BASE_FILE_SIZE, String.valueOf(totalBaseFileSizeInBytes));
    stats.put(STAT_TOTAL_LOG_FILE_SIZE, String.valueOf(totalLogFileSizeInBytes));
    stats.put(STAT_COUNT_BASE_FILES, String.valueOf(baseFileCount));
    stats.put(STAT_COUNT_LOG_FILES, String.valueOf(logFileCount));

    if (detailed) {
      stats.put(STAT_COUNT_PARTITION, String.valueOf(getAllPartitionPaths().size()));
      stats.put(STAT_IN_SYNC, String.valueOf(isInSync()));
      stats.put(STAT_LAST_COMPACTION_TIMESTAMP, getLatestCompactionTimestamp().orElseGet(() -> "none"));
    }

    return stats;
  }

  protected void updateMetrics(String action, long durationInMs) {
    if (metricsRegistry == null) {
      return;
    }

    // Update sum of duration and total for count
    String countKey = action + ".count";
    String durationKey = action + ".totalDuration";
    metricsRegistry.add(countKey, 1);
    metricsRegistry.add(durationKey, durationInMs);

    LOG.info(String.format("Updating metadata metrics (%s=%dms, %s=1)", durationKey, durationInMs, countKey));
  }

  protected void updateMetrics(long totalBaseFileSizeInBytes, long totalLogFileSizeInBytes, int baseFileCount,
                               int logFileCount) {
    if (metricsRegistry == null) {
      return;
    }

    // Update sizes and count for metadata table's data files
    metricsRegistry.add("basefile.size", totalBaseFileSizeInBytes);
    metricsRegistry.add("logfile.size", totalLogFileSizeInBytes);
    metricsRegistry.add("basefile.count", baseFileCount);
    metricsRegistry.add("logfile.count", logFileCount);

    LOG.info(String.format("Updating metadata size metrics (basefile.size=%d, logfile.size=%d, basefile.count=%d, "
        + "logfile.count=%d)", totalBaseFileSizeInBytes, totalLogFileSizeInBytes, baseFileCount, logFileCount));
  }

  /**
   * Return the base path of the Metadata Table.
   *
   * @param tableBasePath The base path of the dataset
   */
  public static String getMetadataTableBasePath(String tableBasePath) {
    if (metadataBaseDirectory != null) {
      return metadataBaseDirectory;
    }

    return tableBasePath + Path.SEPARATOR + METADATA_TABLE_REL_PATH;
  }

  /**
   * Returns {@code True} if the given path contains a metadata table.
   *
   * @param basePath The base path to check
   */
  public static boolean isMetadataTable(String basePath) {
    return basePath.endsWith(METADATA_TABLE_REL_PATH);
  }

  /**
   * Sets the directory to store/read Metadata Table.
   *
   * This can be used to store the metadata table away from the dataset directory.
   *  - Useful for testing as well as for using via the HUDI CLI so that the actual dataset is not written to.
   *  - Useful for testing Metadata Table performance and operations on existing datasets before enabling.
   */
  public static void setMetadataBaseDirectory(String metadataDir) {
    ValidationUtils.checkState(metadataBaseDirectory == null,
        "metadataBaseDirectory is already set to " + metadataBaseDirectory);
    metadataBaseDirectory = metadataDir;
  }

  public boolean enabled() {
    return enabled;
  }
}
