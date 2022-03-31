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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Table metadata provided by an internal DFS backed Hudi metadata table.
 */
public class HoodieBackedTableMetadata extends BaseTableMetadata {

  private static final Logger LOG = LogManager.getLogger(HoodieBackedTableMetadata.class);

  private String metadataBasePath;
  // Metadata table's timeline and metaclient
  private HoodieTableMetaClient metadataMetaClient;
  private HoodieTableConfig metadataTableConfig;

  // Readers for latest file slice corresponding to file groups in the metadata partition of interest
  private transient Map<String, Pair<HoodieFileReader, HoodieMetadataMergedLogRecordReader>> fileSliceReaders = new ConcurrentHashMap<>();
  // Latest file slices in the metadata partitions
  private final Map<String, List<FileSlice>> partitionFileSliceMap = new ConcurrentHashMap<>();

  public HoodieBackedTableMetadata(HoodieEngineContext engineContext, HoodieMetadataConfig metadataConfig,
                                   String datasetBasePath) {
    super(engineContext, metadataConfig, datasetBasePath);
    initIfNeeded();
  }

  private void initIfNeeded() {
    this.metadataBasePath = HoodieTableMetadata.getMetadataTableBasePath(dataBasePath);
    if (this.metadataMetaClient == null) {
      try {
        this.metadataMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf.get()).setBasePath(metadataBasePath).build();
        this.metadataTableConfig = metadataMetaClient.getTableConfig();
      } catch (TableNotFoundException e) {
        LOG.warn("Metadata table was not found at path " + metadataBasePath);
        this.enabled = false;
        this.metadataMetaClient = null;
        this.metadataTableConfig = null;
      } catch (Exception e) {
        LOG.error("Failed to initialize metadata table at path " + metadataBasePath, e);
        this.enabled = false;
        this.metadataMetaClient = null;
        this.metadataTableConfig = null;
      }
    }
  }

  @Override
  protected Option<HoodieRecord<HoodieMetadataPayload>> getRecordByKey(String key, String partitionName) {
    Map<String, HoodieRecord<HoodieMetadataPayload>> recordsByKeys =
        getRecordsByKeys(Collections.singletonList(key), partitionName);
    return Option.ofNullable(recordsByKeys.getOrDefault(key, null));
  }

  @Override
  protected Map<String, HoodieRecord<HoodieMetadataPayload>> getRecordsByKeys(List<String> keys, String partitionName) {
    if (keys.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, HoodieRecord<HoodieMetadataPayload>> result;

    // Load the file slices for the partition. Each file slice is a shard which saves a portion of the keys.
    List<FileSlice> latestFileSlices = partitionFileSliceMap.computeIfAbsent(partitionName,
        k -> HoodieTableMetadataUtil.getPartitionLatestMergedFileSlices(metadataMetaClient, partitionName));
    final int numFileSlices = latestFileSlices.size();

    // Lookup keys from each shard
    if (numFileSlices == 1) {
      // Optimization for a single shard which is for smaller metadata table partitions
      result = lookupKeysFromFileSlice(partitionName, keys, latestFileSlices.get(0));
    } else {
      // Parallel lookup for large sized partitions with multiple shards
      // Partition the keys by the shard
      ArrayList<ArrayList<String>> partitionedKeys = new ArrayList<>(numFileSlices);
      for (int i = 0; i < numFileSlices; ++i) {
        partitionedKeys.add(new ArrayList<>());
      }
      keys.stream().forEach(key -> {
        int shardIndex = HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(key, numFileSlices);
        partitionedKeys.get(shardIndex).add(key);
      });

      result = new HashMap<>();
      HoodieEngineContext engineContext = getEngineContext();
      engineContext.setJobStatus(this.getClass().getSimpleName(), "Reading keys from metadata table partition " + partitionName);
      engineContext.map(partitionedKeys, keysList -> {
        if (keysList.isEmpty()) {
          return Collections.emptyMap();
        }
        int shardIndex = HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(keysList.get(0), numFileSlices);
        return lookupKeysFromFileSlice(partitionName, keysList, latestFileSlices.get(shardIndex));
      }, partitionedKeys.size())
        .forEach(lookupResult -> result.putAll((Map<String, HoodieRecord<HoodieMetadataPayload>>) lookupResult));
    }

    return result;
  }

  /**
   * Lookup list of keys from a single file slice.
   *
   * @param partitionName Name of the partition
   * @param keys The list of keys to lookup
   * @param fileSlice The file slice to read
   * @return A {@code Map} of key name to {@code HoodieRecord} for the keys which were found in the file slice
   */
  private Map<String, HoodieRecord<HoodieMetadataPayload>> lookupKeysFromFileSlice(String partitionName,
      List<String> keys, FileSlice fileSlice) {
    Pair<HoodieFileReader, HoodieMetadataMergedLogRecordReader> readers = openReadersIfNeeded(partitionName, fileSlice);
    try {
      HoodieFileReader baseFileReader = readers.getKey();
      HoodieMetadataMergedLogRecordReader logRecordScanner = readers.getRight();

      if (baseFileReader == null && logRecordScanner == null) {
        return Collections.emptyMap();
      }

      // local map to assist in merging with base file records
      Map<String, HoodieRecord<HoodieMetadataPayload>> logRecords = readLogRecords(logRecordScanner, keys);
      Map<String, HoodieRecord<HoodieMetadataPayload>> result = readFromBaseAndMergeWithLogRecords(
          baseFileReader, keys, logRecords, partitionName);
      return result;
    } catch (IOException ioe) {
      throw new HoodieIOException("Error merging records from metadata table for  " + keys.size() + " key : ", ioe);
    }
  }

  private Map<String, HoodieRecord<HoodieMetadataPayload>> readLogRecords(HoodieMetadataMergedLogRecordReader logRecordScanner,
                                                                          List<String> keys) {
    // Retrieve records from log file
    HoodieTimer timer = new HoodieTimer().startTimer();
    Map<String, HoodieRecord<HoodieMetadataPayload>> logRecords = (logRecordScanner != null)
        ? logRecordScanner.getRecordsByKeys(keys) : new HashMap<>();
    final long avgReadTimePerKey = timer.endTimer() / keys.size();
    metrics.ifPresent(m -> m.updateDurationMetric(HoodieMetadataMetrics.LOGFILE_READ_STR, avgReadTimePerKey));
    return logRecords;
  }

  private Map<String, HoodieRecord<HoodieMetadataPayload>> readFromBaseAndMergeWithLogRecords(HoodieFileReader baseFileReader,
      List<String> keys, Map<String, HoodieRecord<HoodieMetadataPayload>> logRecords, String partitionName) throws IOException {
    // merge with base records
    // Retrieve record from base file
    if (baseFileReader != null) {
      HoodieTimer timer = new HoodieTimer().startTimer();
      final Map<String, HoodieRecord<HoodieMetadataPayload>> result = new HashMap<>();
      keys.stream().sorted().forEach(key -> {
        Option<GenericRecord> baseRecord = null;
        try {
          baseRecord = baseFileReader.getRecordByKey(key);
          if (baseRecord.isPresent()) {
            HoodieRecord<HoodieMetadataPayload> hoodieRecord = getRecord(baseRecord, partitionName);
            // merge base file record w/ log record if present
            if (logRecords.containsKey(key)) {
              HoodieRecordPayload mergedPayload = logRecords.get(key).getData().preCombine(hoodieRecord.getData());
              result.put(key, new HoodieRecord(hoodieRecord.getKey(), mergedPayload));
            } else {
              // only base record
              result.put(key, hoodieRecord);
            }
          } else if (logRecords.containsKey(key)) {
            // only log record
            result.put(key, logRecords.get(key));
          }
        } catch (IOException e) {
          throw new HoodieException("Could not read record from base file", e);
        }
      });
      final long avgReadTimePerKey = timer.endTimer() / keys.size();
      metrics.ifPresent(m -> m.updateDurationMetric(HoodieMetadataMetrics.BASEFILE_READ_STR, avgReadTimePerKey));
      return result;
    } else {
      // no base file at all
      return logRecords;
    }
  }

  private HoodieRecord<HoodieMetadataPayload> getRecord(Option<GenericRecord> baseRecord, String partitionName) {
    ValidationUtils.checkState(baseRecord.isPresent());
    if (metadataTableConfig.populateMetaFields()) {
      return SpillableMapUtils.convertToHoodieRecordPayload(baseRecord.get(),
          metadataTableConfig.getPayloadClass(), metadataTableConfig.getPreCombineField(), false);
    }
    return SpillableMapUtils.convertToHoodieRecordPayload(baseRecord.get(),
        metadataTableConfig.getPayloadClass(), metadataTableConfig.getPreCombineField(),
        Pair.of(metadataTableConfig.getRecordKeyFieldProp(), metadataTableConfig.getPartitionFieldProp()),
        false, Option.of(partitionName));
  }

  /**
   * Opens and returns readers to one of the shards of a partition.
   *
   * @param partitionName the name of the partition
   * @param fileSlice the latest file slice for the shard
   */
  private Pair<HoodieFileReader, HoodieMetadataMergedLogRecordReader> openReadersIfNeeded(String partitionName,
      FileSlice fileSlice) {
    synchronized (this) {
      if (fileSliceReaders == null) {
        fileSliceReaders = new ConcurrentHashMap<>();
      }
    }
    return fileSliceReaders.computeIfAbsent(fileSlice.getFileId(), k -> {
      try {
        final long baseFileOpenMs;
        final long logScannerOpenMs;
        HoodieFileReader baseFileReader = null;
        HoodieMetadataMergedLogRecordReader logRecordScanner = null;

        // Metadata is in sync till the latest completed instant on the dataset
        HoodieTimer timer = new HoodieTimer().startTimer();

        // Open base file reader
        Pair<HoodieFileReader, Long> baseFileReaderOpenTimePair = getBaseFileReader(fileSlice, timer);
        baseFileReader = baseFileReaderOpenTimePair.getKey();
        baseFileOpenMs = baseFileReaderOpenTimePair.getValue();

        // Open the log record scanner using the log files from the latest file slice
        Pair<HoodieMetadataMergedLogRecordReader, Long> logRecordScannerOpenTimePair = getLogRecordScanner(fileSlice,
            partitionName);
        logRecordScanner = logRecordScannerOpenTimePair.getKey();
        logScannerOpenMs = logRecordScannerOpenTimePair.getValue();

        metrics.ifPresent(metrics -> metrics.updateDurationMetric(HoodieMetadataMetrics.SCAN_STR, baseFileOpenMs + logScannerOpenMs));
        return Pair.of(baseFileReader, logRecordScanner);
      } catch (IOException e) {
        throw new HoodieIOException(String.format("Error opening readers for file slice %s of metadata table partition %s",
            fileSlice, partitionName), e);
      }
    });
  }

  private Pair<HoodieFileReader, Long> getBaseFileReader(FileSlice slice, HoodieTimer timer) throws IOException {
    HoodieFileReader baseFileReader = null;
    Long baseFileOpenMs;
    // If the base file is present then create a reader
    Option<HoodieBaseFile> basefile = slice.getBaseFile();
    if (basefile.isPresent()) {
      String basefilePath = basefile.get().getPath();
      baseFileReader = HoodieFileReaderFactory.getFileReader(hadoopConf.get(), new Path(basefilePath));
      baseFileOpenMs = timer.endTimer();
      LOG.info(String.format("Opened metadata base file from %s at instant %s in %d ms", basefilePath,
          basefile.get().getCommitTime(), baseFileOpenMs));
    } else {
      baseFileOpenMs = 0L;
      timer.endTimer();
    }
    return Pair.of(baseFileReader, baseFileOpenMs);
  }

  private Set<String> getValidInstantTimestamps() {
    // Only those log files which have a corresponding completed instant on the dataset should be read
    // This is because the metadata table is updated before the dataset instants are committed.
    HoodieActiveTimeline datasetTimeline = dataMetaClient.getActiveTimeline();
    Set<String> validInstantTimestamps = datasetTimeline.filterCompletedInstants().getInstants()
        .map(HoodieInstant::getTimestamp).collect(Collectors.toSet());

    // For any rollbacks and restores, we cannot neglect the instants that they are rolling back.
    // The rollback instant should be more recent than the start of the timeline for it to have rolled back any
    // instant which we have a log block for.
    final String earliestInstantTime = validInstantTimestamps.isEmpty() ? SOLO_COMMIT_TIMESTAMP : Collections.min(validInstantTimestamps);
    datasetTimeline.getRollbackAndRestoreTimeline().filterCompletedInstants().getInstants()
        .filter(instant -> HoodieTimeline.compareTimestamps(instant.getTimestamp(), HoodieTimeline.GREATER_THAN_OR_EQUALS, earliestInstantTime))
        .forEach(instant -> {
          validInstantTimestamps.addAll(getRollbackedCommits(instant, datasetTimeline));
        });

    // When additional partitions are bootstrapped later (i.e after metadata table already exists), they use a separate
    // deltacommit whose exact timestamp is not present on the dataset. These deltacommits should also be included.
    metadataMetaClient.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants().getInstants()
        .filter(instant -> !validInstantTimestamps.contains(instant.getTimestamp()))
        .filter(instant -> HoodieTableMetadataUtil.isIndexInitInstant(instant))
        .forEach(instant -> validInstantTimestamps.add(instant.getTimestamp()));

    // SOLO_COMMIT_TIMESTAMP is used during bootstrap so it is a valid timestamp
    validInstantTimestamps.add(SOLO_COMMIT_TIMESTAMP);
    return validInstantTimestamps;
  }

  private Pair<HoodieMetadataMergedLogRecordReader, Long> getLogRecordScanner(FileSlice slice, String partitionName) {
    HoodieTimer timer = new HoodieTimer().startTimer();
    List<String> logFilePaths = slice.getLogFiles()
        .sorted(HoodieLogFile.getLogFileComparator())
        .map(o -> o.getPath().toString())
        .collect(Collectors.toList());

    // Only those log files which have a corresponding completed instant on the dataset should be read
    // This is because the metadata table is updated before the dataset instants are committed.
    Set<String> validInstantTimestamps = getValidInstantTimestamps();

    Option<HoodieInstant> latestMetadataInstant = metadataMetaClient.getActiveTimeline().filterCompletedInstants().lastInstant();
    String latestMetadataInstantTime = latestMetadataInstant.map(HoodieInstant::getTimestamp).orElse(SOLO_COMMIT_TIMESTAMP);

    // Load the schema
    Schema schema = HoodieAvroUtils.addMetadataFields(HoodieMetadataRecord.getClassSchema());
    HoodieCommonConfig commonConfig = HoodieCommonConfig.newBuilder().fromProperties(metadataConfig.getProps()).build();
    HoodieMetadataMergedLogRecordReader logRecordScanner = HoodieMetadataMergedLogRecordReader.newBuilder()
        .withFileSystem(metadataMetaClient.getFs())
        .withBasePath(metadataBasePath)
        .withLogFilePaths(logFilePaths)
        .withReaderSchema(schema)
        .withLatestInstantTime(latestMetadataInstantTime)
        .withMaxMemorySizeInBytes(metadataConfig.getMaxReaderMemory())
        .withBufferSize(metadataConfig.getMaxReaderBufferSize())
        .withSpillableMapBasePath(metadataConfig.getSplliableMapDir())
        .withDiskMapType(commonConfig.getSpillableDiskMapType())
        .withBitCaskDiskMapCompressionEnabled(commonConfig.isBitCaskDiskMapCompressionEnabled())
        .withLogBlockTimestamps(validInstantTimestamps)
        .enableFullScan(metadataConfig.enableFullScan())
        .withPartition(partitionName)
        .build();

    Long logScannerOpenMs = timer.endTimer();
    LOG.info(String.format("Opened %d metadata log files (dataset instant=%s, metadata instant=%s,) in %d ms",
        logFilePaths.size(), getLatestDataInstantTime(), latestMetadataInstantTime, logScannerOpenMs));
    if (metadataConfig.enableFullScan()) {
      // Only on a full scan we read all the log blocks and these metrics can be published
      metrics.ifPresent(m -> m.setMetric(partitionName + "." + HoodieMetadataMetrics.COUNT_LOG_BLOCKS,
          logRecordScanner.getTotalLogBlocks()));
      metrics.ifPresent(m -> m.setMetric(partitionName + "." + HoodieMetadataMetrics.COUNT_LOG_RECORDS,
          logRecordScanner.getTotalLogRecords()));
    }
    return Pair.of(logRecordScanner, logScannerOpenMs);
  }

  /**
   * Returns a list of commits which were rolled back as part of a Rollback or Restore operation.
   *
   * @param instant  The Rollback operation to read
   * @param timeline instant of timeline from dataset.
   */
  private List<String> getRollbackedCommits(HoodieInstant instant, HoodieActiveTimeline timeline) {
    try {
      if (instant.getAction().equals(HoodieTimeline.ROLLBACK_ACTION)) {
        HoodieRollbackMetadata rollbackMetadata = TimelineMetadataUtils.deserializeHoodieRollbackMetadata(
            timeline.getInstantDetails(instant).get());
        return rollbackMetadata.getCommitsRollback();
      }

      List<String> rollbackedCommits = new LinkedList<>();
      if (instant.getAction().equals(HoodieTimeline.RESTORE_ACTION)) {
        // Restore is made up of several rollbacks
        HoodieRestoreMetadata restoreMetadata = TimelineMetadataUtils.deserializeHoodieRestoreMetadata(
            timeline.getInstantDetails(instant).get());
        restoreMetadata.getHoodieRestoreMetadata().values().forEach(rms -> {
          rms.forEach(rm -> rollbackedCommits.addAll(rm.getCommitsRollback()));
        });
      }
      return rollbackedCommits;
    } catch (IOException e) {
      throw new HoodieMetadataException("Error retrieving rollback commits for instant " + instant, e);
    }
  }

  @Override
  public void close() {
    for (String key : fileSliceReaders.keySet()) {
      Pair<HoodieFileReader, HoodieMetadataMergedLogRecordReader> readers = fileSliceReaders.remove(key);
      if (readers != null) {
        try {
          if (readers.getKey() != null) {
            readers.getKey().close();
          }
          if (readers.getValue() != null) {
            readers.getValue().close();
          }
        } catch (Exception e) {
          throw new HoodieException("Error closing resources during metadata table merge", e);
        }
      }
    }
    fileSliceReaders.clear();
  }

  public boolean enabled() {
    return enabled;
  }

  public SerializableConfiguration getHadoopConf() {
    return hadoopConf;
  }

  public HoodieTableMetaClient getMetadataMetaClient() {
    return metadataMetaClient;
  }

  public Map<String, String> stats() {
    return metrics.map(m -> m.getStats(true, metadataMetaClient, this)).orElse(new HashMap<>());
  }

  @Override
  public Option<String> getSyncedInstantTime() {
    if (metadataMetaClient != null) {
      Option<HoodieInstant> latestInstant = metadataMetaClient.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants().lastInstant();
      if (latestInstant.isPresent()) {
        return Option.of(latestInstant.get().getTimestamp());
      }
    }
    return Option.empty();
  }

  @Override
  public Option<String> getLatestCompactionTime() {
    if (metadataMetaClient != null) {
      Option<HoodieInstant> latestCompaction = metadataMetaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants().lastInstant();
      if (latestCompaction.isPresent()) {
        return Option.of(latestCompaction.get().getTimestamp());
      }
    }
    return Option.empty();
  }

  @Override
  public void reset() {
    metadataMetaClient = null;
    fileSliceReaders.clear();
    partitionFileSliceMap.clear();

    initIfNeeded();
    dataMetaClient.reloadActiveTimeline();
  }
}
