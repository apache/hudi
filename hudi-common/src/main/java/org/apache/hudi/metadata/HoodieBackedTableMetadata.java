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
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
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
  // should we reuse the open file handles, across calls
  private final boolean reuse;

  // Readers for latest file slice corresponding to file groups in the metadata partition of interest
  private Map<String, Pair<HoodieFileReader, HoodieMetadataMergedLogRecordReader>> partitionReaders = new ConcurrentHashMap<>();

  public HoodieBackedTableMetadata(HoodieEngineContext engineContext, HoodieMetadataConfig metadataConfig,
                                   String datasetBasePath, String spillableMapDirectory) {
    this(engineContext, metadataConfig, datasetBasePath, spillableMapDirectory, false);
  }

  public HoodieBackedTableMetadata(HoodieEngineContext engineContext, HoodieMetadataConfig metadataConfig,
                                   String datasetBasePath, String spillableMapDirectory, boolean reuse) {
    super(engineContext, metadataConfig, datasetBasePath, spillableMapDirectory);
    this.reuse = reuse;
    initIfNeeded();
  }

  private void initIfNeeded() {
    this.metadataBasePath = HoodieTableMetadata.getMetadataTableBasePath(dataBasePath);
    if (!enabled) {
      if (!HoodieTableMetadata.isMetadataTable(metadataBasePath)) {
        LOG.info("Metadata table is disabled.");
      }
    } else if (this.metadataMetaClient == null) {
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
    List<Pair<String, Option<HoodieRecord<HoodieMetadataPayload>>>> recordsByKeys = getRecordsByKeys(Collections.singletonList(key), partitionName);
    return recordsByKeys.size() == 0 ? Option.empty() : recordsByKeys.get(0).getValue();
  }

  protected List<Pair<String, Option<HoodieRecord<HoodieMetadataPayload>>>> getRecordsByKeys(List<String> keys, String partitionName) {
    Pair<HoodieFileReader, HoodieMetadataMergedLogRecordReader> readers = openReadersIfNeeded(keys.get(0), partitionName);
    try {
      List<Long> timings = new ArrayList<>();
      HoodieFileReader baseFileReader = readers.getKey();
      HoodieMetadataMergedLogRecordReader logRecordScanner = readers.getRight();

      if (baseFileReader == null && logRecordScanner == null) {
        return Collections.emptyList();
      }

      // local map to assist in merging with base file records
      Map<String, Option<HoodieRecord<HoodieMetadataPayload>>> logRecords = readLogRecords(logRecordScanner, keys, timings);
      List<Pair<String, Option<HoodieRecord<HoodieMetadataPayload>>>> result = readFromBaseAndMergeWithLogRecords(
          baseFileReader, keys, logRecords, timings, partitionName);
      LOG.info(String.format("Metadata read for %s keys took [baseFileRead, logMerge] %s ms", keys.size(), timings));
      return result;
    } catch (IOException ioe) {
      throw new HoodieIOException("Error merging records from metadata table for  " + keys.size() + " key : ", ioe);
    } finally {
      if (!reuse) {
        close(partitionName);
      }
    }
  }

  private Map<String, Option<HoodieRecord<HoodieMetadataPayload>>> readLogRecords(HoodieMetadataMergedLogRecordReader logRecordScanner,
                                                                                  List<String> keys, List<Long> timings) {
    HoodieTimer timer = new HoodieTimer().startTimer();
    Map<String, Option<HoodieRecord<HoodieMetadataPayload>>> logRecords = new HashMap<>();
    // Retrieve records from log file
    timer.startTimer();
    if (logRecordScanner != null) {
      if (metadataConfig.enableFullScan()) {
        // path which does full scan of log files
        for (String key : keys) {
          logRecords.put(key, logRecordScanner.getRecordByKey(key).get(0).getValue());
        }
      } else {
        // this path will do seeks pertaining to the keys passed in
        List<Pair<String, Option<HoodieRecord<HoodieMetadataPayload>>>> logRecordsList = logRecordScanner.getRecordsByKeys(keys);
        for (Pair<String, Option<HoodieRecord<HoodieMetadataPayload>>> entry : logRecordsList) {
          logRecords.put(entry.getKey(), entry.getValue());
        }
      }
    } else {
      for (String key : keys) {
        logRecords.put(key, Option.empty());
      }
    }
    timings.add(timer.endTimer());
    return logRecords;
  }

  private List<Pair<String, Option<HoodieRecord<HoodieMetadataPayload>>>> readFromBaseAndMergeWithLogRecords(HoodieFileReader baseFileReader,
                                                                                                             List<String> keys, Map<String,
      Option<HoodieRecord<HoodieMetadataPayload>>> logRecords, List<Long> timings, String partitionName) throws IOException {
    List<Pair<String, Option<HoodieRecord<HoodieMetadataPayload>>>> result = new ArrayList<>();
    // merge with base records
    HoodieTimer timer = new HoodieTimer().startTimer();
    timer.startTimer();
    HoodieRecord<HoodieMetadataPayload> hoodieRecord = null;
    // Retrieve record from base file
    if (baseFileReader != null) {
      HoodieTimer readTimer = new HoodieTimer();
      for (String key : keys) {
        readTimer.startTimer();
        Option<GenericRecord> baseRecord = baseFileReader.getRecordByKey(key);
        if (baseRecord.isPresent()) {
          hoodieRecord = getRecord(baseRecord, partitionName);
          metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.BASEFILE_READ_STR, readTimer.endTimer()));
          // merge base file record w/ log record if present
          if (logRecords.containsKey(key) && logRecords.get(key).isPresent()) {
            HoodieRecordPayload mergedPayload = logRecords.get(key).get().getData().preCombine(hoodieRecord.getData());
            result.add(Pair.of(key, Option.of(new HoodieRecord(hoodieRecord.getKey(), mergedPayload))));
          } else {
            // only base record
            result.add(Pair.of(key, Option.of(hoodieRecord)));
          }
        } else {
          // only log record
          result.add(Pair.of(key, logRecords.get(key)));
        }
      }
      timings.add(timer.endTimer());
    } else {
      // no base file at all
      timings.add(timer.endTimer());
      for (Map.Entry<String, Option<HoodieRecord<HoodieMetadataPayload>>> entry : logRecords.entrySet()) {
        result.add(Pair.of(entry.getKey(), entry.getValue()));
      }
    }
    return result;
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
   * Returns a new pair of readers to the base and log files.
   */
  private Pair<HoodieFileReader, HoodieMetadataMergedLogRecordReader> openReadersIfNeeded(String key, String partitionName) {
    return partitionReaders.computeIfAbsent(partitionName, k -> {
      try {
        final long baseFileOpenMs;
        final long logScannerOpenMs;
        HoodieFileReader baseFileReader = null;
        HoodieMetadataMergedLogRecordReader logRecordScanner = null;

        // Metadata is in sync till the latest completed instant on the dataset
        HoodieTimer timer = new HoodieTimer().startTimer();
        List<FileSlice> latestFileSlices = HoodieTableMetadataUtil.getPartitionLatestMergedFileSlices(metadataMetaClient, partitionName);
        if (latestFileSlices.size() == 0) {
          // empty partition
          return Pair.of(null, null);
        }
        ValidationUtils.checkArgument(latestFileSlices.size() == 1, String.format("Invalid number of file slices: found=%d, required=%d", latestFileSlices.size(), 1));
        final FileSlice slice = latestFileSlices.get(HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(key, latestFileSlices.size()));

        // Open base file reader
        Pair<HoodieFileReader, Long> baseFileReaderOpenTimePair = getBaseFileReader(slice, timer);
        baseFileReader = baseFileReaderOpenTimePair.getKey();
        baseFileOpenMs = baseFileReaderOpenTimePair.getValue();

        // Open the log record scanner using the log files from the latest file slice
        Pair<HoodieMetadataMergedLogRecordReader, Long> logRecordScannerOpenTimePair = getLogRecordScanner(slice,
            partitionName);
        logRecordScanner = logRecordScannerOpenTimePair.getKey();
        logScannerOpenMs = logRecordScannerOpenTimePair.getValue();

        metrics.ifPresent(metrics -> metrics.updateMetrics(HoodieMetadataMetrics.SCAN_STR, baseFileOpenMs + logScannerOpenMs));
        return Pair.of(baseFileReader, logRecordScanner);
      } catch (IOException e) {
        throw new HoodieIOException("Error opening readers for metadata table partition " + partitionName, e);
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
        .filter(instant -> HoodieTimeline.compareTimestamps(instant.getTimestamp(), HoodieTimeline.GREATER_THAN, earliestInstantTime))
        .forEach(instant -> {
          validInstantTimestamps.addAll(getRollbackedCommits(instant, datasetTimeline));
        });

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
        .withMaxMemorySizeInBytes(MAX_MEMORY_SIZE_IN_BYTES)
        .withBufferSize(BUFFER_SIZE)
        .withSpillableMapBasePath(spillableMapDirectory)
        .withDiskMapType(commonConfig.getSpillableDiskMapType())
        .withBitCaskDiskMapCompressionEnabled(commonConfig.isBitCaskDiskMapCompressionEnabled())
        .withLogBlockTimestamps(validInstantTimestamps)
        .enableFullScan(metadataConfig.enableFullScan())
        .withPartition(partitionName)
        .build();

    Long logScannerOpenMs = timer.endTimer();
    LOG.info(String.format("Opened %d metadata log files (dataset instant=%s, metadata instant=%s) in %d ms",
        logFilePaths.size(), getLatestDataInstantTime(), latestMetadataInstantTime, logScannerOpenMs));
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
    for (String partitionName : partitionReaders.keySet()) {
      close(partitionName);
    }
    partitionReaders.clear();
  }

  private synchronized void close(String partitionName) {
    Pair<HoodieFileReader, HoodieMetadataMergedLogRecordReader> readers = partitionReaders.remove(partitionName);
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
    initIfNeeded();
    dataMetaClient.reloadActiveTimeline();
  }
}
