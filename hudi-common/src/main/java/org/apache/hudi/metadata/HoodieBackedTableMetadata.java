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
  private HoodieTableMetaClient metaClient;
  private HoodieTableConfig tableConfig;
  // should we reuse the open file handles, across calls
  private final boolean reuse;

  // Shards for each partition
  private Map<String, List<FileSlice>> partitionToShardsMap;
  // Readers for each shard identified by the fileId
  private Map<String, Pair<HoodieFileReader, HoodieMetadataMergedLogRecordScanner>> shardReaders = new ConcurrentHashMap<>();

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
    this.metadataBasePath = HoodieTableMetadata.getMetadataTableBasePath(datasetBasePath);
    if (!enabled) {
      if (!HoodieTableMetadata.isMetadataTable(metadataBasePath)) {
        LOG.info("Metadata table is disabled.");
      }
    } else if (this.metaClient == null) {
      try {
        this.metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf.get()).setBasePath(metadataBasePath).build();
        this.tableConfig = metaClient.getTableConfig();
      } catch (TableNotFoundException e) {
        LOG.warn("Metadata table was not found at path " + metadataBasePath);
        this.enabled = false;
        this.metaClient = null;
        this.tableConfig = null;
      } catch (Exception e) {
        LOG.error("Failed to initialize metadata table at path " + metadataBasePath, e);
        this.enabled = false;
        this.metaClient = null;
        this.tableConfig = null;
      }
    }
  }

  @Override
  protected Option<HoodieRecord<HoodieMetadataPayload>> getRecordByKeyFromMetadata(String key, String partitionName) {
    Pair<HoodieFileReader, HoodieMetadataMergedLogRecordScanner> readers;
    try {
      readers = openReadersIfNeeded(key, partitionName);
    } catch (IOException e) {
      throw new HoodieIOException("Error opening readers to partition " + partitionName + " for key " + key, e);
    }

    try {
      List<Long> timings = new ArrayList<>();
      HoodieTimer timer = new HoodieTimer().startTimer();
      HoodieFileReader baseFileReader = readers.getKey();
      HoodieMetadataMergedLogRecordScanner logRecordScanner = readers.getRight();

      // Retrieve record from base file
      HoodieRecord<HoodieMetadataPayload> hoodieRecord = null;
      if (baseFileReader != null) {
        HoodieTimer readTimer = new HoodieTimer().startTimer();
        Option<GenericRecord> baseRecord = baseFileReader.getRecordByKey(key);
        if (baseRecord.isPresent()) {
          hoodieRecord = tableConfig.populateMetaFields() ? SpillableMapUtils.convertToHoodieRecordPayload(baseRecord.get(),
              tableConfig.getPayloadClass()) : SpillableMapUtils.convertToHoodieRecordPayload(baseRecord.get(),
              tableConfig.getPayloadClass(), Pair.of(tableConfig.getRecordKeyFieldProp(),
              tableConfig.getPartitionFieldProp()));
          metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.BASEFILE_READ_STR, readTimer.endTimer()));
        }
      }
      timings.add(timer.endTimer());

      // Retrieve record from log file
      timer.startTimer();
      if (logRecordScanner != null) {
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
      }
      timings.add(timer.endTimer());
      LOG.info(String.format("Metadata read for key %s took [baseFileRead, logMerge] %s ms", key, timings));
      return Option.ofNullable(hoodieRecord);
    } catch (IOException ioe) {
      throw new HoodieIOException("Error merging records from metadata table for key :" + key, ioe);
    } finally {
      if (!reuse) {
        close(partitionName);
      }
    }
  }

  /**
   * Returns a new pair of readers to the base and log files.
   */
  private Pair<HoodieFileReader, HoodieMetadataMergedLogRecordScanner> openReadersIfNeeded(String key, String partitionName) throws IOException {
    return shardReaders.computeIfAbsent(partitionName, k -> {
      try {
        final long baseFileOpenMs;
        final long logScannerOpenMs;
        HoodieFileReader baseFileReader = null;
        HoodieMetadataMergedLogRecordScanner logRecordScanner = null;

        // Metadata is in sync till the latest completed instant on the dataset
        HoodieTimer timer = new HoodieTimer().startTimer();
        List<FileSlice> shards = HoodieTableMetadataUtil.loadPartitionShards(metaClient, partitionName);
        ValidationUtils.checkArgument(shards.size() == 1, String.format("Invalid number of shards: found=%d, required=%d", shards.size(), 1));
        final FileSlice slice = shards.get(HoodieTableMetadataUtil.keyToShard(key, shards.size()));

        // If the base file is present then create a reader
        Option<HoodieBaseFile> basefile = slice.getBaseFile();
        if (basefile.isPresent()) {
          String basefilePath = basefile.get().getPath();
          baseFileReader = HoodieFileReaderFactory.getFileReader(hadoopConf.get(), new Path(basefilePath));
          baseFileOpenMs = timer.endTimer();
          LOG.info(String.format("Opened metadata base file from %s at instant %s in %d ms", basefilePath,
              basefile.get().getCommitTime(), baseFileOpenMs));
        } else {
          baseFileOpenMs = 0;
          timer.endTimer();
        }

        // Open the log record scanner using the log files from the latest file slice
        timer.startTimer();
        List<String> logFilePaths = slice.getLogFiles()
            .sorted(HoodieLogFile.getLogFileComparator())
            .map(o -> o.getPath().toString())
            .collect(Collectors.toList());

        // Only those log files which have a corresponding completed instant on the dataset should be read
        // This is because the metadata table is updated before the dataset instants are committed.
        HoodieActiveTimeline datasetTimeline = datasetMetaClient.getActiveTimeline();
        Set<String> validInstantTimestamps = datasetTimeline.filterCompletedInstants().getInstants()
            .map(i -> i.getTimestamp()).collect(Collectors.toSet());

        // For any rollbacks and restores, we cannot neglect the instants that they are rolling back.
        // The rollback instant should be more recent than the start of the timeline for it to have rolled back any
        // instant which we have a log block for.
        final String minInstantTime = validInstantTimestamps.isEmpty() ? SOLO_COMMIT_TIMESTAMP : Collections.min(validInstantTimestamps);
        datasetTimeline.getRollbackAndRestoreTimeline().filterCompletedInstants().getInstants()
            .filter(instant -> HoodieTimeline.compareTimestamps(instant.getTimestamp(), HoodieTimeline.GREATER_THAN, minInstantTime))
            .forEach(instant -> {
              validInstantTimestamps.addAll(HoodieTableMetadataUtil.getCommitsRolledback(instant, datasetTimeline));
            });

        // SOLO_COMMIT_TIMESTAMP is used during bootstrap so it is a valid timestamp
        validInstantTimestamps.add(SOLO_COMMIT_TIMESTAMP);

        Option<HoodieInstant> lastInstant = metaClient.getActiveTimeline().filterCompletedInstants().lastInstant();
        String latestMetaInstantTimestamp = lastInstant.map(HoodieInstant::getTimestamp).orElse(SOLO_COMMIT_TIMESTAMP);

        // Load the schema
        Schema schema = HoodieAvroUtils.addMetadataFields(HoodieMetadataRecord.getClassSchema());
        HoodieCommonConfig commonConfig = HoodieCommonConfig.newBuilder().fromProperties(metadataConfig.getProps()).build();
        logRecordScanner = HoodieMetadataMergedLogRecordScanner.newBuilder()
            .withFileSystem(metaClient.getFs())
            .withBasePath(metadataBasePath)
            .withLogFilePaths(logFilePaths)
            .withReaderSchema(schema)
            .withLatestInstantTime(latestMetaInstantTimestamp)
            .withMaxMemorySizeInBytes(MAX_MEMORY_SIZE_IN_BYTES)
            .withBufferSize(BUFFER_SIZE)
            .withSpillableMapBasePath(spillableMapDirectory)
            .withDiskMapType(commonConfig.getSpillableDiskMapType())
            .withBitCaskDiskMapCompressionEnabled(commonConfig.isBitCaskDiskMapCompressionEnabled())
            .withLogBlockTimestamps(validInstantTimestamps)
            .build();

        logScannerOpenMs = timer.endTimer();

        LOG.info(String.format("Opened %d metadata log files (dataset instant=%s, metadata instant=%s) in %d ms",
            logFilePaths.size(), getLatestDatasetInstantTime(), latestMetaInstantTimestamp, logScannerOpenMs));

        metrics.ifPresent(metrics -> metrics.updateMetrics(HoodieMetadataMetrics.SCAN_STR, baseFileOpenMs + logScannerOpenMs));
        return Pair.of(baseFileReader, logRecordScanner);
      } catch (Exception e) {
        throw new HoodieMetadataException("Error opening readers for metadata table partition " + partitionName, e);
      }
    });
  }

  @Override
  public void close() {
    for (String partitionName : shardReaders.keySet()) {
      close(partitionName);
    }
    shardReaders.clear();
  }

  private synchronized void close(String partitionName) {
    Pair<HoodieFileReader, HoodieMetadataMergedLogRecordScanner> readers = shardReaders.remove(partitionName);
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

  public HoodieTableMetaClient getMetaClient() {
    return metaClient;
  }

  public Map<String, String> stats() {
    return metrics.map(m -> m.getStats(true, metaClient, this)).orElse(new HashMap<>());
  }

  @Override
  public Option<String> getSyncedInstantTime() {
    if (metaClient != null) {
      Option<HoodieInstant> latestInstant = metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants().lastInstant();
      if (latestInstant.isPresent()) {
        return Option.of(latestInstant.get().getTimestamp());
      }
    }

    return Option.empty();
  }

  @Override
  public Option<String> getLatestCompactionTime() {
    if (metaClient != null) {
      Option<HoodieInstant> latestCompaction = metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants().lastInstant();
      if (latestCompaction.isPresent()) {
        return Option.of(latestCompaction.get().getTimestamp());
      }
    }

    return Option.empty();
  }
}
