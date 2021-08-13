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
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
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
  // should we reuse the open file handles, across calls
  private final boolean reuse;

  // Readers for each shard identified of a partition
  private Map<String, List<ShardReader>> shardReaders = new ConcurrentHashMap<>();
  // Cache of records loaded from metadata
  protected final ExternalSpillableMap<String, HoodieRecord> cachedRecords;

  public HoodieBackedTableMetadata(HoodieEngineContext engineContext, HoodieMetadataConfig metadataConfig,
                                   String datasetBasePath, String spillableMapDirectory) {
    this(engineContext, metadataConfig, datasetBasePath, spillableMapDirectory, false);
  }

  public HoodieBackedTableMetadata(HoodieEngineContext engineContext, HoodieMetadataConfig metadataConfig,
                                   String datasetBasePath, String spillableMapDirectory, boolean reuse) {
    super(engineContext, metadataConfig, datasetBasePath, spillableMapDirectory);
    this.reuse = reuse;
    initIfNeeded();

    try {
      this.cachedRecords = new ExternalSpillableMap<>(metadataConfig.getMaxRecordCacheMemory(), metadataConfig.getSplliableMapDir(),
          new DefaultSizeEstimator(), new HoodieRecordSizeEstimator(HoodieMetadataRecord.SCHEMA$));
    } catch (IOException e) {
      throw new HoodieMetadataException("Could not allocate an ExternalSpillableMap at " + metadataConfig.getSplliableMapDir(), e);
    }
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
      } catch (TableNotFoundException e) {
        LOG.warn("Metadata table was not found at path " + metadataBasePath);
        this.enabled = false;
        this.metaClient = null;
      } catch (Exception e) {
        LOG.error("Failed to initialize metadata table at path " + metadataBasePath, e);
        this.enabled = false;
        this.metaClient = null;
      }
    }
  }

  @Override
  protected Option<HoodieRecord<HoodieMetadataPayload>> getRecordByKeyFromMetadata(String key, String partitionName) {
    openReadersIfNeeded(partitionName);

    try {
      List<ShardReader> readers = shardReaders.get(partitionName);
      int shardIndex = HoodieTableMetadataUtil.keyToShard(key, readers.size());
      return readers.get(shardIndex).getRecordByKey(key);
    } finally {
      if (!reuse) {
        close(partitionName);
      }
    }
  }

  /**
   * Opens readers to the shards of a partition.
   */
  private void openReadersIfNeeded(String partitionName) {
    shardReaders.computeIfAbsent(partitionName, k -> {
      // Load the shards
      List<FileSlice> shards = HoodieTableMetadataUtil.loadPartitionShards(metaClient, partitionName);

      // Only those log files which have a corresponding completed instant on the dataset should be read
      // This is because the metadata table is updated before the dataset instants are committed.
      Set<String> validInstantTimestamps = datasetMetaClient.getActiveTimeline().filterCompletedInstants().getInstants()
          .map(i -> i.getTimestamp()).collect(Collectors.toSet());

      Option<HoodieInstant> lastInstant = metaClient.getActiveTimeline().filterCompletedInstants().lastInstant();
      String latestMetaInstantTimestamp = lastInstant.map(HoodieInstant::getTimestamp).orElse(SOLO_COMMIT_TIMESTAMP);

      // Create a ShardReader for each shard
      return shards.stream().map(slice -> {
        try {
          HoodieFileReader baseFileReader = null;
          HoodieMetadataMergedLogRecordScanner logRecordScanner = null;
          final long baseFileOpenMs;
          final long logScannerOpenMs;

          HoodieTimer timer = new HoodieTimer().startTimer();

          // Metadata is in sync till the latest completed instant on the dataset
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

          Schema schema = HoodieAvroUtils.addMetadataFields(HoodieMetadataRecord.getClassSchema());
          HoodieCommonConfig commonConfig = HoodieCommonConfig.newBuilder().fromProperties(metadataConfig.getProps()).build();
          logRecordScanner = HoodieMetadataMergedLogRecordScanner.newBuilder()
              .withFileSystem(metaClient.getFs())
              .withBasePath(metadataBasePath)
              .withLogFilePaths(logFilePaths)
              .withReaderSchema(schema)
              .withLatestInstantTime(latestMetaInstantTimestamp)
              .withMaxMemorySizeInBytes(metadataConfig.getMaxReaderMemory())
              .withBufferSize(metadataConfig.getMaxReaderBufferSize())
              .withSpillableMapBasePath(metadataConfig.getSplliableMapDir())
              .withDiskMapType(commonConfig.getSpillableDiskMapType())
              .withBitCaskDiskMapCompressionEnabled(commonConfig.isBitCaskDiskMapCompressionEnabled())
              .withLogBlockTimestamps(validInstantTimestamps)
              .build();

          logScannerOpenMs = timer.endTimer();

          LOG.info(String.format("Opened %d metadata log files (dataset instant=%s, metadata instant=%s) in %d ms",
              logFilePaths.size(), getLatestDatasetInstantTime(), latestMetaInstantTimestamp, logScannerOpenMs));

          metrics.ifPresent(metrics -> metrics.updateMetrics(HoodieMetadataMetrics.SCAN_STR, baseFileOpenMs + logScannerOpenMs));
          return new ShardReader(baseFileReader, logRecordScanner, metaClient.getTableConfig(), metrics);
        } catch (Exception e) {
          throw new HoodieMetadataException("Error opening readers for metadata table partition " + partitionName, e);
        }
      }).collect(Collectors.toList());
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
    shardReaders.remove(partitionName).forEach(sr -> sr.close());
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

  @Override
  protected void loadAndCacheKeys(Set<String> recordKeys, String partitionName) {
    cachedRecords.clear();

    if (recordKeys.isEmpty()) {
      return;
    }

    // Open the readers
    openReadersIfNeeded(partitionName);

    // Load keys from all shards involved
    List<ShardReader> readers = shardReaders.get(partitionName);
    for (String key : recordKeys) {
      ShardReader reader = readers.get(HoodieTableMetadataUtil.keyToShard(key, readers.size()));
      Option<HoodieRecord<HoodieMetadataPayload>> record = reader.getRecordByKey(key);
      if (record.isPresent()) {
        cachedRecords.put(key, record.get());
      }
    }
  }

  @Override
  protected void clearKeyCache() {
    cachedRecords.clear();
  }

  @Override
  protected Option<HoodieRecord<HoodieMetadataPayload>> getRecordByKeyFromCache(String key) {
    return Option.ofNullable(cachedRecords.get(key));
  }

  /**
   * A reader for one shard of the metadata table.
   *
   * A shard is made up of a base file and a set of log files. This reader merges the record from base and log files to
   * return the latest record.
   */
  private static class ShardReader {
    private final HoodieFileReader baseFileReader;
    private final HoodieMetadataMergedLogRecordScanner logFileReader;
    private Option<HoodieMetadataMetrics> metrics;
    private final HoodieTableConfig tableConfig;

    public ShardReader(HoodieFileReader baseFileReader, HoodieMetadataMergedLogRecordScanner logFileReader,
        HoodieTableConfig tableConfig, Option<HoodieMetadataMetrics> metrics) {
      this.baseFileReader = baseFileReader;
      this.logFileReader = logFileReader;
      this.tableConfig = tableConfig;
      this.metrics = metrics;
    }

    public void close() {
      try {
        if (baseFileReader != null) {
          baseFileReader.close();
        }
        if (logFileReader != null) {
          logFileReader.close();
        }
      } catch (Exception e) {
        throw new HoodieException("Error closing resources during metadata table merge", e);
      }
    }

    public Option<HoodieRecord<HoodieMetadataPayload>> getRecordByKey(String key) {
      try {
        // Retrieve record from base file
        HoodieRecord<HoodieMetadataPayload> hoodieRecord = null;
        if (baseFileReader != null) {
          HoodieTimer timer = new HoodieTimer().startTimer();
          Option<GenericRecord> baseRecord = baseFileReader.getRecordByKey(key);
          if (baseRecord.isPresent()) {
            hoodieRecord = tableConfig.populateMetaFields() ? SpillableMapUtils.convertToHoodieRecordPayload(baseRecord.get(),
                tableConfig.getPayloadClass()) : SpillableMapUtils.convertToHoodieRecordPayload(baseRecord.get(),
                tableConfig.getPayloadClass(), Pair.of(tableConfig.getRecordKeyFieldProp(),
                tableConfig.getPartitionFieldProp()));
            metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.BASEFILE_READ_STR, timer.endTimer()));
          }
        }

        // Retrieve record from log file
        if (logFileReader != null) {
          HoodieTimer timer = new HoodieTimer().startTimer();
          Option<HoodieRecord<HoodieMetadataPayload>> logHoodieRecord = logFileReader.getRecordByKey(key);
          if (logHoodieRecord.isPresent()) {
            if (hoodieRecord != null) {
              // Merge the payloads
              HoodieRecordPayload mergedPayload = logHoodieRecord.get().getData().preCombine(hoodieRecord.getData());
              hoodieRecord = new HoodieRecord(hoodieRecord.getKey(), mergedPayload);
            } else {
              hoodieRecord = logHoodieRecord.get();
            }
            metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.LOGFILE_READ_STR, timer.endTimer()));
          }
        }
        return Option.ofNullable(hoodieRecord);
      } catch (IOException ioe) {
        throw new HoodieIOException("Error merging records from metadata table for key :" + key, ioe);
      }
    }
  }
}
