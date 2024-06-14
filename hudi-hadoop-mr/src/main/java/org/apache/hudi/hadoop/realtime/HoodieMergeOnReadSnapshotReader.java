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

package org.apache.hudi.hadoop.realtime;

import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.storage.HoodieStorageUtils;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED;
import static org.apache.hudi.common.config.HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE;
import static org.apache.hudi.hadoop.config.HoodieRealtimeConfig.DEFAULT_MAX_DFS_STREAM_BUFFER_SIZE;
import static org.apache.hudi.hadoop.config.HoodieRealtimeConfig.DEFAULT_SPILLABLE_MAP_BASE_PATH;
import static org.apache.hudi.hadoop.config.HoodieRealtimeConfig.ENABLE_OPTIMIZED_LOG_BLOCKS_SCAN;
import static org.apache.hudi.hadoop.config.HoodieRealtimeConfig.MAX_DFS_STREAM_BUFFER_SIZE_PROP;
import static org.apache.hudi.hadoop.config.HoodieRealtimeConfig.SPILLABLE_MAP_BASE_PATH_PROP;
import static org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getBaseFileReader;
import static org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes;
import static org.apache.hudi.internal.schema.InternalSchema.getEmptyInternalSchema;

/**
 * An implementation of {@link AbstractRealtimeRecordReader} that reads from base parquet files and log files,
 * and merges the records on the fly. It differs from {@link HoodieRealtimeRecordReader} in that it does not
 * implement Hadoop's RecordReader interface, and instead implements Iterator interface that returns an iterator
 * of {@link HoodieRecord}s which are {@link HoodieAvroIndexedRecord}s. This can be used by query engines like
 * Trino that do not use Hadoop's RecordReader interface. However, the engine must support reading from iterators
 * and also support Avro (de)serialization.
 */
public class HoodieMergeOnReadSnapshotReader extends AbstractRealtimeRecordReader implements Iterator<HoodieRecord>, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieMergeOnReadSnapshotReader.class);

  private final String tableBasePath;
  private final List<HoodieLogFile> logFilePaths;
  private final String latestInstantTime;
  private final Schema readerSchema;
  private final JobConf jobConf;
  private final HoodieMergedLogRecordScanner logRecordScanner;
  private final HoodieFileReader baseFileReader;
  private final Map<String, HoodieRecord> logRecordsByKey;
  private final Iterator<HoodieRecord> recordsIterator;
  private final ExternalSpillableMap<String, HoodieRecord> mergedRecordsByKey;

  /**
   * In order to instantiate this record reader, one needs to provide following parameters.
   * An example usage is demonstrated in TestHoodieMergeOnReadSnapshotReader.
   *
   * @param tableBasePath     Base path of the Hudi table
   * @param baseFilePath      Path of the base file as of the latest instant time for the split being processed
   * @param logFilePaths      Paths of the log files as of the latest file slices pertaining to file group id of the base file
   * @param latestInstantTime Latest instant time
   * @param readerSchema      Schema of the reader
   * @param jobConf           Any job configuration
   * @param start             Start offset
   * @param length            Length of the split
   */
  public HoodieMergeOnReadSnapshotReader(String tableBasePath,
                                         String baseFilePath,
                                         List<HoodieLogFile> logFilePaths,
                                         String latestInstantTime,
                                         Schema readerSchema,
                                         JobConf jobConf,
                                         long start,
                                         long length) throws IOException {
    super(getRealtimeSplit(tableBasePath, baseFilePath, logFilePaths, latestInstantTime, start, length, new String[0]), jobConf);
    this.tableBasePath = tableBasePath;
    this.logFilePaths = logFilePaths;
    this.latestInstantTime = latestInstantTime;
    this.readerSchema = readerSchema;
    this.jobConf = jobConf;
    HoodieTimer timer = new HoodieTimer().startTimer();
    this.logRecordScanner = getMergedLogRecordScanner();
    LOG.debug("Time taken to scan log records: {}", timer.endTimer());
    this.baseFileReader = getBaseFileReader(new Path(baseFilePath), jobConf);
    this.logRecordsByKey = logRecordScanner.getRecords();
    Set<String> logRecordKeys = new HashSet<>(this.logRecordsByKey.keySet());
    this.mergedRecordsByKey = new ExternalSpillableMap<>(
        getMaxCompactionMemoryInBytes(jobConf),
        jobConf.get(SPILLABLE_MAP_BASE_PATH_PROP, DEFAULT_SPILLABLE_MAP_BASE_PATH),
        new DefaultSizeEstimator(),
        new HoodieRecordSizeEstimator(readerSchema),
        jobConf.getEnum(SPILLABLE_DISK_MAP_TYPE.key(), SPILLABLE_DISK_MAP_TYPE.defaultValue()),
        jobConf.getBoolean(DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(), DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue()));
    try (ClosableIterator<String> baseFileIterator = baseFileReader.getRecordKeyIterator()) {
      timer.startTimer();
      while (baseFileIterator.hasNext()) {
        String key = baseFileIterator.next();
        if (logRecordKeys.contains(key)) {
          logRecordKeys.remove(key);
          Option<HoodieAvroIndexedRecord> mergedRecord = buildGenericRecordWithCustomPayload(logRecordsByKey.get(key));
          if (mergedRecord.isPresent()) {
            HoodieRecord hoodieRecord = mergedRecord.get().copy();
            mergedRecordsByKey.put(key, hoodieRecord);
          }
        }
      }
    }
    LOG.debug("Time taken to merge base file and log file records: {}", timer.endTimer());
    this.recordsIterator = mergedRecordsByKey.values().iterator();
  }

  @Override
  public boolean hasNext() {
    return recordsIterator.hasNext();
  }

  @Override
  public HoodieRecord next() {
    return recordsIterator.next();
  }

  public Map<String, HoodieRecord> getRecordsByKey() {
    return mergedRecordsByKey;
  }

  public Iterator<HoodieRecord> getRecordsIterator() {
    return recordsIterator;
  }

  public Map<String, HoodieRecord> getLogRecordsByKey() {
    return logRecordsByKey;
  }

  private static HoodieRealtimeFileSplit getRealtimeSplit(String tableBasePath, String baseFilePath,
                                                          List<HoodieLogFile> logFilePaths,
                                                          String latestInstantTime,
                                                          long start, long length, String[] hosts) {
    HoodieRealtimePath realtimePath = new HoodieRealtimePath(
        new Path(baseFilePath).getParent(),
        baseFilePath,
        tableBasePath,
        logFilePaths,
        latestInstantTime,
        false, // TODO: Fix this to support incremental queries
        Option.empty());
    return HoodieInputFormatUtils.createRealtimeFileSplit(realtimePath, start, length, hosts);
  }

  private HoodieMergedLogRecordScanner getMergedLogRecordScanner() {
    return HoodieMergedLogRecordScanner.newBuilder()
        .withStorage(HoodieStorageUtils.getStorage(
            split.getPath().toString(), HadoopFSUtils.getStorageConf(jobConf)))
        .withBasePath(tableBasePath)
        .withLogFilePaths(logFilePaths.stream().map(logFile -> logFile.getPath().toString()).collect(Collectors.toList()))
        .withReaderSchema(readerSchema)
        .withLatestInstantTime(latestInstantTime)
        .withMaxMemorySizeInBytes(getMaxCompactionMemoryInBytes(jobConf))
        .withReverseReader(false)
        .withBufferSize(jobConf.getInt(MAX_DFS_STREAM_BUFFER_SIZE_PROP, DEFAULT_MAX_DFS_STREAM_BUFFER_SIZE))
        .withSpillableMapBasePath(jobConf.get(SPILLABLE_MAP_BASE_PATH_PROP, DEFAULT_SPILLABLE_MAP_BASE_PATH))
        .withDiskMapType(jobConf.getEnum(SPILLABLE_DISK_MAP_TYPE.key(), SPILLABLE_DISK_MAP_TYPE.defaultValue()))
        .withBitCaskDiskMapCompressionEnabled(jobConf.getBoolean(DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(), DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue()))
        .withOptimizedLogBlocksScan(jobConf.getBoolean(ENABLE_OPTIMIZED_LOG_BLOCKS_SCAN, false))
        .withInternalSchema(schemaEvolutionContext.internalSchemaOption.orElse(getEmptyInternalSchema()))
        .build();
  }

  private Option<HoodieAvroIndexedRecord> buildGenericRecordWithCustomPayload(HoodieRecord record) throws IOException {
    if (usesCustomPayload) {
      return record.toIndexedRecord(getWriterSchema(), payloadProps);
    } else {
      return record.toIndexedRecord(readerSchema, payloadProps);
    }
  }

  @Override
  public void close() throws Exception {
    if (baseFileReader != null) {
      baseFileReader.close();
    }
    if (logRecordScanner != null) {
      logRecordScanner.close();
    }
  }
}
