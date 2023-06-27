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

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.hadoop.config.HoodieRealtimeConfig;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.io.storage.HoodieFileReader;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HoodieMergeOnReadSnapshotReader extends AbstractRealtimeRecordReader implements Iterator<HoodieRecord> {

  private final String tableBasePath;
  private final String baseFilePath;
  private final List<HoodieLogFile> logFilePaths;
  private final String latestInstantTime;
  private final Schema readerSchema;
  private final JobConf jobConf;
  private final HoodieMergedLogRecordScanner logRecordScanner;
  private final HoodieFileReader baseFileReader;
  private final Map<String, HoodieRecord> logRecordMap;
  private final Set<String> logRecordKeys;
  private final Iterator<HoodieRecord> recordsIterator;

  public HoodieMergeOnReadSnapshotReader(String tableBasePath, String baseFilePath,
                                         List<HoodieLogFile> logFilePaths,
                                         String latestInstantTime,
                                         Schema readerSchema,
                                         JobConf jobConf, long start, long length, String[] hosts) throws IOException {
    super(getRealtimeSplit(tableBasePath, baseFilePath, logFilePaths, latestInstantTime, start, length, hosts), jobConf);
    this.tableBasePath = tableBasePath;
    this.baseFilePath = baseFilePath;
    this.logFilePaths = logFilePaths;
    this.latestInstantTime = latestInstantTime;
    this.readerSchema = readerSchema;
    this.jobConf = jobConf;
    this.logRecordScanner = getMergedLogRecordScanner();
    this.baseFileReader = HoodieRealtimeRecordReaderUtils.getBaseFileReader(new Path(baseFilePath), jobConf);
    this.logRecordMap = logRecordScanner.getRecords();
    this.logRecordKeys = new HashSet<>(this.logRecordMap.keySet());
    List<HoodieRecord> mergedRecords = new ArrayList<>();
    ClosableIterator<HoodieRecord> baseFileIterator = baseFileReader.getRecordIterator(readerSchema);
    while (baseFileIterator.hasNext()) {
      HoodieRecord record = baseFileIterator.next();
      if (logRecordKeys.contains(record.getRecordKey())) {
        logRecordKeys.remove(record.getRecordKey());
        Option<HoodieAvroIndexedRecord> mergedRecord = buildGenericRecordWithCustomPayload(logRecordMap.get(record.getRecordKey()));
        mergedRecord.ifPresent(mergedRecords::add);
      }
    }
    this.recordsIterator = mergedRecords.iterator();
  }

  @Override
  public boolean hasNext() {
    return recordsIterator.hasNext();
  }

  @Override
  public HoodieRecord next() {
    return recordsIterator.next();
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
        false,
        Option.empty());
    return HoodieInputFormatUtils.createRealtimeFileSplit(realtimePath, start, length, hosts);
  }

  private HoodieMergedLogRecordScanner getMergedLogRecordScanner() {
    // NOTE: HoodieCompactedLogRecordScanner will not return records for an in-flight commit
    // but can return records for completed commits > the commit we are trying to read (if using
    // readCommit() API)
    return HoodieMergedLogRecordScanner.newBuilder()
        .withFileSystem(FSUtils.getFs(split.getPath().toString(), jobConf))
        .withBasePath(split.getBasePath())
        .withLogFilePaths(split.getDeltaLogPaths())
        .withReaderSchema(getLogScannerReaderSchema())
        .withLatestInstantTime(split.getMaxCommitTime())
        .withMaxMemorySizeInBytes(HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes(jobConf))
        .withReadBlocksLazily(Boolean.parseBoolean(jobConf.get(HoodieRealtimeConfig.COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP, HoodieRealtimeConfig.DEFAULT_COMPACTION_LAZY_BLOCK_READ_ENABLED)))
        .withReverseReader(false)
        .withBufferSize(jobConf.getInt(HoodieRealtimeConfig.MAX_DFS_STREAM_BUFFER_SIZE_PROP, HoodieRealtimeConfig.DEFAULT_MAX_DFS_STREAM_BUFFER_SIZE))
        .withSpillableMapBasePath(jobConf.get(HoodieRealtimeConfig.SPILLABLE_MAP_BASE_PATH_PROP, HoodieRealtimeConfig.DEFAULT_SPILLABLE_MAP_BASE_PATH))
        .withDiskMapType(jobConf.getEnum(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.key(), HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.defaultValue()))
        .withBitCaskDiskMapCompressionEnabled(jobConf.getBoolean(HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(),
            HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue()))
        .withOptimizedLogBlocksScan(jobConf.getBoolean(HoodieRealtimeConfig.ENABLE_OPTIMIZED_LOG_BLOCKS_SCAN, false))
        .withInternalSchema(schemaEvolutionContext.internalSchemaOption.orElse(InternalSchema.getEmptyInternalSchema()))
        .build();
  }

  private Option<HoodieAvroIndexedRecord> buildGenericRecordWithCustomPayload(HoodieRecord record) throws IOException {
    if (usesCustomPayload) {
      return record.toIndexedRecord(getWriterSchema(), payloadProps);
    } else {
      return record.toIndexedRecord(getReaderSchema(), payloadProps);
    }
  }
}
