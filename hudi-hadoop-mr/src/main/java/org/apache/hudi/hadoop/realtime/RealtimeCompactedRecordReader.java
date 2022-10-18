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

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.config.HoodieRealtimeConfig;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

class RealtimeCompactedRecordReader extends AbstractRealtimeRecordReader
    implements RecordReader<NullWritable, ArrayWritable> {

  private static final Logger LOG = LogManager.getLogger(AbstractRealtimeRecordReader.class);

  protected final RecordReader<NullWritable, ArrayWritable> parquetReader;
  private final Map<String, HoodieRecord<? extends HoodieRecordPayload>> deltaRecordMap;

  private final Set<String> deltaRecordKeys;
  private final HoodieMergedLogRecordScanner mergedLogRecordScanner;
  private final int recordKeyIndex;
  private Iterator<String> deltaItr;

  public RealtimeCompactedRecordReader(RealtimeSplit split, JobConf job,
      RecordReader<NullWritable, ArrayWritable> realReader) throws IOException {
    super(split, job);
    this.parquetReader = realReader;
    this.mergedLogRecordScanner = getMergedLogRecordScanner();
    this.deltaRecordMap = mergedLogRecordScanner.getRecords();
    this.deltaRecordKeys = new HashSet<>(this.deltaRecordMap.keySet());
    this.recordKeyIndex = split.getVirtualKeyInfo()
        .map(HoodieVirtualKeyInfo::getRecordKeyFieldIndex)
        .orElse(HoodieInputFormatUtils.HOODIE_RECORD_KEY_COL_POS);
  }

  /**
   * Goes through the log files and populates a map with latest version of each key logged, since the base split was
   * written.
   */
  private HoodieMergedLogRecordScanner getMergedLogRecordScanner() throws IOException {
    // NOTE: HoodieCompactedLogRecordScanner will not return records for an in-flight commit
    // but can return records for completed commits > the commit we are trying to read (if using
    // readCommit() API)
    return HoodieMergedLogRecordScanner.newBuilder()
        .withFileSystem(FSUtils.getFs(split.getPath().toString(), jobConf))
        .withBasePath(split.getBasePath())
        .withLogFilePaths(split.getDeltaLogPaths())
        .withReaderSchema(usesCustomPayload ? getWriterSchema() : getReaderSchema())
        .withLatestInstantTime(split.getMaxCommitTime())
        .withMaxMemorySizeInBytes(HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes(jobConf))
        .withReadBlocksLazily(Boolean.parseBoolean(jobConf.get(HoodieRealtimeConfig.COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP, HoodieRealtimeConfig.DEFAULT_COMPACTION_LAZY_BLOCK_READ_ENABLED)))
        .withReverseReader(false)
        .withBufferSize(jobConf.getInt(HoodieRealtimeConfig.MAX_DFS_STREAM_BUFFER_SIZE_PROP, HoodieRealtimeConfig.DEFAULT_MAX_DFS_STREAM_BUFFER_SIZE))
        .withSpillableMapBasePath(jobConf.get(HoodieRealtimeConfig.SPILLABLE_MAP_BASE_PATH_PROP, HoodieRealtimeConfig.DEFAULT_SPILLABLE_MAP_BASE_PATH))
        .withDiskMapType(jobConf.getEnum(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.key(), HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.defaultValue()))
        .withBitCaskDiskMapCompressionEnabled(jobConf.getBoolean(HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(),
            HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue()))
        .withUseScanV2(jobConf.getBoolean(HoodieRealtimeConfig.USE_LOG_RECORD_READER_SCAN_V2, false))
        .withInternalSchema(schemaEvolutionContext.internalSchemaOption.orElse(InternalSchema.getEmptyInternalSchema()))
        .build();
  }

  private Option<GenericRecord> buildGenericRecordwithCustomPayload(HoodieRecord record) throws IOException {
    if (usesCustomPayload) {
      return ((HoodieAvroRecord) record).getData().getInsertValue(getWriterSchema(), payloadProps);
    } else {
      return ((HoodieAvroRecord) record).getData().getInsertValue(getReaderSchema(), payloadProps);
    }
  }

  @Override
  public boolean next(NullWritable aVoid, ArrayWritable arrayWritable) throws IOException {
    // Call the underlying parquetReader.next - which may replace the passed in ArrayWritable
    // with a new block of values
    while (this.parquetReader.next(aVoid, arrayWritable)) {
      if (!deltaRecordMap.isEmpty()) {
        String key = arrayWritable.get()[recordKeyIndex].toString();
        if (deltaRecordMap.containsKey(key)) {
          // mark the key as handled
          this.deltaRecordKeys.remove(key);
          // TODO(NA): Invoke preCombine here by converting arrayWritable to Avro. This is required since the
          // deltaRecord may not be a full record and needs values of columns from the parquet
          Option<GenericRecord> rec = buildGenericRecordwithCustomPayload(deltaRecordMap.get(key));
          // If the record is not present, this is a delete record using an empty payload so skip this base record
          // and move to the next record
          if (!rec.isPresent()) {
            continue;
          }
          setUpWritable(rec, arrayWritable, key);
          return true;
        }
      }
      return true;
    }
    if (this.deltaItr == null) {
      this.deltaItr = this.deltaRecordKeys.iterator();
    }
    while (this.deltaItr.hasNext()) {
      final String key = this.deltaItr.next();
      Option<GenericRecord> rec = buildGenericRecordwithCustomPayload(deltaRecordMap.get(key));
      if (rec.isPresent()) {
        setUpWritable(rec, arrayWritable, key);
        return true;
      }
    }
    return false;
  }

  private void setUpWritable(Option<GenericRecord> rec, ArrayWritable arrayWritable, String key) {
    GenericRecord recordToReturn = rec.get();
    if (usesCustomPayload) {
      // If using a custom payload, return only the projection fields. The readerSchema is a schema derived from
      // the writerSchema with only the projection fields
      recordToReturn = HoodieAvroUtils.rewriteRecord(rec.get(), getReaderSchema());
    }
    // we assume, a later safe record in the log, is newer than what we have in the map &
    // replace it. Since we want to return an arrayWritable which is the same length as the elements in the latest
    // schema, we use writerSchema to create the arrayWritable from the latest generic record
    ArrayWritable aWritable = (ArrayWritable) HoodieRealtimeRecordReaderUtils.avroToArrayWritable(recordToReturn, getHiveSchema());
    Writable[] replaceValue = aWritable.get();
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("key %s, base values: %s, log values: %s", key, HoodieRealtimeRecordReaderUtils.arrayWritableToString(arrayWritable),
          HoodieRealtimeRecordReaderUtils.arrayWritableToString(aWritable)));
    }
    Writable[] originalValue = arrayWritable.get();
    try {
      // Sometime originalValue.length > replaceValue.length.
      // This can happen when hive query is looking for pseudo parquet columns like BLOCK_OFFSET_INSIDE_FILE
      System.arraycopy(replaceValue, 0, originalValue, 0,
          Math.min(originalValue.length, replaceValue.length));
      arrayWritable.set(originalValue);
    } catch (RuntimeException re) {
      LOG.error("Got exception when doing array copy", re);
      LOG.error("Base record :" + HoodieRealtimeRecordReaderUtils.arrayWritableToString(arrayWritable));
      LOG.error("Log record :" + HoodieRealtimeRecordReaderUtils.arrayWritableToString(aWritable));
      String errMsg = "Base-record :" + HoodieRealtimeRecordReaderUtils.arrayWritableToString(arrayWritable)
          + " ,Log-record :" + HoodieRealtimeRecordReaderUtils.arrayWritableToString(aWritable) + " ,Error :" + re.getMessage();
      throw new RuntimeException(errMsg, re);
    }
  }

  @Override
  public NullWritable createKey() {
    return parquetReader.createKey();
  }

  @Override
  public ArrayWritable createValue() {
    return parquetReader.createValue();
  }

  @Override
  public long getPos() throws IOException {
    return parquetReader.getPos();
  }

  @Override
  public void close() throws IOException {
    parquetReader.close();
    // need clean the tmp file which created by logScanner
    // Otherwise, for resident process such as presto, the /tmp directory will overflow
    mergedLogRecordScanner.close();
  }

  @Override
  public float getProgress() throws IOException {
    return parquetReader.getProgress();
  }
}
