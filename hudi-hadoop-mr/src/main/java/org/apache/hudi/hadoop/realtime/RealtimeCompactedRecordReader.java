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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.config.HoodieRealtimeConfig;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

class RealtimeCompactedRecordReader extends AbstractRealtimeRecordReader
    implements RecordReader<NullWritable, ArrayWritable> {

  private static final Logger LOG = LogManager.getLogger(AbstractRealtimeRecordReader.class);

  protected final RecordReader<NullWritable, ArrayWritable> parquetReader;
  private final Map<String, HoodieRecord<? extends HoodieRecordPayload>> deltaRecordMap;

  public RealtimeCompactedRecordReader(RealtimeSplit split, JobConf job,
      RecordReader<NullWritable, ArrayWritable> realReader) throws IOException {
    super(split, job);
    this.parquetReader = realReader;
    this.deltaRecordMap = getMergedLogRecordScanner().getRecords();
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
        .build();
  }

  @Override
  public boolean next(NullWritable aVoid, ArrayWritable arrayWritable) throws IOException {
    // Call the underlying parquetReader.next - which may replace the passed in ArrayWritable
    // with a new block of values
    boolean result = this.parquetReader.next(aVoid, arrayWritable);
    if (!result) {
      // if the result is false, then there are no more records
      return false;
    }
    if (!deltaRecordMap.isEmpty()) {
      // TODO(VC): Right now, we assume all records in log, have a matching base record. (which
      // would be true until we have a way to index logs too)
      // return from delta records map if we have some match.
      String key = arrayWritable.get()[HoodieInputFormatUtils.HOODIE_RECORD_KEY_COL_POS].toString();
      if (deltaRecordMap.containsKey(key)) {
        // TODO(NA): Invoke preCombine here by converting arrayWritable to Avro. This is required since the
        // deltaRecord may not be a full record and needs values of columns from the parquet
        Option<GenericRecord> rec;
        if (usesCustomPayload) {
          rec = deltaRecordMap.get(key).getData().getInsertValue(getWriterSchema());
        } else {
          rec = deltaRecordMap.get(key).getData().getInsertValue(getReaderSchema());
        }
        if (!rec.isPresent()) {
          // If the record is not present, this is a delete record using an empty payload so skip this base record
          // and move to the next record
          return next(aVoid, arrayWritable);
        }
        GenericRecord recordToReturn = rec.get();
        if (usesCustomPayload) {
          // If using a custom payload, return only the projection fields. The readerSchema is a schema derived from
          // the writerSchema with only the projection fields
          recordToReturn = HoodieAvroUtils.rewriteRecordWithOnlyNewSchemaFields(rec.get(), getReaderSchema());
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
    }
    return true;
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
  }

  @Override
  public float getProgress() throws IOException {
    return parquetReader.getProgress();
  }
}
