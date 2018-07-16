/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.hadoop.realtime;

import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.table.log.HoodieMergedLogRecordScanner;
import com.uber.hoodie.common.util.FSUtils;
import java.io.IOException;
import java.util.HashMap;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

class RealtimeCompactedRecordReader extends AbstractRealtimeRecordReader implements
    RecordReader<Void, ArrayWritable> {

  protected final RecordReader<Void, ArrayWritable> parquetReader;
  private final HashMap<String, ArrayWritable> deltaRecordMap;

  public RealtimeCompactedRecordReader(HoodieRealtimeFileSplit split, JobConf job,
      RecordReader<Void, ArrayWritable> realReader) throws IOException {
    super(split, job);
    this.parquetReader = realReader;
    this.deltaRecordMap = new HashMap<>();
    readAndCompactLog();
  }

  /**
   * Goes through the log files and populates a map with latest version of each key logged, since
   * the base split was written.
   */
  private void readAndCompactLog() throws IOException {
    HoodieMergedLogRecordScanner compactedLogRecordScanner = new HoodieMergedLogRecordScanner(
        FSUtils.getFs(split.getPath().toString(), jobConf), split.getBasePath(),
        split.getDeltaFilePaths(), getReaderSchema(), split.getMaxCommitTime(), getMaxCompactionMemoryInBytes(),
        Boolean.valueOf(jobConf.get(COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP,
            DEFAULT_COMPACTION_LAZY_BLOCK_READ_ENABLED)),
        false, jobConf.getInt(MAX_DFS_STREAM_BUFFER_SIZE_PROP, DEFAULT_MAX_DFS_STREAM_BUFFER_SIZE),
        jobConf.get(SPILLABLE_MAP_BASE_PATH_PROP, DEFAULT_SPILLABLE_MAP_BASE_PATH));
    // NOTE: HoodieCompactedLogRecordScanner will not return records for an in-flight commit
    // but can return records for completed commits > the commit we are trying to read (if using
    // readCommit() API)
    for (HoodieRecord<? extends HoodieRecordPayload> hoodieRecord : compactedLogRecordScanner) {
      GenericRecord rec = (GenericRecord) hoodieRecord.getData().getInsertValue(getReaderSchema()).get();
      String key = hoodieRecord.getRecordKey();
      // we assume, a later safe record in the log, is newer than what we have in the map &
      // replace it.
      // TODO : handle deletes here
      ArrayWritable aWritable = (ArrayWritable) avroToArrayWritable(rec, getWriterSchema());
      deltaRecordMap.put(key, aWritable);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Log record : " + arrayWritableToString(aWritable));
      }
    }
  }

  @Override
  public boolean next(Void aVoid, ArrayWritable arrayWritable) throws IOException {
    // Call the underlying parquetReader.next - which may replace the passed in ArrayWritable
    // with a new block of values
    boolean result = this.parquetReader.next(aVoid, arrayWritable);
    if (!result) {
      // if the result is false, then there are no more records
      return false;
    } else {
      // TODO(VC): Right now, we assume all records in log, have a matching base record. (which
      // would be true until we have a way to index logs too)
      // return from delta records map if we have some match.
      String key = arrayWritable.get()[HoodieRealtimeInputFormat.HOODIE_RECORD_KEY_COL_POS]
          .toString();
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("key %s, base values: %s, log values: %s", key,
            arrayWritableToString(arrayWritable), arrayWritableToString(deltaRecordMap.get(key))));
      }
      if (deltaRecordMap.containsKey(key)) {
        // TODO(NA): Invoke preCombine here by converting arrayWritable to Avro ?
        Writable[] replaceValue = deltaRecordMap.get(key).get();
        Writable[] originalValue = arrayWritable.get();
        try {
          System.arraycopy(replaceValue, 0, originalValue, 0, originalValue.length);
          arrayWritable.set(originalValue);
        } catch (RuntimeException re) {
          LOG.error("Got exception when doing array copy", re);
          LOG.error("Base record :" + arrayWritableToString(arrayWritable));
          LOG.error("Log record :" + arrayWritableToString(deltaRecordMap.get(key)));
          throw re;
        }
      }
      return true;
    }
  }

  @Override
  public Void createKey() {
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
