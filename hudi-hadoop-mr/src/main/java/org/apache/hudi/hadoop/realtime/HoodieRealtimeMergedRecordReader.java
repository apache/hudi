/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.hadoop.realtime;

import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.hadoop.HoodieColumnProjectionUtils;
import org.apache.hudi.hadoop.HoodieHiveFileSliceReader;
import org.apache.hudi.hadoop.HoodieHiveRecord;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getMergedLogRecordScanner;

public class HoodieRealtimeMergedRecordReader extends HoodieHiveFileSliceReader
    implements RecordReader<NullWritable, ArrayWritable> {
  private static final Logger LOG = LogManager.getLogger(HoodieRealtimeMergedRecordReader.class);

  private final RecordReader<NullWritable, ArrayWritable> baseRecordReader;
  private final Map<String, HoodieRecord> deltaRecordMap;
  private final Set<String> deltaRecordKeys;
  private final HoodieMergedLogRecordScanner mergedLogRecordScanner;
  private final int recordKeyIndex;
  private Iterator<String> deltaItr;

  public HoodieRealtimeMergedRecordReader(RealtimeSplit split, JobConf job, RecordReader<NullWritable, ArrayWritable> baseRecordReader) {
    super(split, job);
    this.baseRecordReader = baseRecordReader;
    this.mergedLogRecordScanner = getMergedLogRecordScanner(split, job, useCustomPayload() ? getWriterSchema() : getReaderSchema(), HoodieAvroRecordMerger.class.getName());
    this.deltaRecordMap = mergedLogRecordScanner.getRecords();
    this.deltaRecordKeys = new HashSet<>(this.deltaRecordMap.keySet());
    this.recordKeyIndex = split.getVirtualKeyInfo()
        .map(HoodieVirtualKeyInfo::getRecordKeyFieldIndex)
        .orElse(HoodieInputFormatUtils.HOODIE_RECORD_KEY_COL_POS);
  }

  public void init(JobConf job) {
    String[] rawColNames = HoodieColumnProjectionUtils.getReadColumnNames(job);
    List<Integer> rawColIds = HoodieColumnProjectionUtils.getReadColumnIDs(job);
    List<Pair<Integer, String>> projectedColsWithIndex =
        IntStream.range(0, rawColIds.size()).mapToObj(idx -> Pair.of(rawColIds.get(idx), rawColNames[idx]))
            .collect(Collectors.toList());
    // TODO: convert to Avro Schema and call project(Schema)
  }

  @Override
  public boolean next(NullWritable nullWritable, ArrayWritable arrayWritable) throws IOException {
    // Call the underlying parquetReader.next - which may replace the passed in ArrayWritable
    // with a new block of values
    while (baseRecordReader.next(nullWritable, arrayWritable)) {
      if (!deltaRecordMap.isEmpty()) {
        String key = arrayWritable.get()[recordKeyIndex].toString();
        if (deltaRecordMap.containsKey(key)) {
          // mark the key as handled
          this.deltaRecordKeys.remove(key);

          // deltaRecord may not be a full record and needs values of columns from the parquet
          // TODO: use new `HoodieRecord` implementation for ArrayWritable to avoid Avro conversion
          Option<HoodieAvroIndexedRecord> rec = buildGenericRecordWithCustomPayload(deltaRecordMap.get(key));
          // If the record is not present, this is a delete record using an empty payload so skip this base record
          // and move to the next record
          if (!rec.isPresent()) {
            continue;
          }
          setUpWritable(deltaRecordMap.get(key), arrayWritable, key);
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
      Option<HoodieAvroIndexedRecord> rec = buildGenericRecordWithCustomPayload(deltaRecordMap.get(key));
      if (rec.isPresent()) {
        setUpWritable(deltaRecordMap.get(key), arrayWritable, key);
        return true;
      }
    }

    return false;
  }

  private Option<HoodieAvroIndexedRecord> buildGenericRecordWithCustomPayload(HoodieRecord record) throws IOException {
    if (useCustomPayload()) {
      return record.toIndexedRecord(getWriterSchema(), getPayloadProps());
    } else {
      return record.toIndexedRecord(getReaderSchema(), getPayloadProps());
    }
  }

  private void setUpWritable(HoodieRecord rec, ArrayWritable arrayWritable, String key) {
    HoodieHiveRecord recordToReturn = (HoodieHiveRecord) rec.getData();
    // TODO: Provide custom payload implementation for BWC. Check how usage of HoodieAvroUtils#rewriteRecord was replaced in Spark.
    /*if (usesCustomPayload) {
      // If using a custom payload, return only the projection fields. The readerSchema is a schema derived from
      // the writerSchema with only the projection fields
      recordToReturn = HoodieAvroUtils.rewriteRecord((GenericRecord) rec.getData(), getReaderSchema());
    }*/

    // we assume, a later safe record in the log, is newer than what we have in the map &
    // replace it. Since we want to return an arrayWritable which is the same length as the elements in the latest
    // schema, we use writerSchema to create the arrayWritable from the latest generic record
    ArrayWritable aWritable = recordToReturn.getData();
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
    return baseRecordReader.createKey();
  }

  @Override
  public ArrayWritable createValue() {
    return baseRecordReader.createValue();
  }

  @Override
  public long getPos() throws IOException {
    return baseRecordReader.getPos();
  }

  @Override
  public void close() throws IOException {
    baseRecordReader.close();
    mergedLogRecordScanner.close();
  }

  @Override
  public float getProgress() throws IOException {
    return baseRecordReader.getProgress();
  }
}
