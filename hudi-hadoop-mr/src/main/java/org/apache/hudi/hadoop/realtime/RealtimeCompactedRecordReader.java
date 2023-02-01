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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.hadoop.config.HoodieRealtimeConfig;
import org.apache.hudi.hadoop.utils.HiveAvroSerializer;
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

public class RealtimeCompactedRecordReader extends AbstractRealtimeRecordReader
    implements RecordReader<NullWritable, ArrayWritable> {

  private static final Logger LOG = LogManager.getLogger(AbstractRealtimeRecordReader.class);

  protected final RecordReader<NullWritable, ArrayWritable> parquetReader;
  private final Map<String, HoodieRecord> deltaRecordMap;

  private final Set<String> deltaRecordKeys;
  private final HoodieMergedLogRecordScanner mergedLogRecordScanner;
  private final HoodieRecordMerger recordMerger;
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
    this.recordMerger = HoodieRecordUtils.loadRecordMerger(HoodieAvroRecordMerger.class.getName());
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
        .withOptimizedLogBlocksScan(jobConf.getBoolean(HoodieRealtimeConfig.USE_LOG_RECORD_READER_SCAN_V2, false))
        .withInternalSchema(schemaEvolutionContext.internalSchemaOption.orElse(InternalSchema.getEmptyInternalSchema()))
        .withRecordMerger(HoodieRecordUtils.loadRecordMerger(HoodieAvroRecordMerger.class.getName()))
        .build();
  }

  private Option<HoodieAvroIndexedRecord> buildGenericRecordwithCustomPayload(HoodieRecord record) throws IOException {
    if (usesCustomPayload) {
      return record.toIndexedRecord(getWriterSchema(), payloadProps);
    } else {
      return record.toIndexedRecord(getReaderSchema(), payloadProps);
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
          Option<HoodieAvroIndexedRecord> rec = supportPayload ? mergeRecord(deltaRecordMap.get(key), arrayWritable) : buildGenericRecordwithCustomPayload(deltaRecordMap.get(key));
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
      Option<HoodieAvroIndexedRecord> rec = buildGenericRecordwithCustomPayload(deltaRecordMap.get(key));
      if (rec.isPresent()) {
        setUpWritable(rec, arrayWritable, key);
        return true;
      }
    }
    return false;
  }

  private void setUpWritable(Option<HoodieAvroIndexedRecord> rec, ArrayWritable arrayWritable, String key) {
    GenericRecord recordToReturn = (GenericRecord) rec.get().getData();
    if (usesCustomPayload) {
      // If using a custom payload, return only the projection fields. The readerSchema is a schema derived from
      // the writerSchema with only the projection fields
      recordToReturn = HoodieAvroUtils.rewriteRecord((GenericRecord) rec.get().getData(), getReaderSchema());
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

  private Option<HoodieAvroIndexedRecord> mergeRecord(HoodieRecord<?> newRecord, ArrayWritable writableFromParquet) throws IOException {
    GenericRecord oldRecord = convertArrayWritableToHoodieRecord(writableFromParquet);
    // presto will not append partition columns to jobConf.get(serdeConstants.LIST_COLUMNS), but hive will do it. This will lead following results
    // eg: current table: col1: int, col2: int, par: string, and column par is partition columns.
    // for hive engine, the hiveSchema will be: col1,col2,par, and the writerSchema will be col1,col2,par
    // for presto engine, the hiveSchema will be: col1,col2, but the writerSchema will be col1,col2,par
    // so to be compatible with hive and presto, we should rewrite oldRecord before we call combineAndGetUpdateValue,
    // once presto on hudi have it's own mor reader, we can remove the rewrite logical.
    GenericRecord genericRecord = HiveAvroSerializer.rewriteRecordIgnoreResultCheck(oldRecord, getLogScannerReaderSchema());
    HoodieRecord record = new HoodieAvroIndexedRecord(genericRecord);
    Option<Pair<HoodieRecord, Schema>> mergeResult = recordMerger.merge(record,
        genericRecord.getSchema(), newRecord, getLogScannerReaderSchema(), new TypedProperties(payloadProps));
    return mergeResult.map(p -> (HoodieAvroIndexedRecord) p.getLeft());
  }

  private GenericRecord convertArrayWritableToHoodieRecord(ArrayWritable arrayWritable) {
    GenericRecord record = serializer.serialize(arrayWritable, getHiveSchema());
    return record;
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
