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

import org.apache.hudi.avro.AvroRecordContext;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hadoop.utils.HiveAvroSerializer;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.storage.HoodieStorageUtils;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class RealtimeCompactedRecordReader extends AbstractRealtimeRecordReader
    implements RecordReader<NullWritable, ArrayWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractRealtimeRecordReader.class);

  protected final RecordReader<NullWritable, ArrayWritable> parquetReader;
  private final Map<String, HoodieRecord> deltaRecordMap;

  private final HoodieRecordMerger merger = new HoodieAvroRecordMerger();
  private final Set<String> deltaRecordKeys;
  private final HoodieMergedLogRecordScanner mergedLogRecordScanner;
  private final int recordKeyIndex;
  private final String[] orderingFields;
  private final DeleteContext deleteContext;
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
    this.orderingFields = ConfigUtils.getOrderingFields(payloadProps);
    HoodieSchema logScannerReaderSchema = getLogScannerReaderSchema();
    this.deleteContext = new DeleteContext(payloadProps, logScannerReaderSchema).withReaderSchema(logScannerReaderSchema);
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
        .withStorage(HoodieStorageUtils.getStorage(
            split.getPath().toString(), HadoopFSUtils.getStorageConf(jobConf)))
        .withBasePath(split.getBasePath())
        .withLogFilePaths(split.getDeltaLogPaths())
        .withReaderSchema(getLogScannerReaderSchema())
        .withLatestInstantTime(split.getMaxCommitTime())
        .withMaxMemorySizeInBytes(HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes(jobConf))
        .withReverseReader(false)
        .withBufferSize(jobConf.getInt(HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE.key(),
            HoodieMemoryConfig.DEFAULT_MR_MAX_DFS_STREAM_BUFFER_SIZE))
        .withSpillableMapBasePath(jobConf.get(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH.key(),
            FileIOUtils.getDefaultSpillableMapBasePath()))
        .withDiskMapType(jobConf.getEnum(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.key(), HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.defaultValue()))
        .withBitCaskDiskMapCompressionEnabled(jobConf.getBoolean(HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(),
            HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue()))
        .withOptimizedLogBlocksScan(jobConf.getBoolean(HoodieReaderConfig.ENABLE_OPTIMIZED_LOG_BLOCKS_SCAN.key(),
            Boolean.parseBoolean(HoodieReaderConfig.ENABLE_OPTIMIZED_LOG_BLOCKS_SCAN.defaultValue())))
        .withInternalSchema(schemaEvolutionContext.internalSchemaOption.orElse(InternalSchema.getEmptyInternalSchema()))
        .build();
  }

  private Option<HoodieAvroIndexedRecord> buildGenericRecordwithCustomPayload(HoodieRecord record) throws IOException {
    if (usesCustomPayload) {
      return record.toIndexedRecord(getWriterSchema().toAvroSchema(), payloadProps);
    } else {
      return record.toIndexedRecord(getReaderSchema().toAvroSchema(), payloadProps);
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
      recordToReturn = HoodieAvroUtils.rewriteRecord((GenericRecord) rec.get().getData(), getReaderSchema().toAvroSchema());
    }
    // we assume, a later safe record in the log, is newer than what we have in the map &
    // replace it. Since we want to return an arrayWritable which is the same length as the elements in the latest
    // schema, we use writerSchema to create the arrayWritable from the latest generic record
    ArrayWritable aWritable = (ArrayWritable) HoodieRealtimeRecordReaderUtils.avroToArrayWritable(recordToReturn, getHiveSchema().toAvroSchema(), isSupportTimestamp());
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
    // once presto on hudi have its own mor reader, we can remove the rewrite logical.
    GenericRecord genericRecord = HiveAvroSerializer.rewriteRecordIgnoreResultCheck(oldRecord, getLogScannerReaderSchema().toAvroSchema());
    RecordContext<IndexedRecord> recordContext = AvroRecordContext.getFieldAccessorInstance();
    BufferedRecord record = BufferedRecords.fromEngineRecord(genericRecord, HoodieSchema.fromAvroSchema(genericRecord.getSchema()), recordContext, orderingFields, newRecord.getRecordKey(), false);
    BufferedRecord newBufferedRecord = BufferedRecords.fromHoodieRecord(newRecord, HoodieSchema.fromAvroSchema(getLogScannerReaderSchema().toAvroSchema()),
        recordContext, payloadProps, orderingFields, deleteContext);
    BufferedRecord mergeResult = merger.merge(record, newBufferedRecord, recordContext, payloadProps);
    if (mergeResult.isDelete()) {
      return Option.empty();
    }
    return Option.of((HoodieAvroIndexedRecord) recordContext.constructFinalHoodieRecord(mergeResult));
  }

  private GenericRecord convertArrayWritableToHoodieRecord(ArrayWritable arrayWritable) {
    GenericRecord record = serializer.serialize(arrayWritable, getHiveSchema().toAvroSchema());
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
