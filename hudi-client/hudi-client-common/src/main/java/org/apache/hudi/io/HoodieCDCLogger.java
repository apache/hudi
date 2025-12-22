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

package org.apache.hudi.io;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.cdc.HoodieCDCOperation;
import org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.table.log.AppendResult;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieCDCDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode.DATA_BEFORE;
import static org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode.DATA_BEFORE_AFTER;

/**
 * This class encapsulates all the cdc-writing functions.
 */
public class HoodieCDCLogger implements Closeable {

  private final String commitTime;

  private final String keyField;

  private final String partitionPath;

  private final FileSystem fs;

  private final Schema dataSchema;

  // writer for cdc data
  private final HoodieLogFormat.Writer cdcWriter;

  private final HoodieCDCSupplementalLoggingMode cdcSupplementalLoggingMode;

  private final Schema cdcSchema;

  // the cdc data
  private final ExternalSpillableMap<String, HoodieAvroPayload> cdcData;

  private final Map<HoodieLogBlock.HeaderMetadataType, String> cdcDataBlockHeader;

  // the cdc record transformer
  private final CDCTransformer transformer;

  // Max block size to limit to for a log block
  private final long maxBlockSize;

  // Average cdc record size. This size is updated at the end of every log block flushed to disk
  private long averageCDCRecordSize = 0;

  // Number of records that must be written to meet the max block size for a log block
  private AtomicInteger numOfCDCRecordsInMemory = new AtomicInteger();

  private final SizeEstimator<HoodieAvroPayload> sizeEstimator;

  private final List<Path> cdcAbsPaths;

  public HoodieCDCLogger(
      String commitTime,
      HoodieWriteConfig config,
      HoodieTableConfig tableConfig,
      String partitionPath,
      FileSystem fs,
      Schema schema,
      HoodieLogFormat.Writer cdcWriter,
      long maxInMemorySizeInBytes) {
    try {
      this.commitTime = commitTime;
      this.keyField = config.populateMetaFields()
          ? HoodieRecord.RECORD_KEY_METADATA_FIELD
          : tableConfig.getRecordKeyFieldProp();
      this.partitionPath = partitionPath;
      this.fs = fs;
      this.dataSchema = HoodieAvroUtils.removeMetadataFields(schema);
      this.cdcWriter = cdcWriter;
      this.cdcSupplementalLoggingMode = tableConfig.cdcSupplementalLoggingMode();
      this.cdcSchema = HoodieCDCUtils.schemaBySupplementalLoggingMode(
          cdcSupplementalLoggingMode,
          dataSchema
      );

      this.cdcDataBlockHeader = new HashMap<>();
      this.cdcDataBlockHeader.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, commitTime);
      this.cdcDataBlockHeader.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, cdcSchema.toString());

      this.sizeEstimator = new DefaultSizeEstimator<>();
      this.cdcData = new ExternalSpillableMap<>(
          maxInMemorySizeInBytes,
          config.getSpillableMapBasePath(),
          new DefaultSizeEstimator<>(),
          new DefaultSizeEstimator<>(),
          config.getCommonConfig().getSpillableDiskMapType(),
          config.getCommonConfig().isBitCaskDiskMapCompressionEnabled());
      this.transformer = getTransformer();
      this.maxBlockSize = config.getLogFileDataBlockMaxSize();

      this.cdcAbsPaths = new ArrayList<>();
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to initialize HoodieCDCLogger", e);
    }
  }

  public void put(HoodieRecord hoodieRecord,
                  GenericRecord oldRecord,
                  Option<IndexedRecord> newRecord) {
    String recordKey = hoodieRecord.getRecordKey();
    GenericData.Record cdcRecord;
    if (newRecord.isPresent()) {
      GenericRecord record = (GenericRecord) newRecord.get();
      if (oldRecord == null) {
        // INSERT cdc record
        cdcRecord = this.transformer.transform(HoodieCDCOperation.INSERT, recordKey,
            null, record);
      } else {
        // UPDATE cdc record
        cdcRecord = this.transformer.transform(HoodieCDCOperation.UPDATE, recordKey,
            oldRecord, record);
      }
    } else {
      // DELETE cdc record
      cdcRecord = this.transformer.transform(HoodieCDCOperation.DELETE, recordKey,
          oldRecord, null);
    }

    flushIfNeeded(false);
    HoodieAvroPayload payload = new HoodieAvroPayload(Option.of(cdcRecord));
    if (cdcData.isEmpty()) {
      averageCDCRecordSize = sizeEstimator.sizeEstimate(payload);
    }
    cdcData.put(recordKey, payload);
    numOfCDCRecordsInMemory.incrementAndGet();
  }

  private void flushIfNeeded(Boolean force) {
    if (force || numOfCDCRecordsInMemory.get() * averageCDCRecordSize >= maxBlockSize) {
      try {
        ArrayList<HoodieRecord> records = new ArrayList<>();
        Iterator<HoodieAvroPayload> recordIter = cdcData.iterator();
        while (recordIter.hasNext()) {
          HoodieAvroPayload record = recordIter.next();
          try {
            records.add(new HoodieAvroIndexedRecord(record.getInsertValue(cdcSchema).get()));
          } catch (IOException e) {
            throw new HoodieIOException("Failed to get cdc record", e);
          }
        }
        HoodieLogBlock block = new HoodieCDCDataBlock(records, cdcDataBlockHeader, keyField);
        AppendResult result = cdcWriter.appendBlocks(Collections.singletonList(block));

        Path cdcAbsPath = new Path(result.logFile().getPath().toUri());
        if (!cdcAbsPaths.contains(cdcAbsPath)) {
          cdcAbsPaths.add(cdcAbsPath);
        }

        // reset stat
        cdcData.clear();
        numOfCDCRecordsInMemory = new AtomicInteger();
      } catch (Exception e) {
        throw new HoodieException("Failed to write the cdc data to " + cdcWriter.getLogFile().getPath(), e);
      }
    }
  }

  public Map<String, Long> getCDCWriteStats() {
    Map<String, Long> stats = new HashMap<>();
    try {
      for (Path cdcAbsPath : cdcAbsPaths) {
        String cdcFileName = cdcAbsPath.getName();
        String cdcPath = StringUtils.isNullOrEmpty(partitionPath) ? cdcFileName : partitionPath + "/" + cdcFileName;
        stats.put(cdcPath, HadoopFSUtils.getFileSize(fs, cdcAbsPath));
      }
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to get cdc write stat", e);
    }
    return stats;
  }

  @Override
  public void close() {
    try {
      flushIfNeeded(true);
      if (cdcWriter != null) {
        cdcWriter.close();
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to close HoodieCDCLogger", e);
    } finally {
      // in case that crash when call `flushIfNeeded`, do the cleanup again.
      cdcData.clear();
    }
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private CDCTransformer getTransformer() {
    if (cdcSupplementalLoggingMode == DATA_BEFORE_AFTER) {
      return (operation, recordKey, oldRecord, newRecord) ->
          HoodieCDCUtils.cdcRecord(cdcSchema, operation.getValue(), commitTime, removeCommitMetadata(oldRecord), removeCommitMetadata(newRecord));
    } else if (cdcSupplementalLoggingMode == DATA_BEFORE) {
      return (operation, recordKey, oldRecord, newRecord) ->
          HoodieCDCUtils.cdcRecord(cdcSchema, operation.getValue(), recordKey, removeCommitMetadata(oldRecord));
    } else {
      return (operation, recordKey, oldRecord, newRecord) ->
          HoodieCDCUtils.cdcRecord(cdcSchema, operation.getValue(), recordKey);
    }
  }

  private GenericRecord removeCommitMetadata(GenericRecord record) {
    return record == null ? null : HoodieAvroUtils.rewriteRecordWithNewSchema(record, dataSchema, Collections.emptyMap());
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

  /**
   * A transformer that transforms normal data records into cdc records.
   */
  private interface CDCTransformer {
    GenericData.Record transform(HoodieCDCOperation operation,
                                 String recordKey,
                                 GenericRecord oldRecord,
                                 GenericRecord newRecord);

  }
}
