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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
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
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieUpsertException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class encapsulates all the cdc-writing functions.
 */
public class HoodieCDCLogger implements Closeable {

  private final String commitTime;

  private final String keyField;

  private final Schema dataSchema;

  // writer for cdc data
  private final HoodieLogFormat.Writer cdcWriter;

  private final HoodieCDCSupplementalLoggingMode cdcSupplementalLoggingMode;

  private final Schema cdcSchema;

  private final String cdcSchemaString;

  // the cdc data
  private final Map<String, HoodieAvroPayload> cdcData;

  // the cdc record transformer
  private final CDCTransformer transformer;

  public HoodieCDCLogger(
      String commitTime,
      HoodieWriteConfig config,
      HoodieTableConfig tableConfig,
      Schema schema,
      HoodieLogFormat.Writer cdcWriter,
      long maxInMemorySizeInBytes) {
    try {
      this.commitTime = commitTime;
      this.dataSchema = HoodieAvroUtils.removeMetadataFields(schema);
      this.keyField = config.populateMetaFields()
          ? HoodieRecord.RECORD_KEY_METADATA_FIELD
          : tableConfig.getRecordKeyFieldProp();
      this.cdcWriter = cdcWriter;
      this.cdcSupplementalLoggingMode = config.getCDCSupplementalLoggingMode();
      this.cdcSchema = HoodieCDCUtils.schemaBySupplementalLoggingMode(
          cdcSupplementalLoggingMode,
          dataSchema
      );
      this.cdcSchemaString = this.cdcSchema.toString();

      this.cdcData = new ExternalSpillableMap<>(
          maxInMemorySizeInBytes,
          config.getSpillableMapBasePath(),
          new DefaultSizeEstimator<>(),
          new DefaultSizeEstimator<>(),
          config.getCommonConfig().getSpillableDiskMapType(),
          config.getCommonConfig().isBitCaskDiskMapCompressionEnabled());
      this.transformer = getTransformer();
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
    cdcData.put(recordKey, new HoodieAvroPayload(Option.of(cdcRecord)));
  }

  public Option<AppendResult> writeCDCData() {
    if (isEmpty()) {
      return Option.empty();
    }

    try {
      List<IndexedRecord> records = cdcData.values().stream()
          .map(record -> {
            try {
              return record.getInsertValue(cdcSchema).get();
            } catch (IOException e) {
              throw new HoodieIOException("Failed to get cdc record", e);
            }
          }).collect(Collectors.toList());

      Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
      header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, commitTime);
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, cdcSchemaString);

      HoodieLogBlock block = new HoodieCDCDataBlock(records, header, keyField);
      AppendResult result = cdcWriter.appendBlocks(Collections.singletonList(block));

      // call close to trigger the data flush.
      this.close();

      return Option.of(result);
    } catch (Exception e) {
      throw new HoodieException("Failed to write the cdc data to " + cdcWriter.getLogFile().getPath(), e);
    }
  }

  @Override
  public void close() {
    try {
      if (cdcWriter != null) {
        cdcWriter.close();
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to close HoodieCDCLogger", e);
    } finally {
      cdcData.clear();
    }
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private CDCTransformer getTransformer() {
    if (cdcSupplementalLoggingMode.equals(HoodieCDCSupplementalLoggingMode.WITH_BEFORE_AFTER)) {
      return (operation, recordKey, oldRecord, newRecord) ->
          HoodieCDCUtils.cdcRecord(cdcSchema, operation.getValue(), commitTime, removeCommitMetadata(oldRecord), newRecord);
    } else if (cdcSupplementalLoggingMode.equals(HoodieCDCSupplementalLoggingMode.WITH_BEFORE)) {
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

  public boolean isEmpty() {
    return this.cdcData.isEmpty();
  }

  public static Option<AppendResult> writeCDCDataIfNeeded(HoodieCDCLogger cdcLogger,
                                                          long recordsWritten,
                                                          long insertRecordsWritten) {
    if (cdcLogger == null || recordsWritten == 0L || (recordsWritten == insertRecordsWritten)) {
      // the following cases where we do not need to write out the cdc file:
      // case 1: all the data from the previous file slice are deleted. and no new data is inserted;
      // case 2: all the incoming data is INSERT.
      return Option.empty();
    }
    return cdcLogger.writeCDCData();
  }

  public static void setCDCStatIfNeeded(HoodieWriteStat stat,
                                        Option<AppendResult> cdcResult,
                                        String partitionPath,
                                        FileSystem fs) {
    try {
      if (cdcResult.isPresent()) {
        Path cdcLogFile = cdcResult.get().logFile().getPath();
        String cdcFileName = cdcLogFile.getName();
        String cdcPath = StringUtils.isNullOrEmpty(partitionPath) ? cdcFileName : partitionPath + "/" + cdcFileName;
        long cdcFileSizeInBytes = FSUtils.getFileSize(fs, cdcLogFile);
        stat.setCdcPath(cdcPath);
        stat.setCdcWriteBytes(cdcFileSizeInBytes);
      }
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to set cdc write stat", e);
    }
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
