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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
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
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class HoodieCDCLogger implements Closeable {

  private final String partitionPath;

  private final String fileName;

  private final String commitTime;

  private final List<String> keyFields;

  private final Schema dataSchema;

  private final int taskPartitionId;

  private final boolean populateMetaFields;

  private final KeyGenerator keyGenerator;

  // writer for cdc data
  private final HoodieLogFormat.Writer cdcWriter;

  private final boolean cdcEnabled;

  private final HoodieCDCSupplementalLoggingMode cdcSupplementalLoggingMode;

  private final Schema cdcSchema;

  // the cdc data
  private final Map<String, HoodieAvroPayload> cdcData;

  private final Function<GenericRecord, GenericRecord> rewriteRecordFunc;

  // the count of records currently being written, used to generate the same seqno for the cdc data
  private final AtomicLong writtenRecordCount = new AtomicLong(-1);

  public HoodieCDCLogger(
      String partitionPath,
      String fileName,
      String commitTime,
      HoodieWriteConfig config,
      HoodieTableConfig tableConfig,
      List<String> keyFields,
      Schema schema,
      int taskPartitionId,
      HoodieLogFormat.Writer cdcWriter,
      long maxInMemorySizeInBytes,
      Function<GenericRecord, GenericRecord> rewriteRecordFunc) {
    try {
      this.partitionPath = partitionPath;
      this.fileName = fileName;
      this.commitTime = commitTime;
      this.keyFields = keyFields;
      this.dataSchema = HoodieAvroUtils.removeMetadataFields(schema);
      this.taskPartitionId = taskPartitionId;
      this.populateMetaFields = config.populateMetaFields();

      TypedProperties props = new TypedProperties();
      props.put(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), tableConfig.getKeyGeneratorClassName());
      props.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), tableConfig.getRecordKeyFieldProp());
      props.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), tableConfig.getPartitionFieldProp());
      this.keyGenerator = KeyGenUtils.createKeyGeneratorByClassName(new TypedProperties(props));
      this.cdcWriter = cdcWriter;
      this.rewriteRecordFunc = rewriteRecordFunc;

      this.cdcEnabled = config.getBooleanOrDefault(HoodieTableConfig.CDC_ENABLED);
      this.cdcSupplementalLoggingMode = HoodieCDCSupplementalLoggingMode.parse(
          config.getStringOrDefault(HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE));

      if (cdcSupplementalLoggingMode.equals(HoodieCDCSupplementalLoggingMode.WITH_BEFORE_AFTER)) {
        this.cdcSchema = HoodieCDCUtils.CDC_SCHEMA;
      } else if (cdcSupplementalLoggingMode.equals(HoodieCDCSupplementalLoggingMode.WITH_BEFORE)) {
        this.cdcSchema = HoodieCDCUtils.CDC_SCHEMA_OP_RECORDKEY_BEFORE;
      } else {
        this.cdcSchema = HoodieCDCUtils.CDC_SCHEMA_OP_AND_RECORDKEY;
      }

      this.cdcData = new ExternalSpillableMap<>(
          maxInMemorySizeInBytes,
          config.getSpillableMapBasePath(),
          new DefaultSizeEstimator<>(),
          new DefaultSizeEstimator<>(),
          config.getCommonConfig().getSpillableDiskMapType(),
          config.getCommonConfig().isBitCaskDiskMapCompressionEnabled()
      );
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to initialize HoodieCDCLogger", e);
    }
  }

  public void put(HoodieRecord hoodieRecord, GenericRecord oldRecord, Option<IndexedRecord> indexedRecord) {
    if (cdcEnabled) {
      String recordKey;
      if (oldRecord == null) {
        recordKey = hoodieRecord.getRecordKey();
      } else {
        recordKey = this.keyGenerator.getKey(oldRecord).getRecordKey();
      }
      GenericData.Record cdcRecord;
      if (indexedRecord.isPresent()) {
        GenericRecord record = (GenericRecord) indexedRecord.get();
        if (oldRecord == null) {
          // inserted cdc record
          cdcRecord = createCDCRecord(HoodieCDCOperation.INSERT, recordKey, partitionPath,
              null, record);
        } else {
          // updated cdc record
          cdcRecord = createCDCRecord(HoodieCDCOperation.UPDATE, recordKey, partitionPath,
              oldRecord, record);
        }
      } else {
        // deleted cdc record
        cdcRecord = createCDCRecord(HoodieCDCOperation.DELETE, recordKey, partitionPath,
            oldRecord, null);
      }
      cdcData.put(recordKey, new HoodieAvroPayload(Option.of(cdcRecord)));
    }
  }

  private GenericData.Record createCDCRecord(HoodieCDCOperation operation,
                                             String recordKey,
                                             String partitionPath,
                                             GenericRecord oldRecord,
                                             GenericRecord newRecord) {
    GenericData.Record record;
    if (cdcSupplementalLoggingMode.equals(HoodieCDCSupplementalLoggingMode.WITH_BEFORE_AFTER)) {
      record = HoodieCDCUtils.cdcRecord(operation.getValue(), commitTime,
          removeCommitMetadata(oldRecord), newRecord);
    } else if (cdcSupplementalLoggingMode.equals(HoodieCDCSupplementalLoggingMode.WITH_BEFORE)) {
      record = HoodieCDCUtils.cdcRecord(operation.getValue(), recordKey, removeCommitMetadata(oldRecord));
    } else {
      record = HoodieCDCUtils.cdcRecord(operation.getValue(), recordKey);
    }
    return record;
  }

  private GenericRecord addCommitMetadata(GenericRecord record, String recordKey, String partitionPath) {
    if (record != null && populateMetaFields) {
      GenericRecord rewriteRecord = rewriteRecordFunc.apply(record);
      String seqId = HoodieRecord.generateSequenceId(commitTime, taskPartitionId, writtenRecordCount.get());
      HoodieAvroUtils.addCommitMetadataToRecord(rewriteRecord, commitTime, seqId);
      HoodieAvroUtils.addHoodieKeyToRecord(rewriteRecord, recordKey, partitionPath, fileName);
      return rewriteRecord;
    }
    return record;
  }

  private GenericRecord removeCommitMetadata(GenericRecord record) {
    if (record != null && populateMetaFields) {
      GenericData.Record newRecord = new GenericData.Record(dataSchema);
      for (Schema.Field field : dataSchema.getFields()) {
        newRecord.put(field.name(), record.get(field.name()));
      }
      return newRecord;
    }
    return record;
  }

  public long getAndIncrement() {
    return writtenRecordCount.getAndIncrement();
  }

  public boolean isEmpty() {
    return !this.cdcEnabled || this.cdcData.isEmpty();
  }

  public Option<AppendResult> writeCDCData() {
    if (isEmpty()) {
      return Option.empty();
    }
    try {
      Map<HoodieLogBlock.HeaderMetadataType, String> header = buildCDCBlockHeader();
      List<IndexedRecord> records = cdcData.values().stream()
          .map( record -> {
            try {
              return record.getInsertValue(cdcSchema).get();
            } catch (IOException e) {
              throw new HoodieIOException("Failed to get cdc record", e);
            }
          }).collect(Collectors.toList());
      HoodieLogBlock block = new HoodieCDCDataBlock(records, header,
          StringUtils.join(keyFields.toArray(new String[0]), ","));
      AppendResult result = cdcWriter.appendBlocks(Collections.singletonList(block));

      // call close to trigger the data flush.
      this.close();

      return Option.of(result);
    } catch (Exception e) {
      throw new HoodieException("Failed to write the cdc data to " + cdcWriter.getLogFile().getPath(), e);
    }
  }

  private Map<HoodieLogBlock.HeaderMetadataType, String> buildCDCBlockHeader() {
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, commitTime);
    if (cdcSupplementalLoggingMode.equals(HoodieCDCSupplementalLoggingMode.WITH_BEFORE_AFTER)) {
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, HoodieCDCUtils.CDC_SCHEMA_STRING);
    } else if (cdcSupplementalLoggingMode.equals(HoodieCDCSupplementalLoggingMode.WITH_BEFORE)) {
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, HoodieCDCUtils.CDC_SCHEMA_OP_RECORDKEY_BEFORE_STRING);
    } else {
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, HoodieCDCUtils.CDC_SCHEMA_OP_AND_RECORDKEY_STRING);
    }
    return header;
  }

  @Override
  public void close() {
    if (cdcWriter != null) {
      try {
        cdcWriter.close();
      } catch (IOException e) {
        throw new HoodieIOException("Failed to close HoodieCDCLogger", e);
      }
    }
    cdcData.clear();
  }
}
