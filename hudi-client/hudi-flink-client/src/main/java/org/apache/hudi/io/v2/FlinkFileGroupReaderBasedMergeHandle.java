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

package org.apache.hudi.io.v2;

import org.apache.hudi.client.model.HoodieFlinkRecord;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.io.BaseFileGroupReaderBasedMergeHandle;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.util.FlinkClientUtil;

import org.apache.avro.Schema;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * An implementation of merge handle for Flink based on the {@link HoodieFileGroupReader}.
 * <p>
 * This merge handle is used for compaction on Flink, which passes a file slice from the
 * compaction operation of a single file group to a file group reader, get an iterator of
 * the RowData records, and writes the records to a new base file.
 */
public class FlinkFileGroupReaderBasedMergeHandle<T, I, K, O> extends BaseFileGroupReaderBasedMergeHandle<T, I, K, O> {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkFileGroupReaderBasedMergeHandle.class);

  public FlinkFileGroupReaderBasedMergeHandle(
      HoodieWriteConfig config,
      String instantTime,
      HoodieTable<T, I, K, O> hoodieTable,
      CompactionOperation operation,
      TaskContextSupplier taskContextSupplier,
      Option<BaseKeyGenerator> keyGeneratorOpt,
      HoodieReaderContext<T> readerContext) {
    super(config, instantTime, hoodieTable, operation,
        taskContextSupplier, keyGeneratorOpt, readerContext);
  }

  @Override
  protected HoodieRecord.HoodieRecordType getRecordType() {
    return HoodieRecord.HoodieRecordType.FLINK;
  }

  @Override
  public void write() {
    Option<InternalSchema> internalSchemaOption = Option.empty();
    if (!StringUtils.isNullOrEmpty(config.getInternalSchema())) {
      internalSchemaOption = SerDeHelper.fromJson(config.getInternalSchema());
    }
    TypedProperties props = FlinkClientUtil.getMergedTableAndWriteProps(hoodieTable.getMetaClient().getTableConfig(), config);
    // Initializes file group reader
    try (HoodieFileGroupReader<T> fileGroupReader = HoodieFileGroupReader.<T>newBuilder()
        .withReaderContext(readerContext).withHoodieTableMetaClient(hoodieTable.getMetaClient())
        .withLatestCommitTime(instantTime).withFileSlice(fileSlice).withDataSchema(writeSchemaWithMetaFields).withRequestedSchema(writeSchemaWithMetaFields)
        .withInternalSchema(internalSchemaOption).withProps(props).withShouldUseRecordPosition(false).build()) {
      // Reads the records from the file slice
      try (ClosableIterator<RowData> recordIterator = (ClosableIterator<RowData>) fileGroupReader.getClosableIterator()) {
        while (recordIterator.hasNext()) {
          // Constructs Flink record for the Flink Parquet file writer
          RowData row = recordIterator.next();
          HoodieKey recordKey = new HoodieKey(
              row.getString(HoodieRecord.RECORD_KEY_META_FIELD_ORD).toString(),
              row.getString(HoodieRecord.PARTITION_PATH_META_FIELD_ORD).toString());
          HoodieFlinkRecord record = new HoodieFlinkRecord(recordKey, row);

          Option<Map<String, String>> recordMetadata = record.getMetadata();
          if (!partitionPath.equals(record.getPartitionPath())) {
            HoodieUpsertException failureEx = new HoodieUpsertException("mismatched partition path, record partition: "
                + record.getPartitionPath() + " but trying to insert into partition: " + partitionPath);
            writeStatus.markFailure(record, failureEx, recordMetadata);
            continue;
          }
          // Writes the record
          try {
            writeToFile(recordKey, (HoodieRecord<T>) record, writeSchemaWithMetaFields, config.getPayloadConfig().getProps(), preserveMetadata);
            writeStatus.markSuccess(record, recordMetadata);
          } catch (Exception e) {
            LOG.error("Error writing record  " + record, e);
            writeStatus.markFailure(record, e, recordMetadata);
          }
        }

        // The stats of inserts, updates, and deletes are updated once at the end
        // These will be set in the write stat when closing the merge handle
        this.readStats = fileGroupReader.getStats();
        this.insertRecordsWritten = readStats.getNumInserts();
        this.updatedRecordsWritten = readStats.getNumUpdates();
        this.recordsDeleted = readStats.getNumDeletes();
        this.recordsWritten = readStats.getNumInserts() + readStats.getNumUpdates();
      }
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to compact file slice: " + fileSlice, e);
    }
  }

  @Override
  protected void writeToFile(HoodieKey key, HoodieRecord<T> record, Schema schema, Properties prop, boolean shouldPreserveRecordMetadata) throws IOException {
    // NOTE: `FILENAME_METADATA_FIELD` has to be rewritten to correctly point to the
    //       file holding this record even in cases when overall metadata is preserved
    HoodieRecord populatedRecord = record.updateMetaField(schema, HoodieRecord.FILENAME_META_FIELD_ORD, newFilePath.getName());

    if (shouldPreserveRecordMetadata) {
      fileWriter.write(key.getRecordKey(), populatedRecord, writeSchemaWithMetaFields);
    } else {
      fileWriter.writeWithMetadata(key, populatedRecord, writeSchemaWithMetaFields);
    }
  }
}
