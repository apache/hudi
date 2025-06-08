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

package org.apache.hudi.io;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;

import static org.apache.hudi.common.config.HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS;

/**
 * A merge handle implementation based on the {@link HoodieFileGroupReader}.
 * <p>
 * This merge handle is used for compaction on Spark, which passes a file slice from the
 * compaction operation of a single file group to a file group reader, get an iterator of
 * the records, and writes the records to a new base file.
 */
@NotThreadSafe
public class HoodieSparkFileGroupReaderBasedMergeHandle<T, I, K, O> extends BaseFileGroupReaderBasedMergeHandle<T, I, K, O> {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieSparkFileGroupReaderBasedMergeHandle.class);

  public HoodieSparkFileGroupReaderBasedMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                                    CompactionOperation operation, TaskContextSupplier taskContextSupplier,
                                                    Option<BaseKeyGenerator> keyGeneratorOpt,
                                                    HoodieReaderContext<T> readerContext) {
    super(config, instantTime, hoodieTable, operation, taskContextSupplier, keyGeneratorOpt, readerContext);
  }

  @Override
  protected HoodieRecord.HoodieRecordType getRecordType() {
    return HoodieRecord.HoodieRecordType.SPARK;
  }

  /**
   * Reads the file slice of a compaction operation using a file group reader,
   * by getting an iterator of the records; then writes the records to a new base file
   * using Spark parquet writer.
   */
  @Override
  public void write() {
    boolean usePosition = config.getBooleanOrDefault(MERGE_USE_RECORD_POSITIONS);
    Option<InternalSchema> internalSchemaOption = Option.empty();
    if (!StringUtils.isNullOrEmpty(config.getInternalSchema())) {
      internalSchemaOption = SerDeHelper.fromJson(config.getInternalSchema());
    }
    TypedProperties props = new TypedProperties();
    hoodieTable.getMetaClient().getTableConfig().getProps().forEach(props::putIfAbsent);
    config.getProps().forEach(props::putIfAbsent);
    long maxMemoryPerCompaction = IOUtils.getMaxMemoryPerCompaction(taskContextSupplier, config);
    props.put(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.key(), String.valueOf(maxMemoryPerCompaction));
    // Initializes file group reader
    try (HoodieFileGroupReader<T> fileGroupReader = HoodieFileGroupReader.<T>newBuilder().withReaderContext(readerContext).withHoodieTableMetaClient(hoodieTable.getMetaClient())
        .withLatestCommitTime(instantTime).withFileSlice(fileSlice).withDataSchema(writeSchemaWithMetaFields).withRequestedSchema(writeSchemaWithMetaFields)
        .withInternalSchema(internalSchemaOption).withProps(props).withShouldUseRecordPosition(usePosition).build()) {
      // Reads the records from the file slice
      try (ClosableIterator<InternalRow> recordIterator = (ClosableIterator<InternalRow>) fileGroupReader.getClosableIterator()) {
        StructType sparkSchema = AvroConversionUtils.convertAvroSchemaToStructType(writeSchemaWithMetaFields);
        while (recordIterator.hasNext()) {
          // Constructs Spark record for the Spark Parquet file writer
          InternalRow row = recordIterator.next();
          HoodieKey recordKey = new HoodieKey(
              row.getString(HoodieRecord.RECORD_KEY_META_FIELD_ORD),
              row.getString(HoodieRecord.PARTITION_PATH_META_FIELD_ORD));
          HoodieSparkRecord record = new HoodieSparkRecord(recordKey, row, sparkSchema, false);
          record.setCurrentLocation(newRecordLocation);
          record.setNewLocation(newRecordLocation);
          Option recordMetadata = record.getMetadata();
          if (!partitionPath.equals(record.getPartitionPath())) {
            HoodieUpsertException failureEx = new HoodieUpsertException("mismatched partition path, record partition: "
                + record.getPartitionPath() + " but trying to insert into partition: " + partitionPath);
            writeStatus.markFailure(record, failureEx, recordMetadata);
            continue;
          }
          // Writes the record
          try {
            writeToFile(recordKey, (HoodieRecord<T>) record, writeSchemaWithMetaFields,
                config.getPayloadConfig().getProps(), preserveMetadata);
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
}
