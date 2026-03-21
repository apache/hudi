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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.io.lance.HoodieBaseLanceWriter;
import org.apache.hudi.io.storage.row.HoodieBloomFilterRowWriteSupport;
import org.apache.hudi.io.storage.row.HoodieInternalRowFileWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import com.lancedb.lance.spark.arrow.LanceArrowWriter;
import lombok.AllArgsConstructor;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.function.Function;

import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.COMMIT_SEQNO_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.COMMIT_TIME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.FILENAME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.PARTITION_PATH_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.RECORD_KEY_METADATA_FIELD;

/**
 * Spark Lance file writer implementing {@link HoodieSparkFileWriter} and {@link HoodieInternalRowFileWriter}.
 *
 * This writer integrates with Hudi's storage I/O layer and supports:
 * - Hudi metadata field population
 * - Record key tracking (for bloom filters)
 * - Sequence ID generation
 * - Min/max record key tracking
 */
public class HoodieSparkLanceWriter extends HoodieBaseLanceWriter<InternalRow, UTF8String>
    implements HoodieSparkFileWriter, HoodieInternalRowFileWriter {

  private static final String DEFAULT_TIMEZONE = "UTC";
  private static final long MIN_RECORDS_FOR_SIZE_CHECK = 100L;
  private static final long MAX_RECORDS_FOR_SIZE_CHECK = 10000L;

  private final StructType sparkSchema;
  private final Schema arrowSchema;
  private final UTF8String fileName;
  private final UTF8String instantTime;
  private final boolean populateMetaFields;
  private final Function<Long, String> seqIdGenerator;
  private final long maxFileSize;
  private long recordCountForNextSizeCheck = MIN_RECORDS_FOR_SIZE_CHECK;

  /**
   * Constructor for Spark Lance writer.
   *
   * @param file Path where Lance file will be written
   * @param sparkSchema Spark schema for the data
   * @param instantTime Instant time for the commit
   * @param taskContextSupplier Task context supplier for partition ID
   * @param storage HoodieStorage instance
   * @param populateMetaFields Whether to populate Hudi metadata fields
   * @param bloomFilterOpt Optional bloom filter for record key tracking
   * @param maxFileSize Target maximum file size in bytes
   * @throws IOException if writer initialization fails
   */
  public HoodieSparkLanceWriter(StoragePath file,
                                StructType sparkSchema,
                                String instantTime,
                                TaskContextSupplier taskContextSupplier,
                                HoodieStorage storage,
                                boolean populateMetaFields,
                                Option<BloomFilter> bloomFilterOpt,
                                long maxFileSize) {
    super(file, DEFAULT_BATCH_SIZE, bloomFilterOpt.map(HoodieBloomFilterRowWriteSupport::new));
    this.sparkSchema = sparkSchema;
    this.arrowSchema = LanceArrowUtils.toArrowSchema(sparkSchema, DEFAULT_TIMEZONE, true, false);
    this.fileName = UTF8String.fromString(file.getName());
    this.instantTime = UTF8String.fromString(instantTime);
    this.populateMetaFields = populateMetaFields;
    this.maxFileSize = maxFileSize;
    this.seqIdGenerator = recordIndex -> {
      Integer partitionId = taskContextSupplier.getPartitionIdSupplier().get();
      return HoodieRecord.generateSequenceId(instantTime, partitionId, recordIndex);
    };
  }

  /**
   * Constructor for Spark Lance writer used for internal row writing with pre-embedded metadata.
   * No file size limit is applied (maxFileSize defaults to {@link Long#MAX_VALUE}).
   *
   * @param file Path where Lance file will be written
   * @param sparkSchema Spark schema for the data
   * @param taskContextSupplier Task context supplier for partition ID
   * @param storage HoodieStorage instance
   */
  public HoodieSparkLanceWriter(StoragePath file,
                                StructType sparkSchema,
                                TaskContextSupplier taskContextSupplier,
                                HoodieStorage storage,
                                long maxFileSize) {
    this(file, sparkSchema, null, taskContextSupplier, storage, false, Option.empty(), maxFileSize);
  }

  /**
   * Constructor for Spark Lance writer used for internal row writing with pre-embedded metadata
   * and a configurable file size limit.
   *
   * @param file Path where Lance file will be written
   * @param sparkSchema Spark schema for the data
   * @param instantTime Instant time for the commit
   * @param taskContextSupplier Task context supplier for partition ID
   * @param storage HoodieStorage instance
   * @param populateMetaFields Whether to populate Hudi metadata fields
   * @param bloomFilterOpt Optional bloom filter for record key tracking
   * @throws IOException if writer initialization fails
   */
  public HoodieSparkLanceWriter(StoragePath file,
                                StructType sparkSchema,
                                String instantTime,
                                TaskContextSupplier taskContextSupplier,
                                HoodieStorage storage,
                                boolean populateMetaFields,
                                Option<BloomFilter> bloomFilterOpt) {
    this(file, sparkSchema, instantTime, taskContextSupplier, storage, populateMetaFields, bloomFilterOpt,
        Long.parseLong(HoodieStorageConfig.LANCE_MAX_FILE_SIZE.defaultValue()));
  }

  /**
   * Constructor for Spark Lance writer used for internal row writing with pre-embedded metadata
   * and a configurable file size limit.
   *
   * @param file Path where Lance file will be written
   * @param sparkSchema Spark schema for the data
   * @param instantTime Instant time for the commit
   * @param taskContextSupplier Task context supplier for partition ID
   * @param storage HoodieStorage instance
   * @param populateMetaFields Whether to populate Hudi metadata fields
   * @param maxFileSize Target maximum file size in bytes
   * @throws IOException if writer initialization fails
   */
  public HoodieSparkLanceWriter(StoragePath file,
                                StructType sparkSchema,
                                String instantTime,
                                TaskContextSupplier taskContextSupplier,
                                HoodieStorage storage,
                                boolean populateMetaFields,
                                long maxFileSize) {
    this(file, sparkSchema, instantTime, taskContextSupplier, storage, populateMetaFields, Option.empty(), maxFileSize);
  }

  @Override
  public void writeRowWithMetadata(HoodieKey key, InternalRow row) throws IOException {
    UTF8String recordKey = UTF8String.fromString(key.getRecordKey());
    bloomFilterWriteSupportOpt.ifPresent(bloomFilterWriteSupport -> bloomFilterWriteSupport.addKey(recordKey));
    if (populateMetaFields) {
      updateRecordMetadata(row, recordKey, key.getPartitionPath(), getWrittenRecordCount());
    }
    super.write(row);
  }

  @Override
  public void writeRow(String recordKey, InternalRow row) throws IOException {
    bloomFilterWriteSupportOpt.ifPresent(bloomFilterWriteSupport ->
        bloomFilterWriteSupport.addKey(UTF8String.fromString(recordKey)));
    super.write(row);
  }
  
  @Override
  public void writeRow(UTF8String key, InternalRow row) throws IOException {
    bloomFilterWriteSupportOpt.ifPresent(bloomFilterWriteSupport -> bloomFilterWriteSupport.addKey(key));
    super.write(row);
  }
  
  @Override
  public void writeRow(InternalRow row) throws IOException {
    super.write(row);
  }

  @Override
  protected ArrowWriter<InternalRow> createArrowWriter(VectorSchemaRoot root) {
    return SparkArrowWriter.of(LanceArrowWriter.create(root, sparkSchema));
  }

  /**
   * Check if writer can accept more records based on estimated data size.
   * Data size is approximated by accumulating Arrow buffer sizes across flushed batches,
   * analogous to {@code ParquetWriter.getDataSize()}.
   * The check is performed periodically (not on every record) and the interval adapts
   * based on the observed average record size.
   *
   * @return true if writer can accept more records, false if file size limit is reached
   */
  public boolean canWrite() {
    long writtenCount = getWrittenRecordCount();
    if (writtenCount >= recordCountForNextSizeCheck) {
      long dataSize = getDataSize();
      // In extreme cases (e.g. all records same value, high compression ratio),
      // dataSize may be 0; force avgRecordSize to at least 1 to avoid division by zero.
      long avgRecordSize = Math.max(dataSize / writtenCount, 1);
      // Return false when within ~2 records of the limit
      if (dataSize > (maxFileSize - avgRecordSize * 2)) {
        return false;
      }
      recordCountForNextSizeCheck = writtenCount + Math.min(
          // Check at halfway between current position and the limit
          Math.max(MIN_RECORDS_FOR_SIZE_CHECK, (maxFileSize / avgRecordSize - writtenCount) / 2),
          MAX_RECORDS_FOR_SIZE_CHECK);
    }
    return true;
  }

  @Override
  protected Schema getArrowSchema() {
    return arrowSchema;
  }

  /**
   * Update Hudi metadata fields in the InternalRow.
   *
   * @param row InternalRow to update
   * @param recordKey Record key
   * @param partitionPath Partition path
   * @param recordCount Current record count for sequence ID generation
   */
  protected void updateRecordMetadata(InternalRow row,
                                      UTF8String recordKey,
                                      String partitionPath,
                                      long recordCount) {
    row.update(COMMIT_TIME_METADATA_FIELD.ordinal(), instantTime);
    row.update(COMMIT_SEQNO_METADATA_FIELD.ordinal(), UTF8String.fromString(seqIdGenerator.apply(recordCount)));
    row.update(RECORD_KEY_METADATA_FIELD.ordinal(), recordKey);
    row.update(PARTITION_PATH_METADATA_FIELD.ordinal(), UTF8String.fromString(partitionPath));
    row.update(FILENAME_METADATA_FIELD.ordinal(), fileName);
  }

  @AllArgsConstructor(staticName = "of")
  private static class SparkArrowWriter implements ArrowWriter<InternalRow> {
    private final LanceArrowWriter lanceArrowWriter;

    @Override
    public void write(InternalRow row) {
      lanceArrowWriter.write(row);
    }

    @Override
    public void reset() {
      lanceArrowWriter.reset();
    }

    @Override
    public void finishBatch() {
      lanceArrowWriter.finish();
    }
  }
}
