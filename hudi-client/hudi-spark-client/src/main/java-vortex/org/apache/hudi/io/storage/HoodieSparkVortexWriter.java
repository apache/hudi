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
import org.apache.hudi.io.storage.row.HoodieBloomFilterRowWriteSupport;
import org.apache.hudi.io.storage.row.HoodieInternalRowFileWriter;
import org.apache.hudi.io.vortex.HoodieBaseVortexWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import lombok.AllArgsConstructor;
import lombok.Builder;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.arrow.ArrowWriter$;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.ArrowUtils$;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.function.Function;

import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.COMMIT_SEQNO_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.COMMIT_TIME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.FILENAME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.PARTITION_PATH_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.RECORD_KEY_METADATA_FIELD;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/**
 * Spark Vortex file writer implementing {@link HoodieSparkFileWriter} and {@link HoodieInternalRowFileWriter}.
 *
 * This writer integrates with Hudi's storage I/O layer and supports:
 * - Hudi metadata field population
 * - Record key tracking (for bloom filters)
 * - Sequence ID generation
 * - Min/max record key tracking
 */
public class HoodieSparkVortexWriter extends HoodieBaseVortexWriter<InternalRow, UTF8String>
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

  @Builder(builderMethodName = "builder")
  private static HoodieSparkVortexWriter create(
      StoragePath file,
      StructType sparkSchema,
      String instantTime,
      TaskContextSupplier taskContextSupplier,
      HoodieStorage storage,
      boolean populateMetaFields,
      Option<BloomFilter> bloomFilterOpt,
      long maxFileSize,
      long allocatorSize,
      long flushByteWatermark) {
    checkArgument(maxFileSize > 0, "maxFileSize must be a positive number");
    checkArgument(allocatorSize > 0, "allocatorSize must be a positive number");
    checkArgument(flushByteWatermark > 0, "flushByteWatermark must be a positive number");
    checkArgument(flushByteWatermark < allocatorSize,
        "flushByteWatermark (" + flushByteWatermark + ") must be less than allocatorSize ("
            + allocatorSize + ") so the byte-aware flush prevents reallocations from exceeding the cap");
    return new HoodieSparkVortexWriter(file, sparkSchema, instantTime,
        taskContextSupplier, storage, populateMetaFields, bloomFilterOpt, maxFileSize,
        allocatorSize, flushByteWatermark);
  }

  /**
   * Public reflection entry point for {@code HoodieSparkFileWriterFactory}. The factory lives in the
   * default (Java 11) source set and cannot reference this Vortex-only class directly, so it invokes
   * this method reflectively (see the vortex profile that compiles this source under JDK 17).
   */
  public static HoodieSparkVortexWriter newWriter(
      StoragePath file,
      StructType sparkSchema,
      String instantTime,
      TaskContextSupplier taskContextSupplier,
      HoodieStorage storage,
      boolean populateMetaFields,
      Option<BloomFilter> bloomFilterOpt,
      long maxFileSize,
      long allocatorSize,
      long flushByteWatermark) {
    return builder()
        .file(file)
        .sparkSchema(sparkSchema)
        .instantTime(instantTime)
        .taskContextSupplier(taskContextSupplier)
        .storage(storage)
        .populateMetaFields(populateMetaFields)
        .bloomFilterOpt(bloomFilterOpt)
        .maxFileSize(maxFileSize)
        .allocatorSize(allocatorSize)
        .flushByteWatermark(flushByteWatermark)
        .build();
  }

  /**
   * Manually declared builder class to provide default values for optional parameters.
   * Lombok fills in the remaining builder methods.
   */
  public static class HoodieSparkVortexWriterBuilder {
    private Option<BloomFilter> bloomFilterOpt = Option.empty();
    private long maxFileSize = Long.parseLong(HoodieStorageConfig.VORTEX_MAX_FILE_SIZE.defaultValue());
    private long allocatorSize = Long.parseLong(HoodieStorageConfig.VORTEX_WRITE_ALLOCATOR_SIZE_BYTES.defaultValue());
    private long flushByteWatermark = Long.parseLong(HoodieStorageConfig.VORTEX_WRITE_FLUSH_BYTE_WATERMARK.defaultValue());
  }

  private HoodieSparkVortexWriter(StoragePath file,
                                  StructType sparkSchema,
                                  String instantTime,
                                  TaskContextSupplier taskContextSupplier,
                                  HoodieStorage storage,
                                  boolean populateMetaFields,
                                  Option<BloomFilter> bloomFilterOpt,
                                  long maxFileSize,
                                  long allocatorSize,
                                  long flushByteWatermark) {
    super(file, DEFAULT_BATCH_SIZE, allocatorSize, flushByteWatermark,
        bloomFilterOpt.map(HoodieBloomFilterRowWriteSupport::new));
    this.sparkSchema = sparkSchema;
    this.arrowSchema = ArrowUtils$.MODULE$.toArrowSchema(sparkSchema, DEFAULT_TIMEZONE, false, false);
    this.fileName = UTF8String.fromString(file.getName());
    this.instantTime = UTF8String.fromString(instantTime);
    this.populateMetaFields = populateMetaFields;
    this.maxFileSize = maxFileSize;
    this.seqIdGenerator = recordIndex -> {
      Integer partitionId = taskContextSupplier.getPartitionIdSupplier().get();
      return HoodieRecord.generateSequenceId(instantTime, partitionId, recordIndex);
    };
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
  protected HoodieBaseVortexWriter.ArrowWriter<InternalRow> createArrowWriter(VectorSchemaRoot root) {
    return SparkArrowWriterAdapter.of(ArrowWriter$.MODULE$.create(root));
  }

  /**
   * Check if writer can accept more records based on estimated data size.
   *
   * @return true if writer can accept more records, false if file size limit is reached
   */
  public boolean canWrite() {
    long writtenCount = getWrittenRecordCount();
    if (writtenCount >= recordCountForNextSizeCheck) {
      long dataSize = getDataSize();
      long avgRecordSize = Math.max(dataSize / writtenCount, 1);
      if (dataSize > (maxFileSize - avgRecordSize * 2)) {
        return false;
      }
      recordCountForNextSizeCheck = writtenCount + Math.min(
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
  private static class SparkArrowWriterAdapter implements HoodieBaseVortexWriter.ArrowWriter<InternalRow> {
    // Fully qualified: the inherited HoodieBaseVortexWriter.ArrowWriter nested type would otherwise
    // shadow this Spark ArrowWriter import.
    private final org.apache.spark.sql.execution.arrow.ArrowWriter sparkArrowWriter;

    @Override
    public void write(InternalRow row) {
      sparkArrowWriter.write(row);
    }

    @Override
    public void reset() {
      sparkArrowWriter.reset();
    }

    @Override
    public void finishBatch() {
      sparkArrowWriter.finish();
    }
  }
}
