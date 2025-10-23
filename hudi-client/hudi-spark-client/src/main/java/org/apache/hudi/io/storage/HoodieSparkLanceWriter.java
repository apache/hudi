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

import com.lancedb.lance.spark.arrow.LanceArrowWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.io.lance.HoodieBaseLanceWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.COMMIT_SEQNO_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.COMMIT_TIME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.FILENAME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.PARTITION_PATH_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.RECORD_KEY_METADATA_FIELD;

/**
 * Spark Lance file writer implementing {@link HoodieSparkFileWriter}.
 *
 * This writer integrates with Hudi's storage I/O layer and supports:
 * - Hudi metadata field population
 * - Record key tracking (for bloom filters - TODO)
 * - Sequence ID generation
 */
public class HoodieSparkLanceWriter extends HoodieBaseLanceWriter<InternalRow> implements HoodieSparkFileWriter {

  private static final long DEFAULT_MAX_FILE_SIZE = 120 * 1024 * 1024; // 120MB
  private static final String DEFAULT_TIMEZONE = "UTC";

  private final StructType sparkSchema;
  private final Schema arrowSchema;
  private final UTF8String fileName;
  private final UTF8String instantTime;
  private final boolean populateMetaFields;
  private final Function<Long, String> seqIdGenerator;
  private LanceArrowWriter writer;

  /**
   * Constructor for Spark Lance writer.
   *
   * @param file Path where Lance file will be written
   * @param sparkSchema Spark schema for the data
   * @param instantTime Instant time for the commit
   * @param taskContextSupplier Task context supplier for partition ID
   * @param storage HoodieStorage instance
   * @param populateMetaFields Whether to populate Hudi metadata fields
   * @throws IOException if writer initialization fails
   */
  public HoodieSparkLanceWriter(StoragePath file,
                                StructType sparkSchema,
                                String instantTime,
                                TaskContextSupplier taskContextSupplier,
                                HoodieStorage storage,
                                boolean populateMetaFields) throws IOException {
    super(storage, file, DEFAULT_BATCH_SIZE, DEFAULT_MAX_FILE_SIZE);
    this.sparkSchema = sparkSchema;
    this.arrowSchema = LanceArrowUtils.toArrowSchema(sparkSchema, DEFAULT_TIMEZONE, true, false);
    this.fileName = UTF8String.fromString(file.getName());
    this.instantTime = UTF8String.fromString(instantTime);
    this.populateMetaFields = populateMetaFields;
    this.seqIdGenerator = recordIndex -> {
      Integer partitionId = taskContextSupplier.getPartitionIdSupplier().get();
      return HoodieRecord.generateSequenceId(instantTime, partitionId, recordIndex);
    };
  }

  @Override
  public void writeRowWithMetadata(HoodieKey key, InternalRow row) throws IOException {
    if (populateMetaFields) {
      UTF8String recordKey = UTF8String.fromString(key.getRecordKey());
      updateRecordMetadata(row, recordKey, key.getPartitionPath(), getWrittenRecordCount());
      super.write(row);
    } else {
      super.write(row);
    }
  }

  @Override
  public void writeRow(String recordKey, InternalRow row) throws IOException {
    super.write(row);
  }

  @Override
  protected void populateVectorSchemaRoot(List<InternalRow> records) {
    if (writer == null) {
      writer = LanceArrowWriter.create(this.root, sparkSchema);
    }
    // Reset writer state from previous batch
    writer.reset();
    for (InternalRow record : records) {
      writer.write(record);
    }
    // Finalize the writer (sets row count)
    writer.finish();
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
}