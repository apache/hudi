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

import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.io.hadoop.HoodieBaseParquetWriter;
import org.apache.hudi.io.storage.row.HoodieRowParquetConfig;
import org.apache.hudi.io.storage.row.HoodieRowParquetWriteSupport;
import org.apache.hudi.storage.StoragePath;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.function.Function;

import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.COMMIT_SEQNO_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.COMMIT_TIME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.FILENAME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.PARTITION_PATH_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.RECORD_KEY_METADATA_FIELD;

public class HoodieSparkParquetWriter extends HoodieBaseParquetWriter<InternalRow> implements HoodieSparkFileWriter {

  private final UTF8String fileName;
  private final UTF8String instantTime;

  private final boolean populateMetaFields;

  private final HoodieRowParquetWriteSupport writeSupport;

  private final Function<Long, String> seqIdGenerator;

  public HoodieSparkParquetWriter(StoragePath file,
                                  HoodieRowParquetConfig parquetConfig,
                                  String instantTime,
                                  TaskContextSupplier taskContextSupplier,
                                  boolean populateMetaFields) throws IOException {
    super(file, parquetConfig);
    this.writeSupport = parquetConfig.getWriteSupport();
    this.fileName = UTF8String.fromString(file.getName());
    this.instantTime = UTF8String.fromString(instantTime);
    this.populateMetaFields = populateMetaFields;
    this.seqIdGenerator = recordIndex -> {
      Integer partitionId = taskContextSupplier.getPartitionIdSupplier().get();
      return HoodieRecord.generateSequenceId(instantTime, partitionId, recordIndex);
    };
  }

  /**
   * Alternate constructor that takes taskPartitionId directly instead of requiring TaskContextSupplier.
   * This is useful when the partition ID is known upfront (e.g., in bulk insert scenarios).
   */
  public HoodieSparkParquetWriter(StoragePath file,
                                  HoodieRowParquetConfig parquetConfig,
                                  String instantTime,
                                  int taskPartitionId,
                                  boolean populateMetaFields) throws IOException {
    super(file, parquetConfig);
    this.writeSupport = parquetConfig.getWriteSupport();
    this.fileName = UTF8String.fromString(file.getName());
    this.instantTime = UTF8String.fromString(instantTime);
    this.populateMetaFields = populateMetaFields;
    this.seqIdGenerator = recordIndex ->
        HoodieRecord.generateSequenceId(instantTime, taskPartitionId, recordIndex);
  }

  @Override
  public void writeRowWithMetadata(HoodieKey key, InternalRow row) throws IOException {
    if (populateMetaFields) {
      UTF8String recordKey = UTF8String.fromString(key.getRecordKey());
      updateRecordMetadata(row, recordKey, key.getPartitionPath(), getWrittenRecordCount());

      super.write(row);
      writeSupport.add(recordKey);
    } else {
      super.write(row);
    }
  }

  @Override
  public void writeRow(String recordKey, InternalRow row) throws IOException {
    super.write(row);
    if (populateMetaFields) {
      writeSupport.add(UTF8String.fromString(recordKey));
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

  protected void updateRecordMetadata(InternalRow row,
                                      UTF8String recordKey,
                                      String partitionPath,
                                      long recordCount)  {
    row.update(COMMIT_TIME_METADATA_FIELD.ordinal(), instantTime);
    row.update(COMMIT_SEQNO_METADATA_FIELD.ordinal(), UTF8String.fromString(seqIdGenerator.apply(recordCount)));
    row.update(RECORD_KEY_METADATA_FIELD.ordinal(), recordKey);
    // TODO set partition path in ctor
    row.update(PARTITION_PATH_METADATA_FIELD.ordinal(), UTF8String.fromString(partitionPath));
    row.update(FILENAME_METADATA_FIELD.ordinal(), fileName);
  }
}
