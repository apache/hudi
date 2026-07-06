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
import java.util.Collections;
import java.util.Map;
import java.util.Set;
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
  // Selective meta-field population when populateMetaFields is false. commit_time enables
  // incremental queries; file_name enables file-level pruning / investigation lookups. Other meta
  // columns stay null. We do NOT register the record key with writeSupport in the selective modes —
  // that would feed the parquet bloom filter, which has no meaning when the record key column
  // is unpopulated.
  private final boolean populateCommitTime;
  private final boolean populateFileName;

  private final HoodieRowParquetWriteSupport writeSupport;

  private final Function<Long, String> seqIdGenerator;

  public HoodieSparkParquetWriter(StoragePath file,
                                  HoodieRowParquetConfig parquetConfig,
                                  String instantTime,
                                  TaskContextSupplier taskContextSupplier,
                                  boolean populateMetaFields) throws IOException {
    this(file, parquetConfig, instantTime, taskContextSupplier, populateMetaFields, Collections.emptySet());
  }

  public HoodieSparkParquetWriter(StoragePath file,
                                  HoodieRowParquetConfig parquetConfig,
                                  String instantTime,
                                  TaskContextSupplier taskContextSupplier,
                                  boolean populateMetaFields,
                                  Set<String> metaFieldsMode) throws IOException {
    super(file, parquetConfig);
    this.writeSupport = parquetConfig.getWriteSupport();
    this.fileName = UTF8String.fromString(file.getName());
    this.instantTime = UTF8String.fromString(instantTime);
    this.populateMetaFields = populateMetaFields;
    Set<String> mode = metaFieldsMode == null ? Collections.emptySet() : metaFieldsMode;
    this.populateCommitTime = !populateMetaFields && mode.contains(HoodieRecord.COMMIT_TIME_METADATA_FIELD);
    this.populateFileName = !populateMetaFields && mode.contains(HoodieRecord.FILENAME_METADATA_FIELD);
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
      writeSupport.add(recordKey);
    } else if (populateCommitTime || populateFileName) {
      if (populateCommitTime) {
        row.update(COMMIT_TIME_METADATA_FIELD.ordinal(), instantTime);
        row.update(COMMIT_SEQNO_METADATA_FIELD.ordinal(),
            UTF8String.fromString(seqIdGenerator.apply(getWrittenRecordCount())));
      }
      if (populateFileName) {
        row.update(FILENAME_METADATA_FIELD.ordinal(), fileName);
      }
      super.write(row);
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
  public void addFooterMetadata(Map<String, String> footerMetadata) {
    writeSupport.addFooterMetadata(footerMetadata);
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
