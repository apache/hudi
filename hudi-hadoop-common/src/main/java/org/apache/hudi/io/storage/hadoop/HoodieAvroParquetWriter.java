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

package org.apache.hudi.io.storage.hadoop;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.config.HoodieParquetConfig;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.core.io.storage.HoodieAvroFileWriter;
import org.apache.hudi.io.hadoop.HoodieBaseParquetWriter;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * HoodieParquetWriter extends the ParquetWriter to help limit the size of underlying file. Provides a way to check if
 * the current file can take more records with the <code>canWrite()</code>
 *
 * ATTENTION: HoodieParquetWriter is not thread safe and developer should take care of the order of write and close
 */
@NotThreadSafe
public class HoodieAvroParquetWriter
    extends HoodieBaseParquetWriter<IndexedRecord>
    implements HoodieAvroFileWriter {

  private final String fileName;
  private final String instantTime;
  private final TaskContextSupplier taskContextSupplier;
  private final boolean populateMetaFields;
  // Meta fields to populate when populateMetaFields is false. Selectively enables
  // _hoodie_commit_time / _hoodie_file_name so incremental queries and file-level lookups keep
  // working on otherwise-minimal-meta-field tables. Ignored when populateMetaFields is true.
  private final boolean populateCommitTime;
  private final boolean populateFileName;
  private final HoodieAvroWriteSupport writeSupport;

  @SuppressWarnings({"unchecked", "rawtypes"})
  public HoodieAvroParquetWriter(StoragePath file,
                                 HoodieParquetConfig<HoodieAvroWriteSupport> parquetConfig,
                                 String instantTime,
                                 TaskContextSupplier taskContextSupplier,
                                 boolean populateMetaFields) throws IOException {
    this(file, parquetConfig, instantTime, taskContextSupplier, populateMetaFields, Collections.emptySet());
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public HoodieAvroParquetWriter(StoragePath file,
                                 HoodieParquetConfig<HoodieAvroWriteSupport> parquetConfig,
                                 String instantTime,
                                 TaskContextSupplier taskContextSupplier,
                                 boolean populateMetaFields,
                                 Set<String> metaFieldsMode) throws IOException {
    super(file, (HoodieParquetConfig) parquetConfig);
    this.fileName = file.getName();
    this.writeSupport = parquetConfig.getWriteSupport();
    this.instantTime = instantTime;
    this.taskContextSupplier = taskContextSupplier;
    this.populateMetaFields = populateMetaFields;
    Set<String> mode = metaFieldsMode == null ? Collections.emptySet() : metaFieldsMode;
    this.populateCommitTime = !populateMetaFields && mode.contains(HoodieRecord.COMMIT_TIME_METADATA_FIELD);
    this.populateFileName = !populateMetaFields && mode.contains(HoodieRecord.FILENAME_METADATA_FIELD);
  }

  @Override
  public void writeAvroWithMetadata(HoodieKey key, IndexedRecord avroRecord) throws IOException {
    if (populateMetaFields) {
      prepRecordWithMetadata(key, avroRecord, instantTime,
          taskContextSupplier.getPartitionIdSupplier().get(), getWrittenRecordCount(), fileName);
      super.write(avroRecord);
      writeSupport.add(key.getRecordKey());
    } else if (populateCommitTime || populateFileName) {
      // Selective meta-field population. The other meta columns stay null on disk, which Parquet
      // stores as definition-level flags (zero data bytes). Bloom filter / record-key index
      // population is intentionally skipped — that requires the record-key column.
      GenericRecord genericRecord = (GenericRecord) avroRecord;
      if (populateCommitTime) {
        String seqId = HoodieRecord.generateSequenceId(instantTime,
            taskContextSupplier.getPartitionIdSupplier().get(), getWrittenRecordCount());
        HoodieAvroUtils.addCommitMetadataToRecord(genericRecord, instantTime, seqId);
      }
      if (populateFileName) {
        genericRecord.put(HoodieRecord.FILENAME_METADATA_FIELD, fileName);
      }
      super.write(avroRecord);
    } else {
      super.write(avroRecord);
    }
  }

  @Override
  public void writeAvro(String key, IndexedRecord object) throws IOException {
    super.write(object);
    if (populateMetaFields) {
      writeSupport.add(key);
    }
  }

  @Override
  public void addFooterMetadata(Map<String, String> footerMetadata) {
    footerMetadata.forEach(writeSupport::addFooterMetadata);
  }

  @Override
  public void close() throws IOException {
    super.close();
  }
}
