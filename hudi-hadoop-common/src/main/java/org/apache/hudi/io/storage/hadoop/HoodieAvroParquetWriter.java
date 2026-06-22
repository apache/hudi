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
import org.apache.hudi.io.hadoop.HoodieBaseParquetWriter;
import org.apache.hudi.io.storage.HoodieAvroFileWriter;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.IndexedRecord;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;

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
  // True when the writer should additionally populate _hoodie_commit_time (and seqId) even though
  // populateMetaFields is false. This is the COMMIT_TIME_ONLY mode used to keep incremental queries
  // functional on otherwise-minimal-meta-field tables. Ignored when populateMetaFields is true.
  private final boolean commitTimeOnly;
  private final HoodieAvroWriteSupport writeSupport;

  @SuppressWarnings({"unchecked", "rawtypes"})
  public HoodieAvroParquetWriter(StoragePath file,
                                 HoodieParquetConfig<HoodieAvroWriteSupport> parquetConfig,
                                 String instantTime,
                                 TaskContextSupplier taskContextSupplier,
                                 boolean populateMetaFields) throws IOException {
    this(file, parquetConfig, instantTime, taskContextSupplier, populateMetaFields, false);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public HoodieAvroParquetWriter(StoragePath file,
                                 HoodieParquetConfig<HoodieAvroWriteSupport> parquetConfig,
                                 String instantTime,
                                 TaskContextSupplier taskContextSupplier,
                                 boolean populateMetaFields,
                                 boolean commitTimeOnly) throws IOException {
    super(file, (HoodieParquetConfig) parquetConfig);
    this.fileName = file.getName();
    this.writeSupport = parquetConfig.getWriteSupport();
    this.instantTime = instantTime;
    this.taskContextSupplier = taskContextSupplier;
    this.populateMetaFields = populateMetaFields;
    this.commitTimeOnly = commitTimeOnly && !populateMetaFields;
  }

  @Override
  public void writeAvroWithMetadata(HoodieKey key, IndexedRecord avroRecord) throws IOException {
    if (populateMetaFields) {
      prepRecordWithMetadata(key, avroRecord, instantTime,
          taskContextSupplier.getPartitionIdSupplier().get(), getWrittenRecordCount(), fileName);
      super.write(avroRecord);
      writeSupport.add(key.getRecordKey());
    } else if (commitTimeOnly) {
      // COMMIT_TIME_ONLY: populate only _hoodie_commit_time (and seq id derived from it) so
      // incremental queries keep working. The other four meta columns stay null on disk, which
      // Parquet stores as definition-level flags (zero data bytes). Bloom filter / record-key
      // index population is intentionally skipped — that requires the record-key column.
      String seqId = HoodieRecord.generateSequenceId(instantTime,
          taskContextSupplier.getPartitionIdSupplier().get(), getWrittenRecordCount());
      HoodieAvroUtils.addCommitMetadataToRecord(
          (org.apache.avro.generic.GenericRecord) avroRecord, instantTime, seqId);
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
  public void close() throws IOException {
    super.close();
  }
}
