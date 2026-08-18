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

import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.config.HoodieParquetConfig;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.MetaFieldsMode;
import org.apache.hudi.core.io.storage.HoodieAvroFileWriter;
import org.apache.hudi.io.hadoop.HoodieBaseParquetWriter;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

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
  private final MetaFieldsMode metaFieldsMode;
  private final HoodieAvroWriteSupport writeSupport;

  /**
   * @deprecated since 1.3.0, use the {@link MetaFieldsMode} overload. Retained for existing callers
   * that only distinguish all-or-nothing meta fields ({@code true} maps to {@link MetaFieldsMode#ALL},
   * {@code false} to {@link MetaFieldsMode#NONE}); it cannot express the selective modes.
   */
  @Deprecated
  @SuppressWarnings({"unchecked", "rawtypes"})
  public HoodieAvroParquetWriter(StoragePath file,
                                 HoodieParquetConfig<HoodieAvroWriteSupport> parquetConfig,
                                 String instantTime,
                                 TaskContextSupplier taskContextSupplier,
                                 boolean populateMetaFields) throws IOException {
    this(file, parquetConfig, instantTime, taskContextSupplier,
        populateMetaFields ? MetaFieldsMode.ALL : MetaFieldsMode.NONE);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public HoodieAvroParquetWriter(StoragePath file,
                                 HoodieParquetConfig<HoodieAvroWriteSupport> parquetConfig,
                                 String instantTime,
                                 TaskContextSupplier taskContextSupplier,
                                 MetaFieldsMode metaFieldsMode) throws IOException {
    super(file, (HoodieParquetConfig) parquetConfig);
    this.fileName = file.getName();
    this.writeSupport = parquetConfig.getWriteSupport();
    this.instantTime = instantTime;
    this.taskContextSupplier = taskContextSupplier;
    this.metaFieldsMode = Objects.requireNonNull(metaFieldsMode, "metaFieldsMode");
  }

  @Override
  public void writeAvroWithMetadata(HoodieKey key, IndexedRecord avroRecord) throws IOException {
    switch (metaFieldsMode) {
      case ALL:
        prepRecordWithMetadata(key, avroRecord, instantTime,
            taskContextSupplier.getPartitionIdSupplier().get(), getWrittenRecordCount(), fileName);
        super.write(avroRecord);
        writeSupport.add(key.getRecordKey());
        break;
      case NONE:
        super.write(avroRecord);
        break;
      default:
        // Selective mode — populate only the opted-in columns. The other meta columns stay null,
        // which Parquet stores as definition-level flags (zero data bytes).
        //
        // No bloom filter is added here, but not because the key is unavailable: the ALL branch above
        // takes it from the in-memory HoodieKey, not from the _hoodie_record_key column, so the same
        // is possible here. It is that HoodieFileWriterFactory#enableBloomFilter short-circuits on
        // populateMetaFields, which every selective mode makes false, so writeSupport holds no filter
        // to add to. A bloom index on such a table is rejected at BaseHoodieWriteClient
        // #validateAgainstTableProperties, since lookups would otherwise NPE on a missing filter.
        GenericRecord genericRecord = (GenericRecord) avroRecord;
        if (metaFieldsMode.isCommitTimePopulated()) {
          genericRecord.put(HoodieRecord.COMMIT_TIME_METADATA_FIELD, instantTime);
        }
        if (metaFieldsMode.isFileNamePopulated()) {
          genericRecord.put(HoodieRecord.FILENAME_METADATA_FIELD, fileName);
        }
        super.write(avroRecord);
        break;
    }
  }

  @Override
  public void writeAvro(String key, IndexedRecord object) throws IOException {
    super.write(object);
    if (metaFieldsMode == MetaFieldsMode.ALL) {
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
