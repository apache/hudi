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

package org.apache.hudi.io.hadoop;

import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.io.storage.HoodieAvroFileWriter;
import org.apache.hudi.io.storage.HoodieParquetConfig;
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
  private final HoodieAvroWriteSupport writeSupport;

  @SuppressWarnings({"unchecked", "rawtypes"})
  public HoodieAvroParquetWriter(StoragePath file,
                                 HoodieParquetConfig<HoodieAvroWriteSupport> parquetConfig,
                                 String instantTime,
                                 TaskContextSupplier taskContextSupplier,
                                 boolean populateMetaFields) throws IOException {
    super(file, (HoodieParquetConfig) parquetConfig);
    this.fileName = file.getName();
    this.writeSupport = parquetConfig.getWriteSupport();
    this.instantTime = instantTime;
    this.taskContextSupplier = taskContextSupplier;
    this.populateMetaFields = populateMetaFields;
  }

  @Override
  public void writeAvroWithMetadata(HoodieKey key, IndexedRecord avroRecord) throws IOException {
    if (populateMetaFields) {
      prepRecordWithMetadata(key, avroRecord, instantTime,
          taskContextSupplier.getPartitionIdSupplier().get(), getWrittenRecordCount(), fileName);
      super.write(avroRecord);
      writeSupport.add(key.getRecordKey());
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
