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

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * HoodieParquetWriter extends the ParquetWriter to help limit the size of underlying file. Provides a way to check if
 * the current file can take more records with the <code>canWrite()</code>
 *
 * ATTENTION: HoodieParquetWriter is not thread safe and developer should take care of the order of write and close
 */
@NotThreadSafe
public class HoodieParquetWriter<T extends HoodieRecordPayload, R extends IndexedRecord>
    extends ParquetWriter<IndexedRecord> implements HoodieFileWriter<R> {

  private static AtomicLong recordIndex = new AtomicLong(1);

  private final Path file;
  private final HoodieWrapperFileSystem fs;
  private final long maxFileSize;
  private final HoodieAvroWriteSupport writeSupport;
  private final String instantTime;
  private final TaskContextSupplier taskContextSupplier;
  private final boolean populateMetaFields;

  public HoodieParquetWriter(String instantTime,
                             Path file,
                             HoodieAvroParquetConfig parquetConfig,
                             Schema schema,
                             TaskContextSupplier taskContextSupplier,
                             boolean populateMetaFields) throws IOException {
    super(HoodieWrapperFileSystem.convertToHoodiePath(file, parquetConfig.getHadoopConf()),
        ParquetFileWriter.Mode.CREATE,
        parquetConfig.getWriteSupport(),
        parquetConfig.getCompressionCodecName(),
        parquetConfig.getBlockSize(),
        parquetConfig.getPageSize(),
        parquetConfig.getPageSize(),
        parquetConfig.dictionaryEnabled(),
        DEFAULT_IS_VALIDATING_ENABLED,
        DEFAULT_WRITER_VERSION,
        FSUtils.registerFileSystem(file, parquetConfig.getHadoopConf()));
    this.file = HoodieWrapperFileSystem.convertToHoodiePath(file, parquetConfig.getHadoopConf());
    this.fs =
        (HoodieWrapperFileSystem) this.file.getFileSystem(FSUtils.registerFileSystem(file, parquetConfig.getHadoopConf()));
    // We cannot accurately measure the snappy compressed output file size. We are choosing a
    // conservative 10%
    // TODO - compute this compression ratio dynamically by looking at the bytes written to the
    // stream and the actual file size reported by HDFS
    this.maxFileSize = parquetConfig.getMaxFileSize()
        + Math.round(parquetConfig.getMaxFileSize() * parquetConfig.getCompressionRatio());
    this.writeSupport = parquetConfig.getWriteSupport();
    this.instantTime = instantTime;
    this.taskContextSupplier = taskContextSupplier;
    this.populateMetaFields = populateMetaFields;
  }

  @Override
  public void writeAvroWithMetadata(HoodieKey key, R avroRecord) throws IOException {
    if (populateMetaFields) {
      prepRecordWithMetadata(key, avroRecord, instantTime,
          taskContextSupplier.getPartitionIdSupplier().get(), recordIndex, file.getName());
      super.write(avroRecord);
      writeSupport.add(key.getRecordKey());
    } else {
      super.write(avroRecord);
    }
  }

  @Override
  public boolean canWrite() {
    return getDataSize() < maxFileSize;
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
