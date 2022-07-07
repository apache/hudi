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

import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.parquet.io.OutputStreamBackedOutputFile;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;

// TODO(HUDI-3035) unify w/ HoodieParquetWriter
public class HoodieParquetStreamWriter<R extends IndexedRecord> implements AutoCloseable {

  private final ParquetWriter<R> writer;
  private final HoodieAvroWriteSupport writeSupport;

  public HoodieParquetStreamWriter(FSDataOutputStream outputStream,
                                   HoodieParquetConfig<HoodieAvroWriteSupport> parquetConfig) throws IOException {
    this.writeSupport = parquetConfig.getWriteSupport();
    this.writer = new Builder<R>(new OutputStreamBackedOutputFile(outputStream), writeSupport)
        .withWriteMode(ParquetFileWriter.Mode.CREATE)
        .withCompressionCodec(parquetConfig.getCompressionCodecName())
        .withRowGroupSize(parquetConfig.getBlockSize())
        .withPageSize(parquetConfig.getPageSize())
        .withDictionaryPageSize(parquetConfig.getPageSize())
        .withDictionaryEncoding(parquetConfig.dictionaryEnabled())
        .withWriterVersion(ParquetWriter.DEFAULT_WRITER_VERSION)
        .withConf(parquetConfig.getHadoopConf())
        .build();
  }

  public void writeAvro(String key, R object) throws IOException {
    writer.write(object);
    writeSupport.add(key);
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  private static class Builder<T> extends ParquetWriter.Builder<T, Builder<T>> {
    private final WriteSupport<T> writeSupport;

    private Builder(Path file, WriteSupport<T> writeSupport) {
      super(file);
      this.writeSupport = writeSupport;
    }

    private Builder(OutputFile file, WriteSupport<T> writeSupport) {
      super(file);
      this.writeSupport = writeSupport;
    }

    @Override
    protected Builder<T> self() {
      return this;
    }

    @Override
    protected WriteSupport<T> getWriteSupport(Configuration conf) {
      return writeSupport;
    }
  }
}