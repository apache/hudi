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

import org.apache.hudi.avro.HoodieAvroWriteSupport;

import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.IOException;
import java.io.OutputStream;

/**
 * HoodieParquetStreamWriter wraps the ParquetWriter.
 */
public class HoodieParquetStreamWriter<R extends IndexedRecord> {

  private final ParquetWriter<R> writer;
  private final HoodieAvroWriteSupport writeSupport;

  public HoodieParquetStreamWriter(OutputStream bufferedOutputStream,
                                   HoodieAvroParquetConfig parquetConfig) throws IOException {
    writer = new Builder<R>(new ParquetBufferedWriter(bufferedOutputStream), parquetConfig.getWriteSupport())
        .withWriteMode(ParquetFileWriter.Mode.CREATE)
        .withCompressionCodec(parquetConfig.getCompressionCodecName())
        .withRowGroupSize(parquetConfig.getBlockSize())
        .withPageSize(parquetConfig.getPageSize())
        .withDictionaryPageSize(parquetConfig.getPageSize())
        .withMaxPaddingSize(ParquetWriter.MAX_PADDING_SIZE_DEFAULT)
        .withDictionaryEncoding(ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED)
        .withValidation(ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED)
        .withWriterVersion(ParquetWriter.DEFAULT_WRITER_VERSION)
        .withConf(parquetConfig.getHadoopConf()).build();

    this.writeSupport = parquetConfig.getWriteSupport();
  }

  public void writeAvro(String key, R object) throws IOException {
    writer.write(object);
    writeSupport.add(key);
  }

  public void close() throws IOException {
    writer.close();
  }

  public long getDataSize() {
    return writer.getDataSize();
  }

  public static class ParquetBufferedWriter implements OutputFile {

    private final OutputStream out;

    public ParquetBufferedWriter(OutputStream out) {
      this.out = out;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
      return createPositionOutputstream();
    }

    private PositionOutputStream createPositionOutputstream() {
      return new PositionOutputStream() {
        @Override
        public long getPos() throws IOException {
          return 0;
        }

        @Override
        public void write(int b) throws IOException {
          out.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
          out.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
          out.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
          out.flush();
        }

        @Override
        public void close() throws IOException {
          out.close();
        }
      };
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
      return createPositionOutputstream();
    }

    @Override
    public boolean supportsBlockSize() {
      return false;
    }

    @Override
    public long defaultBlockSize() {
      return 0;
    }
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
