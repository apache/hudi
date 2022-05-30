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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.parquet.io.OutputStreamBackedOutputFile;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Base class of Hudi's custom {@link ParquetWriter} implementations
 *
 * @param <R> target type of the object being written into Parquet files (for ex,
 *           {@code IndexedRecord}, {@code InternalRow})
 */
public abstract class HoodieBaseParquetWriter<R> implements AutoCloseable {

  private static final int WRITTEN_RECORDS_THRESHOLD_FOR_FILE_SIZE_CHECK = 1000;

  protected final ParquetWriter<R> writer;
  protected final WriteSupport<R> writeSupport;
  private final AtomicLong writtenRecordCount = new AtomicLong(0);
  private final long maxFileSize;
  private long lastCachedDataSize = -1;
  private boolean isStream = false;

  public HoodieBaseParquetWriter(Path file,
                                 HoodieBaseParquetConfig<? extends WriteSupport<R>> parquetConfig) throws IOException {
    this.writeSupport = parquetConfig.getWriteSupport();
    this.writer = new Builder<R>(HoodieWrapperFileSystem.convertToHoodiePath(file, parquetConfig.getHadoopConf()), writeSupport)
        .withWriteMode(ParquetFileWriter.Mode.CREATE)
        .withCompressionCodec(parquetConfig.getCompressionCodecName())
        .withRowGroupSize(parquetConfig.getBlockSize())
        .withPageSize(parquetConfig.getPageSize())
        .withDictionaryPageSize(parquetConfig.getPageSize())
        .withDictionaryEncoding(parquetConfig.dictionaryEnabled())
        .withConf(FSUtils.registerFileSystem(file, parquetConfig.getHadoopConf())).build();
    // We cannot accurately measure the snappy compressed output file size. We are choosing a
    // conservative 10%
    // TODO - compute this compression ratio dynamically by looking at the bytes written to the
    // stream and the actual file size reported by HDFS
    this.maxFileSize = parquetConfig.getMaxFileSize()
        + Math.round(parquetConfig.getMaxFileSize() * parquetConfig.getCompressionRatio());
  }

  public HoodieBaseParquetWriter(FSDataOutputStream outputStream,
                                 HoodieBaseParquetConfig<? extends WriteSupport<R>> parquetConfig) throws IOException {
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
    this.maxFileSize = -1;
  }

  public boolean canWrite() {
    if (maxFileSize == -1) {
      return true;
    }
    // TODO we can actually do evaluation more accurately:
    //      if we cache last data size check, since we account for how many records
    //      were written we can accurately project avg record size, and therefore
    //      estimate how many more records we can write before cut off
    if (lastCachedDataSize == -1 || getWrittenRecordCount() % WRITTEN_RECORDS_THRESHOLD_FOR_FILE_SIZE_CHECK == 0) {
      lastCachedDataSize = writer.getDataSize();
    }
    return lastCachedDataSize < maxFileSize;
  }

  public void write(R object) throws IOException {
    writer.write(object);
    writtenRecordCount.incrementAndGet();
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  protected long getWrittenRecordCount() {
    return writtenRecordCount.get();
  }

  private static class Builder<R> extends ParquetWriter.Builder<R, Builder<R>> {
    private final WriteSupport<R> writeSupport;

    private Builder(Path file, WriteSupport<R> writeSupport) {
      super(file);
      this.writeSupport = writeSupport;
    }

    private Builder(OutputFile file, WriteSupport<R> writeSupport) {
      super(file);
      this.writeSupport = writeSupport;
    }

    @Override
    protected Builder<R> self() {
      return this;
    }

    @Override
    protected WriteSupport<R> getWriteSupport(Configuration conf) {
      return writeSupport;
    }
  }
}
