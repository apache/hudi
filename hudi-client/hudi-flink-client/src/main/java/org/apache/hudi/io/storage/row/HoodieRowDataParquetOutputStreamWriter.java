/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.io.storage.row;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.io.storage.HoodieParquetConfig;
import org.apache.hudi.parquet.io.OutputStreamBackedOutputFile;

import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;

import java.io.IOException;

/**
 * An implementation for {@link HoodieRowDataFileWriter} which is used to write hoodie records into an {@link FSDataOutputStream}.
 *
 * <p>{@link HoodieRowDataParquetOutputStreamWriter} also supports collect and report column statistics based on the parquet metadata.
 */
public class HoodieRowDataParquetOutputStreamWriter implements HoodieRowDataFileWriter {
  private final ParquetWriter writer;
  private boolean isClosed;
  private final HoodieRowDataParquetWriteSupport writeSupport;

  public HoodieRowDataParquetOutputStreamWriter(
      FSDataOutputStream outputStream,
      HoodieRowDataParquetWriteSupport writeSupport,
      HoodieParquetConfig<HoodieRowDataParquetWriteSupport> parquetConfig) throws IOException {
    this.writeSupport = writeSupport;
    ParquetWriter.Builder parquetWriterbuilder = new ParquetWriter.Builder(
        new OutputStreamBackedOutputFile(outputStream)) {
      @Override
      protected ParquetWriter.Builder self() {
        return this;
      }

      @Override
      protected WriteSupport getWriteSupport(Configuration conf) {
        return parquetConfig.getWriteSupport();
      }
    };
    parquetWriterbuilder.withWriteMode(ParquetFileWriter.Mode.CREATE);
    parquetWriterbuilder.withCompressionCodec(parquetConfig.getCompressionCodecName());
    parquetWriterbuilder.withRowGroupSize(parquetConfig.getBlockSize());
    parquetWriterbuilder.withPageSize(parquetConfig.getPageSize());
    parquetWriterbuilder.withDictionaryPageSize(parquetConfig.getPageSize());
    parquetWriterbuilder.withDictionaryEncoding(parquetConfig.isDictionaryEnabled());
    parquetWriterbuilder.withWriterVersion(ParquetWriter.DEFAULT_WRITER_VERSION);
    parquetWriterbuilder.withConf(parquetConfig.getStorageConf().unwrapAs(Configuration.class));
    this.writer = parquetWriterbuilder.build();
  }

  @Override
  public boolean canWrite() {
    return true;
  }

  @Override
  public void writeRow(String key, RowData row) throws IOException {
    writer.write(row);
    writeSupport.add(key);
  }

  @Override
  public void writeRowWithMetaData(HoodieKey key, RowData row) throws IOException {
    writeRow(key.getRecordKey(), row);
  }

  @Override
  public void close() throws IOException {
    if (isClosed) {
      return;
    }
    this.writer.close();
    isClosed = true;
  }

  @Override
  public Object getFileFormatMetadata() {
    ValidationUtils.checkArgument(isClosed, "Column range metadata can only be fetched after the Parquet writer is closed.");
    return writer.getFooter();
  }
}
