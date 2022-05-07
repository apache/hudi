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

package org.apache.hudi.io.storage.row;

import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;

import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.IOException;

/**
 * Parquet's impl of {@link HoodieInternalRowFileWriter} to write {@link InternalRow}s.
 */
public class HoodieInternalRowParquetWriter extends ParquetWriter<InternalRow>
    implements HoodieInternalRowFileWriter {

  private final Path file;
  private final HoodieWrapperFileSystem fs;
  private final long maxFileSize;
  private final HoodieRowParquetWriteSupport writeSupport;

  public HoodieInternalRowParquetWriter(Path file, HoodieRowParquetConfig parquetConfig)
      throws IOException {
    super(HoodieWrapperFileSystem.convertToHoodiePath(file, parquetConfig.getHadoopConf()),
        ParquetFileWriter.Mode.CREATE, parquetConfig.getWriteSupport(), parquetConfig.getCompressionCodecName(),
        parquetConfig.getBlockSize(), parquetConfig.getPageSize(), parquetConfig.getPageSize(),
        DEFAULT_IS_DICTIONARY_ENABLED, DEFAULT_IS_VALIDATING_ENABLED,
        DEFAULT_WRITER_VERSION, FSUtils.registerFileSystem(file, parquetConfig.getHadoopConf()));
    this.file = HoodieWrapperFileSystem.convertToHoodiePath(file, parquetConfig.getHadoopConf());
    this.fs = (HoodieWrapperFileSystem) this.file.getFileSystem(FSUtils.registerFileSystem(file,
            parquetConfig.getHadoopConf()));
    this.maxFileSize = parquetConfig.getMaxFileSize()
            + Math.round(parquetConfig.getMaxFileSize() * parquetConfig.getCompressionRatio());
    this.writeSupport = parquetConfig.getWriteSupport();
  }

  @Override
  public boolean canWrite() {
    return getDataSize() < maxFileSize;
  }

  @Override
  public void writeRow(String key, InternalRow row) throws IOException {
    super.write(row);
    writeSupport.add(key);
  }

  @Override
  public void writeRow(InternalRow row) throws IOException {
    super.write(row);
  }

  @Override
  public void close() throws IOException {
    super.close();
  }
}
