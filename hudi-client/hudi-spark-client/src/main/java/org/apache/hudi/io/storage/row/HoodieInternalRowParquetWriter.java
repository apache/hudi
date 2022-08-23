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
import org.apache.hudi.io.storage.HoodieParquetConfig;
import org.apache.hudi.io.storage.HoodieBaseParquetWriter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;

/**
 * Parquet's impl of {@link HoodieInternalRowFileWriter} to write {@link InternalRow}s.
 */
public class HoodieInternalRowParquetWriter extends HoodieBaseParquetWriter<InternalRow>
    implements HoodieInternalRowFileWriter {

  private final HoodieRowParquetWriteSupport writeSupport;

  public HoodieInternalRowParquetWriter(Path file, HoodieParquetConfig<HoodieRowParquetWriteSupport> parquetConfig)
      throws IOException {
    super(file, parquetConfig);

    this.writeSupport = parquetConfig.getWriteSupport();
  }

  @Override
  public void writeRow(UTF8String key, InternalRow row) throws IOException {
    super.write(row);
    writeSupport.add(key);
  }

  @Override
  public void writeRow(InternalRow row) throws IOException {
    super.write(row);
  }
}
