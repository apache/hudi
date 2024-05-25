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

import org.apache.hudi.io.hadoop.HoodieBaseParquetWriter;
import org.apache.hudi.io.storage.HoodieParquetConfig;
import org.apache.hudi.storage.StoragePath;

import org.apache.flink.table.data.RowData;

import java.io.IOException;

/**
 * Parquet's impl of {@link HoodieRowDataFileWriter} to write fink {@link RowData}s.
 */
public class HoodieRowDataParquetWriter extends HoodieBaseParquetWriter<RowData>
    implements HoodieRowDataFileWriter {

  private final HoodieRowDataParquetWriteSupport writeSupport;

  public HoodieRowDataParquetWriter(StoragePath file, HoodieParquetConfig<HoodieRowDataParquetWriteSupport> parquetConfig)
      throws IOException {
    super(file, parquetConfig);

    this.writeSupport = parquetConfig.getWriteSupport();
  }

  @Override
  public void writeRow(String key, RowData row) throws IOException {
    super.write(row);
    writeSupport.add(key);
  }

  @Override
  public void writeRow(RowData row) throws IOException {
    super.write(row);
  }
}
