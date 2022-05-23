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

import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.io.storage.row.HoodieRowParquetConfig;
import org.apache.hudi.io.storage.row.HoodieRowParquetWriteSupport;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.IOException;

public class HoodieSparkParquetWriter extends HoodieBaseParquetWriter implements HoodieSparkFileWriter {


  private final HoodieRowParquetWriteSupport writeSupport;

  public HoodieSparkParquetWriter(Path file, HoodieRowParquetConfig parquetConfig) throws IOException {
    super(file, parquetConfig);
    this.writeSupport = parquetConfig.getWriteSupport();
  }

  @Override
  public void writeRowWithMetadata(HoodieKey key, InternalRow row) throws IOException {

  }

  @Override
  public void writeRow(String recordKey, InternalRow row) throws IOException {
    super.write(row);
    writeSupport.add(recordKey); // todo: whether to has BF
  }

  @Override
  public void close() throws IOException {
    super.close();
  }
}
