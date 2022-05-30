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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.io.storage.row.HoodieRowParquetConfig;
import org.apache.hudi.io.storage.row.HoodieRowParquetWriteSupport;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.IOException;

public class HoodieSparkParquetWriter extends HoodieBaseParquetWriter implements HoodieSparkFileWriter {

  // TODO: better code reuse
  private final String fileName;
  private final String instantTime;
  private final TaskContextSupplier taskContextSupplier;
  private final boolean populateMetaFields;

  public HoodieSparkParquetWriter(Path file,
                                  HoodieRowParquetConfig parquetConfig,
                                  String instantTime,
                                  TaskContextSupplier taskContextSupplier,
                                  boolean populateMetaFields) throws IOException {
    super(file, parquetConfig);
    this.fileName = file.getName();
    this.instantTime = instantTime;
    this.taskContextSupplier = taskContextSupplier;
    this.populateMetaFields = populateMetaFields;
  }

  public HoodieSparkParquetWriter(FSDataOutputStream outputStream,
                                  HoodieRowParquetConfig parquetConfig,
                                 boolean populateMetaFields) throws IOException {
    super(outputStream, parquetConfig);
    this.fileName = null;
    this.instantTime = null;
    this.taskContextSupplier = null;
    this.populateMetaFields = populateMetaFields;
  }

  @Override
  public void writeRowWithMetadata(HoodieKey key, InternalRow row) throws IOException {
    if (populateMetaFields) {
      super.write(prepRecordWithMetadata(key, row, instantTime,
          taskContextSupplier.getPartitionIdSupplier().get(), getWrittenRecordCount(), fileName));
      ((HoodieRowParquetWriteSupport) writeSupport).add(key.getRecordKey());
    } else {
      super.write(row);
    }
  }

  @Override
  public void writeRow(String recordKey, InternalRow row) throws IOException {
    super.write(row);
    if (populateMetaFields) {
      ((HoodieRowParquetWriteSupport) writeSupport).add(recordKey);
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
  }
}
