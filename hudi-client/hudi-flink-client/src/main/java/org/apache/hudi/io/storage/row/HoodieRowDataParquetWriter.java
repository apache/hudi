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

import org.apache.hudi.client.model.HoodieRowDataCreation;
import org.apache.hudi.common.config.HoodieParquetConfig;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.MetaFieldsMode;
import org.apache.hudi.io.hadoop.HoodieBaseParquetWriter;
import org.apache.hudi.storage.StoragePath;

import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

/**
 * Parquet's impl of {@link HoodieRowDataFileWriter} to write fink {@link RowData}s.
 */
public class HoodieRowDataParquetWriter extends HoodieBaseParquetWriter<RowData>
    implements HoodieRowDataFileWriter {
  private final HoodieRowDataParquetWriteSupport writeSupport;
  private final String fileName;

  private final String instantTime;
  private final MetaFieldsMode metaFieldsMode;
  private final boolean withOperation;
  private final Function<Long, String> seqIdGenerator;

  public HoodieRowDataParquetWriter(
      StoragePath file,
      HoodieParquetConfig<HoodieRowDataParquetWriteSupport> parquetConfig,
      String instantTime,
      TaskContextSupplier taskContextSupplier,
      MetaFieldsMode metaFieldsMode,
      boolean withOperation) throws IOException {
    super(file, parquetConfig);
    this.fileName = file.getName();
    this.writeSupport = parquetConfig.getWriteSupport();
    this.instantTime = instantTime;
    this.metaFieldsMode = metaFieldsMode;
    this.withOperation = withOperation;
    this.seqIdGenerator = recordIndex -> {
      Integer partitionId = taskContextSupplier.getPartitionIdSupplier().get();
      return HoodieRecord.generateSequenceId(instantTime, partitionId, recordIndex);
    };
  }

  @Override
  public void writeRow(String key, RowData row) throws IOException {
    super.write(row);
    writeSupport.add(key);
  }

  @Override
  public void addFooterMetadata(Map<String, String> footerMetadata) {
    writeSupport.addFooterMetadata(footerMetadata);
  }

  @Override
  public void writeRowWithMetaData(HoodieKey key, RowData row) throws IOException {
    RowData rowWithMeta;
    if (metaFieldsMode == MetaFieldsMode.ALL) {
      rowWithMeta = HoodieRowDataCreation.create(instantTime, seqIdGenerator.apply(getWrittenRecordCount()),
          key.getRecordKey(), key.getPartitionPath(), fileName, row, withOperation, true);
    } else if (metaFieldsMode == MetaFieldsMode.NONE) {
      rowWithMeta = row;
    } else {
      rowWithMeta = HoodieRowDataCreation.create(
          metaFieldsMode.isCommitTimePopulated() ? instantTime : null,
          null, null, null,
          metaFieldsMode.isFileNamePopulated() ? fileName : null,
          row, withOperation, true);
    }
    writeRow(key.getRecordKey(), rowWithMeta);
  }
}
