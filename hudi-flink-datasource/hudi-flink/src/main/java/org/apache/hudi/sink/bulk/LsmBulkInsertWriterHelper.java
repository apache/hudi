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

package org.apache.hudi.sink.bulk;

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;

/**
 * Bulk-insert writer helper for LSM input rows.
 *
 * <p>The input row contains partition path, encoded record key, and the original table row. The
 * routing and record keys are retained after sorting so the writer does not generate them again.
 */
public class LsmBulkInsertWriterHelper extends BulkInsertWriterHelper {

  private final int recordArity;

  public LsmBulkInsertWriterHelper(
      Configuration conf,
      HoodieTable<?, ?, ?, ?> hoodieTable,
      HoodieWriteConfig writeConfig,
      String instantTime,
      int taskPartitionId,
      long taskId,
      long taskEpochId,
      RowType rowType) {
    super(conf, hoodieTable, writeConfig, instantTime, taskPartitionId, taskId, taskEpochId, rowType);
    this.recordArity = rowType.getFieldCount();
  }

  @Override
  public void write(RowData sortRow) throws IOException {
    String partitionPath = sortRow.getString(0).toString();
    String recordKey = sortRow.getString(1).toString();
    RowData record = sortRow.getRow(2, recordArity);
    writeRecord(recordKey, partitionPath, record);
  }

  /**
   * Decorates a table row with the partition path and encoded record key used by the LSM
   * sorter.
   *
   * @param partitionPath partition path
   * @param record        original table row used to generate the record key
   * @param keyGen        RowData key generator
   * @return internal LSM sort row containing partition path, record key, and original table row
   */
  public static RowData rowWithPartitionAndKey(
      String partitionPath,
      RowData record,
      RowDataKeyGen keyGen) {
    return GenericRowData.of(
        StringData.fromString(partitionPath),
        StringData.fromString(keyGen.getRecordKey(record)),
        record);
  }
}
