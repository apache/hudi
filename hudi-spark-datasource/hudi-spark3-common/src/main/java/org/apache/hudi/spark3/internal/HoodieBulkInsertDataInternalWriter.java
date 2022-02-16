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

package org.apache.hudi.spark3.internal;

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.internal.BulkInsertDataInternalWriterHelper;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;

/**
 * Hoodie's Implementation of {@link DataWriter<InternalRow>}. This is used in data source "hudi.spark3.internal" implementation for bulk insert.
 */
public class HoodieBulkInsertDataInternalWriter implements DataWriter<InternalRow> {

  private final BulkInsertDataInternalWriterHelper bulkInsertWriterHelper;

  public HoodieBulkInsertDataInternalWriter(HoodieTable hoodieTable, HoodieWriteConfig writeConfig,
                                            String instantTime, int taskPartitionId, long taskId, StructType structType, boolean populateMetaFields,
                                            boolean arePartitionRecordsSorted) {
    this.bulkInsertWriterHelper = new BulkInsertDataInternalWriterHelper(hoodieTable,
        writeConfig, instantTime, taskPartitionId, taskId, 0, structType, populateMetaFields, arePartitionRecordsSorted);
  }

  @Override
  public void write(InternalRow record) throws IOException {
    bulkInsertWriterHelper.write(record);
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    return new HoodieWriterCommitMessage(bulkInsertWriterHelper.getWriteStatuses());
  }

  @Override
  public void abort() {
    bulkInsertWriterHelper.abort();
  }

  @Override
  public void close() throws IOException {
    bulkInsertWriterHelper.close();
  }
}