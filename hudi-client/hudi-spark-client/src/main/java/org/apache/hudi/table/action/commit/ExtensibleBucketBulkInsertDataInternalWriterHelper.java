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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;

// TODO: complete the implementation of this class for extensible bucket
public class ExtensibleBucketBulkInsertDataInternalWriterHelper extends BucketBulkInsertDataInternalWriterHelper {

  public ExtensibleBucketBulkInsertDataInternalWriterHelper(HoodieTable hoodieTable, HoodieWriteConfig writeConfig, String instantTime, int taskPartitionId,
                                                            long taskId, long taskEpochId, StructType structType, boolean populateMetaFields,
                                                            boolean arePartitionRecordsSorted) {
    super(hoodieTable, writeConfig, instantTime, taskPartitionId, taskId, taskEpochId, structType, populateMetaFields, arePartitionRecordsSorted);
  }

  public ExtensibleBucketBulkInsertDataInternalWriterHelper(HoodieTable hoodieTable, HoodieWriteConfig writeConfig, String instantTime, int taskPartitionId, long taskId, long taskEpochId,
                                                            StructType structType, boolean populateMetaFields, boolean arePartitionRecordsSorted, boolean shouldPreserveHoodieMetadata) {
    super(hoodieTable, writeConfig, instantTime, taskPartitionId, taskId, taskEpochId, structType, populateMetaFields, arePartitionRecordsSorted, shouldPreserveHoodieMetadata);
  }

  @Override
  public void write(InternalRow row) throws IOException {
    super.write(row);
  }

  @Override
  public void close() throws IOException {
    super.close();
  }
}
