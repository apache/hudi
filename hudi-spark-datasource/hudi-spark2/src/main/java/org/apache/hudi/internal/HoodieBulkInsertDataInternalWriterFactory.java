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

package org.apache.hudi.internal;

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.types.StructType;

/**
 * Factory to assist in instantiating {@link HoodieBulkInsertDataInternalWriter}.
 */
public class HoodieBulkInsertDataInternalWriterFactory implements DataWriterFactory<InternalRow> {

  private final String instantTime;
  private final HoodieTable hoodieTable;
  private final HoodieWriteConfig writeConfig;
  private final StructType structType;
  private final boolean arePartitionRecordsSorted;
  private final boolean populateMetaFields;

  public HoodieBulkInsertDataInternalWriterFactory(HoodieTable hoodieTable, HoodieWriteConfig writeConfig,
                                                   String instantTime, StructType structType, boolean populateMetaFields,
                                                   boolean arePartitionRecordsSorted) {
    this.hoodieTable = hoodieTable;
    this.writeConfig = writeConfig;
    this.instantTime = instantTime;
    this.structType = structType;
    this.populateMetaFields = populateMetaFields;
    this.arePartitionRecordsSorted = arePartitionRecordsSorted;
  }

  @Override
  public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId) {
    return new HoodieBulkInsertDataInternalWriter(hoodieTable, writeConfig, instantTime, partitionId, taskId, epochId,
        structType, populateMetaFields, arePartitionRecordsSorted);
  }
}
