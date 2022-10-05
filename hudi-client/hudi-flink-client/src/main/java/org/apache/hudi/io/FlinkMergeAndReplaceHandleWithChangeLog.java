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

package org.apache.hudi.io;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.table.log.AppendResult;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;

import java.util.Properties;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * A flink merge and replace handle that supports logging change logs.
 *
 * <p>The cdc about logic is copied from {@link HoodieMergeHandleWithChangeLog},
 * we should refactor it out when there are good abstractions.
 */
public class FlinkMergeAndReplaceHandleWithChangeLog<T, I, K, O>
    extends FlinkMergeAndReplaceHandle<T, I, K, O> {
  private final HoodieCDCLogger cdcLogger;

  public FlinkMergeAndReplaceHandleWithChangeLog(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                                 Iterator<HoodieRecord<T>> recordItr, String partitionPath, String fileId,
                                                 TaskContextSupplier taskContextSupplier, Path basePath) {
    super(config, instantTime, hoodieTable, recordItr, partitionPath, fileId, taskContextSupplier, basePath);
    this.cdcLogger = new HoodieCDCLogger(
        instantTime,
        config,
        hoodieTable.getMetaClient().getTableConfig(),
        tableSchema,
        createLogWriter(instantTime, HoodieCDCUtils.CDC_LOGFILE_SUFFIX),
        IOUtils.getMaxMemoryPerPartitionMerge(taskContextSupplier, config));
  }

  protected boolean writeUpdateRecord(HoodieRecord<T> hoodieRecord, HoodieRecord<T> oldRecord, Option<HoodieRecord> combineRecordOp, Schema combineRecordSchema)
      throws IOException {
    final boolean result = super.writeUpdateRecord(hoodieRecord, oldRecord, combineRecordOp, combineRecordSchema);
    if (result) {
      boolean isDelete = HoodieOperation.isDelete(hoodieRecord.getOperation());
      Option<IndexedRecord> combineRecord;
      if (combineRecordOp.isPresent()) {
        combineRecord = combineRecordOp.get().toIndexedRecord(combineRecordSchema, new Properties()).map(HoodieAvroIndexedRecord::getData);
      } else {
        combineRecord = Option.empty();
      }
      cdcLogger.put(hoodieRecord, (GenericRecord) oldRecord.getData(), isDelete ? Option.empty() : combineRecord);
      hoodieRecord.deflate();
    }
    return result;
  }

  protected void writeInsertRecord(HoodieRecord<T> hoodieRecord, Schema schema) throws IOException {
    super.writeInsertRecord(hoodieRecord, schema);
    if (!HoodieOperation.isDelete(hoodieRecord.getOperation())) {
      cdcLogger.put(hoodieRecord, null, hoodieRecord.toIndexedRecord(schema, new Properties()).map(HoodieAvroIndexedRecord::getData));
      hoodieRecord.deflate();
    }
  }

  @Override
  public List<WriteStatus> close() {
    List<WriteStatus> writeStatuses = super.close();
    // if there are cdc data written, set the CDC-related information.
    Option<AppendResult> cdcResult = cdcLogger.writeCDCData();
    HoodieCDCLogger.setCDCStatIfNeeded(writeStatuses.get(0).getStat(), cdcResult, partitionPath, fs);
    return writeStatuses;
  }
}
