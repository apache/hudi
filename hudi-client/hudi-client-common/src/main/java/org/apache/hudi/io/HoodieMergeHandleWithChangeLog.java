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

package org.apache.hudi.io;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A merge handle that supports logging change logs.
 */
public class HoodieMergeHandleWithChangeLog<T extends HoodieRecordPayload, I, K, O> extends HoodieMergeHandle<T, I, K, O> {
  protected final HoodieCDCLogger cdcLogger;

  public HoodieMergeHandleWithChangeLog(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                        Iterator<HoodieRecord<T>> recordItr, String partitionPath, String fileId,
                                        TaskContextSupplier taskContextSupplier, Option<BaseKeyGenerator> keyGeneratorOpt) {
    super(config, instantTime, hoodieTable, recordItr, partitionPath, fileId, taskContextSupplier, keyGeneratorOpt);
    this.cdcLogger = new HoodieCDCLogger(
        instantTime,
        config,
        hoodieTable.getMetaClient().getTableConfig(),
        partitionPath,
        fs,
        tableSchema,
        createLogWriter(instantTime, HoodieCDCUtils.CDC_LOGFILE_SUFFIX),
        IOUtils.getMaxMemoryPerPartitionMerge(taskContextSupplier, config));
  }

  /**
   * Called by compactor code path.
   */
  public HoodieMergeHandleWithChangeLog(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                        Map<String, HoodieRecord<T>> keyToNewRecords, String partitionPath, String fileId,
                                        HoodieBaseFile dataFileToBeMerged, TaskContextSupplier taskContextSupplier, Option<BaseKeyGenerator> keyGeneratorOpt) {
    super(config, instantTime, hoodieTable, keyToNewRecords, partitionPath, fileId, dataFileToBeMerged, taskContextSupplier, keyGeneratorOpt);
    this.cdcLogger = new HoodieCDCLogger(
        instantTime,
        config,
        hoodieTable.getMetaClient().getTableConfig(),
        partitionPath,
        fs,
        tableSchema,
        createLogWriter(instantTime, HoodieCDCUtils.CDC_LOGFILE_SUFFIX),
        IOUtils.getMaxMemoryPerPartitionMerge(taskContextSupplier, config));
  }

  protected boolean writeUpdateRecord(HoodieRecord<T> hoodieRecord, GenericRecord oldRecord, Option<IndexedRecord> indexedRecord) {
    final boolean result = super.writeUpdateRecord(hoodieRecord, oldRecord, indexedRecord);
    if (result) {
      boolean isDelete = HoodieOperation.isDelete(hoodieRecord.getOperation());
      cdcLogger.put(hoodieRecord, oldRecord, isDelete ? Option.empty() : indexedRecord);
    }
    return result;
  }

  protected void writeInsertRecord(HoodieRecord<T> hoodieRecord, Option<IndexedRecord> insertRecord) {
    super.writeInsertRecord(hoodieRecord, insertRecord);
    if (!HoodieOperation.isDelete(hoodieRecord.getOperation())) {
      cdcLogger.put(hoodieRecord, null, insertRecord);
    }
  }

  @Override
  public List<WriteStatus> close() {
    List<WriteStatus> writeStatuses = super.close();
    // if there are cdc data written, set the CDC-related information.

    if (cdcLogger == null || recordsWritten == 0L || (recordsWritten == insertRecordsWritten)) {
      // the following cases where we do not need to write out the cdc file:
      // case 1: all the data from the previous file slice are deleted. and no new data is inserted;
      // case 2: all the incoming data is INSERT.
      return writeStatuses;
    }

    cdcLogger.close();
    Pair<List<String>, List<Long>> cdcWriteStats = cdcLogger.getCDCWriteStats();
    HoodieWriteStat stat = writeStatuses.get(0).getStat();
    stat.setCdcPath(cdcWriteStats.getLeft());
    stat.setCdcWriteBytes(cdcWriteStats.getRight());
    return writeStatuses;
  }
}
