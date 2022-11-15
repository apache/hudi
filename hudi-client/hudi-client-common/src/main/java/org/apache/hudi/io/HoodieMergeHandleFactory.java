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

import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.table.HoodieTable;

import java.util.Iterator;
import java.util.Map;

/**
 * Factory class for hoodie merge handle.
 */
public class HoodieMergeHandleFactory {
  /**
   * Creates a merge handle for normal write path.
   */
  public static <T extends HoodieRecordPayload, I, K, O> HoodieMergeHandle<T, I, K, O> create(
      WriteOperationType operationType,
      HoodieWriteConfig writeConfig,
      String instantTime,
      HoodieTable<T, I, K, O> table,
      Iterator<HoodieRecord<T>> recordItr,
      String partitionPath,
      String fileId,
      TaskContextSupplier taskContextSupplier,
      Option<BaseKeyGenerator> keyGeneratorOpt) {
    if (table.requireSortedRecords()) {
      if (table.getMetaClient().getTableConfig().isCDCEnabled()) {
        return new HoodieSortedMergeHandleWithChangeLog<>(writeConfig, instantTime, table, recordItr, partitionPath, fileId, taskContextSupplier,
            keyGeneratorOpt);
      } else {
        return new HoodieSortedMergeHandle<>(writeConfig, instantTime, table, recordItr, partitionPath, fileId, taskContextSupplier,
            keyGeneratorOpt);
      }
    } else if (!WriteOperationType.isChangingRecords(operationType) && writeConfig.allowDuplicateInserts()) {
      return new HoodieConcatHandle<>(writeConfig, instantTime, table, recordItr, partitionPath, fileId, taskContextSupplier, keyGeneratorOpt);
    } else {
      if (table.getMetaClient().getTableConfig().isCDCEnabled()) {
        return new HoodieMergeHandleWithChangeLog<>(writeConfig, instantTime, table, recordItr, partitionPath, fileId, taskContextSupplier, keyGeneratorOpt);
      } else {
        return new HoodieMergeHandle<>(writeConfig, instantTime, table, recordItr, partitionPath, fileId, taskContextSupplier, keyGeneratorOpt);
      }
    }
  }

  /**
   * Creates a merge handle for compaction path.
   */
  public static <T extends HoodieRecordPayload, I, K, O> HoodieMergeHandle<T, I, K, O> create(
      HoodieWriteConfig writeConfig,
      String instantTime,
      HoodieTable<T, I, K, O> table,
      Map<String, HoodieRecord<T>> keyToNewRecords,
      String partitionPath,
      String fileId,
      HoodieBaseFile dataFileToBeMerged,
      TaskContextSupplier taskContextSupplier,
      Option<BaseKeyGenerator> keyGeneratorOpt) {
    if (table.requireSortedRecords()) {
      if (table.getMetaClient().getTableConfig().isCDCEnabled()) {
        return new HoodieSortedMergeHandleWithChangeLog<>(writeConfig, instantTime, table, keyToNewRecords, partitionPath, fileId,
            dataFileToBeMerged, taskContextSupplier, keyGeneratorOpt);
      } else {
        return new HoodieSortedMergeHandle<>(writeConfig, instantTime, table, keyToNewRecords, partitionPath, fileId,
            dataFileToBeMerged, taskContextSupplier, keyGeneratorOpt);
      }
    } else {
      if (table.getMetaClient().getTableConfig().isCDCEnabled()) {
        return new HoodieMergeHandleWithChangeLog<>(writeConfig, instantTime, table, keyToNewRecords, partitionPath, fileId,
            dataFileToBeMerged, taskContextSupplier, keyGeneratorOpt);
      } else {
        return new HoodieMergeHandle<>(writeConfig, instantTime, table, keyToNewRecords, partitionPath, fileId,
            dataFileToBeMerged, taskContextSupplier, keyGeneratorOpt);
      }
    }
  }
}
