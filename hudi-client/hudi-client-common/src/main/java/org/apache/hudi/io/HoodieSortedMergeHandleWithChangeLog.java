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
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Properties;
import java.util.Iterator;
import java.util.Map;

/**
 * A sorted merge handle that supports logging change logs.
 */
public class HoodieSortedMergeHandleWithChangeLog<T, I, K, O> extends HoodieMergeHandleWithChangeLog<T, I, K, O> {
  public HoodieSortedMergeHandleWithChangeLog(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                              Iterator<HoodieRecord<T>> recordItr, String partitionPath, String fileId,
                                              TaskContextSupplier taskContextSupplier, Option<BaseKeyGenerator> keyGeneratorOpt) {
    super(config, instantTime, hoodieTable, recordItr, partitionPath, fileId, taskContextSupplier, keyGeneratorOpt);
  }

  /**
   * Called by compactor code path.
   */
  public HoodieSortedMergeHandleWithChangeLog(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                           Map<String, HoodieRecord<T>> keyToNewRecords, String partitionPath, String fileId,
                           HoodieBaseFile dataFileToBeMerged, TaskContextSupplier taskContextSupplier, Option<BaseKeyGenerator> keyGeneratorOpt) {
    super(config, instantTime, hoodieTable, keyToNewRecords, partitionPath, fileId, dataFileToBeMerged, taskContextSupplier, keyGeneratorOpt);
  }

  protected boolean writeRecord(HoodieRecord<T> hoodieRecord, Option<HoodieRecord> insertRecord, Schema schema, Properties props)
      throws IOException {
    final boolean result = super.writeRecord(hoodieRecord, insertRecord, schema, props);
    this.cdcLogger.put(hoodieRecord, null, insertRecord.map(rec -> ((HoodieAvroIndexedRecord) rec).getData()));
    return result;
  }
}
