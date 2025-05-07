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

package org.apache.hudi.table.action.commit.delta;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.FlinkAppendHandle;
import org.apache.hudi.io.HoodieWriteHandle;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.commit.BaseFlinkCommitActionExecutor;
import org.apache.hudi.table.action.commit.BucketInfo;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Flink RowData upsert delta commit action executor.
 */
public class FlinkUpsertDeltaCommitActionExecutor<T> extends BaseFlinkCommitActionExecutor<T> {

  private final Iterator<HoodieRecord<T>> inputRecords;

  public FlinkUpsertDeltaCommitActionExecutor(HoodieEngineContext context,
                                              HoodieWriteHandle<?, ?, ?, ?> writeHandle,
                                              BucketInfo bucketInfo,
                                              HoodieWriteConfig config,
                                              HoodieTable table,
                                              String instantTime,
                                              Iterator<HoodieRecord<T>> inputRecords) {
    super(context, writeHandle, bucketInfo, config, table, instantTime, WriteOperationType.UPSERT);
    this.inputRecords = inputRecords;
  }

  @Override
  public Iterator<List<WriteStatus>> handleUpdate(String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr) {
    return handleWrite();
  }

  @Override
  public Iterator<List<WriteStatus>> handleInsert(String idPfx, Iterator<HoodieRecord<T>> recordItr) {
    return handleWrite();
  }

  /**
   * When using RowData writing mode for MOR upsert path, same write logic is used for UPSERT and INSERT operation.
   */
  private Iterator<List<WriteStatus>> handleWrite() {
    FlinkAppendHandle logWriteHandle = (FlinkAppendHandle) writeHandle;
    logWriteHandle.doAppend();
    List<WriteStatus> writeStatuses = logWriteHandle.close();
    return Collections.singletonList(writeStatuses).iterator();
  }

  @Override
  public HoodieWriteMetadata execute() {
    // no need for index lookup duration from flink executor.
    // Instant lookupBegin = Instant.now();
    // Duration indexLookupDuration = Duration.between(lookupBegin, Instant.now());
    HoodieWriteMetadata result = this.execute(inputRecords, bucketInfo);
    result.setIndexLookupDuration(Duration.ZERO);
    return result;
  }
}
