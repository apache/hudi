/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieData;
import org.apache.hudi.data.HoodieListData;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.execution.FlinkLazyInsertIterable;
import org.apache.hudi.io.ExplicitWriteHandleFactory;
import org.apache.hudi.io.HoodieCreateHandle;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.io.HoodieWriteHandle;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class FlinkCommitHelper<T extends HoodieRecordPayload<T>> extends BaseCommitHelper<T> {

  private static final Logger LOG = LogManager.getLogger(FlinkCommitHelper.class);
  protected HoodieWriteHandle<?, ?, ?, ?> writeHandle;

  public FlinkCommitHelper(
      HoodieEngineContext context, HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      HoodieWriteConfig config, HoodieTable table,
      String instantTime, WriteOperationType operationType) {
    this(context, writeHandle, config, table, instantTime, operationType, Option.empty());
  }

  public FlinkCommitHelper(
      HoodieEngineContext context, HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      HoodieWriteConfig config, HoodieTable table,
      String instantTime, WriteOperationType operationType,
      Option<Map<String, String>> extraMetadata) {
    super(context, config, table, instantTime, operationType, extraMetadata);
    this.writeHandle = writeHandle;
  }

  public static <U> List<U> getList(HoodieData<U> hoodieData) {
    return ((HoodieListData<U>) hoodieData).get();
  }

  @Override
  public void init(HoodieWriteConfig config) {
    // No OP
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> execute(HoodieData<HoodieRecord<T>> inputRecords) {
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = new HoodieWriteMetadata<>();

    List<WriteStatus> writeStatuses = new LinkedList<>();
    final HoodieRecord<?> record = getList(inputRecords).get(0);
    final String partitionPath = record.getPartitionPath();
    final String fileId = record.getCurrentLocation().getFileId();
    final BucketType bucketType = record.getCurrentLocation().getInstantTime().equals("I")
        ? BucketType.INSERT
        : BucketType.UPDATE;
    handleUpsertPartition(
        instantTime,
        partitionPath,
        fileId,
        bucketType,
        getList(inputRecords).iterator())
        .forEachRemaining(writeStatuses::addAll);
    setUpWriteMetadata(writeStatuses, result);
    return result;
  }

  protected void setUpWriteMetadata(
      List<WriteStatus> statuses,
      HoodieWriteMetadata<HoodieData<WriteStatus>> result) {
    // No need to update the index because the update happens before the write.
    result.setWriteStatuses(HoodieListData.of(statuses));
    result.setIndexUpdateDuration(Duration.ZERO);
  }

  protected Iterator<List<WriteStatus>> handleUpsertPartition(
      String instantTime,
      String partitionPath,
      String fileIdHint,
      BucketType bucketType,
      Iterator recordItr) {
    try {
      if (this.writeHandle instanceof HoodieCreateHandle) {
        // During one checkpoint interval, an insert record could also be updated,
        // for example, for an operation sequence of a record:
        //    I, U,   | U, U
        // - batch1 - | - batch2 -
        // the first batch(batch1) operation triggers an INSERT bucket,
        // the second batch batch2 tries to reuse the same bucket
        // and append instead of UPDATE.
        return handleInsert(fileIdHint, recordItr);
      } else if (this.writeHandle instanceof HoodieMergeHandle) {
        return handleUpdate(partitionPath, fileIdHint, recordItr);
      } else {
        switch (bucketType) {
          case INSERT:
            return handleInsert(fileIdHint, recordItr);
          case UPDATE:
            return handleUpdate(partitionPath, fileIdHint, recordItr);
          default:
            throw new AssertionError();
        }
      }
    } catch (Throwable t) {
      String msg = "Error upsetting bucketType " + bucketType + " for partition :" + partitionPath;
      LOG.error(msg, t);
      throw new HoodieUpsertException(msg, t);
    }
  }

  @Override
  public Iterator<List<WriteStatus>> handleInsert(String idPfx, Iterator<HoodieRecord<T>> recordItr) throws Exception {
    // This is needed since sometimes some buckets are never picked in getPartition() and end up with 0 records
    if (!recordItr.hasNext()) {
      LOG.info("Empty partition");
      return Collections.singletonList((List<WriteStatus>) Collections.EMPTY_LIST).iterator();
    }
    return new FlinkLazyInsertIterable<>(recordItr, true, config, instantTime, table, idPfx,
        context.getTaskContextSupplier(), new ExplicitWriteHandleFactory<>(writeHandle));
  }

  @Override
  protected void syncTableMetadata() {
    // No OP
  }

  @Override
  protected HoodieMergeHandle getUpdateHandle(String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr) {
    return (HoodieMergeHandle<?, ?, ?, ?>) this.writeHandle;
  }

  @Override
  protected BaseMergeHelper getMergeHelper() {
    return FlinkMergeHelper.newInstance();
  }
}
