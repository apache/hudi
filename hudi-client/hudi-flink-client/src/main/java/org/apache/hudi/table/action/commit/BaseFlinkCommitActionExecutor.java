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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.execution.FlinkLazyInsertIterable;
import org.apache.hudi.io.ExplicitWriteHandleFactory;
import org.apache.hudi.io.HoodieCreateHandle;
import org.apache.hudi.io.HoodieWriteMergeHandle;
import org.apache.hudi.io.HoodieWriteHandle;
import org.apache.hudi.io.IOUtils;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * With {@code org.apache.hudi.operator.partitioner.BucketAssigner}, each hoodie record
 * is tagged with a bucket ID (partition path + fileID) in streaming way. All the records consumed by this
 * executor should be tagged with bucket IDs and belong to one data bucket.
 *
 * <p>These bucket IDs make it possible to shuffle the records first by the bucket ID
 * (see org.apache.hudi.operator.partitioner.BucketAssignerFunction), and this executor
 * only needs to handle the data buffer that belongs to one data bucket once at a time. So there is no need to
 * partition the buffer.
 *
 * <p>Computing the records batch locations all at a time is a pressure to the engine,
 * we should avoid that in streaming system.
 */
public abstract class BaseFlinkCommitActionExecutor<T> extends
    BaseCommitActionExecutor<T, Iterator<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>, HoodieWriteMetadata> {

  private static final Logger LOG = LoggerFactory.getLogger(BaseFlinkCommitActionExecutor.class);

  protected HoodieWriteHandle<?, ?, ?, ?> writeHandle;

  protected final BucketInfo bucketInfo;

  public BaseFlinkCommitActionExecutor(HoodieEngineContext context,
                                       HoodieWriteHandle<?, ?, ?, ?> writeHandle,
                                       BucketInfo bucketInfo,
                                       HoodieWriteConfig config,
                                       HoodieTable table,
                                       String instantTime,
                                       WriteOperationType operationType) {
    this(context, writeHandle, bucketInfo, config, table, instantTime, operationType, Option.empty());
  }

  public BaseFlinkCommitActionExecutor(HoodieEngineContext context,
                                       HoodieWriteHandle<?, ?, ?, ?> writeHandle,
                                       BucketInfo bucketInfo,
                                       HoodieWriteConfig config,
                                       HoodieTable table,
                                       String instantTime,
                                       WriteOperationType operationType,
                                       Option extraMetadata) {
    super(context, config, table, instantTime, operationType, extraMetadata);
    this.bucketInfo = bucketInfo;
    this.writeHandle = writeHandle;
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> execute(Iterator<HoodieRecord<T>> inputRecords) {
    return execute(inputRecords, Option.empty());
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> execute(Iterator<HoodieRecord<T>> inputRecords, Option<HoodieTimer> sourceReadAndIndexTimer) {
    return execute(inputRecords, sourceReadAndIndexTimer, bucketInfo.getPartitionPath(), bucketInfo.getFileIdPrefix(), bucketInfo.getBucketType());
  }

  public HoodieWriteMetadata<List<WriteStatus>> execute(Iterator<HoodieRecord<T>> recordItr, BucketInfo bucketInfo) {
    return execute(recordItr, Option.empty(), bucketInfo.getPartitionPath(), bucketInfo.getFileIdPrefix(), bucketInfo.getBucketType());
  }

  private HoodieWriteMetadata<List<WriteStatus>> execute(
      Iterator<HoodieRecord<T>> recordItr,
      Option<HoodieTimer> sourceReadAndIndexTimer,
      String partitionPath,
      String fileId,
      BucketType bucketType) {
    HoodieWriteMetadata<List<WriteStatus>> result = new HoodieWriteMetadata<>();

    List<WriteStatus> writeStatuses = new LinkedList<>();
    handleUpsertPartition(
        partitionPath,
        fileId,
        bucketType,
        recordItr)
        .forEachRemaining(writeStatuses::addAll);
    setUpWriteMetadata(writeStatuses, result);
    return result;
  }

  protected void setUpWriteMetadata(
      List<WriteStatus> statuses,
      HoodieWriteMetadata<List<WriteStatus>> result) {
    // No need to update the index because the update happens before the write.
    result.setWriteStatuses(statuses);
    result.setIndexUpdateDuration(Duration.ZERO);
  }

  @Override
  protected String getCommitActionType() {
    return table.getMetaClient().getCommitActionType();
  }

  @Override
  protected void commit(HoodieWriteMetadata<List<WriteStatus>> result) {
    commit(result, result.getWriteStatuses().stream().map(WriteStatus::getStat).collect(Collectors.toList()));
  }

  protected void setCommitMetadata(HoodieWriteMetadata<List<WriteStatus>> result) {
    result.setCommitMetadata(Option.of(CommitUtils.buildMetadata(result.getWriteStatuses().stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        result.getPartitionToReplaceFileIds(),
        extraMetadata, operationType, getSchemaToStoreInCommit(), getCommitActionType())));
  }

  @SuppressWarnings("unchecked")
  protected Iterator<List<WriteStatus>> handleUpsertPartition(
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
      } else if (this.writeHandle instanceof HoodieWriteMergeHandle) {
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
      String msg = "Error upserting bucketType " + bucketType + " for partition :" + partitionPath;
      LOG.error(msg, t);
      throw new HoodieUpsertException(msg, t);
    }
  }

  @Override
  public Iterator<List<WriteStatus>> handleUpdate(String partitionPath, String fileId,
                                                  Iterator<HoodieRecord<T>> recordItr)
      throws IOException {
    // This is needed since sometimes some buckets are never picked in getPartition() and end up with 0 records
    HoodieWriteMergeHandle<?, ?, ?, ?> upsertHandle = (HoodieWriteMergeHandle<?, ?, ?, ?>) this.writeHandle;
    if (upsertHandle.isEmptyNewRecords() && !recordItr.hasNext()) {
      LOG.info("Empty partition with fileId => {}.", fileId);
      return Collections.singletonList((List<WriteStatus>) Collections.EMPTY_LIST).iterator();
    }
    // these are updates
    return IOUtils.runMerge(upsertHandle, instantTime, fileId);
  }

  @Override
  public Iterator<List<WriteStatus>> handleInsert(String idPfx, Iterator<HoodieRecord<T>> recordItr)
      throws Exception {
    // This is needed since sometimes some buckets are never picked in getPartition() and end up with 0 records
    if (!recordItr.hasNext()) {
      LOG.info("Empty partition");
      return Collections.singletonList((List<WriteStatus>) Collections.EMPTY_LIST).iterator();
    }
    return new FlinkLazyInsertIterable<>(recordItr, true, config, instantTime, table, idPfx,
        taskContextSupplier, new ExplicitWriteHandleFactory<>(writeHandle));
  }

  @Override
  protected void updateColumnsToIndexForColumnStats(HoodieTableMetaClient metaClient, List<String> columnsToIndex) {
    // no-op
  }
}
