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
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.common.util.queue.BoundedInMemoryQueueConsumer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.execution.LazyInsertIterable;
import org.apache.hudi.execution.SparkBoundedInMemoryExecutor;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class CommitActionExecutor<T extends HoodieRecordPayload<T>>
    extends BaseCommitActionExecutor<T> {

  private static final Logger LOG = LogManager.getLogger(CommitActionExecutor.class);

  public CommitActionExecutor(JavaSparkContext jsc,
      HoodieWriteConfig config, HoodieTable table,
      String instantTime, WriteOperationType operationType) {
    super(jsc, config, table, instantTime, operationType);
  }

  @Override
  public Iterator<List<WriteStatus>> handleUpdate(String partitionPath, String fileId,
      Iterator<HoodieRecord<T>> recordItr)
      throws IOException {
    // This is needed since sometimes some buckets are never picked in getPartition() and end up with 0 records
    if (!recordItr.hasNext()) {
      LOG.info("Empty partition with fileId => " + fileId);
      return Collections.singletonList((List<WriteStatus>) Collections.EMPTY_LIST).iterator();
    }
    // these are updates
    HoodieMergeHandle upsertHandle = getUpdateHandle(partitionPath, fileId, recordItr);
    return handleUpdateInternal(upsertHandle, fileId);
  }

  public Iterator<List<WriteStatus>> handleUpdate(String partitionPath, String fileId,
      Map<String, HoodieRecord<T>> keyToNewRecords, HoodieBaseFile oldDataFile) throws IOException {
    // these are updates
    HoodieMergeHandle upsertHandle = getUpdateHandle(partitionPath, fileId, keyToNewRecords, oldDataFile);
    return handleUpdateInternal(upsertHandle, fileId);
  }

  protected Iterator<List<WriteStatus>> handleUpdateInternal(HoodieMergeHandle upsertHandle, String fileId)
      throws IOException {
    if (upsertHandle.getOldFilePath() == null) {
      throw new HoodieUpsertException(
          "Error in finding the old file path at commit " + instantTime + " for fileId: " + fileId);
    } else {
      BoundedInMemoryExecutor<GenericRecord, GenericRecord, Void> wrapper = null;
      try {
        HoodieFileReader<IndexedRecord> storageReader =
            HoodieFileReaderFactory.getFileReader(table.getHadoopConf(), upsertHandle.getOldFilePath());
        wrapper =
            new SparkBoundedInMemoryExecutor(config, storageReader.getRecordIterator(upsertHandle.getWriterSchema()),
            new UpdateHandler(upsertHandle), x -> x);
        wrapper.execute();
      } catch (Exception e) {
        throw new HoodieException(e);
      } finally {
        upsertHandle.close();
        if (null != wrapper) {
          wrapper.shutdownNow();
        }
      }
    }

    // TODO(vc): This needs to be revisited
    if (upsertHandle.getWriteStatus().getPartitionPath() == null) {
      LOG.info("Upsert Handle has partition path as null " + upsertHandle.getOldFilePath() + ", "
          + upsertHandle.getWriteStatus());
    }
    return Collections.singletonList(Collections.singletonList(upsertHandle.getWriteStatus())).iterator();
  }

  protected HoodieMergeHandle getUpdateHandle(String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr) {
    return new HoodieMergeHandle<>(config, instantTime, (HoodieTable<T>)table, recordItr, partitionPath, fileId, sparkTaskContextSupplier);
  }

  protected HoodieMergeHandle getUpdateHandle(String partitionPath, String fileId,
      Map<String, HoodieRecord<T>> keyToNewRecords, HoodieBaseFile dataFileToBeMerged) {
    return new HoodieMergeHandle<>(config, instantTime, (HoodieTable<T>)table, keyToNewRecords,
        partitionPath, fileId, dataFileToBeMerged, sparkTaskContextSupplier);
  }

  @Override
  public Iterator<List<WriteStatus>> handleInsert(String idPfx, Iterator<HoodieRecord<T>> recordItr)
      throws Exception {
    // This is needed since sometimes some buckets are never picked in getPartition() and end up with 0 records
    if (!recordItr.hasNext()) {
      LOG.info("Empty partition");
      return Collections.singletonList((List<WriteStatus>) Collections.EMPTY_LIST).iterator();
    }
    return new LazyInsertIterable<>(recordItr, config, instantTime, (HoodieTable<T>)table, idPfx,
        sparkTaskContextSupplier);
  }

  @Override
  public Partitioner getUpsertPartitioner(WorkloadProfile profile) {
    if (profile == null) {
      throw new HoodieUpsertException("Need workload profile to construct the upsert partitioner.");
    }
    return new UpsertPartitioner(profile, jsc, table, config);
  }

  @Override
  public Partitioner getInsertPartitioner(WorkloadProfile profile) {
    return getUpsertPartitioner(profile);
  }

  /**
   * Consumer that dequeues records from queue and sends to Merge Handle.
   */
  private static class UpdateHandler extends BoundedInMemoryQueueConsumer<GenericRecord, Void> {

    private final HoodieMergeHandle upsertHandle;

    private UpdateHandler(HoodieMergeHandle upsertHandle) {
      this.upsertHandle = upsertHandle;
    }

    @Override
    protected void consumeOneRecord(GenericRecord record) {
      upsertHandle.write(record);
    }

    @Override
    protected void finish() {}

    @Override
    protected Void getResult() {
      return null;
    }
  }
}
