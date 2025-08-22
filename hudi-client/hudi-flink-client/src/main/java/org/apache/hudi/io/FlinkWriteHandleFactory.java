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

import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.v2.RowDataLogWriteHandle;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.table.action.commit.BucketType;

import org.apache.hadoop.fs.Path;

import java.util.Iterator;
import java.util.Map;

import static org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToStoragePath;

/**
 * Factory clazz for flink write handles.
 */
public class FlinkWriteHandleFactory {

  /**
   * Returns the write handle factory with given write config.
   */
  public static <T, I, K, O> Factory<T, I, K, O> getFactory(
      HoodieTableConfig tableConfig,
      HoodieWriteConfig writeConfig,
      boolean overwrite) {
    if (overwrite) {
      return CommitWriteHandleFactory.getInstance();
    }
    if (writeConfig.allowDuplicateInserts()) {
      return ClusterWriteHandleFactory.getInstance();
    }
    if (tableConfig.getTableType().equals(HoodieTableType.MERGE_ON_READ)) {
      return HoodieTableMetadata.isMetadataTable(writeConfig.getBasePath())
          ? DeltaCommitWriteHandleFactory.getInstance() : DeltaCommitRowDataHandleFactory.getInstance();
    } else if (tableConfig.isCDCEnabled()) {
      return CdcWriteHandleFactory.getInstance();
    } else {
      return CommitWriteHandleFactory.getInstance();
    }
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

  public interface Factory<T, I, K, O> {
    /**
     * Get or create a new write handle in order to reuse the file handles.
     *
     * <p>CAUTION: the method is not thread safe.
     *
     * @param bucketToHandles The existing write handles
     * @param bucketInfo      Bucket info for the records.
     * @param config          Write config
     * @param instantTime     The instant time
     * @param table           The table
     * @param recordItr       Record iterator
     *
     * @return Existing write handle or create a new one
     */
    HoodieWriteHandle<?, ?, ?, ?> create(
        Map<String, Path> bucketToHandles,
        BucketInfo bucketInfo,
        HoodieWriteConfig config,
        String instantTime,
        HoodieTable<T, I, K, O> table,
        Iterator<HoodieRecord<T>> recordItr
    );
  }

  /**
   * Base clazz for commit write handle factory,
   * it encapsulates the handle switching logic: INSERT OR UPSERT.
   */
  private abstract static class BaseCommitWriteHandleFactory<T, I, K, O> implements Factory<T, I, K, O> {
    @Override
    public HoodieWriteHandle<?, ?, ?, ?> create(
        Map<String, Path> bucketToHandles,
        BucketInfo bucketInfo,
        HoodieWriteConfig config,
        String instantTime,
        HoodieTable<T, I, K, O> table,
        Iterator<HoodieRecord<T>> recordItr) {
      final String fileID = bucketInfo.getFileIdPrefix();
      final String partitionPath = bucketInfo.getPartitionPath();

      Path writePath = bucketToHandles.get(fileID);
      if (writePath != null) {
        HoodieWriteHandle<?, ?, ?, ?> writeHandle =
            createReplaceHandle(config, instantTime, table, recordItr, partitionPath, fileID, convertToStoragePath(writePath));
        bucketToHandles.put(fileID, new Path(((MiniBatchHandle) writeHandle).getWritePath().toUri())); // override with new replace handle
        return writeHandle;
      }

      final HoodieWriteHandle<?, ?, ?, ?> writeHandle;
      if (bucketInfo.getBucketType() == BucketType.INSERT) {
        writeHandle = new FlinkCreateHandle<>(config, instantTime, table, partitionPath,
            fileID, table.getTaskContextSupplier());
      } else {
        writeHandle = createMergeHandle(config, instantTime, table, recordItr, partitionPath, fileID);
      }
      bucketToHandles.put(fileID, new Path(((MiniBatchHandle) writeHandle).getWritePath().toUri()));
      return writeHandle;
    }

    protected abstract HoodieWriteHandle<?, ?, ?, ?> createReplaceHandle(
        HoodieWriteConfig config,
        String instantTime,
        HoodieTable<T, I, K, O> table,
        Iterator<HoodieRecord<T>> recordItr,
        String partitionPath,
        String fileId,
        StoragePath basePath);

    protected abstract HoodieWriteHandle<?, ?, ?, ?> createMergeHandle(
        HoodieWriteConfig config,
        String instantTime,
        HoodieTable<T, I, K, O> table,
        Iterator<HoodieRecord<T>> recordItr,
        String partitionPath,
        String fileId);
  }

  private static boolean isFileGroupReaderBasedHandle(HoodieWriteConfig writeConfig) {
    String mergeHandleClass = writeConfig.getMergeHandleClassName();
    return FileGroupReaderBasedMergeHandle.class.getName().equalsIgnoreCase(mergeHandleClass);
  }

  /**
   * Write handle factory for commit.
   */
  private static class CommitWriteHandleFactory<T, I, K, O>
      extends BaseCommitWriteHandleFactory<T, I, K, O> {
    private static final CommitWriteHandleFactory<?, ?, ?, ?> INSTANCE = new CommitWriteHandleFactory<>();

    @SuppressWarnings("unchecked")
    public static <T, I, K, O> CommitWriteHandleFactory<T, I, K, O> getInstance() {
      return (CommitWriteHandleFactory<T, I, K, O>) INSTANCE;
    }

    @Override
    protected HoodieWriteHandle<?, ?, ?, ?> createReplaceHandle(
        HoodieWriteConfig config,
        String instantTime,
        HoodieTable<T, I, K, O> table,
        Iterator<HoodieRecord<T>> recordItr,
        String partitionPath,
        String fileId,
        StoragePath basePath) {
      if (isFileGroupReaderBasedHandle(config)) {
        return new FlinkFileGroupReaderBasedMergeAndReplaceHandle<>(config, instantTime, table, recordItr, partitionPath, fileId,
            table.getTaskContextSupplier(), basePath);
      } else {
        return new FlinkMergeAndReplaceHandle<>(config, instantTime, table, recordItr, partitionPath, fileId,
            table.getTaskContextSupplier(), basePath);
      }
    }

    @Override
    protected HoodieWriteHandle<?, ?, ?, ?> createMergeHandle(
        HoodieWriteConfig config,
        String instantTime,
        HoodieTable<T, I, K, O> table,
        Iterator<HoodieRecord<T>> recordItr,
        String partitionPath,
        String fileId) {
      if (isFileGroupReaderBasedHandle(config)) {
        return new FlinkFileGroupReaderBasedMergeHandle<>(config, instantTime, table, recordItr, partitionPath,
            fileId, table.getTaskContextSupplier());
      } else {
        return new FlinkMergeHandle<>(config, instantTime, table, recordItr, partitionPath,
            fileId, table.getTaskContextSupplier());
      }
    }
  }

  /**
   * Write handle factory for inline clustering.
   */
  private static class ClusterWriteHandleFactory<T, I, K, O>
      extends BaseCommitWriteHandleFactory<T, I, K, O> {
    private static final ClusterWriteHandleFactory<?, ?, ?, ?> INSTANCE = new ClusterWriteHandleFactory<>();

    @SuppressWarnings("unchecked")
    public static <T, I, K, O> ClusterWriteHandleFactory<T, I, K, O> getInstance() {
      return (ClusterWriteHandleFactory<T, I, K, O>) INSTANCE;
    }

    @Override
    protected HoodieWriteHandle<?, ?, ?, ?> createReplaceHandle(
        HoodieWriteConfig config,
        String instantTime,
        HoodieTable<T, I, K, O> table,
        Iterator<HoodieRecord<T>> recordItr,
        String partitionPath,
        String fileId,
        StoragePath basePath) {
      return new FlinkConcatAndReplaceHandle<>(config, instantTime, table, recordItr, partitionPath, fileId,
          table.getTaskContextSupplier(), basePath);
    }

    @Override
    protected HoodieWriteHandle<?, ?, ?, ?> createMergeHandle(
        HoodieWriteConfig config,
        String instantTime,
        HoodieTable<T, I, K, O> table,
        Iterator<HoodieRecord<T>> recordItr,
        String partitionPath,
        String fileId) {
      return new FlinkConcatHandle<>(config, instantTime, table, recordItr, partitionPath,
          fileId, table.getTaskContextSupplier());
    }
  }

  /**
   * Write handle factory for commit, the write handle supports logging change logs.
   */
  private static class CdcWriteHandleFactory<T, I, K, O>
      extends BaseCommitWriteHandleFactory<T, I, K, O> {
    private static final CdcWriteHandleFactory<?, ?, ?, ?> INSTANCE = new CdcWriteHandleFactory<>();

    @SuppressWarnings("unchecked")
    public static <T, I, K, O> CdcWriteHandleFactory<T, I, K, O> getInstance() {
      return (CdcWriteHandleFactory<T, I, K, O>) INSTANCE;
    }

    @Override
    protected HoodieWriteHandle<?, ?, ?, ?> createReplaceHandle(
        HoodieWriteConfig config,
        String instantTime,
        HoodieTable<T, I, K, O> table,
        Iterator<HoodieRecord<T>> recordItr,
        String partitionPath,
        String fileId,
        StoragePath basePath) {
      if (isFileGroupReaderBasedHandle(config)) {
        return new FlinkFileGroupReaderBasedMergeAndReplaceHandle<>(config, instantTime, table, recordItr, partitionPath, fileId,
            table.getTaskContextSupplier(), basePath);
      } else {
        return new FlinkMergeAndReplaceHandleWithChangeLog<>(config, instantTime, table, recordItr, partitionPath, fileId,
            table.getTaskContextSupplier(), basePath);
      }
    }

    @Override
    protected HoodieWriteHandle<?, ?, ?, ?> createMergeHandle(
        HoodieWriteConfig config,
        String instantTime,
        HoodieTable<T, I, K, O> table,
        Iterator<HoodieRecord<T>> recordItr,
        String partitionPath,
        String fileId) {
      if (isFileGroupReaderBasedHandle(config)) {
        return new FlinkFileGroupReaderBasedMergeHandle<>(config, instantTime, table, recordItr, partitionPath,
            fileId, table.getTaskContextSupplier());
      } else {
        return new FlinkMergeHandleWithChangeLog<>(config, instantTime, table, recordItr, partitionPath,
            fileId, table.getTaskContextSupplier());
      }
    }
  }

  /**
   * Write handle factory for delta commit, currently only used by metadata writer {@code FlinkHoodieBackedTableMetadataWriter}.
   */
  private static class DeltaCommitWriteHandleFactory<T, I, K, O> implements Factory<T, I, K, O> {
    private static final DeltaCommitWriteHandleFactory<?, ?, ?, ?> INSTANCE = new DeltaCommitWriteHandleFactory<>();

    @SuppressWarnings("unchecked")
    public static <T, I, K, O> DeltaCommitWriteHandleFactory<T, I, K, O> getInstance() {
      return (DeltaCommitWriteHandleFactory<T, I, K, O>) INSTANCE;
    }

    @Override
    public HoodieWriteHandle<?, ?, ?, ?> create(
        Map<String, Path> bucketToHandles,
        BucketInfo bucketInfo,
        HoodieWriteConfig config,
        String instantTime,
        HoodieTable<T, I, K, O> table,
        Iterator<HoodieRecord<T>> recordItr) {
      final String fileID = bucketInfo.getFileIdPrefix();
      final String partitionPath = bucketInfo.getPartitionPath();
      final TaskContextSupplier contextSupplier = table.getTaskContextSupplier();
      return new FlinkAppendHandle<>(config, instantTime, table, partitionPath, fileID, bucketInfo.getBucketType(), recordItr, contextSupplier);
    }
  }

  /**
   * {@code RowData} write handle factory for delta commit.
   */
  private static class DeltaCommitRowDataHandleFactory<T, I, K, O> implements Factory<T, I, K, O> {
    private static final DeltaCommitRowDataHandleFactory<?, ?, ?, ?> INSTANCE = new DeltaCommitRowDataHandleFactory<>();

    @SuppressWarnings("unchecked")
    public static <T, I, K, O> DeltaCommitRowDataHandleFactory<T, I, K, O> getInstance() {
      return (DeltaCommitRowDataHandleFactory<T, I, K, O>) INSTANCE;
    }

    @Override
    public HoodieWriteHandle<?, ?, ?, ?> create(
        Map<String, Path> bucketToHandles,
        BucketInfo bucketInfo,
        HoodieWriteConfig config,
        String instantTime,
        HoodieTable<T, I, K, O> table,
        Iterator<HoodieRecord<T>> recordIterator) {
      return new RowDataLogWriteHandle<>(
          config,
          instantTime,
          table,
          recordIterator,
          bucketInfo.getFileIdPrefix(),
          bucketInfo.getPartitionPath(),
          bucketInfo.getBucketType(),
          table.getTaskContextSupplier());
    }
  }
}
