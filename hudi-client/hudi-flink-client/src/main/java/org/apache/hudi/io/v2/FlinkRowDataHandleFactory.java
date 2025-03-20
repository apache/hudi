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

package org.apache.hudi.io.v2;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.HoodieWriteHandle;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.commit.BucketInfo;

import org.apache.hadoop.fs.Path;

import java.util.Iterator;
import java.util.Map;

/**
 * Factory clazz for creating {@code RowData} write handles.
 */
public class FlinkRowDataHandleFactory {

  public static <T, I, K, O> Factory<T, I, K, O> getFactory(
      HoodieTableConfig tableConfig,
      HoodieWriteConfig writeConfig,
      boolean overwrite) {
    if (tableConfig.getTableType().equals(HoodieTableType.MERGE_ON_READ)) {
      return DeltaCommitRowDataHandleFactory.getInstance();
    }
    throw new HoodieException("Not supported yet for COW.");
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

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

  public interface Factory<T, I, K, O> {
    /**
     * Get or create a new {@code RowData} handle in order to reuse the file handles.
     *
     * <p>CAUTION: the method is not thread safe.
     *
     * @param bucketToHandles The existing write handles
     * @param bucketInfo      Bucket info for the records.
     * @param config          Write config
     * @param instantTime     The instant time
     * @param table           The table
     *
     * @return Existing write handle or create a new one
     */
    HoodieWriteHandle<?, ?, ?, ?> create(
        Map<String, Path> bucketToHandles,
        BucketInfo bucketInfo,
        HoodieWriteConfig config,
        String instantTime,
        HoodieTable<T, I, K, O> table,
        Iterator<HoodieRecord<T>> recordIterator
    );
  }
}
