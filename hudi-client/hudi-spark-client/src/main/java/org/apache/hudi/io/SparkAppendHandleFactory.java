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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.util.CommonClientUtils;

import java.util.Iterator;

/**
 * Spark-specific append-handle factory for native logs.
 *
 * <p>It selects {@link SparkNativeLogAppendHandle} only after native logs are enabled. The handle then uses the
 * write config's record type to decide whether delete records must be represented as Spark rows or Avro records.</p>
 */
public class SparkAppendHandleFactory<T, I, K, O> extends AppendHandleFactory<T, I, K, O> {

  @Override
  public HoodieAppendHandle<T, I, K, O> create(HoodieWriteConfig hoodieConfig, String commitTime,
                                                HoodieTable<T, I, K, O> hoodieTable, String partitionPath,
                                                String fileIdPrefix, TaskContextSupplier taskContextSupplier) {
    String fileId = getNextFileId(fileIdPrefix);
    if (CommonClientUtils.shouldWriteNativeLogs(hoodieConfig)) {
      return new SparkNativeLogAppendHandle<>(hoodieConfig, commitTime, hoodieTable, partitionPath,
          fileId, taskContextSupplier);
    }
    return new HoodieInlineLogAppendHandle<>(hoodieConfig, commitTime, hoodieTable, partitionPath,
        fileId, taskContextSupplier);
  }

  @Override
  public HoodieAppendHandle<T, I, K, O> create(HoodieWriteConfig hoodieConfig, String commitTime,
                                                HoodieTable<T, I, K, O> hoodieTable, String partitionPath,
                                                String fileId, Iterator<HoodieRecord<T>> recordItr,
                                                TaskContextSupplier taskContextSupplier) {
    if (CommonClientUtils.shouldWriteNativeLogs(hoodieConfig)) {
      return new SparkNativeLogAppendHandle<>(hoodieConfig, commitTime, hoodieTable, partitionPath,
          fileId, recordItr, taskContextSupplier);
    }
    return new HoodieInlineLogAppendHandle<>(hoodieConfig, commitTime, hoodieTable, partitionPath,
        fileId, recordItr, taskContextSupplier);
  }
}
