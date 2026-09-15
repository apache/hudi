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

package org.apache.hudi.sink.partitioner.index;

import org.apache.hudi.configuration.FlinkOptions;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;

/**
 * Factory to create a {@link PartitionedIndexBackend} used by the dynamic bucket assign function.
 */
@Slf4j
public class PartitionedIndexBackendFactory {
  private static final String ROCKSDB_BACKEND_TYPE = "rocksdb";

  /**
   * Creates the partitioned index backend used to look up and record {@code recordKey -> fileGroupId}
   * mappings for the partitioned record level index.
   *
   * @param conf Flink write configuration
   * @param isInsertOverwrite whether the write operation is an insert overwrite, in which case indexing is skipped
   * @param bootstrapFilter filter for deciding whether a bootstrapped RLI record belongs to this task,
   *                        used only by the metadata-table-backed backend
   * @return partitioned index backend for record-key lookups scoped to a data partition
   */
  public static PartitionedIndexBackend create(
      Configuration conf,
      boolean isInsertOverwrite,
      RecordLevelIndexBackend.BootstrapFilter bootstrapFilter) {
    if (isInsertOverwrite) {
      return new DummyPartitionedIndexBackend();
    }
    String backendType = conf.get(FlinkOptions.INDEX_RLI_BACKEND_TYPE);
    if (ROCKSDB_BACKEND_TYPE.equalsIgnoreCase(backendType)) {
      // TODO: RocksDBPartitionedIndexBackend does not yet bootstrap or fall back to the MDT-backed
      // record level index, so on a non-empty table (or after a task/job restart) it would miss
      // committed keys and mis-route them as inserts. Keep it unselectable until partition
      // bootstrap / on-demand MDT loading is implemented.
      log.warn("Backend type '{}' is not yet supported for selection; falling back to the metadata-table-backed "
          + "record level index backend.", backendType);
    }
    return new RecordLevelIndexBackend(conf, bootstrapFilter);
  }
}
