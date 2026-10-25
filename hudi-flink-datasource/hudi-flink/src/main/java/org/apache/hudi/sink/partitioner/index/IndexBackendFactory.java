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

import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.utils.StateTtlConfigUtils;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;

/**
 * Factory to create a {@link GlobalIndexBackend} based on the configured index type.
 */
public class IndexBackendFactory {
  /**
   * Creates the global index backend used by the legacy bucket assign function.
   *
   * <p>Flink state index stores locations in keyed state. Global RLI either uses the bootstrap
   * RocksDB cache or a metadata-table-backed backend with checkpoint-aware cache eviction.
   *
   * @param conf Flink write configuration
   * @param context Flink function initialization context
   * @return global index backend for record-key lookups
   */
  public static GlobalIndexBackend create(
          Configuration conf,
          FunctionInitializationContext context) throws Exception {
    HoodieIndex.IndexType indexType = OptionsResolver.getIndexType(conf);
    switch (indexType) {
      case FLINK_STATE:
        ValueStateDescriptor<HoodieRecordGlobalLocation> indexStateDesc =
            new ValueStateDescriptor<>(
                "indexState",
                TypeInformation.of(HoodieRecordGlobalLocation.class));
        double ttl = conf.get(FlinkOptions.INDEX_STATE_TTL) * 24 * 60 * 60 * 1000;
        if (ttl > 0) {
          indexStateDesc.enableTimeToLive(StateTtlConfigUtils.createTtlConfig((long) ttl));
        }
        ValueState<HoodieRecordGlobalLocation> indexState = context.getKeyedStateStore().getState(indexStateDesc);
        ValidationUtils.checkArgument(indexState != null, "indexState should not be null when using FLINK_STATE index!");
        return new FlinkStateIndexBackend(indexState);
      case GLOBAL_RECORD_LEVEL_INDEX:
        if (conf.get(FlinkOptions.INDEX_BOOTSTRAP_ENABLED)) {
          return new RocksDBIndexBackend(conf.get(FlinkOptions.INDEX_BOOTSTRAP_ROCKSDB_PATH), OptionsResolver.isPartitionedTable(conf));
        } else {
          // Match the writer's checkpoint ID so uncommitted index entries remain protected from eviction.
          long initCheckpointId = context.isRestored() ? context.getRestoredCheckpointId().orElse(-1L) : -1L;
          return new GlobalRecordLevelIndexBackend(conf, initCheckpointId);
        }
      default:
        throw new UnsupportedOperationException("Index type " + indexType + " is not supported for bucket assigning yet.");
    }
  }
}
