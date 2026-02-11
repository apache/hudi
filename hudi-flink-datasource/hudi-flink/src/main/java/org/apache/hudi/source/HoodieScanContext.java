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

package org.apache.hudi.source;

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.source.prune.PartitionPruners;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.storage.StoragePath;

import java.io.Serializable;
import java.time.Duration;

/**
 * Hudi source scan context for finding completed commits for streaming and incremental read.
 */
@Internal
@Getter
@Builder
@AllArgsConstructor
public class HoodieScanContext implements Serializable {
  private final Configuration conf;
  private final StoragePath path;
  private final RowType rowType;
  private final String startInstant;
  private final String endInstant;
  private final long maxCompactionMemoryInBytes;
  // max pending splits that are not assigned in split provider
  private final long maxPendingSplits;
  // skip compaction
  private final boolean skipCompaction;
  // skip clustering
  private final boolean skipClustering;
  // skip insert overwrite
  private final boolean skipInsertOverwrite;
  // cdc enabled
  private final boolean cdcEnabled;
  // is streaming mode
  private final boolean isStreaming;
  // Partition pruner
  private final PartitionPruners.PartitionPruner partitionPruner;

  public Duration getScanInterval() {
    return Duration.ofSeconds(conf.get(FlinkOptions.READ_STREAMING_CHECK_INTERVAL));
  }

}
