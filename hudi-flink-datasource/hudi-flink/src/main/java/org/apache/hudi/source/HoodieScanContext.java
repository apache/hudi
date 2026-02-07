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
public class HoodieScanContext implements Serializable {
  private final Configuration conf;
  private final StoragePath path;
  private final RowType rowType;
  private final String startCommit;
  private final String endCommit;
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

  public HoodieScanContext(
      Configuration conf,
      StoragePath path,
      RowType rowType,
      String startCommit,
      String endCommit,
      long maxCompactionMemoryInBytes,
      long maxPendingSplits,
      boolean skipCompaction,
      boolean skipClustering,
      boolean skipInsertOverwrite,
      boolean cdcEnabled,
      boolean isStreaming,
      PartitionPruners.PartitionPruner partitionPruner) {
    this.conf = conf;
    this.path = path;
    this.rowType = rowType;
    this.startCommit = startCommit;
    this.endCommit = endCommit;
    this.maxCompactionMemoryInBytes = maxCompactionMemoryInBytes;
    this.maxPendingSplits = maxPendingSplits;
    this.skipCompaction = skipCompaction;
    this.skipClustering = skipClustering;
    this.skipInsertOverwrite = skipInsertOverwrite;
    this.cdcEnabled = cdcEnabled;
    this.isStreaming = isStreaming;
    this.partitionPruner = partitionPruner;
  }

  public Configuration getConf() {
    return conf;
  }

  public StoragePath getPath() {
    return path;
  }

  public RowType getRowType() {
    return rowType;
  }

  public String getStartCommit() {
    return startCommit;
  }

  public String getEndCommit() {
    return endCommit;
  }

  public long getMaxCompactionMemoryInBytes() {
    return maxCompactionMemoryInBytes;
  }

  public long getMaxPendingSplits() {
    return maxPendingSplits;
  }

  public boolean skipCompaction() {
    return skipCompaction;
  }

  public PartitionPruners.PartitionPruner partitionPruner() {
    return partitionPruner;
  }

  public boolean skipClustering() {
    return skipClustering;
  }

  public boolean skipInsertOverwrite() {
    return skipInsertOverwrite;
  }

  public boolean cdcEnabled() {
    return cdcEnabled;
  }

  public boolean isStreaming() {
    return isStreaming;
  }

  public Duration getScanInterval() {
    return Duration.ofSeconds(conf.get(FlinkOptions.READ_STREAMING_CHECK_INTERVAL));
  }

  /**
   * Builder for {@link HoodieScanContext}.
   */
  public static class Builder {
    private Configuration conf;
    private StoragePath path;
    private RowType rowType;
    private String startInstant;
    private String endInstant;
    private long maxCompactionMemoryInBytes;
    private long maxPendingSplits;
    private boolean skipCompaction;
    private boolean skipClustering;
    private boolean skipInsertOverwrite;
    private boolean cdcEnabled;
    private boolean isStreaming;
    private PartitionPruners.PartitionPruner partitionPruner;

    public Builder conf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public Builder path(StoragePath path) {
      this.path = path;
      return this;
    }

    public Builder rowType(RowType rowType) {
      this.rowType = rowType;
      return this;
    }

    public Builder startInstant(String startInstant) {
      this.startInstant = startInstant;
      return this;
    }

    public Builder endInstant(String endInstant) {
      this.endInstant = endInstant;
      return this;
    }

    public Builder maxCompactionMemoryInBytes(long maxCompactionMemoryInBytes) {
      this.maxCompactionMemoryInBytes = maxCompactionMemoryInBytes;
      return this;
    }

    public Builder maxPendingSplits(long maxPendingSplits) {
      this.maxPendingSplits = maxPendingSplits;
      return this;
    }

    public Builder skipCompaction(boolean skipCompaction) {
      this.skipCompaction = skipCompaction;
      return this;
    }

    public Builder skipClustering(boolean skipClustering) {
      this.skipClustering = skipClustering;
      return this;
    }

    public Builder skipInsertOverwrite(boolean skipInsertOverwrite) {
      this.skipInsertOverwrite = skipInsertOverwrite;
      return this;
    }

    public Builder cdcEnabled(boolean cdcEnabled) {
      this.cdcEnabled = cdcEnabled;
      return this;
    }

    public Builder isStreaming(boolean isStreaming) {
      this.isStreaming = isStreaming;
      return this;
    }

    public Builder partitionPruner(PartitionPruners.PartitionPruner partitionPruner) {
      this.partitionPruner = partitionPruner;
      return this;
    }

    public HoodieScanContext build() {
      return new HoodieScanContext(
          conf,
          path,
          rowType,
          startInstant,
          endInstant,
          maxCompactionMemoryInBytes,
          maxPendingSplits,
          skipCompaction,
          skipClustering,
          skipInsertOverwrite,
          cdcEnabled,
          isStreaming,
          partitionPruner);
    }
  }
}
