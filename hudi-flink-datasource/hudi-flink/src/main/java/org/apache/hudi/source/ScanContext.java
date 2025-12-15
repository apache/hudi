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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.time.Duration;

/**
 * Hudi source scan context.
 */
@Internal
public class ScanContext implements Serializable {
  private final Configuration conf;
  private final Path path;
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

  public ScanContext(
      Configuration conf,
      Path path,
      RowType rowType,
      String startInstant,
      String endInstant,
      long maxCompactionMemoryInBytes,
      long maxPendingSplits,
      boolean skipCompaction,
      boolean skipClustering,
      boolean skipInsertOverwrite,
      boolean cdcEnabled) {
    this.conf = conf;
    this.path = path;
    this.rowType = rowType;
    this.startInstant = startInstant;
    this.endInstant = endInstant;
    this.maxCompactionMemoryInBytes = maxCompactionMemoryInBytes;
    this.maxPendingSplits = maxPendingSplits;
    this.skipCompaction = skipCompaction;
    this.skipClustering = skipClustering;
    this.skipInsertOverwrite = skipInsertOverwrite;
    this.cdcEnabled = cdcEnabled;
  }

  public Configuration getConf() {
    return conf;
  }

  public Path getPath() {
    return path;
  }

  public RowType getRowType() {
    return rowType;
  }

  public String getStartInstant() {
    return startInstant;
  }

  public String getEndInstant() {
    return endInstant;
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

  public boolean skipClustering() {
    return skipClustering;
  }

  public boolean skipInsertOverwrite() {
    return skipInsertOverwrite;
  }

  public boolean cdcEnabled() {
    return cdcEnabled;
  }

  public Duration getScanInterval() {
    return Duration.ofSeconds(conf.get(FlinkOptions.READ_STREAMING_CHECK_INTERVAL));
  }

  /**
   * Builder for {@link ScanContext}.
   */
  public static class Builder {
    private Configuration conf;
    private Path path;
    private RowType rowType;
    private String startInstant;
    private String endInstant;
    private long maxCompactionMemoryInBytes;
    private long maxPendingSplits;
    private boolean skipCompaction;
    private boolean skipClustering;
    private boolean skipInsertOverwrite;
    private boolean cdcEnabled;

    public Builder conf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public Builder path(Path path) {
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

    public ScanContext build() {
      return new ScanContext(
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
          cdcEnabled);
    }
  }
}
