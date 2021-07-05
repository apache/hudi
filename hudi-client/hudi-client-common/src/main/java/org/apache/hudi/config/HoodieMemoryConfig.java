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

package org.apache.hudi.config;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Memory related config.
 */
@Immutable
public class HoodieMemoryConfig extends HoodieConfig {

  // Default max memory fraction during hash-merge, excess spills to disk
  public static final ConfigProperty<String> MAX_MEMORY_FRACTION_FOR_MERGE_PROP = ConfigProperty
      .key("hoodie.memory.merge.fraction")
      .defaultValue(String.valueOf(0.6))
      .withDocumentation("This fraction is multiplied with the user memory fraction (1 - spark.memory.fraction) "
          + "to get a final fraction of heap space to use during merge");

  // Default max memory fraction during compaction, excess spills to disk
  public static final ConfigProperty<String> MAX_MEMORY_FRACTION_FOR_COMPACTION_PROP = ConfigProperty
      .key("hoodie.memory.compaction.fraction")
      .defaultValue(String.valueOf(0.6))
      .withDocumentation("HoodieCompactedLogScanner reads logblocks, converts records to HoodieRecords and then "
          + "merges these log blocks and records. At any point, the number of entries in a log block can be "
          + "less than or equal to the number of entries in the corresponding parquet file. This can lead to "
          + "OOM in the Scanner. Hence, a spillable map helps alleviate the memory pressure. Use this config to "
          + "set the max allowable inMemory footprint of the spillable map");

  // Default memory size (1GB) per compaction (used if SparkEnv is absent), excess spills to disk
  public static final long DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES = 1024 * 1024 * 1024L;
  // Minimum memory size (100MB) for the spillable map.
  public static final long DEFAULT_MIN_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES = 100 * 1024 * 1024L;

  public static final ConfigProperty<Long> MAX_MEMORY_FOR_MERGE_PROP = ConfigProperty
      .key("hoodie.memory.merge.max.size")
      .defaultValue(DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES)
      .withDocumentation("Property to set the max memory for merge");

  public static final ConfigProperty<String> MAX_MEMORY_FOR_COMPACTION_PROP = ConfigProperty
      .key("hoodie.memory.compaction.max.size")
      .noDefaultValue()
      .withDocumentation("Property to set the max memory for compaction");

  public static final ConfigProperty<Integer> MAX_DFS_STREAM_BUFFER_SIZE_PROP = ConfigProperty
      .key("hoodie.memory.dfs.buffer.max.size")
      .defaultValue(16 * 1024 * 1024)
      .withDocumentation("Property to set the max memory for dfs inputstream buffer size");

  public static final ConfigProperty<String> SPILLABLE_MAP_BASE_PATH_PROP = ConfigProperty
      .key("hoodie.memory.spillable.map.path")
      .defaultValue("/tmp/")
      .withDocumentation("Default file path prefix for spillable file");

  public static final ConfigProperty<Double> WRITESTATUS_FAILURE_FRACTION_PROP = ConfigProperty
      .key("hoodie.memory.writestatus.failure.fraction")
      .defaultValue(0.1)
      .withDocumentation("Property to control how what fraction of the failed record, exceptions we report back to driver. "
          + "Default is 10%. If set to 100%, with lot of failures, this can cause memory pressure, cause OOMs and "
          + "mask actual data errors.");

  private HoodieMemoryConfig() {
    super();
  }

  public static HoodieMemoryConfig.Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private final HoodieMemoryConfig memoryConfig = new HoodieMemoryConfig();

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.memoryConfig.getProps().load(reader);
        return this;
      }
    }

    public Builder fromProperties(Properties props) {
      this.memoryConfig.getProps().putAll(props);
      return this;
    }

    public Builder withMaxMemoryFractionPerPartitionMerge(double maxMemoryFractionPerPartitionMerge) {
      memoryConfig.setValue(MAX_MEMORY_FRACTION_FOR_MERGE_PROP, String.valueOf(maxMemoryFractionPerPartitionMerge));
      return this;
    }

    public Builder withMaxMemoryMaxSize(long mergeMaxSize, long compactionMaxSize) {
      memoryConfig.setValue(MAX_MEMORY_FOR_MERGE_PROP, String.valueOf(mergeMaxSize));
      memoryConfig.setValue(MAX_MEMORY_FOR_COMPACTION_PROP, String.valueOf(compactionMaxSize));
      return this;
    }

    public Builder withMaxMemoryFractionPerCompaction(double maxMemoryFractionPerCompaction) {
      memoryConfig.setValue(MAX_MEMORY_FRACTION_FOR_COMPACTION_PROP, String.valueOf(maxMemoryFractionPerCompaction));
      return this;
    }

    public Builder withMaxDFSStreamBufferSize(int maxStreamBufferSize) {
      memoryConfig.setValue(MAX_DFS_STREAM_BUFFER_SIZE_PROP, String.valueOf(maxStreamBufferSize));
      return this;
    }

    public Builder withWriteStatusFailureFraction(double failureFraction) {
      memoryConfig.setValue(WRITESTATUS_FAILURE_FRACTION_PROP, String.valueOf(failureFraction));
      return this;
    }

    public HoodieMemoryConfig build() {
      memoryConfig.setDefaults(HoodieMemoryConfig.class.getName());
      return memoryConfig;
    }
  }
}
