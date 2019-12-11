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

import org.apache.spark.SparkEnv;
import org.apache.spark.util.Utils;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Memory related config.
 */
@Immutable
public class HoodieMemoryConfig extends DefaultHoodieConfig {

  // This fraction is multiplied with the spark.memory.fraction to get a final fraction of heap space to use
  // during merge. This makes it easier to scale this value as one increases the spark.executor.memory
  public static final String MAX_MEMORY_FRACTION_FOR_MERGE_PROP = "hoodie.memory.merge.fraction";
  // Default max memory fraction during hash-merge, excess spills to disk
  public static final String DEFAULT_MAX_MEMORY_FRACTION_FOR_MERGE = String.valueOf(0.6);
  public static final String MAX_MEMORY_FRACTION_FOR_COMPACTION_PROP = "hoodie.memory.compaction.fraction";
  // Default max memory fraction during compaction, excess spills to disk
  public static final String DEFAULT_MAX_MEMORY_FRACTION_FOR_COMPACTION = String.valueOf(0.6);
  // Default memory size (1GB) per compaction (used if SparkEnv is absent), excess spills to disk
  public static final long DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES = 1024 * 1024 * 1024L;
  // Minimum memory size (100MB) for the spillable map.
  public static final long DEFAULT_MIN_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES = 100 * 1024 * 1024L;
  // Property to set the max memory for merge
  public static final String MAX_MEMORY_FOR_MERGE_PROP = "hoodie.memory.merge.max.size";
  // Property to set the max memory for compaction
  public static final String MAX_MEMORY_FOR_COMPACTION_PROP = "hoodie.memory.compaction.max.size";
  // Property to set the max memory for dfs inputstream buffer size
  public static final String MAX_DFS_STREAM_BUFFER_SIZE_PROP = "hoodie.memory.dfs.buffer.max.size";
  public static final int DEFAULT_MAX_DFS_STREAM_BUFFER_SIZE = 16 * 1024 * 1024; // 16MB
  public static final String SPILLABLE_MAP_BASE_PATH_PROP = "hoodie.memory.spillable.map.path";
  // Default file path prefix for spillable file
  public static final String DEFAULT_SPILLABLE_MAP_BASE_PATH = "/tmp/";

  // Property to control how what fraction of the failed record, exceptions we report back to driver.
  public static final String WRITESTATUS_FAILURE_FRACTION_PROP = "hoodie.memory.writestatus.failure.fraction";
  // Default is 10%. If set to 100%, with lot of failures, this can cause memory pressure, cause OOMs and
  // mask actual data errors.
  public static final double DEFAULT_WRITESTATUS_FAILURE_FRACTION = 0.1;

  private HoodieMemoryConfig(Properties props) {
    super(props);
  }

  public static HoodieMemoryConfig.Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private final Properties props = new Properties();

    public Builder fromFile(File propertiesFile) throws IOException {
      FileReader reader = new FileReader(propertiesFile);
      try {
        this.props.load(reader);
        return this;
      } finally {
        reader.close();
      }
    }

    public Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public Builder withMaxMemoryFractionPerPartitionMerge(double maxMemoryFractionPerPartitionMerge) {
      props.setProperty(MAX_MEMORY_FRACTION_FOR_MERGE_PROP, String.valueOf(maxMemoryFractionPerPartitionMerge));
      return this;
    }

    public Builder withMaxMemoryMaxSize(long mergeMaxSize, long compactionMaxSize) {
      props.setProperty(MAX_MEMORY_FOR_MERGE_PROP, String.valueOf(mergeMaxSize));
      props.setProperty(MAX_MEMORY_FOR_COMPACTION_PROP, String.valueOf(compactionMaxSize));
      return this;
    }

    public Builder withMaxMemoryFractionPerCompaction(double maxMemoryFractionPerCompaction) {
      props.setProperty(MAX_MEMORY_FRACTION_FOR_COMPACTION_PROP, String.valueOf(maxMemoryFractionPerCompaction));
      return this;
    }

    public Builder withMaxDFSStreamBufferSize(int maxStreamBufferSize) {
      props.setProperty(MAX_DFS_STREAM_BUFFER_SIZE_PROP, String.valueOf(maxStreamBufferSize));
      return this;
    }

    public Builder withWriteStatusFailureFraction(double failureFraction) {
      props.setProperty(WRITESTATUS_FAILURE_FRACTION_PROP, String.valueOf(failureFraction));
      return this;
    }

    /**
     * Dynamic calculation of max memory to use for for spillable map. user.available.memory = spark.executor.memory *
     * (1 - spark.memory.fraction) spillable.available.memory = user.available.memory * hoodie.memory.fraction. Anytime
     * the spark.executor.memory or the spark.memory.fraction is changed, the memory used for spillable map changes
     * accordingly
     */
    private long getMaxMemoryAllowedForMerge(String maxMemoryFraction) {
      final String SPARK_EXECUTOR_MEMORY_PROP = "spark.executor.memory";
      final String SPARK_EXECUTOR_MEMORY_FRACTION_PROP = "spark.memory.fraction";
      // This is hard-coded in spark code {@link
      // https://github.com/apache/spark/blob/576c43fb4226e4efa12189b41c3bc862019862c6/core/src/main/scala/org/apache/
      // spark/memory/UnifiedMemoryManager.scala#L231} so have to re-define this here
      final String DEFAULT_SPARK_EXECUTOR_MEMORY_FRACTION = "0.6";
      // This is hard-coded in spark code {@link
      // https://github.com/apache/spark/blob/576c43fb4226e4efa12189b41c3bc862019862c6/core/src/main/scala/org/apache/
      // spark/SparkContext.scala#L471} so have to re-define this here
      final String DEFAULT_SPARK_EXECUTOR_MEMORY_MB = "1024"; // in MB

      if (SparkEnv.get() != null) {
        // 1 GB is the default conf used by Spark, look at SparkContext.scala
        long executorMemoryInBytes = Utils.memoryStringToMb(
            SparkEnv.get().conf().get(SPARK_EXECUTOR_MEMORY_PROP, DEFAULT_SPARK_EXECUTOR_MEMORY_MB)) * 1024 * 1024L;
        // 0.6 is the default value used by Spark,
        // look at {@link
        // https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/SparkConf.scala#L507}
        double memoryFraction = Double.valueOf(
            SparkEnv.get().conf().get(SPARK_EXECUTOR_MEMORY_FRACTION_PROP, DEFAULT_SPARK_EXECUTOR_MEMORY_FRACTION));
        double maxMemoryFractionForMerge = Double.valueOf(maxMemoryFraction);
        double userAvailableMemory = executorMemoryInBytes * (1 - memoryFraction);
        long maxMemoryForMerge = (long) Math.floor(userAvailableMemory * maxMemoryFractionForMerge);
        return Math.max(DEFAULT_MIN_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES, maxMemoryForMerge);
      } else {
        return DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES;
      }
    }

    public HoodieMemoryConfig build() {
      HoodieMemoryConfig config = new HoodieMemoryConfig(props);
      setDefaultOnCondition(props, !props.containsKey(MAX_MEMORY_FRACTION_FOR_COMPACTION_PROP),
          MAX_MEMORY_FRACTION_FOR_COMPACTION_PROP, DEFAULT_MAX_MEMORY_FRACTION_FOR_COMPACTION);
      setDefaultOnCondition(props, !props.containsKey(MAX_MEMORY_FRACTION_FOR_MERGE_PROP),
          MAX_MEMORY_FRACTION_FOR_MERGE_PROP, DEFAULT_MAX_MEMORY_FRACTION_FOR_MERGE);
      setDefaultOnCondition(props, !props.containsKey(MAX_MEMORY_FOR_MERGE_PROP), MAX_MEMORY_FOR_MERGE_PROP,
          String.valueOf(getMaxMemoryAllowedForMerge(props.getProperty(MAX_MEMORY_FRACTION_FOR_MERGE_PROP))));
      setDefaultOnCondition(props, !props.containsKey(MAX_MEMORY_FOR_COMPACTION_PROP), MAX_MEMORY_FOR_COMPACTION_PROP,
          String.valueOf(getMaxMemoryAllowedForMerge(props.getProperty(MAX_MEMORY_FRACTION_FOR_COMPACTION_PROP))));
      setDefaultOnCondition(props, !props.containsKey(MAX_DFS_STREAM_BUFFER_SIZE_PROP), MAX_DFS_STREAM_BUFFER_SIZE_PROP,
          String.valueOf(DEFAULT_MAX_DFS_STREAM_BUFFER_SIZE));
      setDefaultOnCondition(props, !props.containsKey(SPILLABLE_MAP_BASE_PATH_PROP), SPILLABLE_MAP_BASE_PATH_PROP,
          DEFAULT_SPILLABLE_MAP_BASE_PATH);
      setDefaultOnCondition(props, !props.containsKey(WRITESTATUS_FAILURE_FRACTION_PROP),
          WRITESTATUS_FAILURE_FRACTION_PROP, String.valueOf(DEFAULT_WRITESTATUS_FAILURE_FRACTION));
      return config;
    }
  }
}
