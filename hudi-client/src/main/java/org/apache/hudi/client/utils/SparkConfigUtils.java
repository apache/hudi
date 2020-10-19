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

package org.apache.hudi.client.utils;

import org.apache.hudi.config.HoodieIndexConfig;

import org.apache.spark.SparkEnv;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.Utils;

import java.util.Properties;

import static org.apache.hudi.config.HoodieMemoryConfig.DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES;
import static org.apache.hudi.config.HoodieMemoryConfig.DEFAULT_MAX_MEMORY_FRACTION_FOR_COMPACTION;
import static org.apache.hudi.config.HoodieMemoryConfig.DEFAULT_MAX_MEMORY_FRACTION_FOR_MERGE;
import static org.apache.hudi.config.HoodieMemoryConfig.DEFAULT_MIN_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES;
import static org.apache.hudi.config.HoodieMemoryConfig.MAX_MEMORY_FOR_COMPACTION_PROP;
import static org.apache.hudi.config.HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE_PROP;
import static org.apache.hudi.config.HoodieMemoryConfig.MAX_MEMORY_FRACTION_FOR_COMPACTION_PROP;
import static org.apache.hudi.config.HoodieMemoryConfig.MAX_MEMORY_FRACTION_FOR_MERGE_PROP;
import static org.apache.hudi.config.HoodieWriteConfig.WRITE_STATUS_STORAGE_LEVEL;

/**
 * Spark config utils.
 */
public class SparkConfigUtils {

  /**
   * Dynamic calculation of max memory to use for for spillable map. user.available.memory = spark.executor.memory *
   * (1 - spark.memory.fraction) spillable.available.memory = user.available.memory * hoodie.memory.fraction. Anytime
   * the spark.executor.memory or the spark.memory.fraction is changed, the memory used for spillable map changes
   * accordingly
   */
  public static long getMaxMemoryAllowedForMerge(String maxMemoryFraction) {
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
      double memoryFraction = Double.parseDouble(
          SparkEnv.get().conf().get(SPARK_EXECUTOR_MEMORY_FRACTION_PROP, DEFAULT_SPARK_EXECUTOR_MEMORY_FRACTION));
      double maxMemoryFractionForMerge = Double.parseDouble(maxMemoryFraction);
      double userAvailableMemory = executorMemoryInBytes * (1 - memoryFraction);
      long maxMemoryForMerge = (long) Math.floor(userAvailableMemory * maxMemoryFractionForMerge);
      return Math.max(DEFAULT_MIN_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES, maxMemoryForMerge);
    } else {
      return DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES;
    }
  }

  public static StorageLevel getWriteStatusStorageLevel(Properties properties) {
    return StorageLevel.fromString(properties.getProperty(WRITE_STATUS_STORAGE_LEVEL));
  }

  public static StorageLevel getBloomIndexInputStorageLevel(Properties properties) {
    return StorageLevel.fromString(properties.getProperty(HoodieIndexConfig.BLOOM_INDEX_INPUT_STORAGE_LEVEL));
  }

  public static long getMaxMemoryPerPartitionMerge(Properties properties) {
    if (properties.containsKey(MAX_MEMORY_FOR_MERGE_PROP)) {
      return Long.parseLong(properties.getProperty(MAX_MEMORY_FOR_MERGE_PROP));
    }
    String fraction = properties.getProperty(MAX_MEMORY_FRACTION_FOR_MERGE_PROP, DEFAULT_MAX_MEMORY_FRACTION_FOR_MERGE);
    return getMaxMemoryAllowedForMerge(fraction);
  }

  public static long getMaxMemoryPerCompaction(Properties properties) {
    if (properties.containsKey(MAX_MEMORY_FOR_COMPACTION_PROP)) {
      return Long.parseLong(properties.getProperty(MAX_MEMORY_FOR_COMPACTION_PROP));
    }
    String fraction = properties.getProperty(MAX_MEMORY_FRACTION_FOR_COMPACTION_PROP, DEFAULT_MAX_MEMORY_FRACTION_FOR_COMPACTION);
    return getMaxMemoryAllowedForMerge(fraction);
  }

  public static StorageLevel getSimpleIndexInputStorageLevel(Properties properties) {
    return StorageLevel.fromString(properties.getProperty(HoodieIndexConfig.SIMPLE_INDEX_INPUT_STORAGE_LEVEL));
  }
}
