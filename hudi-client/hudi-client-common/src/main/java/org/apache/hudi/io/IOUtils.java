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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieUpsertException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.config.HoodieMemoryConfig.DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES;
import static org.apache.hudi.common.config.HoodieMemoryConfig.DEFAULT_MIN_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES;
import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FOR_COMPACTION;
import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE;
import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FRACTION_FOR_COMPACTION;
import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FRACTION_FOR_MERGE;

public class IOUtils {
  private static final Logger LOG = LoggerFactory.getLogger(IOUtils.class);

  /**
   * Dynamic calculation of max memory to use for spillable map. There is always more than one task
   * running on an executor and each task maintains a spillable map.
   * user.available.memory = executor.memory * (1 - memory.fraction)
   * spillable.available.memory = user.available.memory * hoodie.memory.fraction / executor.cores.
   * Anytime the engine memory fractions/total memory is changed, the memory used for spillable map
   * changes accordingly.
   */
  public static long getMaxMemoryAllowedForMerge(TaskContextSupplier context, String maxMemoryFraction) {
    Option<String> totalMemoryOpt = context.getProperty(EngineProperty.TOTAL_MEMORY_AVAILABLE);
    Option<String> memoryFractionOpt = context.getProperty(EngineProperty.MEMORY_FRACTION_IN_USE);
    Option<String> totalCoresOpt = context.getProperty(EngineProperty.TOTAL_CORES_PER_EXECUTOR);

    if (totalMemoryOpt.isPresent() && memoryFractionOpt.isPresent() && totalCoresOpt.isPresent()) {
      long executorMemoryInBytes = Long.parseLong(totalMemoryOpt.get());
      double memoryFraction = Double.parseDouble(memoryFractionOpt.get());
      double maxMemoryFractionForMerge = Double.parseDouble(maxMemoryFraction);
      long executorCores = Long.parseLong(totalCoresOpt.get());
      Option<String> singleTaskCoresOpt = context.getProperty(EngineProperty.SINGLE_TASK_CORES);
      long executorTaskNum = executorCores;
      if (singleTaskCoresOpt.isPresent()) {
        executorTaskNum = executorCores / Long.parseLong(singleTaskCoresOpt.get());
      }
      double userAvailableMemory = executorMemoryInBytes * (1 - memoryFraction) / executorTaskNum;
      long maxMemoryForMerge = (long) Math.floor(userAvailableMemory * maxMemoryFractionForMerge);
      return Math.max(DEFAULT_MIN_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES, maxMemoryForMerge);
    } else {
      return DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES;
    }
  }

  public static long getMaxMemoryPerPartitionMerge(TaskContextSupplier context, HoodieConfig hoodieConfig) {
    if (hoodieConfig.contains(MAX_MEMORY_FOR_MERGE)) {
      return hoodieConfig.getLong(MAX_MEMORY_FOR_MERGE);
    }
    String fraction = hoodieConfig.getStringOrDefault(MAX_MEMORY_FRACTION_FOR_MERGE);
    return getMaxMemoryAllowedForMerge(context, fraction);
  }

  public static long getMaxMemoryPerCompaction(TaskContextSupplier context, HoodieConfig hoodieConfig) {
    if (hoodieConfig.contains(MAX_MEMORY_FOR_COMPACTION)) {
      return hoodieConfig.getLong(MAX_MEMORY_FOR_COMPACTION);
    }
    String fraction = hoodieConfig.getStringOrDefault(MAX_MEMORY_FRACTION_FOR_COMPACTION);
    return getMaxMemoryAllowedForMerge(context, fraction);
  }

  public static long getMaxMemoryPerCompaction(TaskContextSupplier context, Map<String, String> options) {
    if (options.containsKey(MAX_MEMORY_FOR_COMPACTION.key())) {
      return Long.parseLong(options.get(MAX_MEMORY_FOR_COMPACTION.key()));
    }
    String fraction = options.getOrDefault(MAX_MEMORY_FRACTION_FOR_COMPACTION.key(), MAX_MEMORY_FRACTION_FOR_COMPACTION.defaultValue());
    return getMaxMemoryAllowedForMerge(context, fraction);
  }

  /**
   * Triggers the merge action with given merge handle {@code HoodieMergeHandle}.
   *
   * <p>Note: it can be either regular write path merging
   * or compact merging based on impls of the {@link HoodieMergeHandle}.
   *
   * @param mergeHandle The merge handle
   * @param instantTime The instant time
   * @param fileId      The file ID
   *
   * @return the write status iterator
   */
  public static Iterator<List<WriteStatus>> runMerge(HoodieMergeHandle<?, ?, ?, ?> mergeHandle,
                                                     String instantTime,
                                                     String fileId) throws IOException {
    if (mergeHandle.getOldFilePath() == null) {
      throw new HoodieUpsertException(
          "Error in finding the old file path at commit " + instantTime + " for fileId: " + fileId);
    } else {
      mergeHandle.doMerge();
    }

    // TODO(vc): This needs to be revisited
    if (mergeHandle.getPartitionPath() == null) {
      LOG.info("Upsert Handle has partition path as null " + mergeHandle.getOldFilePath() + ", " + mergeHandle.getWriteStatuses());
    }

    mergeHandle.close();
    return Collections.singletonList(mergeHandle.getWriteStatuses()).iterator();
  }
}
