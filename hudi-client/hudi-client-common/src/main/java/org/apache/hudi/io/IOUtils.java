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

import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.util.Option;

import java.util.Properties;

import static org.apache.hudi.config.HoodieMemoryConfig.DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES;
import static org.apache.hudi.config.HoodieMemoryConfig.DEFAULT_MAX_MEMORY_FRACTION_FOR_COMPACTION;
import static org.apache.hudi.config.HoodieMemoryConfig.DEFAULT_MAX_MEMORY_FRACTION_FOR_MERGE;
import static org.apache.hudi.config.HoodieMemoryConfig.DEFAULT_MIN_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES;
import static org.apache.hudi.config.HoodieMemoryConfig.MAX_MEMORY_FOR_COMPACTION_PROP;
import static org.apache.hudi.config.HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE_PROP;
import static org.apache.hudi.config.HoodieMemoryConfig.MAX_MEMORY_FRACTION_FOR_COMPACTION_PROP;
import static org.apache.hudi.config.HoodieMemoryConfig.MAX_MEMORY_FRACTION_FOR_MERGE_PROP;

public class IOUtils {
  /**
   * Dynamic calculation of max memory to use for for spillable map. user.available.memory = executor.memory *
   * (1 - memory.fraction) spillable.available.memory = user.available.memory * hoodie.memory.fraction. Anytime
   * the engine memory fractions/total memory is changed, the memory used for spillable map changes
   * accordingly
   */
  public static long getMaxMemoryAllowedForMerge(TaskContextSupplier context, String maxMemoryFraction) {
    Option<String> totalMemoryOpt = context.getProperty(EngineProperty.TOTAL_MEMORY_AVAILABLE);
    Option<String> memoryFractionOpt = context.getProperty(EngineProperty.MEMORY_FRACTION_IN_USE);

    if (totalMemoryOpt.isPresent() && memoryFractionOpt.isPresent()) {
      long executorMemoryInBytes = Long.parseLong(totalMemoryOpt.get());
      double memoryFraction = Double.parseDouble(memoryFractionOpt.get());
      double maxMemoryFractionForMerge = Double.parseDouble(maxMemoryFraction);
      double userAvailableMemory = executorMemoryInBytes * (1 - memoryFraction);
      long maxMemoryForMerge = (long) Math.floor(userAvailableMemory * maxMemoryFractionForMerge);
      return Math.max(DEFAULT_MIN_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES, maxMemoryForMerge);
    } else {
      return DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES;
    }
  }

  public static long getMaxMemoryPerPartitionMerge(TaskContextSupplier context, Properties properties) {
    if (properties.containsKey(MAX_MEMORY_FOR_MERGE_PROP)) {
      return Long.parseLong(properties.getProperty(MAX_MEMORY_FOR_MERGE_PROP));
    }
    String fraction = properties.getProperty(MAX_MEMORY_FRACTION_FOR_MERGE_PROP, DEFAULT_MAX_MEMORY_FRACTION_FOR_MERGE);
    return getMaxMemoryAllowedForMerge(context, fraction);
  }

  public static long getMaxMemoryPerCompaction(TaskContextSupplier context, Properties properties) {
    if (properties.containsKey(MAX_MEMORY_FOR_COMPACTION_PROP)) {
      return Long.parseLong(properties.getProperty(MAX_MEMORY_FOR_COMPACTION_PROP));
    }
    String fraction = properties.getProperty(MAX_MEMORY_FRACTION_FOR_COMPACTION_PROP, DEFAULT_MAX_MEMORY_FRACTION_FOR_COMPACTION);
    return getMaxMemoryAllowedForMerge(context, fraction);
  }
}
