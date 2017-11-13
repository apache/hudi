/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.io.compact.strategy;

import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.io.compact.CompactionOperation;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Strategy for compaction. Pluggable implementation of define how compaction should be done. The
 * implementations of this interface can capture the relevant metrics to order and filter the final
 * list of compaction operation to run in a single compaction.
 *
 * Implementation of CompactionStrategy cannot hold any state. Difference instantiations can be
 * passed in every time
 *
 * @see com.uber.hoodie.io.compact.HoodieRealtimeTableCompactor
 * @see CompactionOperation
 */
public interface CompactionStrategy extends Serializable {

  /**
   * Callback hook when a CompactionOperation is created. Individual strategies can capture the
   * metrics they need to decide on the priority.
   *
   * @param dataFile - Base file to compact
   * @param partitionPath - Partition path
   * @param logFiles - List of log files to compact with the base file
   * @return Map[String, Object] - metrics captured
   */
  Map<String, Object> captureMetrics(HoodieDataFile dataFile, String partitionPath,
      List<HoodieLogFile> logFiles);

  /**
   * Order and Filter the list of compactions. Use the metrics captured with the captureMetrics to
   * order and filter out compactions
   *
   * @param writeConfig - HoodieWriteConfig - config for this compaction is passed in
   * @param operations - list of compactions collected
   * @return list of compactions to perform in this run
   */
  List<CompactionOperation> orderAndFilter(HoodieWriteConfig writeConfig,
      List<CompactionOperation> operations);
}
