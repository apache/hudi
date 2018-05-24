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

package com.uber.hoodie.io.compact;

import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.avro.model.HoodieCompactionPlan;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.table.HoodieTable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Set;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * A HoodieCompactor runs compaction on a hoodie table
 */
public interface HoodieCompactor extends Serializable {

  /**
   * Generate a new compaction plan for scheduling
   *
   * @param jsc                  Spark Context
   * @param hoodieTable          Hoodie Table
   * @param config               Hoodie Write Configuration
   * @param compactionCommitTime scheduled compaction commit time
   * @return Compaction Plan
   * @throws IOException when encountering errors
   */
  HoodieCompactionPlan generateCompactionPlan(JavaSparkContext jsc,
      HoodieTable hoodieTable, HoodieWriteConfig config, String compactionCommitTime,
      Set<String> fileIdsWithPendingCompactions)
      throws IOException;

  /**
   * Execute compaction operations and report back status
   */
  JavaRDD<WriteStatus> compact(JavaSparkContext jsc,
      HoodieCompactionPlan compactionPlan, HoodieTable hoodieTable, HoodieWriteConfig config,
      String compactionInstantTime) throws IOException;
}