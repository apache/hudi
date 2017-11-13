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

import com.uber.hoodie.common.model.HoodieCompactionMetadata;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.table.HoodieTable;
import java.io.Serializable;
import java.util.Date;
import java.util.Optional;

import org.apache.spark.api.java.JavaSparkContext;

/**
 * A HoodieCompactor runs compaction on a hoodie table
 */
public interface HoodieCompactor extends Serializable {

  /**
   * Compact the delta files with the data files
   */
  HoodieCompactionMetadata compact(JavaSparkContext jsc, final HoodieWriteConfig config,
      HoodieTable hoodieTable, String compactionCommitTime) throws Exception;


  // Helper methods
  default String startCompactionCommit(HoodieTable hoodieTable) {
    String commitTime = HoodieActiveTimeline.COMMIT_FORMATTER.format(new Date());
    HoodieActiveTimeline activeTimeline = hoodieTable.getActiveTimeline();
    activeTimeline
        .createInflight(new HoodieInstant(true, HoodieTimeline.COMPACTION_ACTION, commitTime));
    return commitTime;
  }
}
