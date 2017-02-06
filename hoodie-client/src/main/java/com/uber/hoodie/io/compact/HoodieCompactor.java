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

import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.table.view.RealtimeTableView;
import com.uber.hoodie.config.HoodieWriteConfig;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Optional;

/**
 * A HoodieCompactor runs compaction on a hoodie table
 */
public interface HoodieCompactor extends Serializable {
    /**
     * Compact the delta files with the data files
     * @throws Exception
     */
    HoodieCompactionMetadata compact(JavaSparkContext jsc, final HoodieWriteConfig config,
        HoodieTableMetaClient metaClient, RealtimeTableView fsView,
        CompactionFilter compactionFilter) throws Exception;


    // Helper methods
    default String startCompactionCommit(HoodieTableMetaClient metaClient) {
        String commitTime = HoodieActiveTimeline.COMMIT_FORMATTER.format(new Date());
        HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
        activeTimeline
            .createInflight(new HoodieInstant(true, HoodieTimeline.COMPACTION_ACTION, commitTime));
        return commitTime;
    }
}
