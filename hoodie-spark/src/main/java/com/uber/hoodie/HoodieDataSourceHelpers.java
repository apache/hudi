/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie;

import com.google.common.collect.Sets;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileSystem;

/**
 * List of helpers to aid, construction of instanttime for read and write operations using
 * datasource
 */
public class HoodieDataSourceHelpers {

  /**
   * Checks if the Hoodie dataset has new data since given timestamp. This can be subsequently fed
   * to an incremental view read, to perform incremental processing.
   */
  public static boolean hasNewCommits(FileSystem fs, String basePath, String commitTimestamp) {
    return listCommitsSince(fs, basePath, commitTimestamp).size() > 0;
  }

  /**
   * Get a list of instant times that have occurred, from the given instant timestamp.
   */
  public static List<String> listCommitsSince(FileSystem fs, String basePath,
      String instantTimestamp) {
    HoodieTimeline timeline = allCompletedCommitsCompactions(fs, basePath);
    return timeline.findInstantsAfter(instantTimestamp, Integer.MAX_VALUE).getInstants()
        .map(HoodieInstant::getTimestamp).collect(Collectors.toList());
  }

  /**
   * Returns the last successful write operation's instant time
   */
  public static String latestCommit(FileSystem fs, String basePath) {
    HoodieTimeline timeline = allCompletedCommitsCompactions(fs, basePath);
    return timeline.lastInstant().get().getTimestamp();
  }

  /**
   * Obtain all the commits, compactions that have occurred on the timeline, whose instant times
   * could be fed into the datasource options.
   */
  public static HoodieTimeline allCompletedCommitsCompactions(FileSystem fs, String basePath) {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs.getConf(), basePath, true);
    if (metaClient.getTableType().equals(HoodieTableType.MERGE_ON_READ)) {
      return metaClient.getActiveTimeline().getTimelineOfActions(
          Sets.newHashSet(HoodieActiveTimeline.COMMIT_ACTION,
              HoodieActiveTimeline.DELTA_COMMIT_ACTION));
    } else {
      return metaClient.getCommitTimeline().filterCompletedInstants();
    }
  }
}
