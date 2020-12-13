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

package org.apache.hudi;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CollectionUtils;

import org.apache.hadoop.fs.FileSystem;

import java.util.List;
import java.util.stream.Collectors;

/**
 * List of helpers to aid, construction of instanttime for read and write operations using datasource.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.STABLE)
public class HoodieDataSourceHelpers {

  /**
   * Checks if the Hoodie table has new data since given timestamp. This can be subsequently fed to an incremental
   * view read, to perform incremental processing.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  public static boolean hasNewCommits(FileSystem fs, String basePath, String commitTimestamp) {
    return listCommitsSince(fs, basePath, commitTimestamp).size() > 0;
  }

  /**
   * Get a list of instant times that have occurred, from the given instant timestamp.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  public static List<String> listCommitsSince(FileSystem fs, String basePath, String instantTimestamp) {
    HoodieTimeline timeline = allCompletedCommitsCompactions(fs, basePath);
    return timeline.findInstantsAfter(instantTimestamp, Integer.MAX_VALUE).getInstants()
        .map(HoodieInstant::getTimestamp).collect(Collectors.toList());
  }

  /**
   * Returns the last successful write operation's instant time.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  public static String latestCommit(FileSystem fs, String basePath) {
    HoodieTimeline timeline = allCompletedCommitsCompactions(fs, basePath);
    return timeline.lastInstant().get().getTimestamp();
  }

  /**
   * Obtain all the commits, compactions that have occurred on the timeline, whose instant times could be fed into the
   * datasource options.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  public static HoodieTimeline allCompletedCommitsCompactions(FileSystem fs, String basePath) {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs.getConf(), basePath, true);
    if (metaClient.getTableType().equals(HoodieTableType.MERGE_ON_READ)) {
      return metaClient.getActiveTimeline().getTimelineOfActions(
          CollectionUtils.createSet(HoodieActiveTimeline.COMMIT_ACTION,
              HoodieActiveTimeline.DELTA_COMMIT_ACTION,
              HoodieActiveTimeline.REPLACE_COMMIT_ACTION)).filterCompletedInstants();
    } else {
      return metaClient.getCommitTimeline().filterCompletedInstants();
    }
  }
}
