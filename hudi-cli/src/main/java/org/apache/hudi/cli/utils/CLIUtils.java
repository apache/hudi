/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.cli.utils;

import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.BaseHoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;

import static org.apache.hudi.cli.utils.CommitUtil.getTimeDaysAgo;
import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;
import static org.apache.hudi.common.util.StringUtils.nonEmpty;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/**
 * Utils class for cli commands.
 */
public class CLIUtils {
  /**
   * Gets a {@link HoodieTimeline} instance containing the instants in the specified range.
   *
   * @param startTs                 Start instant time.
   * @param endTs                   End instant time.
   * @param includeArchivedTimeline Whether to include intants from the archived timeline.
   * @return a {@link BaseHoodieTimeline} instance containing the instants in the specified range.
   */
  public static HoodieTimeline getTimelineInRange(String startTs, String endTs, boolean includeArchivedTimeline) {
    if (isNullOrEmpty(startTs)) {
      startTs = getTimeDaysAgo(10);
    }
    if (isNullOrEmpty(endTs)) {
      endTs = getTimeDaysAgo(1);
    }
    checkArgument(nonEmpty(startTs), "startTs is null or empty");
    checkArgument(nonEmpty(endTs), "endTs is null or empty");
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    if (includeArchivedTimeline) {
      HoodieArchivedTimeline archivedTimeline = metaClient.getArchivedTimeline();
      archivedTimeline.loadInstantDetailsInMemory(startTs, endTs);
      return archivedTimeline.findInstantsInRange(startTs, endTs).mergeTimeline(activeTimeline);
    }
    return activeTimeline;
  }

}
