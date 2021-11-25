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

package org.apache.hudi.table.action.archive;

import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ValidationUtils;

import java.util.stream.Stream;

import static org.apache.hudi.common.util.CollectionUtils.createSet;

public final class ArchiveUtils {

  public static Stream<HoodieInstant> getCleanRollbackInstantsToArchive(HoodieActiveTimeline activeTimeline, int minToKeep, int maxToKeep) {
    ValidationUtils.checkArgument(minToKeep < maxToKeep);
    HoodieTimeline cleanAndRollbackTimeline = activeTimeline
        .getTimelineOfActions(createSet(HoodieTimeline.CLEAN_ACTION, HoodieTimeline.ROLLBACK_ACTION))
        .filterCompletedInstants();
    int count = cleanAndRollbackTimeline.countInstants();
    if (count > maxToKeep) {
      return cleanAndRollbackTimeline.getInstants().limit(count - minToKeep);
    } else {
      return Stream.empty();
    }
  }
}
