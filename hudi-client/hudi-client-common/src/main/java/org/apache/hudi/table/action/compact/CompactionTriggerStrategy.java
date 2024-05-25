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

package org.apache.hudi.table.action.compact;

import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;

@EnumDescription("Controls when compaction is scheduled.")
public enum CompactionTriggerStrategy {

    // trigger compaction when reach N delta commits
    @EnumFieldDescription("triggers compaction when there are at least N delta commits after "
        + "last completed compaction.")
    NUM_COMMITS,

    // trigger compaction when reach N delta commits since last compaction request
    @EnumFieldDescription("triggers compaction when there are at least N delta commits after "
        + "last completed or requested compaction.")
    NUM_COMMITS_AFTER_LAST_REQUEST,

    // trigger compaction when time elapsed > N seconds since last compaction
    @EnumFieldDescription("triggers compaction after N seconds since last compaction.")
    TIME_ELAPSED,

    // trigger compaction when both NUM_COMMITS and TIME_ELAPSED are satisfied
    @EnumFieldDescription("triggers compaction when both there are at least N delta commits and "
        + "N seconds elapsed (both must be satisfied) after last completed compaction.")
    NUM_AND_TIME,

    // trigger compaction when NUM_COMMITS or TIME_ELAPSED is satisfied
    @EnumFieldDescription("triggers compaction when both there are at least N delta commits or "
        + "N seconds elapsed (either condition is satisfied) after last completed compaction.")
    NUM_OR_TIME
}
