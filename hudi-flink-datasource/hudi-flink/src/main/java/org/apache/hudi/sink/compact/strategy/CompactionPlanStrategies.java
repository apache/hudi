/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.compact.strategy;

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.sink.compact.FlinkCompactionConfig;
import org.apache.hudi.util.CompactionUtil;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Factory clazz for CompactionPlanStrategy.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class CompactionPlanStrategies {

  public static CompactionPlanStrategy getStrategy(FlinkCompactionConfig config) {
    switch (config.compactionPlanSelectStrategy.toLowerCase(Locale.ROOT)) {
      case CompactionPlanStrategy.ALL:
        return HoodieTimeline::getInstants;
      case CompactionPlanStrategy.INSTANTS:
        return pendingCompactionTimeline -> {
          if (StringUtils.isNullOrEmpty(config.compactionPlanInstant)) {
            log.warn("None instant is selected");
            return Collections.emptyList();
          }
          List<String> instants = Arrays.asList(config.compactionPlanInstant.split(","));
          return pendingCompactionTimeline.getInstantsAsStream()
              .filter(instant -> instants.contains(instant.requestedTime()))
              .collect(Collectors.toList());
        };
      case CompactionPlanStrategy.NUM_INSTANTS:
        return pendingCompactionTimeline -> {
          List<HoodieInstant> pendingCompactionPlanInstants = pendingCompactionTimeline.getInstants();
          if (CompactionUtil.isLIFO(config.compactionSeq)) {
            Collections.reverse(pendingCompactionPlanInstants);
          }
          int range = Math.min(config.maxNumCompactionPlans, pendingCompactionPlanInstants.size());
          return pendingCompactionPlanInstants.subList(0, range);
        };
      default:
        throw new UnsupportedOperationException("Unknown compaction plan strategy: "
            + config.compactionPlanSelectStrategy
            + ", supported strategies:[num_instants,instants,all]");
    }
  }
}
