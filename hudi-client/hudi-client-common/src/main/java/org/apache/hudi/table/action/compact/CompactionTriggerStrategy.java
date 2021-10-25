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

import java.text.ParseException;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCompactionException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

@SuppressWarnings("checkstyle:LineLength")
public enum CompactionTriggerStrategy {

  // trigger compaction when reach N delta commits
  NUM_COMMITS {
    @Override
    public boolean isCompactable(
        HoodieWriteConfig config,
        Pair<Integer, String> latestDeltaCommitInfo,
        String instantTime) {

      boolean compactable = config.getInlineCompactDeltaCommitMax() <= latestDeltaCommitInfo.getLeft();
      if (compactable) {
        LOG.info(String.format("The delta commits >= %s, trigger compaction scheduler.",
            config.getInlineCompactDeltaCommitMax()));
      }
      return compactable;
    }
  },
  // trigger compaction when time elapsed > N seconds since last compaction
  TIME_ELAPSED {
    @Override
    public boolean isCompactable(
        HoodieWriteConfig config,
        Pair<Integer, String> latestDeltaCommitInfo,
        String instantTime) {

      long deltaSeconds = parsedToSeconds(instantTime) - parsedToSeconds(latestDeltaCommitInfo.getRight());
      boolean compactable = config.getInlineCompactDeltaSecondsMax() <= deltaSeconds;
      if (compactable) {
        LOG.info(String.format("The elapsed time >=%ss, trigger compaction scheduler.",
            config.getInlineCompactDeltaSecondsMax()));
      }
      return compactable;
    }
  },
  // trigger compaction when both NUM_COMMITS and TIME_ELAPSED are satisfied
  NUM_AND_TIME {
    @Override
    public boolean isCompactable(
        HoodieWriteConfig config,
        Pair<Integer, String> latestDeltaCommitInfo,
        String instantTime) {

      long deltaSeconds = parsedToSeconds(instantTime) - parsedToSeconds(latestDeltaCommitInfo.getRight());
      boolean compactableBasedNum = config.getInlineCompactDeltaCommitMax() <= latestDeltaCommitInfo.getLeft();
      boolean compactableBasedTime = config.getInlineCompactDeltaSecondsMax() <= deltaSeconds;
      boolean compactable = compactableBasedNum && compactableBasedTime;
      if (compactable) {
        LOG.info(String.format("The delta commits >= %s or elapsed_time >=%ss, trigger compaction scheduler.",
            config.getInlineCompactDeltaCommitMax(), config.getInlineCompactDeltaSecondsMax()));
      }
      return compactable;
    }
  },
  // trigger compaction when NUM_COMMITS or TIME_ELAPSED is satisfied
  NUM_OR_TIME {
    @Override
    public boolean isCompactable(
        HoodieWriteConfig config,
        Pair<Integer, String> latestDeltaCommitInfo,
        String instantTime) {

      long deltaSeconds = parsedToSeconds(instantTime) - parsedToSeconds(latestDeltaCommitInfo.getRight());
      boolean compactableBasedNum = config.getInlineCompactDeltaCommitMax() <= latestDeltaCommitInfo.getLeft();
      boolean compactableBasedTime = config.getInlineCompactDeltaSecondsMax() <= deltaSeconds;
      boolean compactable = compactableBasedNum || compactableBasedTime;
      if (compactable) {
        LOG.info(String.format("The delta commits >= %s and elapsed_time >=%ss, trigger compaction scheduler.",
            config.getInlineCompactDeltaCommitMax(), config.getInlineCompactDeltaSecondsMax()));
      }
      return compactable;
    }
  };

  /**
   * Judge if the strategy could fire based on the condition.
   * @param config HoodieWriteConfig
   * @param latestDeltaCommitInfo delta commits since last compaction and delta seconds since last compaction in
   *                              current judge action
   * @param instantTime current instant time.
   * @return true if it could be triggered, false else
   */
  public abstract boolean isCompactable(
      HoodieWriteConfig config,
      Pair<Integer, String> latestDeltaCommitInfo,
      String instantTime);

  private static Long parsedToSeconds(String time) {

    long timestamp;
    try {
      timestamp = HoodieActiveTimeline.parseDateFromInstantTime(time).getTime() / 1000;
    } catch (ParseException e) {
      throw new HoodieCompactionException(e.getMessage(), e);
    }
    return timestamp;
  }

  private static final Logger LOG = LogManager.getLogger(CompactionTriggerStrategy.class);
}
