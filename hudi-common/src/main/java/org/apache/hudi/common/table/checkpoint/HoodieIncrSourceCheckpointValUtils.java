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

package org.apache.hudi.common.table.checkpoint;

/**
 * Utility class providing methods to check if a string starts with specific resume-related prefixes.
 */
public class HoodieIncrSourceCheckpointValUtils {
  public static final String RESET_CHECKPOINT_V2_SEPARATOR = ":";
  public static final String REQUEST_TIME_PREFIX = "resumeFromInstantRequestTime";
  public static final String COMPLETION_TIME_PREFIX = "resumeFromInstantCompletionTime";

  /**
   * For hoodie incremental source ingestion, if the target table is version 8 or higher, the checkpoint
   * key set by streamer config can be in either of the following format:
   * - resumeFromInstantRequestTime:[checkpoint value based on request time]
   * - resumeFromInstantCompletionTime:[checkpoint value based on completion time]
   *
   * StreamerCheckpointV2FromCfgCkp class itself captured the fact that this is version 8 and higher, plus
   * the checkpoint source is from streamer config override.
   *
   * When the checkpoint is consumed by individual data sources, we need to convert them to either vanilla
   * checkpoint v1 (request time based) or checkpoint v2 (completion time based).
   */
  public static Checkpoint resolveToActualCheckpointVersion(UnresolvedStreamerCheckpointBasedOnCfg checkpoint) {
    String[] parts = extractKeyValues(checkpoint);
    switch (parts[0]) {
      case REQUEST_TIME_PREFIX: {
        return new StreamerCheckpointV1(checkpoint).setCheckpointKey(parts[1]);
      }
      case COMPLETION_TIME_PREFIX: {
        return new StreamerCheckpointV2(checkpoint).setCheckpointKey(parts[1]);
      }
      default:
        throw new IllegalArgumentException("Unknown event ordering mode " + parts[0]);
    }
  }

  private static String [] extractKeyValues(UnresolvedStreamerCheckpointBasedOnCfg checkpoint) {
    String checkpointKey = checkpoint.getCheckpointKey();
    String[] parts = checkpointKey.split(RESET_CHECKPOINT_V2_SEPARATOR);
    if (parts.length != 2
        || (
          !parts[0].trim().equals(REQUEST_TIME_PREFIX)
          && !parts[0].trim().equals(COMPLETION_TIME_PREFIX)
        )) {
      throw new IllegalArgumentException(
          "Illegal checkpoint key override `" + checkpointKey + "`. Valid format is either `resumeFromInstantRequestTime:<checkpoint value>` or "
          + "`resumeFromInstantCompletionTime:<checkpoint value>`.");
    }
    parts[0] = parts[0].trim();
    parts[1] = parts[1].trim();
    return parts;
  }
}