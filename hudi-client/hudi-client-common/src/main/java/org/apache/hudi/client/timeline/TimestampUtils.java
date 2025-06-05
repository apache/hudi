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

package org.apache.hudi.client.timeline;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Either;
import org.apache.hudi.common.util.ValidationUtils;

public class TimestampUtils {

  public static void validateForLatestTimestamp(Either<HoodieTableMetaClient, HoodieActiveTimeline> metaClientOrActiveTimeline, boolean isMetadataTable, String instantTime) {
    // validate that the instant for which requested is about to be created is the latest in the timeline.
    if (!isMetadataTable) { // lets validate data table that timestamps are generated in monotically increasing order.
      HoodieActiveTimeline reloadedActiveTimeline = metaClientOrActiveTimeline.isLeft() ? metaClientOrActiveTimeline.asLeft().reloadActiveTimeline() : metaClientOrActiveTimeline.asRight();
      reloadedActiveTimeline.getWriteTimeline().lastInstant().ifPresent(entry -> {
        ValidationUtils.checkArgument(HoodieTimeline.compareTimestamps(entry.getTimestamp(), HoodieTimeline.LESSER_THAN_OR_EQUALS, instantTime),
            "Found later commit time " + entry + ", compared to the current instant " + instantTime + ", hence failing to create requested commit meta file");
      });
    }
  }
}
