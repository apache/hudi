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

package org.apache.hudi.index;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.HoodieMergedReadHandle;
import org.apache.hudi.table.HoodieTable;

public class HoodieIndexUtilsHelper {

  /**
   * Read existing records based on the given partition path and {@link HoodieRecordLocation} info.
   * <p>
   * This will perform merged read for MOR table, in case a FileGroup contains log files.
   *
   * @return {@link HoodieRecord}s that have the current location being set.
   */
  static <R> HoodieData<HoodieRecord<R>> getExistingRecords(
      HoodieData<Pair<String, String>> partitionLocations, HoodieWriteConfig config, HoodieTable hoodieTable) {
    final Option<String> instantTime = hoodieTable
        .getMetaClient()
        .getActiveTimeline() // we need to include all actions and completed
        .filterCompletedInstants()
        .lastInstant()
        .map(HoodieInstant::getTimestamp);
    return partitionLocations.flatMap(p
        -> new HoodieMergedReadHandle(config, instantTime, hoodieTable, Pair.of(p.getKey(), p.getValue()))
        .getMergedRecords().iterator());
  }
}
