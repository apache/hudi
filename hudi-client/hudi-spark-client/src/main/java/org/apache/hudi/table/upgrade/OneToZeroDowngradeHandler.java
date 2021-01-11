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

package org.apache.hudi.table.upgrade;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.MarkerFiles;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Downgrade handle to assist in downgrading hoodie table from version 1 to 0.
 */
public  class OneToZeroDowngradeHandler implements DowngradeHandler {

  @Override
  public void downgrade(HoodieWriteConfig config, HoodieEngineContext context, String instantTime) {
    // fetch pending commit info
    HoodieSparkTable table = HoodieSparkTable.create(config, context);
    HoodieTimeline inflightTimeline = table.getMetaClient().getCommitsTimeline().filterPendingExcludingCompaction();
    List<HoodieInstant> commits = inflightTimeline.getReverseOrderedInstants().collect(Collectors.toList());
    for (HoodieInstant commitInstant : commits) {
      // delete existing marker files
      MarkerFiles markerFiles = new MarkerFiles(table, commitInstant.getTimestamp());
      markerFiles.quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());
    }
  }
}
