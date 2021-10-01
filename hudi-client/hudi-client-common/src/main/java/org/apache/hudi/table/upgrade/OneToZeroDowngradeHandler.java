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

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OneToZeroDowngradeHandler implements DowngradeHandler {

  @Override
  public Map<ConfigProperty, String> downgrade(
      HoodieTableMetaClient metaClient, HoodieWriteConfig config, HoodieEngineContext context, String instantTime) {
    // fetch pending commit info
    HoodieTimeline inflightTimeline = metaClient.getCommitsTimeline().filterPendingExcludingCompaction();
    List<HoodieInstant> commits = inflightTimeline.getReverseOrderedInstants().collect(Collectors.toList());
    for (HoodieInstant inflightInstant : commits) {
      // delete existing markers
      WriteMarkers writeMarkers = WriteMarkersFactory.get(
          config.getMarkersType(), metaClient, config, context, inflightInstant.getTimestamp());
      writeMarkers.quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());
    }
    return Collections.EMPTY_MAP;
  }
}
