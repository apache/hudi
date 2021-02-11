/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpgradeDowngradeException;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SixToFiveDowngradeHandler implements DowngradeHandler {
  @Override
  public Map<ConfigProperty, String> downgrade(HoodieWriteConfig config, HoodieEngineContext context, String instantTime, SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    HoodieTableMetaClient metaClient = upgradeDowngradeHelper.getTable(config, context).getMetaClient();
    HoodieTimeline versionTwoActiveTimeline = HoodieTableMetaClient.builder().setConf(metaClient.getHadoopConf()).setBasePath(config.getBasePath())
        .setLoadActiveTimelineOnLoad(true).setConsistencyGuardConfig(config.getConsistencyGuardConfig())
        .setLayoutVersion(Option.of(new TimelineLayoutVersion(TimelineLayoutVersion.VERSION_2))).build().getActiveTimeline();
    HoodieTimeline versionOneActiveTimeline = HoodieTableMetaClient.builder().setConf(metaClient.getHadoopConf()).setBasePath(config.getBasePath())
        .setLoadActiveTimelineOnLoad(true).setConsistencyGuardConfig(config.getConsistencyGuardConfig())
        .setLayoutVersion(Option.of(new TimelineLayoutVersion(TimelineLayoutVersion.VERSION_1))).build().getActiveTimeline();
    List<HoodieInstant> versionTwoInstants = versionTwoActiveTimeline.getInstants();
    List<HoodieInstant> versionOneInstants = versionOneActiveTimeline.getInstants();

    for (HoodieInstant instant : versionTwoInstants) {
      String versionOneFileName = instant.getFileName();
      String versionTwoFileName = versionOneInstants.stream().filter(s -> s.getTimestamp()
          .equals(instant.getTimestamp())).findFirst().get().getFileName();
      String metaPath = metaClient.getMetaPath();
      try {
        metaClient.getFs().rename(new Path(metaPath, versionTwoFileName), new Path(metaPath, versionOneFileName));
      } catch (IOException io) {
        throw new HoodieUpgradeDowngradeException("Unable to rename instant filename ", io);
      }
    }
    return Collections.EMPTY_MAP;
  }
}
