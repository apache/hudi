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
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.table.timeline.HoodieInstant.UNDERSCORE;


/**
 * Version 7 is going to be placeholder version for bridge release 0.16.0.
 * Version 8 is the placeholder version to track 1.x.
 */
public class EightToSevenDowngradeHandler implements DowngradeHandler {

  private static final Logger LOG = LoggerFactory.getLogger(EightToSevenDowngradeHandler.class);

  @Override
  public Map<ConfigProperty, String> downgrade(HoodieWriteConfig config, HoodieEngineContext context, String instantTime, SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    final HoodieTable table = upgradeDowngradeHelper.getTable(config, context);
    UpgradeDowngradeUtils.runCompaction(table, context, config, upgradeDowngradeHelper);
    UpgradeDowngradeUtils.syncCompactionRequestedFileToAuxiliaryFolder(table);

    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(context.getStorageConf().newInstance()).setBasePath(config.getBasePath()).build();
    List<HoodieInstant> instants = metaClient.getActiveTimeline().getInstants();
    if (!instants.isEmpty()) {
      context.map(instants, instant -> {
        if (instant.getFileName().contains(UNDERSCORE)) {
          try {
            // Rename the metadata file name from the ${instant_time}_${completion_time}.action[.state] format in version 1.x to the ${instant_time}.action[.state] format in version 0.x.
            StoragePath fromPath = new StoragePath(metaClient.getMetaPath(), instant.getFileName());
            StoragePath toPath = new StoragePath(metaClient.getMetaPath(), instant.getFileName().replaceAll(UNDERSCORE + "\\d+", ""));
            boolean success = metaClient.getStorage().rename(fromPath, toPath);
            // TODO: We need to rename the action-related part of the metadata file name here when we bring separate action name for clustering/compaction in 1.x as well.
            if (!success) {
              throw new HoodieIOException("an error that occurred while renaming " + fromPath + " to: " + toPath);
            }
            return true;
          } catch (IOException e) {
            LOG.warn("Can not to complete the downgrade from version eight to version seven. The reason for failure is {}", e.getMessage());
          }
        }
        return false;
      }, instants.size());
    }
    return Collections.emptyMap();
  }
}
