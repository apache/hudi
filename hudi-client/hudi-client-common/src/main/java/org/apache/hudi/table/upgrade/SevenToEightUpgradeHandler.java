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
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
import org.apache.hudi.common.table.timeline.versioning.v1.ActiveTimelineV1;
import org.apache.hudi.common.table.timeline.versioning.v1.CommitMetadataSerDeV1;
import org.apache.hudi.common.table.timeline.versioning.v2.ActiveTimelineV2;
import org.apache.hudi.common.table.timeline.versioning.v2.CommitMetadataSerDeV2;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.table.timeline.HoodieInstant.UNDERSCORE;
import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.SIX_TO_EIGHT_TIMELINE_ACTION_MAP;
import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.renameTimelineV1InstantFileToV2Format;

/**
 * Version 7 is going to be placeholder version for bridge release 0.16.0.
 * Version 8 is the placeholder version to track 1.x.
 */
public class SevenToEightUpgradeHandler implements UpgradeHandler {

  private static final Logger LOG = LoggerFactory.getLogger(SevenToEightUpgradeHandler.class);

  @Override
  public Map<ConfigProperty, String> upgrade(HoodieWriteConfig config, HoodieEngineContext context,
                                             String instantTime, SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    HoodieTable table = upgradeDowngradeHelper.getTable(config, context);
    HoodieTableMetaClient metaClient = table.getMetaClient();
    HoodieTableConfig tableConfig = metaClient.getTableConfig();

    Map<ConfigProperty, String> tablePropsToAdd = new HashMap<>();
    tablePropsToAdd.put(HoodieTableConfig.TIMELINE_PATH, HoodieTableConfig.TIMELINE_PATH.defaultValue());
    upgradePartitionFields(config, tableConfig, tablePropsToAdd);

    // Handle timeline upgrade:
    //  - Rewrite instants in active timeline to new format
    //  - TODO: Convert archived timeline to new LSM timeline format. It will be handled in a separate PR.
    List<HoodieInstant> instants = new ArrayList<>();
    try {
      // We need to move all the instants - not just completed ones.
      instants = metaClient.scanHoodieInstantsFromFileSystem(metaClient.getTimelinePath(),
          ActiveTimelineV1.VALID_EXTENSIONS_IN_ACTIVE_TIMELINE, false);
    } catch (IOException ioe) {
      LOG.error("Failed to get instants from filesystem", ioe);
      throw new HoodieIOException("Failed to get instants from filesystem", ioe);
    }

    if (!instants.isEmpty()) {
      InstantFileNameGenerator instantFileNameGenerator = metaClient.getInstantFileNameGenerator();
      CommitMetadataSerDeV2 commitMetadataSerDeV2 = new CommitMetadataSerDeV2();
      CommitMetadataSerDeV1 commitMetadataSerDeV1 = new CommitMetadataSerDeV1();
      ActiveTimelineV2 activeTimelineV2 = new ActiveTimelineV2(metaClient);
      context.map(instants, instant -> {
        String originalFileName = instantFileNameGenerator.getFileName(instant);
        String replacedFileName = originalFileName;
        boolean isCompleted = instant.isCompleted();
        // Rename the metadata file name from the ${instant_time}.action[.state] format in version 0.x
        // to the ${instant_time}_${completion_time}.action[.state] format in version 1.x.
        if (isCompleted) {
          String completionTime = instant.getCompletionTime(); // this is the file modification time
          String startTime = instant.requestedTime();
          replacedFileName = replacedFileName.replace(startTime, startTime + UNDERSCORE + completionTime);
        }
        // Rename the action if necessary (e.g., REPLACE_COMMIT_ACTION to CLUSTERING_ACTION).
        // NOTE: New action names were only applied for pending instants. Completed instants do not have any change in action names.
        if (SIX_TO_EIGHT_TIMELINE_ACTION_MAP.containsKey(instant.getAction()) && !isCompleted) {
          replacedFileName = replacedFileName.replace(instant.getAction(), SIX_TO_EIGHT_TIMELINE_ACTION_MAP.get(instant.getAction()));
        }
        try {
          return renameTimelineV1InstantFileToV2Format(instant, metaClient, originalFileName, replacedFileName, commitMetadataSerDeV1, commitMetadataSerDeV2, activeTimelineV2);
        } catch (IOException e) {
          LOG.warn("Can not to complete the upgrade from version seven to version eight. The reason for failure is {}", e.getMessage());
        }
        return false;
      }, instants.size());
    }

    return tablePropsToAdd;
  }

  private static void upgradePartitionFields(HoodieWriteConfig config, HoodieTableConfig tableConfig, Map<ConfigProperty, String> tablePropsToAdd) {
    String keyGenerator = tableConfig.getKeyGeneratorClassName();
    String partitionPathField = config.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key());
    if (keyGenerator != null && partitionPathField != null
        && (keyGenerator.equals(KeyGeneratorType.CUSTOM.getClassName()) || keyGenerator.equals(KeyGeneratorType.CUSTOM_AVRO.getClassName()))) {
      tablePropsToAdd.put(HoodieTableConfig.PARTITION_FIELDS, partitionPathField);
    }
  }
}
