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
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpgradeDowngradeException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Upgrade handle to assist in upgrading hoodie table from version 5 to 6.
 * Since we do not write/read compaction plan from .aux folder anyone, the
 * upgrade handler will delete compaction files from .aux folder.
 */
public class FiveToSixUpgradeHandler implements UpgradeHandler {

  private static final Logger LOG = LoggerFactory.getLogger(FiveToSixUpgradeHandler.class);

  @Override
  public UpgradeDowngrade.TableConfigChangeSet upgrade(HoodieWriteConfig config,
                                                                         HoodieEngineContext context,
                                                                         String instantTime,
                                                                         SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    final HoodieTable table = upgradeDowngradeHelper.getTable(config, context);

    deleteCompactionRequestedFileFromAuxiliaryFolder(table);

    return new UpgradeDowngrade.TableConfigChangeSet();
  }

  /**
   * See HUDI-6040.
   */
  private void deleteCompactionRequestedFileFromAuxiliaryFolder(HoodieTable table) {
    HoodieTableMetaClient metaClient = table.getMetaClient();
    HoodieTimeline compactionTimeline = metaClient.getActiveTimeline().filterPendingCompactionTimeline()
        .filter(instant -> instant.getState() == HoodieInstant.State.REQUESTED);
    InstantFileNameGenerator factory = table.getMetaClient().getInstantFileNameGenerator();

    compactionTimeline.getInstantsAsStream().forEach(
        deleteInstant -> {
          LOG.info("Deleting instant " + deleteInstant + " in auxiliary meta path " + metaClient.getMetaAuxiliaryPath());
          StoragePath metaFile = new StoragePath(metaClient.getMetaAuxiliaryPath(), factory.getFileName(deleteInstant));
          try {
            if (metaClient.getStorage().exists(metaFile)) {
              metaClient.getStorage().deleteFile(metaFile);
              LOG.info("Deleted instant file in auxiliary meta path : " + metaFile);
            }
          } catch (IOException e) {
            throw new HoodieUpgradeDowngradeException(HoodieTableVersion.FIVE.versionCode(), HoodieTableVersion.SIX.versionCode(), true, e);
          }
        }
    );
  }

}
