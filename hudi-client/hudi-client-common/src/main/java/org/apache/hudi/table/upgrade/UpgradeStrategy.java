/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.upgrade;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpgradeDowngradeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpgradeStrategy implements UpgradeDowngradeStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(UpgradeStrategy.class);
  private final HoodieTableMetaClient metaClient;
  private final HoodieWriteConfig config;

  public UpgradeStrategy(HoodieTableMetaClient metaClient,
                         HoodieWriteConfig config) {
    this.metaClient = metaClient;
    this.config = config;
  }

  @Override
  public boolean requiresMigration(HoodieTableVersion toWriteVersion) {
    HoodieTableVersion fromTableVersion = metaClient.getTableConfig().getTableVersion();
    boolean shouldUpgrade = true;
    if (fromTableVersion.greaterThanOrEquals(toWriteVersion)) {
      LOG.warn("Table version {} is greater than write version {}. No upgrade needed",
          fromTableVersion, toWriteVersion);
      shouldUpgrade = false;
    } else {
      if (fromTableVersion.lesserThan(HoodieTableVersion.SIX)) {
        throw new HoodieUpgradeDowngradeException(
            String.format("Hudi 1.x release only supports table version greater than "
                    + "version 6 or above. Please upgrade table from version %s to %s "
                    + "using a Hudi release prior to 1.0.0",
                fromTableVersion.versionCode(), HoodieTableVersion.SIX.versionCode()));
      }
      if (!config.autoUpgrade()) {
        shouldUpgrade = false;
      }
    }
    if (!shouldUpgrade && fromTableVersion != toWriteVersion) {
      if (!config.autoUpgrade()) {
        LOG.warn("Table version {} does not match write version {} and skip upgrade. "
                + "Setting hoodie.write.table.version={} to match 'hoodie.table.version'",
            fromTableVersion, toWriteVersion, fromTableVersion);
        config.setWriteVersion(fromTableVersion);
      } else {
        throw new HoodieUpgradeDowngradeException(String.format(
            "Table version %s is different from write version %s. Since we cannot do upgrade "
                + "in this case, we need to make table version and write version equal.",
            fromTableVersion, toWriteVersion));
      }
    }
    return shouldUpgrade;
  }
}
