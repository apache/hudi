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

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpgradeDowngradeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

public class DowngradeStrategy implements UpgradeDowngradeStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(DowngradeStrategy.class);
  private HoodieTableMetaClient metaClient;
  private HoodieWriteConfig config;
  private HoodieEngineContext context;
  private SupportsUpgradeDowngrade upgradeDowngradeHelper;

  public DowngradeStrategy(HoodieTableMetaClient metaClient,
                           HoodieWriteConfig config,
                           HoodieEngineContext context,
                           SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    this.metaClient = metaClient;
    this.config = config;
    this.context = context;
    this.upgradeDowngradeHelper = upgradeDowngradeHelper;
  }

  @Override
  public boolean shouldExecute(HoodieTableVersion toWriteVersion) {
    HoodieTableVersion fromTableVersion = metaClient.getTableConfig().getTableVersion();
    if (toWriteVersion.lesserThan(HoodieTableVersion.SIX)) {
      throw new HoodieUpgradeDowngradeException(
          String.format("Hudi 1.x release only supports table version greater than "
                  + "version 6 or above. Please downgrade table from version %s to %s "
                  + "using a Hudi release prior to 1.0.0",
              HoodieTableVersion.SIX.versionCode(), toWriteVersion.versionCode()));
    }
    return toWriteVersion.lesserThan(fromTableVersion);
  }

  @Override
  public UpgradeDowngrade.TableConfigChangeSet execute(HoodieTableVersion toVersion,
                                                       String instantTime) {
    // Fetch version from property file and current version
    HoodieTableVersion fromVersion = metaClient.getTableConfig().getTableVersion();
    LOG.info("Attempting to move table from version {} to {}", fromVersion, toVersion);

    Map<ConfigProperty, String> tablePropsToAdd = new Hashtable<>();
    Set<ConfigProperty> tablePropsToRemove = new HashSet<>();
    while (fromVersion.versionCode() > toVersion.versionCode()) {
      HoodieTableVersion prevVersion = HoodieTableVersion.fromVersionCode(fromVersion.versionCode() - 1);
      UpgradeDowngrade.TableConfigChangeSet tableConfigChangeSet = downgrade(fromVersion, prevVersion, instantTime);
      tablePropsToAdd.putAll(tableConfigChangeSet.propertiesToUpdate());
      tablePropsToRemove.addAll(tableConfigChangeSet.propertiesToDelete());
      fromVersion = prevVersion;
    }
    return new UpgradeDowngrade.TableConfigChangeSet(tablePropsToAdd, tablePropsToRemove);
  }

  private UpgradeDowngrade.TableConfigChangeSet downgrade(HoodieTableVersion fromVersion,
                                                          HoodieTableVersion toVersion,
                                                          String instantTime) {
    if (fromVersion == HoodieTableVersion.ONE && toVersion == HoodieTableVersion.ZERO) {
      return new OneToZeroDowngradeHandler().downgrade(config, context, instantTime, upgradeDowngradeHelper);
    } else if (fromVersion == HoodieTableVersion.TWO && toVersion == HoodieTableVersion.ONE) {
      return new TwoToOneDowngradeHandler().downgrade(config, context, instantTime, upgradeDowngradeHelper);
    } else if (fromVersion == HoodieTableVersion.THREE && toVersion == HoodieTableVersion.TWO) {
      return new ThreeToTwoDowngradeHandler().downgrade(config, context, instantTime, upgradeDowngradeHelper);
    } else if (fromVersion == HoodieTableVersion.FOUR && toVersion == HoodieTableVersion.THREE) {
      return new FourToThreeDowngradeHandler().downgrade(config, context, instantTime, upgradeDowngradeHelper);
    } else if (fromVersion == HoodieTableVersion.FIVE && toVersion == HoodieTableVersion.FOUR) {
      return new FiveToFourDowngradeHandler().downgrade(config, context, instantTime, upgradeDowngradeHelper);
    } else if (fromVersion == HoodieTableVersion.SIX && toVersion == HoodieTableVersion.FIVE) {
      return new SixToFiveDowngradeHandler().downgrade(config, context, instantTime, upgradeDowngradeHelper);
    } else if (fromVersion == HoodieTableVersion.SEVEN && toVersion == HoodieTableVersion.SIX) {
      return new SevenToSixDowngradeHandler().downgrade(config, context, instantTime, upgradeDowngradeHelper);
    } else if (fromVersion == HoodieTableVersion.EIGHT && toVersion == HoodieTableVersion.SEVEN) {
      return new EightToSevenDowngradeHandler().downgrade(config, context, instantTime, upgradeDowngradeHelper);
    } else if (fromVersion == HoodieTableVersion.NINE && toVersion == HoodieTableVersion.EIGHT) {
      return new NineToEightDowngradeHandler().downgrade(config, context, instantTime, upgradeDowngradeHelper);
    } else {
      throw new HoodieUpgradeDowngradeException(fromVersion.versionCode(), toVersion.versionCode(), false);
    }
  }
}
