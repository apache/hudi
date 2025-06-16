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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieUpgradeDowngradeException;
import org.apache.hudi.metadata.HoodieMetadataWriteUtils;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

/**
 * Helper class to assist in upgrading/downgrading Hoodie when there is a version change.
 */
public class UpgradeDowngrade {

  private static final Logger LOG = LoggerFactory.getLogger(UpgradeDowngrade.class);
  public static final String HOODIE_UPDATED_PROPERTY_FILE = "hoodie.properties.updated";

  private final SupportsUpgradeDowngrade upgradeDowngradeHelper;
  private HoodieTableMetaClient metaClient;
  protected HoodieWriteConfig config;
  protected HoodieEngineContext context;

  public UpgradeDowngrade(
      HoodieTableMetaClient metaClient, HoodieWriteConfig config, HoodieEngineContext context,
      SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    this.metaClient = metaClient;
    this.config = config;
    this.context = context;
    this.upgradeDowngradeHelper = upgradeDowngradeHelper;
  }

  public static boolean needsUpgradeOrDowngrade(HoodieTableMetaClient metaClient, HoodieWriteConfig config, HoodieTableVersion toWriteVersion) {
    HoodieTableVersion fromTableVersion = metaClient.getTableConfig().getTableVersion();
    return needsUpgrade(metaClient, config, toWriteVersion) || toWriteVersion.versionCode() < fromTableVersion.versionCode();
  }

  public static boolean needsUpgrade(HoodieTableMetaClient metaClient, HoodieWriteConfig config, HoodieTableVersion toWriteVersion) {
    HoodieTableVersion fromTableVersion = metaClient.getTableConfig().getTableVersion();
    // If table version is less than SIX, then we need to upgrade to SIX first before upgrading to any other version, irrespective of autoUpgrade flag
    if (fromTableVersion.versionCode() < HoodieTableVersion.SIX.versionCode() && toWriteVersion.versionCode() >= HoodieTableVersion.EIGHT.versionCode()) {
      throw new HoodieUpgradeDowngradeException(
          String.format("Please upgrade table from version %s to %s before upgrading to version %s.", fromTableVersion, HoodieTableVersion.SIX.versionCode(), toWriteVersion));
    }

    // allow upgrades otherwise.
    return toWriteVersion.versionCode() > fromTableVersion.versionCode();
  }

  public boolean needsUpgradeOrDowngrade(HoodieTableVersion toWriteVersion) {
    return needsUpgradeOrDowngrade(metaClient, config, toWriteVersion);
  }

  public boolean needsUpgrade(HoodieTableVersion toWriteVersion) {
    return needsUpgrade(metaClient, config, toWriteVersion);
  }

  /**
   * Perform Upgrade or Downgrade steps if required and updated table version if need be.
   * <p>
   * Starting from version 0.6.0, this upgrade/downgrade step will be added in all write paths.
   * <p>
   * Essentially, if a dataset was created using an previous table version in an older release,
   * and Hoodie version was upgraded to a new release with new table version supported,
   * Hoodie table version gets bumped to the new version and there are some upgrade steps need
   * to be executed before doing any writes.
   * <p>
   * Similarly, if a dataset was created using an newer table version in an newer release,
   * and then hoodie was downgraded to an older release or to older Hoodie table version,
   * then some downgrade steps need to be executed before proceeding w/ any writes.
   * <p>
   * Below shows the table version corresponding to the Hudi release:
   * Hudi release -> table version
   * pre 0.6.0 -> v0
   * 0.6.0 to 0.8.0 -> v1
   * 0.9.0 -> v2
   * 0.10.0 -> v3
   * 0.11.0 -> v4
   * 0.12.0 to 0.13.0 -> v5
   * 0.14.0 to current -> v6
   * <p>
   * On a high level, these are the steps performed
   * <p>
   * Step1 : Understand current hoodie table version and table version from hoodie.properties file
   * Step2 : Delete any left over .updated from previous upgrade/downgrade
   * Step3 : If version are different, perform upgrade/downgrade.
   * Step4 : Copy hoodie.properties -> hoodie.properties.updated with the version updated
   * Step6 : Rename hoodie.properties.updated to hoodie.properties
   * </p>
   *
   * @param toVersion   version to which upgrade or downgrade has to be done.
   * @param instantTime current instant time that should not be touched.
   */
  public void run(HoodieTableVersion toVersion, String instantTime) {
    // Change metadata table version automatically
    if (toVersion.versionCode() >= HoodieTableVersion.FOUR.versionCode()) {
      String metadataTablePath = HoodieTableMetadata.getMetadataTableBasePath(
          metaClient.getBasePath().toString());
      try {
        if (metaClient.getStorage().exists(new StoragePath(metadataTablePath))) {
          HoodieTableMetaClient mdtMetaClient = HoodieTableMetaClient.builder()
              .setConf(metaClient.getStorageConf().newInstance()).setBasePath(metadataTablePath).build();
          HoodieWriteConfig mdtWriteConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(
              config, HoodieFailedWritesCleaningPolicy.EAGER, metaClient.getTableConfig().getTableVersion());
          new UpgradeDowngrade(mdtMetaClient, mdtWriteConfig, context, upgradeDowngradeHelper)
              .run(toVersion, instantTime);
        }
      } catch (Exception e) {
        LOG.warn("Unable to upgrade or downgrade the metadata table to version " + toVersion
            + ", ignoring the error and continue.", e);
      }
    }

    // Fetch version from property file and current version
    HoodieTableVersion fromVersion = metaClient.getTableConfig().getTableVersion();
    if (!needsUpgradeOrDowngrade(toVersion)) {
      return;
    }

    // Perform the actual upgrade/downgrade; this has to be idempotent, for now.
    LOG.info("Attempting to move table from version " + fromVersion + " to " + toVersion);
    Map<ConfigProperty, String> tablePropsToAdd = new Hashtable<>();
    List<ConfigProperty> tablePropsToRemove = new ArrayList<>();
    boolean isDowngrade = false;
    if (fromVersion.versionCode() < toVersion.versionCode()) {
      // upgrade
      while (fromVersion.versionCode() < toVersion.versionCode()) {
        HoodieTableVersion nextVersion = HoodieTableVersion.fromVersionCode(fromVersion.versionCode() + 1);
        tablePropsToAdd.putAll(upgrade(fromVersion, nextVersion, instantTime));
        fromVersion = nextVersion;
      }
    } else {
      // downgrade
      isDowngrade = true;
      while (fromVersion.versionCode() > toVersion.versionCode()) {
        HoodieTableVersion prevVersion = HoodieTableVersion.fromVersionCode(fromVersion.versionCode() - 1);
        Pair<Map<ConfigProperty, String>, List<ConfigProperty>> tablePropsToAddAndRemove = downgrade(fromVersion, prevVersion, instantTime);
        tablePropsToAdd.putAll(tablePropsToAddAndRemove.getLeft());
        tablePropsToRemove.addAll(tablePropsToAddAndRemove.getRight());
        fromVersion = prevVersion;
      }
    }
    // Reload the meta client to get the latest table config (which could have been updated due to metadata table)
    if (metaClient.getTableConfig().isMetadataTableAvailable()) {
      metaClient = HoodieTableMetaClient.reload(metaClient);
    }
    // Write out the current version in hoodie.properties.updated file
    for (Map.Entry<ConfigProperty, String> entry : tablePropsToAdd.entrySet()) {
      // add alternate keys.
      metaClient.getTableConfig().setValue(entry.getKey(), entry.getValue());
      entry.getKey().getAlternatives().forEach(alternateKey -> {
        metaClient.getTableConfig().setValue((String)alternateKey, entry.getValue());
      });
    }
    for (ConfigProperty configProperty : tablePropsToRemove) {
      metaClient.getTableConfig().clearValue(configProperty);
    }
    // user could have disabled auto upgrade (probably to deploy the new binary only),
    // in which case, we should not update the table version
    if (config.autoUpgrade()) {
      metaClient.getTableConfig().setTableVersion(toVersion);
    }

    HoodieTableConfig.update(metaClient.getStorage(),
        metaClient.getMetaPath(), metaClient.getTableConfig().getProps());

    if (metaClient.getTableConfig().isMetadataTableAvailable() && toVersion.equals(HoodieTableVersion.SIX) && isDowngrade) {
      // NOTE: Add empty deltacommit to metadata table. The compaction instant format has changed in version 8.
      //       It no longer has a suffix of "001" for the compaction instant. Due to that, the timeline instant
      //       comparison logic in metadata table will fail after LSM timeline downgrade.
      //       To avoid that, we add an empty deltacommit to metadata table in the downgrade step.
      TypedProperties typedProperties = config.getProps();
      typedProperties.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "6");
      typedProperties.setProperty(HoodieWriteConfig.AUTO_UPGRADE_VERSION.key(), "false");
      HoodieWriteConfig updatedConfig = HoodieWriteConfig.newBuilder().withPath(config.getBasePath()).withProperties(typedProperties).build();

      HoodieTable table = upgradeDowngradeHelper.getTable(updatedConfig, context);
      String newInstant = table.getMetaClient().createNewInstantTime(false);
      Option<HoodieTableMetadataWriter> mdtWriterOpt = table.getMetadataWriter(newInstant);
      mdtWriterOpt.ifPresent(mdtWriter -> {
        HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
        commitMetadata.setOperationType(WriteOperationType.UPSERT);
        mdtWriter.update(commitMetadata, newInstant);
        try {
          mdtWriter.close();
        } catch (Exception e) {
          throw new HoodieException("Failed to close MDT writer for table " + table.getConfig().getBasePath());
        }
      });
    }
  }

  protected Map<ConfigProperty, String> upgrade(HoodieTableVersion fromVersion, HoodieTableVersion toVersion, String instantTime) {
    if (fromVersion == HoodieTableVersion.ZERO && toVersion == HoodieTableVersion.ONE) {
      return new ZeroToOneUpgradeHandler().upgrade(config, context, instantTime, upgradeDowngradeHelper);
    } else if (fromVersion == HoodieTableVersion.ONE && toVersion == HoodieTableVersion.TWO) {
      return new OneToTwoUpgradeHandler().upgrade(config, context, instantTime, upgradeDowngradeHelper);
    } else if (fromVersion == HoodieTableVersion.TWO && toVersion == HoodieTableVersion.THREE) {
      return new TwoToThreeUpgradeHandler().upgrade(config, context, instantTime, upgradeDowngradeHelper);
    } else if (fromVersion == HoodieTableVersion.THREE && toVersion == HoodieTableVersion.FOUR) {
      return new ThreeToFourUpgradeHandler().upgrade(config, context, instantTime, upgradeDowngradeHelper);
    } else if (fromVersion == HoodieTableVersion.FOUR && toVersion == HoodieTableVersion.FIVE) {
      return new FourToFiveUpgradeHandler().upgrade(config, context, instantTime, upgradeDowngradeHelper);
    } else if (fromVersion == HoodieTableVersion.FIVE && toVersion == HoodieTableVersion.SIX) {
      return new FiveToSixUpgradeHandler().upgrade(config, context, instantTime, upgradeDowngradeHelper);
    } else if (fromVersion == HoodieTableVersion.SIX && toVersion == HoodieTableVersion.SEVEN) {
      return new SixToSevenUpgradeHandler().upgrade(config, context, instantTime, upgradeDowngradeHelper);
    } else if (fromVersion == HoodieTableVersion.SEVEN && toVersion == HoodieTableVersion.EIGHT) {
      return new SevenToEightUpgradeHandler().upgrade(config, context, instantTime, upgradeDowngradeHelper);
    } else {
      throw new HoodieUpgradeDowngradeException(fromVersion.versionCode(), toVersion.versionCode(), true);
    }
  }

  protected Pair<Map<ConfigProperty, String>, List<ConfigProperty>> downgrade(HoodieTableVersion fromVersion, HoodieTableVersion toVersion, String instantTime) {
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
    } else {
      throw new HoodieUpgradeDowngradeException(fromVersion.versionCode(), toVersion.versionCode(), false);
    }
  }
}
