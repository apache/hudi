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
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
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

import java.util.Collections;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Helper class to assist in upgrading/downgrading Hoodie when there is a version change.
 */
public class UpgradeDowngrade {

  private static final Logger LOG = LoggerFactory.getLogger(UpgradeDowngrade.class);
  public static final String HOODIE_UPDATED_PROPERTY_FILE = "hoodie.properties.updated";

  private static final Set<Pair<Integer, Integer>> UPGRADE_HANDLERS_REQUIRING_ROLLBACK_AND_COMPACT = new HashSet<>(Arrays.asList(
      Pair.of(7, 8), // SevenToEightUpgradeHandler
      Pair.of(8, 9)  // EightToNineUpgradeHandler
  ));

  private static final Set<Pair<Integer, Integer>> DOWNGRADE_HANDLERS_REQUIRING_ROLLBACK_ANDCOMPACT = new HashSet<>(Arrays.asList(
      Pair.of(8, 7), // EightToSevenDowngradeHandler
      Pair.of(9, 8), // NineToEightDowngradeHandler
      Pair.of(6, 5)  // SixToFiveDowngradeHandler
  ));

  private final SupportsUpgradeDowngrade upgradeDowngradeHelper;
  private final UpgradeDowngradeStrategy strategy;
  private HoodieTableMetaClient metaClient;
  protected HoodieWriteConfig config;
  protected HoodieEngineContext context;

  /**
   * Use fromVersion and toVersion to decide which strategy should be utilized.
   * This requires the toVersion is set purposely in 'config'. Otherwise, we should
   * prefer the next constructor.
   * The main usage of this constructor is to generate the object, and
   * call its `needsUpgradeOrDowngrade` function to tell if either upgrade or
   * downgrade operation is needed.
   */
  public UpgradeDowngrade(HoodieTableMetaClient metaClient,
                          HoodieWriteConfig config,
                          HoodieEngineContext context,
                          SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    this.metaClient = metaClient;
    this.config = config;
    this.context = context;
    this.upgradeDowngradeHelper = upgradeDowngradeHelper;

    HoodieTableVersion fromVersion = metaClient.getTableConfig().getTableVersion();
    HoodieTableVersion toVersion = config.getWriteVersion();
    strategy = fromVersion.greaterThan(toVersion)
        ? new DowngradeStrategy(metaClient, config, context, upgradeDowngradeHelper)
        : new UpgradeStrategy(metaClient, config, context, upgradeDowngradeHelper);
  }

  /**
   * Given either upgrade or downgrade strategy, generate the object to
   * execute the intended operation.
   */
  public UpgradeDowngrade(HoodieTableMetaClient metaClient,
                          HoodieWriteConfig config,
                          HoodieEngineContext context,
                          SupportsUpgradeDowngrade upgradeDowngradeHelper,
                          UpgradeDowngradeStrategy strategy) {
    this.metaClient = metaClient;
    this.config = config;
    this.context = context;
    this.upgradeDowngradeHelper = upgradeDowngradeHelper;
    this.strategy = strategy;
  }

  public boolean needsUpgradeOrDowngrade(HoodieTableVersion toVersion) {
    return this.strategy.shouldExecute(toVersion);
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
   * .
   * .
   * 0.12.0 to 0.13.0 -> v5
   * 0.14.0 to 0.x -> v6
   * 1.0.0 to 1.0.2 -> v8
   * 1.1.0 to current -> v9
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
    // If strategy decides not to execute, abort the upgrade/downgrade operation.
    if (!strategy.shouldExecute(toVersion)) {
      return;
    }

    HoodieTableVersion fromVersion = metaClient.getTableConfig().getTableVersion();
    // Perform rollback and compaction only if a specific handler requires it, before upgrade/downgrade process
    boolean isUpgrade = strategy instanceof UpgradeStrategy;
    performRollbackAndCompactionIfRequired(fromVersion, toVersion, isUpgrade);
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
          new UpgradeDowngrade(mdtMetaClient, mdtWriteConfig, context, upgradeDowngradeHelper, strategy)
              .run(toVersion, instantTime);
        }
      } catch (Exception e) {
        throw new HoodieUpgradeDowngradeException("Upgrade/downgrade for the Hudi metadata table failed. "
            + "Please try again. If the failure repeats for metadata table, it is recommended to disable "
            + "the metadata table so that the upgrade and downgrade can continue for the data table.", e);
      }
    }

    // Perform the actual upgrade/downgrade; this has to be idempotent, for now.
    TableConfigChangeSet configChangeSet = strategy.execute(toVersion, instantTime);
    Map<ConfigProperty, String> tablePropsToAdd = configChangeSet.propertiesToUpdate;
    Set<ConfigProperty> tablePropsToRemove = configChangeSet.propertiesToDelete;

    // Reload the meta client to get the latest table config (which could have been updated due to metadata table)
    if (metaClient.getTableConfig().isMetadataTableAvailable()) {
      metaClient = HoodieTableMetaClient.reload(metaClient);
    }

    for (ConfigProperty configProperty : tablePropsToRemove) {
      metaClient.getTableConfig().clearValue(configProperty);
    }
    for (Map.Entry<ConfigProperty, String> entry : tablePropsToAdd.entrySet()) {
      // add alternate keys.
      metaClient.getTableConfig().setValue(entry.getKey(), entry.getValue());
      entry.getKey().getAlternatives().forEach(alternateKey -> {
        metaClient.getTableConfig().setValue((String) alternateKey, entry.getValue());
      });
    }

    // Write out the current version in hoodie.properties.updated file
    metaClient.getTableConfig().setTableVersion(toVersion);
    // Update modified properties.
    Set<String> propertiesToRemove =
        tablePropsToRemove.stream().map(ConfigProperty::key).collect(Collectors.toSet());
    HoodieTableConfig.updateAndDeleteProps(
        metaClient.getStorage(), metaClient.getMetaPath(), metaClient.getTableConfig().getProps(), propertiesToRemove);

    if (metaClient.getTableConfig().isMetadataTableAvailable() && toVersion.equals(HoodieTableVersion.SIX) && !isUpgrade) {
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

  /**
   * Class to hold the change set required to update or delete from table config properties.
   */
  public static class TableConfigChangeSet {
    private final Map<ConfigProperty, String> propertiesToUpdate;
    private final Set<ConfigProperty> propertiesToDelete;

    public TableConfigChangeSet() {
      this.propertiesToUpdate = Collections.emptyMap();
      this.propertiesToDelete = Collections.emptySet();
    }

    public TableConfigChangeSet(Map<ConfigProperty, String> propertiesToUpdate, Set<ConfigProperty> propertiesToDelete) {
      this.propertiesToUpdate = propertiesToUpdate;
      this.propertiesToDelete = propertiesToDelete;
    }

    public Map<ConfigProperty, String> propertiesToUpdate() {
      return propertiesToUpdate;
    }

    public Set<ConfigProperty> propertiesToDelete() {
      return propertiesToDelete;
    }
  }

  /**
   * Checks if any handlers in the upgrade/downgrade path require running rollback and compaction before starting process.
   *
   * @param fromVersion the current table version
   * @param toVersion   the target table version
   */
  private void performRollbackAndCompactionIfRequired(HoodieTableVersion fromVersion, HoodieTableVersion toVersion, boolean isUpgrade) {
    boolean requireRollbackAndCompaction = false;
    if (isUpgrade) {
      // Check upgrade handlers
      HoodieTableVersion checkVersion = fromVersion;
      while (checkVersion.versionCode() < toVersion.versionCode()) {
        HoodieTableVersion nextVersion = HoodieTableVersion.fromVersionCode(checkVersion.versionCode() + 1);
        if (UPGRADE_HANDLERS_REQUIRING_ROLLBACK_AND_COMPACT.contains(Pair.of(checkVersion.versionCode(), nextVersion.versionCode()))) {
          requireRollbackAndCompaction = true;
          break;
        }
        checkVersion = nextVersion;
      }
    } else {
      // Check downgrade handlers
      HoodieTableVersion checkVersion = fromVersion;
      while (checkVersion.versionCode() > toVersion.versionCode()) {
        HoodieTableVersion prevVersion = HoodieTableVersion.fromVersionCode(checkVersion.versionCode() - 1);
        if (DOWNGRADE_HANDLERS_REQUIRING_ROLLBACK_ANDCOMPACT.contains(Pair.of(checkVersion.versionCode(), prevVersion.versionCode()))) {
          requireRollbackAndCompaction = true;
          break;
        }
        checkVersion = prevVersion;
      }
    }
    if (requireRollbackAndCompaction) {
      LOG.info("Rolling back failed writes and compacting table before upgrade/downgrade");
      // For version SEVEN to EIGHT upgrade, use SIX as tableVersion to avoid hitting issue with WRITE_TABLE_VERSION,
      // as table version SEVEN is not considered as a valid value due to being a bridge release.
      // otherwise use current table version
      HoodieTableVersion tableVersion = fromVersion == HoodieTableVersion.SEVEN
          ? HoodieTableVersion.SIX 
          : metaClient.getTableConfig().getTableVersion();

      UpgradeDowngradeUtils.rollbackFailedWritesAndCompact(
          upgradeDowngradeHelper.getTable(config, context),
          context, 
          config, 
          upgradeDowngradeHelper, 
          HoodieTableType.MERGE_ON_READ.equals(metaClient.getTableType()),
          tableVersion);
    }
  }
}
