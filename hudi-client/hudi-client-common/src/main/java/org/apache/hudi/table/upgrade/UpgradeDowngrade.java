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
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpgradeDowngradeException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Helper class to assist in upgrading/downgrading Hoodie when there is a version change.
 */
public class UpgradeDowngrade {

  private static final Logger LOG = LogManager.getLogger(UpgradeDowngrade.class);
  public static final String HOODIE_UPDATED_PROPERTY_FILE = "hoodie.properties.updated";

  private final BaseUpgradeDowngradeHelper upgradeDowngradeHelper;
  private HoodieTableMetaClient metaClient;
  protected HoodieWriteConfig config;
  protected HoodieEngineContext context;
  private transient FileSystem fs;
  private Path updatedPropsFilePath;
  private Path propsFilePath;

  public UpgradeDowngrade(
      HoodieTableMetaClient metaClient, HoodieWriteConfig config, HoodieEngineContext context,
      BaseUpgradeDowngradeHelper upgradeDowngradeHelper) {
    this.metaClient = metaClient;
    this.config = config;
    this.context = context;
    this.fs = metaClient.getFs();
    this.updatedPropsFilePath = new Path(metaClient.getMetaPath(), HOODIE_UPDATED_PROPERTY_FILE);
    this.propsFilePath = new Path(metaClient.getMetaPath(), HoodieTableConfig.HOODIE_PROPERTIES_FILE);
    this.upgradeDowngradeHelper = upgradeDowngradeHelper;
  }

  public boolean needsUpgradeOrDowngrade(HoodieTableVersion toVersion) {
    HoodieTableVersion fromVersion = metaClient.getTableConfig().getTableVersion();
    // Ensure versions are same
    return toVersion.versionCode() != fromVersion.versionCode();
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
   * 0.10.0 to current -> v3
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
    try {
      // Fetch version from property file and current version
      HoodieTableVersion fromVersion = metaClient.getTableConfig().getTableVersion();
      if (!needsUpgradeOrDowngrade(toVersion)) {
        return;
      }

      if (fs.exists(updatedPropsFilePath)) {
        // this can be left over .updated file from a failed attempt before. Many cases exist here.
        // a) We failed while writing the .updated file and it's content is partial (e.g hdfs)
        // b) We failed without renaming the file to hoodie.properties. We will re-attempt everything now anyway
        // c) rename() is not atomic in cloud stores. so hoodie.properties is fine, but we failed before deleting the .updated file
        // All cases, it simply suffices to delete the file and proceed.
        LOG.info("Deleting existing .updated file with content :" + FileIOUtils.readAsUTFString(fs.open(updatedPropsFilePath)));
        fs.delete(updatedPropsFilePath, false);
      }

      // Perform the actual upgrade/downgrade; this has to be idempotent, for now.
      LOG.info("Attempting to move table from version " + fromVersion + " to " + toVersion);
      Map<ConfigProperty, String> tableProps = new HashMap<>();
      if (fromVersion.versionCode() < toVersion.versionCode()) {
        // upgrade
        while (fromVersion.versionCode() < toVersion.versionCode()) {
          HoodieTableVersion nextVersion = HoodieTableVersion.versionFromCode(fromVersion.versionCode() + 1);
          tableProps.putAll(upgrade(fromVersion, nextVersion, instantTime));
          fromVersion = nextVersion;
        }
      } else {
        // downgrade
        while (fromVersion.versionCode() > toVersion.versionCode()) {
          HoodieTableVersion prevVersion = HoodieTableVersion.versionFromCode(fromVersion.versionCode() - 1);
          tableProps.putAll(downgrade(fromVersion, prevVersion, instantTime));
          fromVersion = prevVersion;
        }
      }

      // Write out the current version in hoodie.properties.updated file
      for (Map.Entry<ConfigProperty, String> entry : tableProps.entrySet()) {
        metaClient.getTableConfig().setValue(entry.getKey(), entry.getValue());
      }
      metaClient.getTableConfig().setTableVersion(toVersion);
      createUpdatedFile(metaClient.getTableConfig().getProps());

      // because for different fs the fs.rename have different action,such as:
      // a) for hdfs : if propsFilePath already exist,fs.rename will not replace propsFilePath, but just return false
      // b) for localfs: if propsFilePath already exist,fs.rename will replace propsFilePath, and return ture
      // c) for aliyun ossfs: if propsFilePath already exist,will throw FileAlreadyExistsException
      // so we should delete the old propsFilePath. also upgrade and downgrade is Idempotent
      if (fs.exists(propsFilePath)) {
        fs.delete(propsFilePath, false);
      }
      // Rename the .updated file to hoodie.properties. This is atomic in hdfs, but not in cloud stores.
      // But as long as this does not leave a partial hoodie.properties file, we are okay.
      fs.rename(updatedPropsFilePath, propsFilePath);
    } catch (IOException e) {
      throw new HoodieUpgradeDowngradeException("Error during upgrade/downgrade to version:" + toVersion, e);
    }
  }

  private void createUpdatedFile(Properties props) throws IOException {
    try (FSDataOutputStream outputStream = fs.create(updatedPropsFilePath)) {
      props.store(outputStream, "Properties saved on " + new Date(System.currentTimeMillis()));
    }
  }

  protected Map<ConfigProperty, String> upgrade(HoodieTableVersion fromVersion, HoodieTableVersion toVersion, String instantTime) {
    if (fromVersion == HoodieTableVersion.ZERO && toVersion == HoodieTableVersion.ONE) {
      return new ZeroToOneUpgradeHandler().upgrade(config, context, instantTime, upgradeDowngradeHelper);
    } else if (fromVersion == HoodieTableVersion.ONE && toVersion == HoodieTableVersion.TWO) {
      return new OneToTwoUpgradeHandler().upgrade(config, context, instantTime, upgradeDowngradeHelper);
    } else if (fromVersion == HoodieTableVersion.TWO && toVersion == HoodieTableVersion.THREE) {
      return new TwoToThreeUpgradeHandler().upgrade(config, context, instantTime, upgradeDowngradeHelper);
    } else {
      throw new HoodieUpgradeDowngradeException(fromVersion.versionCode(), toVersion.versionCode(), true);
    }
  }

  protected Map<ConfigProperty, String> downgrade(HoodieTableVersion fromVersion, HoodieTableVersion toVersion, String instantTime) {
    if (fromVersion == HoodieTableVersion.ONE && toVersion == HoodieTableVersion.ZERO) {
      return new OneToZeroDowngradeHandler().downgrade(config, context, instantTime, upgradeDowngradeHelper);
    } else if (fromVersion == HoodieTableVersion.TWO && toVersion == HoodieTableVersion.ONE) {
      return new TwoToOneDowngradeHandler().downgrade(config, context, instantTime, upgradeDowngradeHelper);
    } else if (fromVersion == HoodieTableVersion.THREE && toVersion == HoodieTableVersion.TWO) {
      return new ThreeToTwoDowngradeHandler().downgrade(config, context, instantTime, upgradeDowngradeHelper);
    } else {
      throw new HoodieUpgradeDowngradeException(fromVersion.versionCode(), toVersion.versionCode(), false);
    }
  }
}
