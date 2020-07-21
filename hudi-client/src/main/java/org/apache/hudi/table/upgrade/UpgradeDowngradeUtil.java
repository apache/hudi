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

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

/**
 * Helper class to assist in upgrading/downgrading Hoodie when there is a version change.
 */
public class UpgradeDowngradeUtil {

  private static final Logger LOG = LogManager.getLogger(UpgradeDowngradeUtil.class);
  public static final String HOODIE_ORIG_PROPERTY_FILE = "hoodie.properties.orig";

  /**
   * Perform Upgrade or Downgrade steps if required and updated table version if need be.
   * <p>
   * Starting from version 0.6.0, this upgrade/downgrade step will be added in all write paths. Essentially, if a dataset was created using any pre 0.6.0(for eg 0.5.3),
   * and Hoodie version was upgraded to 0.6.0, Hoodie table version gets bumped to 1 and there are some upgrade steps need to be executed before doing any writes.
   * Similarly, if a dataset was created using Hoodie version 0.6.0 or Hoodie table version 1 and then hoodie was downgraded to pre 0.6.0 or to Hoodie table version 0,
   * then some downgrade steps need to be executed before proceeding w/ any writes.
   * On a high level, these are the steps performed
   * Step1 : Understand current hoodie table version and table version from hoodie.properties file
   * Step2 : Fix any residues from previous upgrade/downgrade
   * Step3 : If there are no residues, Check for version upgrade/downgrade. If version mismatch, perform upgrade/downgrade.
   * Step4 : If there are residues, clean them up and skip upgrade/downgrade since those steps would have been completed last time.
   * Step5 : Copy hoodie.properties -> hoodie.properties.orig
   * Step6 : Update hoodie.properties file with current table version
   * Step7 : Delete hoodie.properties.orig
   * </p>
   *
   * @param metaClient instance of {@link HoodieTableMetaClient} to use
   * @param toVersion version to which upgrade or downgrade has to be done.
   * @param config instance of {@link HoodieWriteConfig} to use.
   * @param jsc instance of {@link JavaSparkContext} to use.
   * @param instantTime current instant time that should not be touched.
   */
  public static void doUpgradeOrDowngrade(HoodieTableMetaClient metaClient, HoodieTableVersion toVersion, HoodieWriteConfig config, JavaSparkContext jsc, String instantTime) throws IOException {
    // Fetch version from property file and current version
    HoodieTableVersion versionFromPropertyFile = metaClient.getTableConfig().getHoodieTableVersionFromPropertyFile();

    Path metaPath = new Path(metaClient.getMetaPath());
    Path originalHoodiePropertyFile = getOrigHoodiePropertyFilePath(metaPath.toString());

    boolean updateTableVersionInPropertyFile = false;

    // check if there are any residues from previous upgrade/downgrade execution and clean them up.
    if (metaClient.getFs().exists(originalHoodiePropertyFile)) {
      // if hoodie.properties.orig exists, rename to hoodie.properties and skip upgrade/downgrade step
      metaClient.getFs().rename(originalHoodiePropertyFile, getHoodiePropertyFilePath(metaPath.toString()));
      updateTableVersionInPropertyFile = true;
    } else { // if there are no such residues
      // upgrade or downgrade if there is a version mismatch
      if (versionFromPropertyFile != toVersion) {
        updateTableVersionInPropertyFile = true;
        if (versionFromPropertyFile == HoodieTableVersion.ZERO && toVersion == HoodieTableVersion.ONE) {
          new UpgradeHandleFromZeroToOne().upgrade(config, jsc, instantTime);
        } else if (versionFromPropertyFile == HoodieTableVersion.ONE && toVersion == HoodieTableVersion.ZERO) {
          new DowngradeHandleOneToZero().downgrade(config, jsc, instantTime);
        } else {
          throw new HoodieException("Illegal state wrt table versions. Version from proerpty file " + versionFromPropertyFile + " and current version " + toVersion);
        }
      }
    }

    /**
     * If table version needs to be updated in hoodie.properties file.
     * Step1: Copy hoodie.properties to hoodie.properties.orig
     * Step2: add table.version to hoodie.properties
     * Step3: delete hoodie.properties.orig
     */
    if (updateTableVersionInPropertyFile) {
      updateTableVersionInHoodiePropertyFile(metaClient, toVersion);
    }
  }

  /**
   * Update hoodie.properties file with table version property.
   *
   * @param metaClient instance of {@link HoodieTableMetaClient} to use
   */
  private static void updateTableVersionInHoodiePropertyFile(HoodieTableMetaClient metaClient, HoodieTableVersion toVersion) throws IOException {

    Path metaPath = new Path(metaClient.getMetaPath());
    Path originalHoodiePropertyPath = getOrigHoodiePropertyFilePath(metaPath.toString());
    Path hoodiePropertyPath = getHoodiePropertyFilePath(metaPath.toString());

    // Step1: Copy hoodie.properties to hoodie.properties.orig
    FileUtil.copy(metaClient.getFs(), hoodiePropertyPath, metaClient.getFs(), originalHoodiePropertyPath,
        false, metaClient.getHadoopConf());

    // Step 2: update cur version in hoodie.properties file
    metaClient.getTableConfig().setHoodieTableVersion(toVersion, metaClient.getFs(), metaPath.toString());

    // Step3: Delete hoodie.properties.orig
    metaClient.getFs().delete(originalHoodiePropertyPath);
  }

  /**
   * @param metaPath meta path of hoodie dataset.
   * @return the path of hoodie.properties.orig file
   */
  private static Path getOrigHoodiePropertyFilePath(String metaPath) {
    return new Path(metaPath + "/" + HOODIE_ORIG_PROPERTY_FILE);
  }

  /**
   * @param metaPath meta path of hoodie dataset.
   * @return the path of hoodie.properties file
   */
  private static Path getHoodiePropertyFilePath(String metaPath) {
    return new Path(metaPath + "/" + HoodieTableConfig.HOODIE_PROPERTIES_FILE);
  }

}
