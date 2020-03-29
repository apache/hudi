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

package org.apache.hudi.utilities.adhoc;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieCompactionConfig;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * This is an one-time use class meant for migrating the configuration for "hoodie.compaction.payload.class" in
 * .hoodie/hoodie.properties from com.uber.hoodie to org.apache.hudi . It takes in a file containing base-paths for a set
 * of hudi tables and does the migration
 */
public class UpgradePayloadFromUberToApache implements Serializable {

  private static final Logger LOG = LogManager.getLogger(UpgradePayloadFromUberToApache.class);

  private final Config cfg;

  public UpgradePayloadFromUberToApache(Config cfg) {
    this.cfg = cfg;
  }

  public void run() throws IOException {
    String basePath = null;
    try (BufferedReader reader = new BufferedReader(new FileReader(cfg.inputPath))) {
      basePath = reader.readLine();
    } catch (IOException e) {
      LOG.error("Read from path: " + cfg.inputPath + " error.", e);
    }

    while (basePath != null) {
      basePath = basePath.trim();
      if (!basePath.startsWith("#")) {
        LOG.info("Performing upgrade for " + basePath);
        String metaPath = String.format("%s/.hoodie", basePath);
        HoodieTableMetaClient metaClient =
            new HoodieTableMetaClient(FSUtils.prepareHadoopConf(new Configuration()), basePath, false);
        HoodieTableConfig tableConfig = metaClient.getTableConfig();
        if (tableConfig.getTableType().equals(HoodieTableType.MERGE_ON_READ)) {
          Map<String, String> propsMap = tableConfig.getProps();
          if (propsMap.containsKey(HoodieCompactionConfig.PAYLOAD_CLASS_PROP)) {
            String payloadClass = propsMap.get(HoodieCompactionConfig.PAYLOAD_CLASS_PROP);
            LOG.info("Found payload class=" + payloadClass);
            if (payloadClass.startsWith("com.uber.hoodie")) {
              String newPayloadClass = payloadClass.replace("com.uber.hoodie", "org.apache.hudi");
              LOG.info("Replacing payload class (" + payloadClass + ") with (" + newPayloadClass + ")");
              Map<String, String> newPropsMap = new HashMap<>(propsMap);
              newPropsMap.put(HoodieCompactionConfig.PAYLOAD_CLASS_PROP, newPayloadClass);
              Properties props = new Properties();
              props.putAll(newPropsMap);
              HoodieTableConfig.createHoodieProperties(metaClient.getFs(), new Path(metaPath), props);
              LOG.info("Finished upgrade for " + basePath);
            }
          }
        } else {
          LOG.info("Skipping as this table is COW table. BasePath=" + basePath);

        }
      }
    }
  }

  public static class Config implements Serializable {

    @Parameter(names = {"--datasets_list_path", "-sp"},
        description = "Local File containing list of base-paths for which migration needs to be performed",
        required = true)
    public String inputPath = null;
    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;
  }

  public static void main(String[] args) throws Exception {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    UpgradePayloadFromUberToApache upgrader = new UpgradePayloadFromUberToApache(cfg);
    upgrader.run();
  }

}
