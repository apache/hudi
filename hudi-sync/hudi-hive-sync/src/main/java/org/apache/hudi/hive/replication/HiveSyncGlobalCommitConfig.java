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

package org.apache.hudi.hive.replication;

import com.beust.jcommander.Parameter;

import com.beust.jcommander.Parameters;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;
import org.apache.hudi.common.util.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

// TODO: stop extending HiveSyncConfig and take all the variables needed from config file
@Parameters(commandDescription = "A tool to sync the hudi table to hive from different clusters. Similar to HiveSyncTool but syncs it to more"
    + "than one hive cluster ( currently a local and remote cluster). The common timestamp that was synced is stored as a new table property "
    + "This is most useful when we want to ensure that across different hive clusters we want ensure consistent reads. If that is not a requirement"
    + "then it is better to run HiveSyncTool separately."
    + "Note: "
    + "  The tool tries to be transactional but does not guarantee it. If the sync fails midway in one cluster it will try to roll back the committed "
    + "  timestamp from already successful sync on other clusters but that can also fail."
    + "  The tool does not roll back any synced partitions but only the timestamp.")
public class HiveSyncGlobalCommitConfig extends GlobalHiveSyncConfig {

  private static final Logger LOG = LogManager.getLogger(HiveSyncGlobalCommitConfig.class);

  public static String LOCAL_HIVE_SITE_URI = "hivesyncglobal.local_hive_site_uri";
  public static String REMOTE_HIVE_SITE_URI = "hivesyncglobal.remote_hive_site_uri";
  public static String REMOTE_BASE_PATH = "hivesyncglobal.remote_base_path";
  public static String LOCAL_BASE_PATH = "hivesyncglobal.local_base_path";
  public static String REMOTE_HIVE_SERVER_JDBC_URLS = "hivesyncglobal.remote_hs2_jdbc_urls";
  public static String LOCAL_HIVE_SERVER_JDBC_URLS = "hivesyncglobal.local_hs2_jdbc_urls";

  @Parameter(names = {
      "--config-xml-file"}, description = "path to the config file in Hive", required = true)
  public String configFile;

  public Properties properties = new Properties();

  private boolean finalize = false;

  public void load() throws IOException {
    if (finalize) {
      throw new RuntimeException("trying to modify finalized config");
    }
    finalize = true;
    try (InputStream configStream = new FileInputStream(new File(configFile))) {
      properties.loadFromXML(configStream);
    }
    if (StringUtils.isNullOrEmpty(globallyReplicatedTimeStamp)) {
      throw new RuntimeException("globally replicated timestamp not set");
    }
  }

  GlobalHiveSyncConfig mkGlobalHiveSyncConfig(boolean forRemote) {
    GlobalHiveSyncConfig cfg = GlobalHiveSyncConfig.copy(this);
    cfg.hoodieSyncConfigParams.basePath = forRemote ? properties.getProperty(REMOTE_BASE_PATH)
            : properties.getProperty(LOCAL_BASE_PATH, cfg.hoodieSyncConfigParams.basePath);
    cfg.hiveSyncConfigParams.jdbcUrl = forRemote ? properties.getProperty(REMOTE_HIVE_SERVER_JDBC_URLS)
            : properties.getProperty(LOCAL_HIVE_SERVER_JDBC_URLS, cfg.hiveSyncConfigParams.jdbcUrl);
    LOG.info("building hivesync config forRemote: " + forRemote + " " + cfg.hiveSyncConfigParams.jdbcUrl + " "
        + cfg.hoodieSyncConfigParams.basePath);
    return cfg;
  }

  @Override
  public String toString() {
    return "HiveSyncGlobalCommitConfig{ " + "configFile=" + configFile + ", properties="
        + properties + ", " + super.toString()
        + " }";
  }

  public void storeToXML(OutputStream configStream) throws IOException {
    this.properties.storeToXML(configStream, "hivesync global config");
  }
}
