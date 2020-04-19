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

package org.apache.hudi.utilities;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.hudi.utilities.deltastreamer.HoodieMultiTableDeltaStreamer;
import org.apache.hudi.utilities.deltastreamer.TableExecutionContext;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;

public class DeltaStreamerUtility {

  public static String getDefaultConfigFilePath(String configFolder, String database, String currentTable) {
    return configFolder + Constants.FILEDELIMITER + database + Constants.UNDERSCORE + currentTable + Constants.DEFAULT_CONFIG_FILE_NAME_SUFFIX;
  }

  public static String getTableWithDatabase(TableExecutionContext context) {
    return context.getDatabase() + Constants.DELIMITER + context.getTableName();
  }

  public static void checkIfPropsFileAndConfigFolderExist(String commonPropsFile, String configFolder, FileSystem fs) throws IOException {
    if (!fs.exists(new Path(commonPropsFile))) {
      throw new IllegalArgumentException("Please provide valid common config file path!");
    }

    if (!fs.exists(new Path(configFolder))) {
      fs.mkdirs(new Path(configFolder));
    }
  }

  public static void checkIfTableConfigFileExists(String configFolder, FileSystem fs, String configFilePath) throws IOException {
    if (!fs.exists(new Path(configFilePath)) || !fs.isFile(new Path(configFilePath))) {
      throw new IllegalArgumentException("Please provide valid table config file path!");
    }

    Path path = new Path(configFilePath);
    Path filePathInConfigFolder = new Path(configFolder, path.getName());
    if (!fs.exists(filePathInConfigFolder)) {
      FileUtil.copy(fs, path, fs, filePathInConfigFolder, false, fs.getConf());
    }
  }

  public static TypedProperties getTablePropertiesFromConfigFolder(HoodieDeltaStreamer.Config cfg, FileSystem fs) throws IOException {
    if (StringUtils.isNullOrEmpty(cfg.configFolder)) {
      return UtilHelpers.readConfig(fs, new Path(cfg.propsFilePath), cfg.configs).getConfig();
    }

    String commonPropsFile = cfg.propsFilePath;
    String configFolder = cfg.configFolder;
    configFolder = configFolder.charAt(configFolder.length() - 1) == '/' ? configFolder.substring(0, configFolder.length() - 1) : configFolder;
    checkIfPropsFileAndConfigFolderExist(commonPropsFile, configFolder, fs);
    TypedProperties globalProperties = UtilHelpers.readConfig(fs, new Path(commonPropsFile), cfg.configs).getConfig();
    String configProp = Constants.INGESTION_PREFIX + "default" + Constants.DELIMITER + cfg.targetTableName + Constants.INGESTION_CONFIG_SUFFIX;
    String configFilePath = globalProperties.getString(configProp, DeltaStreamerUtility.getDefaultConfigFilePath(configFolder, "default", cfg.targetTableName));
    checkIfTableConfigFileExists(configFolder, fs, configFilePath);
    TypedProperties tableProperties = UtilHelpers.readConfig(fs, new Path(configFilePath), new ArrayList<>()).getConfig();
    globalProperties.forEach((k,v) -> {
      tableProperties.setProperty(k.toString(), v.toString());
    });
    return tableProperties;
  }

  public static void deepCopyConfigs(HoodieMultiTableDeltaStreamer.Config globalConfig, HoodieDeltaStreamer.Config tableConfig) {
    tableConfig.enableHiveSync = globalConfig.enableHiveSync;
    tableConfig.schemaProviderClassName = globalConfig.schemaProviderClassName;
    tableConfig.sourceOrderingField = globalConfig.sourceOrderingField;
    tableConfig.sourceClassName = globalConfig.sourceClassName;
    tableConfig.tableType = globalConfig.tableType;
    tableConfig.targetTableName = globalConfig.targetTableName;
    tableConfig.operation = globalConfig.operation;
    tableConfig.sourceLimit = globalConfig.sourceLimit;
    tableConfig.checkpoint = globalConfig.checkpoint;
    tableConfig.continuousMode = globalConfig.continuousMode;
    tableConfig.filterDupes = globalConfig.filterDupes;
    tableConfig.payloadClassName = globalConfig.payloadClassName;
    tableConfig.forceDisableCompaction = globalConfig.forceDisableCompaction;
    tableConfig.maxPendingCompactions = globalConfig.maxPendingCompactions;
    tableConfig.minSyncIntervalSeconds = globalConfig.minSyncIntervalSeconds;
    tableConfig.transformerClassNames = globalConfig.transformerClassNames;
    tableConfig.commitOnErrors = globalConfig.commitOnErrors;
    tableConfig.compactSchedulingMinShare = globalConfig.compactSchedulingMinShare;
    tableConfig.compactSchedulingWeight = globalConfig.compactSchedulingWeight;
    tableConfig.deltaSyncSchedulingMinShare = globalConfig.deltaSyncSchedulingMinShare;
    tableConfig.deltaSyncSchedulingWeight = globalConfig.deltaSyncSchedulingWeight;
    tableConfig.sparkMaster = globalConfig.sparkMaster;
  }

  public static class Constants {
    public static final String KAFKA_TOPIC_PROP = "hoodie.deltastreamer.source.kafka.topic";
    public static final String SOURCE_SCHEMA_REGISTRY_URL_PROP = "hoodie.deltastreamer.schemaprovider.registry.url";
    public static final String TARGET_SCHEMA_REGISTRY_URL_PROP = "hoodie.deltastreamer.schemaprovider.registry.targetUrl";
    public static final String SCHEMA_REGISTRY_BASE_URL_PROP = "hoodie.deltastreamer.schemaprovider.registry.baseUrl";
    public static final String SCHEMA_REGISTRY_URL_SUFFIX_PROP = "hoodie.deltastreamer.schemaprovider.registry.urlSuffix";
    public static final String TABLES_TO_BE_INGESTED_PROP = "hoodie.deltastreamer.ingestion.tablesToBeIngested";
    public static final String INGESTION_PREFIX = "hoodie.deltastreamer.ingestion.";
    public static final String INGESTION_CONFIG_SUFFIX = ".configFile";
    public static final String DEFAULT_CONFIG_FILE_NAME_SUFFIX = "_config.properties";
    public static final String TARGET_BASE_PATH_PROP = "hoodie.deltastreamer.ingestion.targetBasePath";
    public static final String LOCAL_SPARK_MASTER = "local[2]";
    public static final String FILEDELIMITER = "/";
    public static final String DELIMITER = ".";
    public static final String UNDERSCORE = "_";
    public static final String COMMA_SEPARATOR = ",";
  }
}
