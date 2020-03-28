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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer.Config;
import org.apache.hudi.utilities.schema.SchemaRegistryProvider;

import com.beust.jcommander.JCommander;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Wrapper over HoodieDeltaStreamer.java class.
 * Helps with ingesting incremental data into hoodie datasets for multiple tables.
 * Currently supports only COPY_ON_WRITE storage type.
 */
public class HoodieMultiTableDeltaStreamer {

  private static Logger logger = LogManager.getLogger(HoodieMultiTableDeltaStreamer.class);

  private List<TableExecutionContext> tableExecutionContexts;
  private transient JavaSparkContext jssc;
  private Set<String> successTables;
  private Set<String> failedTables;

  public HoodieMultiTableDeltaStreamer(Config config, JavaSparkContext jssc) throws IOException {
    this.tableExecutionContexts = new ArrayList<>();
    this.successTables = new HashSet<>();
    this.failedTables = new HashSet<>();
    this.jssc = jssc;
    String commonPropsFile = config.propsFilePath;
    String configFolder = config.configFolder;
    FileSystem fs = FSUtils.getFs(commonPropsFile, jssc.hadoopConfiguration());
    configFolder = configFolder.charAt(configFolder.length() - 1) == '/' ? configFolder.substring(0, configFolder.length() - 1) : configFolder;
    checkIfPropsFileAndConfigFolderExist(commonPropsFile, configFolder, fs);
    TypedProperties properties = UtilHelpers.readConfig(fs, new Path(commonPropsFile), new ArrayList<>()).getConfig();
    //get the tables to be ingested and their corresponding config files from this properties instance
    populateTableExecutionContextList(properties, configFolder, fs, config);
  }

  private void checkIfPropsFileAndConfigFolderExist(String commonPropsFile, String configFolder, FileSystem fs) throws IOException {
    if (!fs.exists(new Path(commonPropsFile))) {
      throw new IllegalArgumentException("Please provide valid common config file path!");
    }

    if (!fs.exists(new Path(configFolder))) {
      fs.mkdirs(new Path(configFolder));
    }
  }

  private void checkIfTableConfigFileExists(String configFolder, FileSystem fs, String configFilePath) throws IOException {
    if (!fs.exists(new Path(configFilePath)) || !fs.isFile(new Path(configFilePath))) {
      throw new IllegalArgumentException("Please provide valid table config file path!");
    }

    Path path = new Path(configFilePath);
    Path filePathInConfigFolder = new Path(configFolder, path.getName());
    if (!fs.exists(filePathInConfigFolder)) {
      FileUtil.copy(fs, path, fs, filePathInConfigFolder, false, fs.getConf());
    }
  }

  //commonProps are passed as parameter which contain table to config file mapping
  private void populateTableExecutionContextList(TypedProperties properties, String configFolder, FileSystem fs, Config config) throws IOException {
    List<String> tablesToBeIngested = getTablesToBeIngested(properties);
    logger.info("tables to be ingested via MultiTableDeltaStreamer : " + tablesToBeIngested);
    TableExecutionContext executionContext;
    for (String table : tablesToBeIngested) {
      String[] tableWithDatabase = table.split("\\.");
      String database = tableWithDatabase.length > 1 ? tableWithDatabase[0] : "default";
      String currentTable = tableWithDatabase.length > 1 ? tableWithDatabase[1] : table;
      String configProp = Constants.INGESTION_PREFIX + database + Constants.DELIMITER + currentTable + Constants.INGESTION_CONFIG_SUFFIX;
      String configFilePath = properties.getString(configProp, Helpers.getDefaultConfigFilePath(configFolder, database, currentTable));
      checkIfTableConfigFileExists(configFolder, fs, configFilePath);
      TypedProperties tableProperties = UtilHelpers.readConfig(fs, new Path(configFilePath), new ArrayList<>()).getConfig();
      properties.forEach((k,v) -> {
        tableProperties.setProperty(k.toString(), v.toString());
      });
      final Config cfg = new Config();
      //copy all the values from config to cfgF
      Helpers.deepCopyConfigs(config, cfg);
      String targetBasePath = resetTarget(config, database, currentTable);
      String overriddenTargetBasePath = tableProperties.getString(Constants.TARGET_BASE_PATH_PROP, "");
      cfg.targetBasePath = StringUtils.isNullOrEmpty(overriddenTargetBasePath) ? targetBasePath : overriddenTargetBasePath;
      if (cfg.enableHiveSync && StringUtils.isNullOrEmpty(tableProperties.getString(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY(), ""))) {
        throw new HoodieException("Hive sync table field not provided!");
      }
      populateSchemaProviderProps(cfg, tableProperties);
      executionContext = new TableExecutionContext();
      executionContext.setProperties(tableProperties);
      executionContext.setConfig(cfg);
      executionContext.setDatabase(database);
      executionContext.setTableName(currentTable);
      this.tableExecutionContexts.add(executionContext);
    }
  }

  private List<String> getTablesToBeIngested(TypedProperties properties) {
    String combinedTablesString = properties.getString(Constants.TABLES_TO_BE_INGESTED_PROP);
    if (combinedTablesString == null) {
      return new ArrayList<>();
    }
    String[] tablesArray = combinedTablesString.split(Constants.COMMA_SEPARATOR);
    return Arrays.asList(tablesArray);
  }

  private void populateSchemaProviderProps(Config cfg, TypedProperties typedProperties) {
    if (cfg.schemaProviderClassName.equals(SchemaRegistryProvider.class.getName())) {
      String schemaRegistryBaseUrl = typedProperties.getString(Constants.SCHEMA_REGISTRY_BASE_URL_PROP);
      String schemaRegistrySuffix = typedProperties.getString(Constants.SCHEMA_REGISTRY_URL_SUFFIX_PROP);
      typedProperties.setProperty(Constants.SOURCE_SCHEMA_REGISTRY_URL_PROP, schemaRegistryBaseUrl + typedProperties.getString(Constants.KAFKA_TOPIC_PROP) + schemaRegistrySuffix);
      typedProperties.setProperty(Constants.TARGET_SCHEMA_REGISTRY_URL_PROP, schemaRegistryBaseUrl + typedProperties.getString(Constants.KAFKA_TOPIC_PROP) + schemaRegistrySuffix);
    }
  }

  public static class Helpers {

    static String getDefaultConfigFilePath(String configFolder, String database, String currentTable) {
      return configFolder + Constants.FILEDELIMITER + database + Constants.UNDERSCORE + currentTable + Constants.DEFAULT_CONFIG_FILE_NAME_SUFFIX;
    }

    static String getTableWithDatabase(TableExecutionContext context) {
      return context.getDatabase() + Constants.DELIMITER + context.getTableName();
    }

    static void deepCopyConfigs(Config globalConfig, Config tableConfig) {
      tableConfig.enableHiveSync = globalConfig.enableHiveSync;
      tableConfig.schemaProviderClassName = globalConfig.schemaProviderClassName;
      tableConfig.sourceOrderingField = globalConfig.sourceOrderingField;
      tableConfig.sourceClassName = globalConfig.sourceClassName;
      tableConfig.tableType = globalConfig.tableType;
      tableConfig.propsFilePath = globalConfig.propsFilePath;
      tableConfig.basePathPrefix = globalConfig.basePathPrefix;
      tableConfig.targetTableName = globalConfig.targetTableName;
      tableConfig.configFolder = globalConfig.configFolder;
      tableConfig.operation = globalConfig.operation;
      tableConfig.sourceLimit = globalConfig.sourceLimit;
      tableConfig.checkpoint = globalConfig.checkpoint;
      tableConfig.continuousMode = globalConfig.continuousMode;
      tableConfig.filterDupes = globalConfig.filterDupes;
      tableConfig.payloadClassName = globalConfig.payloadClassName;
      tableConfig.configs = globalConfig.configs;
      tableConfig.forceDisableCompaction = globalConfig.forceDisableCompaction;
      tableConfig.maxPendingCompactions = globalConfig.maxPendingCompactions;
      tableConfig.minSyncIntervalSeconds = globalConfig.minSyncIntervalSeconds;
      tableConfig.transformerClassName = globalConfig.transformerClassName;
      tableConfig.commitOnErrors = globalConfig.commitOnErrors;
      tableConfig.compactSchedulingMinShare = globalConfig.compactSchedulingMinShare;
      tableConfig.compactSchedulingWeight = globalConfig.compactSchedulingWeight;
      tableConfig.deltaSyncSchedulingMinShare = globalConfig.deltaSyncSchedulingMinShare;
      tableConfig.deltaSyncSchedulingWeight = globalConfig.deltaSyncSchedulingWeight;
      tableConfig.sparkMaster = globalConfig.sparkMaster;
    }
  }

  public static void main(String[] args) throws IOException {
    final Config config = new Config();
    JCommander cmd = new JCommander(config, null, args);
    if (config.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    JavaSparkContext jssc = UtilHelpers.buildSparkContext("multi-table-delta-streamer", Constants.LOCAL_SPARK_MASTER);
    try {
      new HoodieMultiTableDeltaStreamer(config, jssc).sync();
    } finally {
      jssc.stop();
    }
  }

  /**
   * Resets target table name and target path using base-path-prefix.
   * @param configuration
   * @param database
   * @param tableName
   * @return
   */
  private static String resetTarget(Config configuration, String database, String tableName) {
    String basePathPrefix = configuration.basePathPrefix;
    basePathPrefix = basePathPrefix.charAt(basePathPrefix.length() - 1) == '/' ? basePathPrefix.substring(0, basePathPrefix.length() - 1) : basePathPrefix;
    String targetBasePath = basePathPrefix + Constants.FILEDELIMITER + database + Constants.FILEDELIMITER + tableName;
    configuration.targetTableName = database + Constants.DELIMITER + tableName;
    return targetBasePath;
  }

  /*
  Creates actual HoodieDeltaStreamer objects for every table/topic and does incremental sync
   */
  public void sync() {
    for (TableExecutionContext context : tableExecutionContexts) {
      try {
        new HoodieDeltaStreamer(context.getConfig(), jssc, context.getProperties()).sync();
        successTables.add(Helpers.getTableWithDatabase(context));
      } catch (Exception e) {
        logger.error("error while running MultiTableDeltaStreamer for table: " + context.getTableName(), e);
        failedTables.add(Helpers.getTableWithDatabase(context));
      }
    }

    logger.info("Ingestion was successful for topics: " + successTables);
    if (!failedTables.isEmpty()) {
      logger.info("Ingestion failed for topics: " + failedTables);
    }
  }

  public static class Constants {
    public static final String KAFKA_TOPIC_PROP = "hoodie.deltastreamer.source.kafka.topic";
    private static final String SOURCE_SCHEMA_REGISTRY_URL_PROP = "hoodie.deltastreamer.schemaprovider.registry.url";
    private static final String TARGET_SCHEMA_REGISTRY_URL_PROP = "hoodie.deltastreamer.schemaprovider.registry.targetUrl";
    public static final String HIVE_SYNC_TABLE_PROP = "hoodie.datasource.hive_sync.table";
    private static final String SCHEMA_REGISTRY_BASE_URL_PROP = "hoodie.deltastreamer.schemaprovider.registry.baseUrl";
    private static final String SCHEMA_REGISTRY_URL_SUFFIX_PROP = "hoodie.deltastreamer.schemaprovider.registry.urlSuffix";
    private static final String TABLES_TO_BE_INGESTED_PROP = "hoodie.deltastreamer.ingestion.tablesToBeIngested";
    private static final String INGESTION_PREFIX = "hoodie.deltastreamer.ingestion.";
    private static final String INGESTION_CONFIG_SUFFIX = ".configFile";
    private static final String DEFAULT_CONFIG_FILE_NAME_SUFFIX = "_config.properties";
    private static final String TARGET_BASE_PATH_PROP = "hoodie.deltastreamer.ingestion.targetBasePath";
    private static final String LOCAL_SPARK_MASTER = "local[2]";
    private static final String FILEDELIMITER = "/";
    private static final String DELIMITER = ".";
    private static final String UNDERSCORE = "_";
    private static final String COMMA_SEPARATOR = ",";
  }

  public Set<String> getSuccessTables() {
    return successTables;
  }

  public Set<String> getFailedTables() {
    return failedTables;
  }

  public List<TableExecutionContext> getTableExecutionContexts() {
    return this.tableExecutionContexts;
  }
}
