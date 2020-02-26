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

import org.apache.hadoop.fs.FileUtil;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer.Config;
import org.apache.hudi.utilities.schema.SchemaRegistryProvider;

import com.beust.jcommander.JCommander;
import com.google.common.base.Strings;
import org.apache.hadoop.fs.FileSystem;
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

  private List<TableExecutionObject> tableExecutionObjects;
  private transient JavaSparkContext jssc;
  private Set<String> successTables;
  private Set<String> failedTables;

  public HoodieMultiTableDeltaStreamer(String[] args, JavaSparkContext jssc) throws IOException {
    this.tableExecutionObjects = new ArrayList<>();
    this.successTables = new HashSet<>();
    this.failedTables = new HashSet<>();
    this.jssc = jssc;
    String commonPropsFile = getCommonPropsFileName(args);
    String configFolder = getConfigFolder(args);
    FileSystem fs = FSUtils.getFs(commonPropsFile, jssc.hadoopConfiguration());
    configFolder = configFolder.charAt(configFolder.length() - 1) == '/' ? configFolder.substring(0, configFolder.length() - 1) : configFolder;
    checkIfPropsFileAndConfigFolderExist(commonPropsFile, configFolder, fs);
    TypedProperties properties = UtilHelpers.readConfig(fs, new Path(commonPropsFile), new ArrayList<>()).getConfig();
    //get the tables to be ingested and their corresponding config files from this properties instance
    populateTableExecutionObjectList(properties, configFolder, fs, args);
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
  private void populateTableExecutionObjectList(TypedProperties properties, String configFolder, FileSystem fs, String[] args) throws IOException {
    List<String> tablesToBeIngested = getTablesToBeIngested(properties);
    TableExecutionObject executionObject;
    for (String table : tablesToBeIngested) {
      String[] tableWithDatabase = table.split("\\.");
      String database = tableWithDatabase.length > 1 ? tableWithDatabase[0] : "default";
      String currentTable = tableWithDatabase.length > 1 ? tableWithDatabase[1] : table;
      String configProp = Constants.INGESTION_PREFIX + database + Constants.DELIMITER + currentTable + Constants.INGESTION_CONFIG_SUFFIX;
      String configFilePath = properties.getString(configProp, configFolder + "/" + database + "_" + currentTable + Constants.DEFAULT_CONFIG_FILE_NAME_SUFFIX);
      checkIfTableConfigFileExists(configFolder, fs, configFilePath);
      TypedProperties tableProperties = UtilHelpers.readConfig(fs, new Path(configFilePath), new ArrayList<>()).getConfig();
      properties.forEach((k,v) -> {
        tableProperties.setProperty(k.toString(), v.toString());
      });
      final Config cfg = new Config();
      String[] tableArgs = args.clone();
      String targetBasePath = resetTarget(tableArgs, database, currentTable);
      JCommander cmd = new JCommander(cfg);
      cmd.parse(tableArgs);
      String overriddenTargetBasePath = tableProperties.getString(Constants.TARGET_BASE_PATH_PROP, "");
      cfg.targetBasePath = Strings.isNullOrEmpty(overriddenTargetBasePath) ? targetBasePath : overriddenTargetBasePath;
      if (cfg.enableHiveSync && Strings.isNullOrEmpty(tableProperties.getString(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY(), ""))) {
        throw new HoodieException("Hive sync table field not provided!");
      }
      populateSchemaProviderProps(cfg, tableProperties);
      executionObject = new TableExecutionObject();
      executionObject.setProperties(tableProperties);
      executionObject.setConfig(cfg);
      executionObject.setDatabase(database);
      executionObject.setTableName(currentTable);
      this.tableExecutionObjects.add(executionObject);
    }
  }

  private List<String> getTablesToBeIngested(TypedProperties properties) {
    String combinedTablesString = properties.getString(Constants.TABLES_TO_BE_INGESTED_PROP);
    if (combinedTablesString == null) {
      return new ArrayList<>();
    }
    String[] tablesArray = combinedTablesString.split(",");
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

  public static void main(String[] args) throws IOException {
    JavaSparkContext jssc = UtilHelpers.buildSparkContext("multi-table-delta-streamer", Constants.LOCAL_SPARK_MASTER);
    try {
      new HoodieMultiTableDeltaStreamer(args, jssc).sync();
    } finally {
      jssc.stop();
    }
  }

  private static String getCommonPropsFileName(String[] args) {
    String commonPropsFileName = "common_props.properties";
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals(Constants.PROPS_FILE_PROP)) {
        commonPropsFileName = args[i + 1];
        break;
      }
    }
    return commonPropsFileName;
  }

  private static String getConfigFolder(String[] args) {
    String configFolder = "";
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals(Constants.CONFIG_FOLDER_PROP)) {
        configFolder = args[i + 1];
        break;
      }
    }
    return configFolder;
  }

  /**
   * Resets target table name and target path using base-path-prefix.
   * @param args
   * @param database
   * @param tableName
   * @return
   */
  private static String resetTarget(String[] args, String database, String tableName) {
    int counter = 0;
    String targetBasePath = "";
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals(Constants.BASE_PATH_PREFIX_PROP)) {
        args[i + 1] = args[i + 1].charAt(args[i + 1].length() - 1) == '/' ? args[i + 1].substring(0, args[i + 1].length() - 1) : args[i + 1];
        targetBasePath = args[i + 1] + Constants.FILEDELIMITER + database + Constants.FILEDELIMITER + tableName;
        counter += 1;
      } else if (args[i].equals(Constants.TARGET_TABLE_ARG)) {
        args[i + 1] = database + Constants.DELIMITER + tableName;
        counter += 1;
      }
      if (counter == 2) {
        break;
      }
    }

    return targetBasePath;
  }

  /*
  Creates actual HoodieDeltaStreamer objects for every table/topic and does incremental sync
   */
  public void sync() {
    for (TableExecutionObject object : tableExecutionObjects) {
      try {
        new HoodieDeltaStreamer(object.getConfig(), jssc, object.getProperties()).sync();
        successTables.add(object.getDatabase() + Constants.DELIMITER + object.getTableName());
      } catch (Exception e) {
        logger.error("error while running MultiTableDeltaStreamer for table: " + object.getTableName(), e);
        failedTables.add(object.getDatabase() + Constants.DELIMITER + object.getTableName());
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
    private static final String TARGET_TABLE_ARG = "--target-table";
    private static final String PROPS_FILE_PROP = "--props";
    private static final String CONFIG_FOLDER_PROP = "--config-folder";
    private static final String BASE_PATH_PREFIX_PROP = "--base-path-prefix";
  }

  public Set<String> getSuccessTables() {
    return successTables;
  }

  public Set<String> getFailedTables() {
    return failedTables;
  }

  public List<TableExecutionObject> getTableExecutionObjects() {
    return this.tableExecutionObjects;
  }
}
