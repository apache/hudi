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
import org.apache.hudi.utilities.model.TableConfig;
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

import java.util.ArrayList;
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
  private Set<String> successTopics;
  private Set<String> failedTopics;

  public HoodieMultiTableDeltaStreamer(String[] args, JavaSparkContext jssc) {
    this.tableExecutionObjects = new ArrayList<>();
    this.successTopics = new HashSet<>();
    this.failedTopics = new HashSet<>();
    this.jssc = jssc;
    String tableConfigFile = getCustomPropsFileName(args);
    FileSystem fs = FSUtils.getFs(tableConfigFile, jssc.hadoopConfiguration());
    List<TableConfig> configList = UtilHelpers.readTableConfig(fs, new Path(tableConfigFile)).getConfigs();

    for (TableConfig config : configList) {
      validateTableConfigObject(config);
      populateTableExecutionObjectList(config, args);
    }
  }

  /*
  validate if given object has all the necessary fields.
  Throws IllegalArgumentException if any of the required fields are missing
   */
  private void validateTableConfigObject(TableConfig config) {
    if (Strings.isNullOrEmpty(config.getDatabase()) || Strings.isNullOrEmpty(config.getTableName()) || Strings.isNullOrEmpty(config.getPrimaryKeyField())
        || Strings.isNullOrEmpty(config.getTopic())) {
      throw new IllegalArgumentException("Please provide valid table config arguments!");
    }
  }

  private void populateTableExecutionObjectList(TableConfig config, String[] args) {
    TableExecutionObject executionObject;
    try {
      final Config cfg = new Config();
      String[] tableArgs = args.clone();
      String targetBasePath = resetTarget(tableArgs, config.getDatabase(), config.getTableName());
      JCommander cmd = new JCommander(cfg);
      cmd.parse(tableArgs);
      cfg.targetBasePath = Strings.isNullOrEmpty(config.getTargetBasePath()) ? targetBasePath : config.getTargetBasePath();
      FileSystem fs = FSUtils.getFs(cfg.targetBasePath, jssc.hadoopConfiguration());
      TypedProperties typedProperties = UtilHelpers.readConfig(fs, new Path(cfg.propsFilePath), cfg.configs).getConfig();
      populateIngestionProps(typedProperties, config);
      populateSchemaProviderProps(cfg, typedProperties, config);
      populateHiveSyncProps(cfg, typedProperties, config);
      executionObject = new TableExecutionObject();
      executionObject.setConfig(cfg);
      executionObject.setProperties(typedProperties);
      executionObject.setTableConfig(config);
      this.tableExecutionObjects.add(executionObject);
    } catch (Exception e) {
      logger.error("Error while creating execution object for topic: " + config.getTopic(), e);
      throw e;
    }
  }

  private void populateSchemaProviderProps(Config cfg, TypedProperties typedProperties, TableConfig config) {
    if (cfg.schemaProviderClassName.equals(SchemaRegistryProvider.class.getName())) {
      String schemaRegistryBaseUrl = typedProperties.getString(Constants.SCHEMA_REGISTRY_BASE_URL_PROP);
      String schemaRegistrySuffix = typedProperties.getString(Constants.SCHEMA_REGISTRY_URL_SUFFIX_PROP);
      typedProperties.setProperty(Constants.SOURCE_SCHEMA_REGISTRY_URL_PROP, schemaRegistryBaseUrl + config.getTopic() + schemaRegistrySuffix);
      typedProperties.setProperty(Constants.TARGET_SCHEMA_REGISTRY_URL_PROP, schemaRegistryBaseUrl + config.getTopic() + schemaRegistrySuffix);
    }
  }

  private void populateHiveSyncProps(Config cfg, TypedProperties typedProperties, TableConfig config) {
    if (cfg.enableHiveSync && Strings.isNullOrEmpty(config.getHiveSyncTable())) {
      throw new HoodieException("Hive sync table field not provided!");
    }
    typedProperties.setProperty(Constants.HIVE_SYNC_TABLE_PROP, config.getHiveSyncTable());
    typedProperties.setProperty(Constants.HIVE_SYNC_DATABASE_NAME_PROP, Strings.isNullOrEmpty(config.getHiveSyncDatabase())
        ? typedProperties.getString(Constants.HIVE_SYNC_DATABASE_NAME_PROP, DataSourceWriteOptions.DEFAULT_HIVE_DATABASE_OPT_VAL())
        : config.getHiveSyncDatabase());
    typedProperties.setProperty(DataSourceWriteOptions.HIVE_ASSUME_DATE_PARTITION_OPT_KEY(), String.valueOf(config.getAssumeDatePartitioningForHiveSync()));
    typedProperties.setProperty(DataSourceWriteOptions.HIVE_USE_PRE_APACHE_INPUT_FORMAT_OPT_KEY(), String.valueOf(config.getUsePreApacheInputFormatForHiveSync()));
  }

  private void populateIngestionProps(TypedProperties typedProperties, TableConfig config) {
    typedProperties.setProperty(Constants.KAFKA_TOPIC_PROP, config.getTopic());
    typedProperties.setProperty(Constants.PARTITION_TIMESTAMP_TYPE_PROP, config.getPartitionTimestampType());
    typedProperties.setProperty(Constants.PARTITION_FIELD_INPUT_FORMAT_PROP, config.getPartitionInputFormat());
    typedProperties.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), config.getPrimaryKeyField());
    typedProperties.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), config.getPartitionKeyField());
    typedProperties.setProperty(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY(), Strings.isNullOrEmpty(config.getKeyGeneratorClassName())
        ? typedProperties.getString(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY(), DataSourceWriteOptions.DEFAULT_KEYGENERATOR_CLASS_OPT_VAL())
        : config.getKeyGeneratorClassName());
  }

  public static void main(String[] args) {
    JavaSparkContext jssc = UtilHelpers.buildSparkContext("multi-table-delta-streamer", Constants.LOCAL_SPARK_MASTER);
    try {
      new HoodieMultiTableDeltaStreamer(args, jssc).sync();
    } finally {
      jssc.stop();
    }
  }

  /**
   * Gets customPropsFileName from given args.
   * @param args
   * @return
   */
  private static String getCustomPropsFileName(String[] args) {
    String customPropsFileName = "custom_config.json";
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals(Constants.CUSTOM_PROPS_FILE_PROP)) {
        customPropsFileName = args[i + 1];
        break;
      }
    }
    return customPropsFileName;
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
        successTopics.add(object.getTableConfig().getTopic());
      } catch (Exception e) {
        logger.error("error while running CDCStreamer for topic: " + object.getTableConfig().getTopic(), e);
        failedTopics.add(object.getTableConfig().getTopic());
      }
    }

    logger.info("Ingestion was successful for topics: " + successTopics);
    if (!failedTopics.isEmpty()) {
      logger.info("Ingestion failed for topics: " + failedTopics);
    }
  }

  public static class Constants {
    public static final String KAFKA_TOPIC_PROP = "hoodie.deltastreamer.source.kafka.topic";
    private static final String SOURCE_SCHEMA_REGISTRY_URL_PROP = "hoodie.deltastreamer.schemaprovider.registry.url";
    private static final String TARGET_SCHEMA_REGISTRY_URL_PROP = "hoodie.deltastreamer.schemaprovider.registry.targetUrl";
    public static final String HIVE_SYNC_TABLE_PROP = "hoodie.datasource.hive_sync.table";
    private static final String PARTITION_TIMESTAMP_TYPE_PROP = "hoodie.deltastreamer.keygen.timebased.timestamp.type";
    private static final String PARTITION_FIELD_INPUT_FORMAT_PROP = "hoodie.deltastreamer.keygen.timebased.input.dateformat";
    private static final String SCHEMA_REGISTRY_BASE_URL_PROP = "hoodie.deltastreamer.schemaprovider.registry.baseUrl";
    private static final String SCHEMA_REGISTRY_URL_SUFFIX_PROP = "hoodie.deltastreamer.schemaprovider.registry.urlSuffix";
    private static final String HIVE_SYNC_DATABASE_NAME_PROP = "hoodie.datasource.hive_sync.database";
    private static final String LOCAL_SPARK_MASTER = "local[2]";
    private static final String FILEDELIMITER = "/";
    private static final String DELIMITER = ".";
    private static final String TARGET_TABLE_ARG = "--target-table";
    private static final String CUSTOM_PROPS_FILE_PROP = "--custom-props";
    private static final String BASE_PATH_PREFIX_PROP = "--base-path-prefix";
  }

  public Set<String> getSuccessTopics() {
    return successTopics;
  }

  public Set<String> getFailedTopics() {
    return failedTopics;
  }

  public List<TableExecutionObject> getTableExecutionObjects() {
    return this.tableExecutionObjects;
  }
}
