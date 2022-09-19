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

import com.beust.jcommander.Parameter;
import org.apache.hudi.client.utils.OperationConverter;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.sync.common.HoodieSyncConfig;
import org.apache.hudi.utilities.IdentitySplitter;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.schema.SchemaRegistryProvider;

import com.beust.jcommander.JCommander;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.utilities.sources.JsonDFSSource;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.hudi.utilities.schema.SchemaRegistryProvider.Config.SRC_SCHEMA_REGISTRY_URL_PROP;
import static org.apache.hudi.utilities.schema.SchemaRegistryProvider.Config.TARGET_SCHEMA_REGISTRY_URL_PROP;

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
    this.successTables = Collections.synchronizedSet(new HashSet<>());
    this.failedTables = Collections.synchronizedSet(new HashSet<>());
    this.jssc = jssc;
    String commonPropsFile = config.propsFilePath;
    String configFolder = config.configFolder;
    ValidationUtils.checkArgument(!config.filterDupes || config.operation != WriteOperationType.UPSERT,
        "'--filter-dupes' needs to be disabled when '--op' is 'UPSERT' to ensure updates are not missed.");
    FileSystem fs = FSUtils.getFs(commonPropsFile, jssc.hadoopConfiguration());
    configFolder = configFolder.charAt(configFolder.length() - 1) == '/' ? configFolder.substring(0, configFolder.length() - 1) : configFolder;
    checkIfPropsFileAndConfigFolderExist(commonPropsFile, configFolder, fs);
    TypedProperties commonProperties = UtilHelpers.readConfig(fs.getConf(), new Path(commonPropsFile), new ArrayList<String>()).getProps();
    //get the tables to be ingested and their corresponding config files from this properties instance
    populateTableExecutionContextList(commonProperties, configFolder, fs, config);
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
      TypedProperties tableProperties = UtilHelpers.readConfig(fs.getConf(), new Path(configFilePath), new ArrayList<String>()).getProps();
      properties.forEach((k, v) -> {
        if (tableProperties.get(k) == null) {
          tableProperties.setProperty(k.toString(), v.toString());
        }
      });
      final HoodieDeltaStreamer.Config cfg = new HoodieDeltaStreamer.Config();
      //copy all the values from config to cfg
      String targetBasePath = resetTarget(config, database, currentTable);
      Helpers.deepCopyConfigs(config, cfg);
      String overriddenTargetBasePath = tableProperties.getString(Constants.TARGET_BASE_PATH_PROP, "");
      cfg.targetBasePath = StringUtils.isNullOrEmpty(overriddenTargetBasePath) ? targetBasePath : overriddenTargetBasePath;
      if (cfg.enableMetaSync && StringUtils.isNullOrEmpty(tableProperties.getString(HoodieSyncConfig.META_SYNC_TABLE_NAME.key(), ""))) {
        throw new HoodieException("Meta sync table field not provided!");
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

  private void populateSchemaProviderProps(HoodieDeltaStreamer.Config cfg, TypedProperties typedProperties) {
    if (Objects.equals(cfg.schemaProviderClassName, SchemaRegistryProvider.class.getName())) {
      populateSourceRegistryProp(typedProperties);
      populateTargetRegistryProp(typedProperties);
    }
  }

  private void populateTargetRegistryProp(TypedProperties typedProperties) {
    String schemaRegistryTargetUrl = typedProperties.getString(TARGET_SCHEMA_REGISTRY_URL_PROP, null);
    if (StringUtils.isNullOrEmpty(schemaRegistryTargetUrl)) {
      String schemaRegistryBaseUrl = typedProperties.getString(Constants.SCHEMA_REGISTRY_BASE_URL_PROP);
      String schemaRegistrySuffix = typedProperties.getString(Constants.SCHEMA_REGISTRY_URL_SUFFIX_PROP, null);
      String targetSchemaRegistrySuffix;
      if (StringUtils.isNullOrEmpty(schemaRegistrySuffix)) {
        targetSchemaRegistrySuffix = typedProperties.getString(Constants.SCHEMA_REGISTRY_TARGET_URL_SUFFIX);
      } else {
        targetSchemaRegistrySuffix = schemaRegistrySuffix;
      }
      typedProperties.setProperty(TARGET_SCHEMA_REGISTRY_URL_PROP, schemaRegistryBaseUrl + typedProperties.getString(Constants.KAFKA_TOPIC_PROP) + targetSchemaRegistrySuffix);
    }
  }

  private void populateSourceRegistryProp(TypedProperties typedProperties) {
    String schemaRegistrySourceUrl = typedProperties.getString(SRC_SCHEMA_REGISTRY_URL_PROP, null);
    if (StringUtils.isNullOrEmpty(schemaRegistrySourceUrl)) {
      String schemaRegistryBaseUrl = typedProperties.getString(Constants.SCHEMA_REGISTRY_BASE_URL_PROP);
      String schemaRegistrySuffix = typedProperties.getString(Constants.SCHEMA_REGISTRY_URL_SUFFIX_PROP, null);
      String sourceSchemaRegistrySuffix;
      if (StringUtils.isNullOrEmpty(schemaRegistrySuffix)) {
        sourceSchemaRegistrySuffix = typedProperties.getString(Constants.SCHEMA_REGISTRY_SOURCE_URL_SUFFIX);
      } else {
        sourceSchemaRegistrySuffix = schemaRegistrySuffix;
      }
      typedProperties.setProperty(SRC_SCHEMA_REGISTRY_URL_PROP, schemaRegistryBaseUrl + typedProperties.getString(Constants.KAFKA_TOPIC_PROP) + sourceSchemaRegistrySuffix);
    }
  }

  public static class Helpers {

    static String getDefaultConfigFilePath(String configFolder, String database, String currentTable) {
      return configFolder + Constants.FILE_DELIMITER + database + Constants.UNDERSCORE + currentTable + Constants.DEFAULT_CONFIG_FILE_NAME_SUFFIX;
    }

    static String getTableWithDatabase(TableExecutionContext context) {
      return context.getDatabase() + Constants.DELIMITER + context.getTableName();
    }

    static void deepCopyConfigs(Config globalConfig, HoodieDeltaStreamer.Config tableConfig) {
      tableConfig.enableHiveSync = globalConfig.enableHiveSync;
      tableConfig.enableMetaSync = globalConfig.enableMetaSync;
      tableConfig.syncClientToolClassNames = globalConfig.syncClientToolClassNames;
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
      tableConfig.maxPendingClustering = globalConfig.maxPendingClustering;
      tableConfig.minSyncIntervalSeconds = globalConfig.minSyncIntervalSeconds;
      tableConfig.transformerClassNames = globalConfig.transformerClassNames;
      tableConfig.commitOnErrors = globalConfig.commitOnErrors;
      tableConfig.compactSchedulingMinShare = globalConfig.compactSchedulingMinShare;
      tableConfig.compactSchedulingWeight = globalConfig.compactSchedulingWeight;
      tableConfig.deltaSyncSchedulingMinShare = globalConfig.deltaSyncSchedulingMinShare;
      tableConfig.deltaSyncSchedulingWeight = globalConfig.deltaSyncSchedulingWeight;
      tableConfig.clusterSchedulingWeight = globalConfig.clusterSchedulingWeight;
      tableConfig.clusterSchedulingMinShare = globalConfig.clusterSchedulingMinShare;
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

    if (config.enableHiveSync) {
      logger.warn("--enable-hive-sync will be deprecated in a future release; please use --enable-sync instead for Hive syncing");
    }

    if (config.targetTableName != null) {
      logger.warn(String.format("--target-table is deprecated and will be removed in a future release due to it's useless;"
              + " please use %s to configure multiple target tables", Constants.TABLES_TO_BE_INGESTED_PROP));
    }

    JavaSparkContext jssc = UtilHelpers.buildSparkContext("multi-table-delta-streamer", Constants.LOCAL_SPARK_MASTER);
    try {
      new HoodieMultiTableDeltaStreamer(config, jssc).sync();
    } finally {
      jssc.stop();
    }
  }

  public static class Config implements Serializable {

    @Parameter(names = {"--base-path-prefix"},
        description = "base path prefix for multi table support via HoodieMultiTableDeltaStreamer class")
    public String basePathPrefix;

    @Deprecated
    @Parameter(names = {"--target-table"}, description = "name of the target table")
    public String targetTableName;

    @Parameter(names = {"--table-type"}, description = "Type of table. COPY_ON_WRITE (or) MERGE_ON_READ", required = true)
    public String tableType;

    @Parameter(names = {"--config-folder"}, description = "Path to folder which contains all the properties file", required = true)
    public String configFolder;

    @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
        + "hoodie client, schema provider, key generator and data source. For hoodie client props, sane defaults are "
        + "used, but recommend use to provide basic things like metrics endpoints, hive configs etc. For sources, refer"
        + "to individual classes, for supported properties.")
    public String propsFilePath =
        "file://" + System.getProperty("user.dir") + "/src/test/resources/delta-streamer-config/dfs-source.properties";

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated",
            splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();

    @Parameter(names = {"--source-class"},
        description = "Subclass of org.apache.hudi.utilities.sources to read data. "
        + "Built-in options: org.apache.hudi.utilities.sources.{JsonDFSSource (default), AvroDFSSource, "
        + "JsonKafkaSource, AvroKafkaSource, HiveIncrPullSource}")
    public String sourceClassName = JsonDFSSource.class.getName();

    @Parameter(names = {"--source-ordering-field"}, description = "Field within source record to decide how"
        + " to break ties between records with same key in input data. Default: 'ts' holding unix timestamp of record")
    public String sourceOrderingField = "ts";

    @Parameter(names = {"--payload-class"}, description = "subclass of HoodieRecordPayload, that works off "
        + "a GenericRecord. Implement your own, if you want to do something other than overwriting existing value")
    public String payloadClassName = OverwriteWithLatestAvroPayload.class.getName();

    @Parameter(names = {"--schemaprovider-class"}, description = "subclass of org.apache.hudi.utilities.schema"
        + ".SchemaProvider to attach schemas to input & target table data, built in options: "
        + "org.apache.hudi.utilities.schema.FilebasedSchemaProvider."
        + "Source (See org.apache.hudi.utilities.sources.Source) implementation can implement their own SchemaProvider."
        + " For Sources that return Dataset<Row>, the schema is obtained implicitly. "
        + "However, this CLI option allows overriding the schemaprovider returned by Source.")
    public String schemaProviderClassName = null;

    @Parameter(names = {"--transformer-class"},
        description = "A subclass or a list of subclasses of org.apache.hudi.utilities.transform.Transformer"
        + ". Allows transforming raw source Dataset to a target Dataset (conforming to target schema) before "
        + "writing. Default : Not set. E:g - org.apache.hudi.utilities.transform.SqlQueryBasedTransformer (which "
        + "allows a SQL query templated to be passed as a transformation function). "
        + "Pass a comma-separated list of subclass names to chain the transformations.")
    public List<String> transformerClassNames = null;

    @Parameter(names = {"--source-limit"}, description = "Maximum amount of data to read from source. "
        + "Default: No limit, e.g: DFS-Source => max bytes to read, Kafka-Source => max events to read")
    public long sourceLimit = Long.MAX_VALUE;

    @Parameter(names = {"--op"}, description = "Takes one of these values : UPSERT (default), INSERT (use when input "
        + "is purely new data/inserts to gain speed)", converter = OperationConverter.class)
    public WriteOperationType operation = WriteOperationType.UPSERT;

    @Parameter(names = {"--filter-dupes"},
        description = "Should duplicate records from source be dropped/filtered out before insert/bulk-insert")
    public Boolean filterDupes = false;

    @Parameter(names = {"--enable-hive-sync"}, description = "Enable syncing to hive")
    public Boolean enableHiveSync = false;

    @Parameter(names = {"--enable-sync"}, description = "Enable syncing meta")
    public Boolean enableMetaSync = false;

    @Parameter(names = {"--sync-tool-classes"}, description = "Meta sync client tool, using comma to separate multi tools")
    public String syncClientToolClassNames = HiveSyncTool.class.getName();

    @Parameter(names = {"--max-pending-compactions"},
        description = "Maximum number of outstanding inflight/requested compactions. Delta Sync will not happen unless"
        + "outstanding compactions is less than this number")
    public Integer maxPendingCompactions = 5;

    @Parameter(names = {"--max-pending-clustering"},
        description = "Maximum number of outstanding inflight/requested clustering. Delta Sync will not happen unless"
            + "outstanding clustering is less than this number")
    public Integer maxPendingClustering = 5;

    @Parameter(names = {"--continuous"}, description = "Delta Streamer runs in continuous mode running"
        + " source-fetch -> Transform -> Hudi Write in loop")
    public Boolean continuousMode = false;

    @Parameter(names = {"--min-sync-interval-seconds"},
        description = "the min sync interval of each sync in continuous mode")
    public Integer minSyncIntervalSeconds = 0;

    @Parameter(names = {"--spark-master"},
        description = "spark master to use, if not defined inherits from your environment taking into "
            + "account Spark Configuration priority rules (e.g. not using spark-submit command).")
    public String sparkMaster = "";

    @Parameter(names = {"--commit-on-errors"}, description = "Commit even when some records failed to be written")
    public Boolean commitOnErrors = false;

    @Parameter(names = {"--delta-sync-scheduling-weight"},
        description = "Scheduling weight for delta sync as defined in "
        + "https://spark.apache.org/docs/latest/job-scheduling.html")
    public Integer deltaSyncSchedulingWeight = 1;

    @Parameter(names = {"--compact-scheduling-weight"}, description = "Scheduling weight for compaction as defined in "
        + "https://spark.apache.org/docs/latest/job-scheduling.html")
    public Integer compactSchedulingWeight = 1;

    @Parameter(names = {"--delta-sync-scheduling-minshare"}, description = "Minshare for delta sync as defined in "
        + "https://spark.apache.org/docs/latest/job-scheduling.html")
    public Integer deltaSyncSchedulingMinShare = 0;

    @Parameter(names = {"--compact-scheduling-minshare"}, description = "Minshare for compaction as defined in "
        + "https://spark.apache.org/docs/latest/job-scheduling.html")
    public Integer compactSchedulingMinShare = 0;

    /**
     * Compaction is enabled for MoR table by default. This flag disables it
     */
    @Parameter(names = {"--disable-compaction"},
        description = "Compaction is enabled for MoR table by default. This flag disables it ")
    public Boolean forceDisableCompaction = false;

    /**
     * Resume Delta Streamer from this checkpoint.
     */
    @Parameter(names = {"--checkpoint"}, description = "Resume Delta Streamer from this checkpoint.")
    public String checkpoint = null;

    @Parameter(names = {"--cluster-scheduling-weight"}, description = "Scheduling weight for clustering as defined in "
        + "https://spark.apache.org/docs/latest/job-scheduling.html")
    public Integer clusterSchedulingWeight = 1;

    @Parameter(names = {"--cluster-scheduling-minshare"}, description = "Minshare for clustering as defined in "
        + "https://spark.apache.org/docs/latest/job-scheduling.html")
    public Integer clusterSchedulingMinShare = 0;

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;
  }

  /**
   * Resets target table name and target path using base-path-prefix.
   *
   * @param configuration
   * @param database
   * @param tableName
   * @return
   */
  private static String resetTarget(Config configuration, String database, String tableName) {
    String basePathPrefix = configuration.basePathPrefix;
    basePathPrefix = basePathPrefix.charAt(basePathPrefix.length() - 1) == '/' ? basePathPrefix.substring(0, basePathPrefix.length() - 1) : basePathPrefix;
    String targetBasePath = basePathPrefix + Constants.FILE_DELIMITER + database + Constants.FILE_DELIMITER + tableName;
    configuration.targetTableName = database + Constants.DELIMITER + tableName;
    return targetBasePath;
  }

  /**
   * Creates actual HoodieDeltaStreamer objects for every table/topic and does incremental sync.
   */
  public void sync() {
    ExecutorService executorService = Executors.newFixedThreadPool(tableExecutionContexts.size());
    tableExecutionContexts.forEach(context -> {
      executorService.execute(new Runnable() {
        @Override
        public void run() {
          try {
            HoodieDeltaStreamer streamerInstance = new HoodieDeltaStreamer(context.getConfig(), jssc, Option.ofNullable(context.getProperties()));
            if (streamerInstance.getDeltaSyncService().getDeltaSync().isDataAvailableForIngestion()) {
              streamerInstance.sync();
              successTables.add(Helpers.getTableWithDatabase(context));
            }
          } catch (Exception e) {
            logger.error("error while running MultiTableDeltaStreamer for table: " + context.getTableName(), e);
            failedTables.add(Helpers.getTableWithDatabase(context));
          }
        }
      });
    });
    executorService.shutdown();

    logger.info("Ingestion was successful for topics: " + successTables);
    if (!failedTables.isEmpty()) {
      logger.info("Ingestion failed for topics: " + failedTables);
    }
  }

  public static class Constants {
    public static final String KAFKA_TOPIC_PROP = "hoodie.deltastreamer.source.kafka.topic";
    public static final String HIVE_SYNC_TABLE_PROP = "hoodie.datasource.hive_sync.table";
    private static final String SCHEMA_REGISTRY_BASE_URL_PROP = "hoodie.deltastreamer.schemaprovider.registry.baseUrl";
    private static final String SCHEMA_REGISTRY_URL_SUFFIX_PROP = "hoodie.deltastreamer.schemaprovider.registry.urlSuffix";
    private static final String SCHEMA_REGISTRY_SOURCE_URL_SUFFIX = "hoodie.deltastreamer.schemaprovider.registry.sourceUrlSuffix";
    private static final String SCHEMA_REGISTRY_TARGET_URL_SUFFIX = "hoodie.deltastreamer.schemaprovider.registry.targetUrlSuffix";
    private static final String TABLES_TO_BE_INGESTED_PROP = "hoodie.deltastreamer.ingestion.tablesToBeIngested";
    private static final String INGESTION_PREFIX = "hoodie.deltastreamer.ingestion.";
    private static final String INGESTION_CONFIG_SUFFIX = ".configFile";
    private static final String DEFAULT_CONFIG_FILE_NAME_SUFFIX = "_config.properties";
    private static final String TARGET_BASE_PATH_PROP = "hoodie.deltastreamer.ingestion.targetBasePath";
    private static final String LOCAL_SPARK_MASTER = "local[2]";
    private static final String FILE_DELIMITER = "/";
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
