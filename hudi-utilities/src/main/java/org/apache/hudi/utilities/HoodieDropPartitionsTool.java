/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HiveSyncConfigHolder;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.sync.common.HoodieSyncConfig;
import org.apache.hudi.table.HoodieSparkTable;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import scala.Tuple2;

/**
 * A tool with spark-submit to drop Hudi table partitions.
 *
 * <p>
 * You can dry run this tool with the following command to look and print for the table partitions and corresponding data files which will be deleted.
 * ```
 * spark-submit \
 * --class org.apache.hudi.utilities.HoodieDropPartitionsTool \
 * --packages org.apache.spark:spark-avro_2.11:2.4.4 \
 * --master local[*]
 * --driver-memory 1g \
 * --executor-memory 1g \
 * $HUDI_DIR/hudi/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.11-0.11.0-SNAPSHOT.jar \
 * --base-path basePath \
 * --table-name tableName \
 * --mode dry_run \
 * --partitions partition1,partition2
 * ```
 *
 * <p>
 *
 * You can delete the table partitions with '--mode delete'
 *
 * - DELETE ("delete"): This tool will mask/tombstone these partitions and corresponding data files and let cleaner delete these files later.
 * - Also you can set --sync-hive-meta to sync current drop partition into hive
 * <p>
 * Example command:
 * ```
 * spark-submit \
 * --class org.apache.hudi.utilities.HoodieDropPartitionsTool \
 * --packages org.apache.spark:spark-avro_2.11:2.4.4 \
 * --master local[*]
 * --driver-memory 1g \
 * --executor-memory 1g \
 * $HUDI_DIR/hudi/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.11-0.11.0-SNAPSHOT.jar \
 * --base-path basePath \
 * --table-name tableName \
 * --mode delete \
 * --partitions partition1,partition2
 * ```
 *
 * Also you can use --help to find more configs to use.
 */
public class HoodieDropPartitionsTool implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(HoodieDropPartitionsTool.class);
  // Spark context
  private final transient JavaSparkContext jsc;
  // config
  private final Config cfg;
  // Properties with source, hoodie client, key generator etc.
  private TypedProperties props;

  private final HoodieTableMetaClient metaClient;

  public HoodieDropPartitionsTool(JavaSparkContext jsc, Config cfg) {
    this.jsc = jsc;
    this.cfg = cfg;

    this.props = cfg.propsFilePath == null
        ? UtilHelpers.buildProperties(cfg.configs)
        : readConfigFromFileSystem(jsc, cfg);
    this.metaClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()))
        .setBasePath(cfg.basePath)
        .setLoadActiveTimelineOnLoad(true)
        .build();
  }

  /**
   * Reads config from the file system.
   *
   * @param jsc {@link JavaSparkContext} instance.
   * @param cfg {@link Config} instance.
   * @return the {@link TypedProperties} instance.
   */
  private TypedProperties readConfigFromFileSystem(JavaSparkContext jsc, Config cfg) {
    return UtilHelpers.readConfig(jsc.hadoopConfiguration(), new Path(cfg.propsFilePath), cfg.configs)
        .getProps(true);
  }

  public enum Mode {
    // Mask/Tombstone these partitions and corresponding data files and let cleaner delete these files later.
    DELETE,
    // Dry run by looking for the table partitions and corresponding data files which will be deleted.
    DRY_RUN
  }

  public static class Config implements Serializable {
    @Parameter(names = {"--base-path", "-sp"}, description = "Base path for the table", required = true)
    public String basePath = null;
    @Parameter(names = {"--mode", "-m"}, description = "Set job mode: "
        + "Set \"delete\" means mask/tombstone these partitions and corresponding data files table partitions and let cleaner delete these files later;"
        + "Set \"dry_run\" means only looking for the table partitions will be deleted and corresponding data files.", required = true)
    public String runningMode = null;
    @Parameter(names = {"--table-name", "-tn"}, description = "Table name", required = true)
    public String tableName = null;
    @Parameter(names = {"--partitions", "-p"}, description = "Comma separated list of partitions to delete.", required = true)
    public String partitions = null;
    @Parameter(names = {"--parallelism", "-pl"}, description = "Parallelism for hoodie insert/upsert/delete", required = false)
    public int parallelism = 1500;
    @Parameter(names = {"--instant-time", "-it"}, description = "instant time for delete table partitions operation.", required = false)
    public String instantTime = null;
    @Parameter(names = {"--sync-hive-meta", "-sync"}, description = "Sync information to HMS.", required = false)
    public boolean syncToHive = false;
    @Parameter(names = {"--hive-database", "-db"}, description = "Database to sync to.", required = false)
    public String hiveDataBase = null;
    @Parameter(names = {"--hive-table-name"}, description = "Table to sync to.", required = false)
    public String hiveTableName = null;
    @Parameter(names = {"--hive-user-name", "-user"}, description = "hive user name to use.", required = false)
    public String hiveUserName = "hive";
    @Parameter(names = {"--hive-pass-word", "-pass"}, description = "hive password to use.", required = false)
    public String hivePassWord = "hive";
    @Parameter(names = {"--hive-jdbc-url", "-jdbc"}, description = "hive url to use.", required = false)
    public String hiveURL = "jdbc:hive2://localhost:10000";
    @Parameter(names = {"--hive-partition-field"}, description = "Comma separated list of field in the hive table to use for determining hive partition columns.", required = false)
    public String hivePartitionsField = "";
    @Parameter(names = {"--hive-sync-use-jdbc"}, description = "Use JDBC when hive synchronization.", required = false)
    public boolean hiveUseJdbc = true;
    @Parameter(names = {"--hive-metastore-uris"}, description = "hive meta store uris to use.", required = false)
    public String hiveHMSUris = null;
    @Parameter(names = {"--hive-sync-mode"}, description = "Mode to choose for Hive ops. Valid values are hms, jdbc and hiveql.", required = false)
    public String hiveSyncMode = "hms";
    @Parameter(names = {"--hive-sync-ignore-exception"}, description = "Ignore hive sync exception.", required = false)
    public boolean hiveSyncIgnoreException = false;
    @Parameter(names = {"--hive-partition-value-extractor-class"}, description = "Class which implements PartitionValueExtractor to extract the partition values,"
        + " default 'SlashEncodedDayPartitionValueExtractor'.", required = false)
    public String partitionValueExtractorClass = "org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor";
    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = false)
    public String sparkMaster = null;
    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = false)
    public String sparkMemory = "1g";
    @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
        + "hoodie client for deleting table partitions")
    public String propsFilePath = null;
    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated",
        splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();
    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Override
    public String toString() {
      return "HoodieDropPartitionsToolConfig {\n"
          + "   --base-path " + basePath + ", \n"
          + "   --mode " + runningMode + ", \n"
          + "   --table-name " + tableName + ", \n"
          + "   --partitions " + partitions + ", \n"
          + "   --parallelism " + parallelism + ", \n"
          + "   --instantTime " + instantTime + ", \n"
          + "   --sync-hive-meta " + syncToHive + ", \n"
          + "   --hive-database " + hiveDataBase + ", \n"
          + "   --hive-table-name " + hiveTableName + ", \n"
          + "   --hive-user-name " + "Masked" + ", \n"
          + "   --hive-pass-word " + "Masked" + ", \n"
          + "   --hive-jdbc-url " + hiveURL + ", \n"
          + "   --hive-partition-field " + hivePartitionsField + ", \n"
          + "   --hive-sync-use-jdbc " + hiveUseJdbc + ", \n"
          + "   --hive-metastore-uris " + hiveHMSUris + ", \n"
          + "   --hive-sync-ignore-exception " + hiveSyncIgnoreException + ", \n"
          + "   --hive-partition-value-extractor-class " + partitionValueExtractorClass + ", \n"
          + "   --spark-master " + sparkMaster + ", \n"
          + "   --spark-memory " + sparkMemory + ", \n"
          + "   --props " + propsFilePath + ", \n"
          + "   --hoodie-conf " + configs
          + "\n}";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Config config = (Config) o;
      return basePath.equals(config.basePath)
          && Objects.equals(runningMode, config.runningMode)
          && Objects.equals(tableName, config.tableName)
          && Objects.equals(partitions, config.partitions)
          && Objects.equals(instantTime, config.instantTime)
          && Objects.equals(syncToHive, config.syncToHive)
          && Objects.equals(hiveDataBase, config.hiveDataBase)
          && Objects.equals(hiveTableName, config.hiveTableName)
          && Objects.equals(hiveUserName, config.hiveUserName)
          && Objects.equals(hivePassWord, config.hivePassWord)
          && Objects.equals(hiveURL, config.hiveURL)
          && Objects.equals(hivePartitionsField, config.hivePartitionsField)
          && Objects.equals(hiveUseJdbc, config.hiveUseJdbc)
          && Objects.equals(hiveHMSUris, config.hiveHMSUris)
          && Objects.equals(partitionValueExtractorClass, config.partitionValueExtractorClass)
          && Objects.equals(sparkMaster, config.sparkMaster)
          && Objects.equals(sparkMemory, config.sparkMemory)
          && Objects.equals(propsFilePath, config.propsFilePath)
          && Objects.equals(configs, config.configs)
          && Objects.equals(hiveSyncIgnoreException, config.hiveSyncIgnoreException);
    }

    @Override
    public int hashCode() {
      return Objects.hash(basePath, runningMode, tableName, partitions, instantTime,
          syncToHive, hiveDataBase, hiveTableName, hiveUserName, hivePassWord, hiveURL,
          hivePartitionsField, hiveUseJdbc, hiveHMSUris, partitionValueExtractorClass,
          sparkMaster, sparkMemory, propsFilePath, configs, hiveSyncIgnoreException, help);
    }
  }

  public static void main(String[] args) {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    SparkConf sparkConf = UtilHelpers.buildSparkConf("Hoodie-Drop-Table-Partitions", cfg.sparkMaster);
    sparkConf.set("spark.executor.memory", cfg.sparkMemory);
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    HoodieDropPartitionsTool tool = new HoodieDropPartitionsTool(jsc, cfg);
    try {
      tool.run();
    } catch (Throwable throwable) {
      LOG.error("Fail to run deleting table partitions for " + cfg.toString(), throwable);
    } finally {
      jsc.stop();
    }
  }

  public void run() {
    try {
      if (StringUtils.isNullOrEmpty(cfg.instantTime)) {
        cfg.instantTime = HoodieActiveTimeline.createNewInstantTime();
      }
      LOG.info(cfg.toString());

      Mode mode = Mode.valueOf(cfg.runningMode.toUpperCase());
      switch (mode) {
        case DELETE:
          LOG.info(" ****** The Hoodie Drop Partitions Tool is in delete mode ****** ");
          doDeleteTablePartitions();
          syncToHiveIfNecessary();
          break;
        case DRY_RUN:
          LOG.info(" ****** The Hoodie Drop Partitions Tool is in dry-run mode ****** ");
          dryRun();
          break;
        default:
          LOG.info("Unsupported running mode [" + cfg.runningMode + "], quit the job directly");
      }
    } catch (Exception e) {
      throw new HoodieException("Unable to delete table partitions in " + cfg.basePath, e);
    }
  }

  public void dryRun() {
    try (SparkRDDWriteClient<HoodieRecordPayload> client =  UtilHelpers.createHoodieClient(jsc, cfg.basePath, "", cfg.parallelism, Option.empty(), props)) {
      HoodieSparkTable<HoodieRecordPayload> table = HoodieSparkTable.create(client.getConfig(), client.getEngineContext());
      List<String> parts = Arrays.asList(cfg.partitions.split(","));
      Map<String, List<String>> partitionToReplaceFileIds = jsc.parallelize(parts, parts.size()).distinct()
          .mapToPair(partitionPath -> new Tuple2<>(partitionPath, table.getSliceView().getLatestFileSlices(partitionPath).map(fg -> fg.getFileId()).distinct().collect(Collectors.toList())))
          .collectAsMap();
      printDeleteFilesInfo(partitionToReplaceFileIds);
    }
  }

  private void syncToHiveIfNecessary() {
    if (cfg.syncToHive) {
      HiveSyncConfig hiveSyncConfig = buildHiveSyncProps();
      syncHive(hiveSyncConfig);
    }
  }

  private void doDeleteTablePartitions() {

    // need to do commit in SparkDeletePartitionCommitActionExecutor#execute
    this.props.put(HoodieWriteConfig.AUTO_COMMIT_ENABLE.key(), "true");
    try (SparkRDDWriteClient<HoodieRecordPayload> client =  UtilHelpers.createHoodieClient(jsc, cfg.basePath, "", cfg.parallelism, Option.empty(), props)) {
      List<String> partitionsToDelete = Arrays.asList(cfg.partitions.split(","));
      client.startCommitWithTime(cfg.instantTime, HoodieTimeline.REPLACE_COMMIT_ACTION);
      client.deletePartitions(partitionsToDelete, cfg.instantTime);
    }
  }

  private HiveSyncConfig buildHiveSyncProps() {
    verifyHiveConfigs();
    TypedProperties props = new TypedProperties();
    props.put(DataSourceWriteOptions.HIVE_DATABASE().key(), cfg.hiveDataBase);
    props.put(DataSourceWriteOptions.HIVE_TABLE().key(), cfg.hiveTableName);
    props.put(DataSourceWriteOptions.HIVE_USER().key(), cfg.hiveUserName);
    props.put(DataSourceWriteOptions.HIVE_PASS().key(), cfg.hivePassWord);
    props.put(DataSourceWriteOptions.HIVE_URL().key(), cfg.hiveURL);
    props.put(DataSourceWriteOptions.HIVE_PARTITION_FIELDS().key(), cfg.hivePartitionsField);
    props.put(DataSourceWriteOptions.HIVE_USE_JDBC().key(), cfg.hiveUseJdbc);
    props.put(DataSourceWriteOptions.HIVE_SYNC_MODE().key(), cfg.hiveSyncMode);
    props.put(DataSourceWriteOptions.HIVE_IGNORE_EXCEPTIONS().key(), cfg.hiveSyncIgnoreException);
    props.put(DataSourceWriteOptions.HIVE_PASS().key(), cfg.hivePassWord);
    props.put(HiveSyncConfig.META_SYNC_BASE_PATH, cfg.basePath);
    props.put(HiveSyncConfig.META_SYNC_BASE_FILE_FORMAT, "PARQUET");
    props.put(DataSourceWriteOptions.PARTITIONS_TO_DELETE().key(), cfg.partitions);
    props.put(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS().key(), cfg.partitionValueExtractorClass);
    props.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), cfg.hivePartitionsField);

    return new HiveSyncConfig(props, new Configuration());
  }

  private void verifyHiveConfigs() {
    ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(cfg.hiveDataBase), "Hive database name couldn't be null or empty when enable sync meta, please set --hive-database/-db.");
    ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(cfg.hiveTableName), "Hive table name couldn't be null or empty when enable sync meta, please set --hive-table-name/-tn.");
  }

  private void syncHive(HiveSyncConfig hiveSyncConfig) {
    LOG.info("Syncing target hoodie table with hive table("
        + hiveSyncConfig.getStringOrDefault(HoodieSyncConfig.META_SYNC_TABLE_NAME)
        + "). Hive metastore URL :"
        + hiveSyncConfig.getStringOrDefault(HiveSyncConfigHolder.HIVE_URL)
        + ", basePath :" + cfg.basePath);
    LOG.info("Hive Sync Conf => " + hiveSyncConfig.toString());
    FileSystem fs = HadoopFSUtils.getFs(cfg.basePath, jsc.hadoopConfiguration());
    HiveConf hiveConf = new HiveConf();
    if (!StringUtils.isNullOrEmpty(cfg.hiveHMSUris)) {
      hiveConf.set("hive.metastore.uris", cfg.hiveHMSUris);
    }
    hiveConf.addResource(fs.getConf());
    LOG.info("Hive Conf => " + hiveConf.getAllProperties().toString());
    try (HiveSyncTool hiveSyncTool = new HiveSyncTool(hiveSyncConfig.getProps(), hiveConf)) {
      hiveSyncTool.syncHoodieTable();
    }
  }

  /**
   * Prints the delete data files info.
   *
   * @param partitionToReplaceFileIds
   */
  private void printDeleteFilesInfo(Map<String, List<String>> partitionToReplaceFileIds) {
    LOG.info("Data files and partitions to delete : ");
    for (Map.Entry<String, List<String>> entry  : partitionToReplaceFileIds.entrySet()) {
      LOG.info(String.format("Partitions : %s, corresponding data file IDs : %s", entry.getKey(), entry.getValue()));
    }
  }
}
