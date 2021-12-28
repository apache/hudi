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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.ReplaceArchivalHelper;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;

/**
 * A tool with spark-submit to drop Hudi table partitions
 * <p>
 * You can dry run this tool with the following command:
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
 * <p>
 * You can specify the running mode of the tool through `--mode`.
 * There are three modes of the {@link HoodieDropPartitionsTool}:
 * - DELETE_PARTITIONS_LAZY ("delete_partitions_lazy"): This tool will mask/tombstone these partitions and corresponding data files and let cleaner delete these files later.
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
 * --mode delete_partitions_lazy \
 * --partitions partition1,partition2
 * ```
 * <p>
 * - DELETE_PARTITIONS_EAGER ("delete_partitions_eager"): This tool will mask/tombstone these partitions and corresponding data files and and delete these data files immediately.
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
 * --mode delete_partitions_eager \
 * --partitions partition1,partition2
 * ```
 * <p>
 * - DRY_RUN ("dry_run"): look and print for the table partitions and corresponding data files which will be deleted.
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
 * --mode dry_run \
 * --partitions partition1,partition2
 * ```
 *
 * Also you can use --help to find more configs to use.
 */
public class HoodieDropPartitionsTool {

  private static final Logger LOG = LogManager.getLogger(HoodieDropPartitionsTool.class);
  // Spark context
  private final JavaSparkContext jsc;
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
        .setConf(jsc.hadoopConfiguration()).setBasePath(cfg.basePath)
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
    DELETE_PARTITIONS_LAZY,
    // Mask/Tombstone these partitions and corresponding data files. And delete these data files immediately.
    DELETE_PARTITIONS_EAGER,
    // Dry run by looking for the table partitions and corresponding data files which will be deleted.
    DRY_RUN
  }

  public static class Config implements Serializable {
    @Parameter(names = {"--base-path", "-sp"}, description = "Base path for the table", required = true)
    public String basePath = null;
    @Parameter(names = {"--mode", "-m"}, description = "Set job mode: "
        + "Set \"delete_partitions_lazy\" means mask/tombstone these partitions and corresponding data files table partitions and let cleaner delete these files later;"
        + "Set \"delete_partitions_eager\" means delete data files and corresponding directory directly through file system but don't change hoodie meta files;"
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
  }

  private String getConfigDetails() {
    final StringBuilder sb = new StringBuilder("HoodieDropPartitionsToolConfig {\n");
    sb.append("   --base-path ").append(cfg.basePath).append(", \n");
    sb.append("   --mode ").append(cfg.runningMode).append(", \n");
    sb.append("   --table-name ").append(cfg.tableName).append(", \n");
    sb.append("   --partitions ").append(cfg.partitions).append(", \n");
    sb.append("   --parallelism ").append(cfg.parallelism).append(", \n");
    sb.append("   --instantTime ").append(cfg.instantTime).append(", \n");
    sb.append("   --sync-hive-meta ").append(cfg.syncToHive).append(", \n");
    sb.append("   --hive-database ").append(cfg.hiveDataBase).append(", \n");
    sb.append("   --hive-table-name ").append(cfg.hiveTableName).append(", \n");
    sb.append("   --hive-user-name ").append("Masked").append(", \n");
    sb.append("   --hive-pass-word ").append("Masked").append(", \n");
    sb.append("   --hive-jdbc-url ").append(cfg.hiveURL).append(", \n");
    sb.append("   --hive-partition-field ").append(cfg.hivePartitionsField).append(", \n");
    sb.append("   --hive-sync-use-jdbc ").append(cfg.hiveUseJdbc).append(", \n");
    sb.append("   --hive-metastore-uris ").append(cfg.hiveHMSUris).append(", \n");
    sb.append("   --hive-sync-mode ").append(cfg.hiveSyncMode).append(", \n");
    sb.append("   --hive-sync-ignore-exception ").append(cfg.hiveSyncIgnoreException).append(", \n");
    sb.append("   --hive-partition-value-extractor-class ").append(cfg.partitionValueExtractorClass).append(", \n");
    sb.append("   --spark-master ").append(cfg.sparkMaster).append(", \n");
    sb.append("   --spark-memory ").append(cfg.sparkMemory).append(", \n");
    sb.append("   --props ").append(cfg.propsFilePath).append(", \n");
    sb.append("   --hoodie-conf ").append(cfg.configs);
    sb.append("\n}");
    return sb.toString();
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
      LOG.error("Fail to run deleting table partitions for " + tool.getConfigDetails(), throwable);
    } finally {
      jsc.stop();
    }
  }

  public void run() {
    try {
      Mode mode = Mode.valueOf(cfg.runningMode.toUpperCase());
      switch (mode) {
        case DELETE_PARTITIONS_LAZY:
          LOG.info(" ****** The Hoodie Drop Partitions Tool is in delete_partition_lazy mode ******");
          doDeleteTablePartitionsLazy();
          break;
        case DELETE_PARTITIONS_EAGER:
          LOG.info(" ****** The Hoodie Drop Partitions Tool is in delete_partition_eager mode ******");
          doDeleteTablePartitionsEager();
          break;
        case DRY_RUN:
          LOG.info(" ****** The Hoodie Drop Partitions Tool is in dry-run mode ******");
          dryRun();
          break;
        default:
          LOG.info("Unsupported running mode [" + cfg.runningMode + "], quit the job directly");
      }
    } catch (Exception e) {
      throw new HoodieException("Unable to delete table partitions in " + cfg.basePath, e);
    }
  }

  public void doDeleteTablePartitionsLazy() {
    doDeleteTablePartitions();
    syncToHiveIfNecessary();
  }

  public void doDeleteTablePartitionsEager() {
    doDeleteTablePartitions();
    deleteDataFiles();
    syncToHiveIfNecessary();
  }

  public void dryRun() {
    try (SparkRDDWriteClient<HoodieRecordPayload> client =  UtilHelpers.createHoodieClient(jsc, cfg.basePath, "", cfg.parallelism, Option.empty(), props)) {
      HoodieSparkTable<HoodieRecordPayload> table = HoodieSparkTable.create(client.getConfig(), client.getEngineContext());
      LOG.info(getConfigDetails());
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

  public void doDeleteTablePartitions() {
    try (SparkRDDWriteClient<HoodieRecordPayload> client =  UtilHelpers.createHoodieClient(jsc, cfg.basePath, "", cfg.parallelism, Option.empty(), props)) {
      List<String> partitionsToDelete = Arrays.asList(cfg.partitions.split(","));
      if (StringUtils.isNullOrEmpty(cfg.instantTime)) {
        cfg.instantTime = HoodieActiveTimeline.createNewInstantTime();
      }
      LOG.info(getConfigDetails());
      client.startCommitWithTime(cfg.instantTime, HoodieTimeline.REPLACE_COMMIT_ACTION);
      HoodieWriteResult hoodieWriteResult = client.deletePartitions(partitionsToDelete, cfg.instantTime);

      // No need to do handle errors operation.
      // Because the writeStatuses from deletePartitions are jsc.emptyRDD().
    }
  }

  private void deleteDataFiles() {
    try (SparkRDDWriteClient<HoodieRecordPayload> client =  UtilHelpers.createHoodieClient(jsc, cfg.basePath, "", cfg.parallelism, Option.empty(), props)) {
      HoodieTimeline completedReplaceTimeline = metaClient.getActiveTimeline().reload().getCompletedReplaceTimeline();
      HoodieSparkTable<HoodieRecordPayload> table = HoodieSparkTable.create(client.getConfig(), client.getEngineContext());
      Option<HoodieInstant> instant = Option.fromJavaOptional(completedReplaceTimeline.getInstants().filter(in -> in.getTimestamp().equals(cfg.instantTime)).findFirst());
      instant.ifPresent(hoodieInstant -> deleteReplacedFileGroups(client.getEngineContext(), hoodieInstant, table));
    }
  }

  private boolean deleteReplacedFileGroups(HoodieEngineContext context, HoodieInstant instant, HoodieSparkTable table) {
    if (!instant.isCompleted() || !HoodieTimeline.REPLACE_COMMIT_ACTION.equals(instant.getAction())) {
      // only delete files for completed replace instants
      return true;
    }
    TableFileSystemView fileSystemView = table.getFileSystemView();
    List<String> replacedPartitions = getReplacedPartitions(instant);
    int replacedFiles = getReplacedFiles(instant);
    return deleteReplacedFileGroups(context, metaClient, fileSystemView, instant, replacedPartitions, Math.min(cfg.parallelism, replacedFiles));
  }

  private List<String> getReplacedPartitions(HoodieInstant instant) {
    try {
      HoodieReplaceCommitMetadata metadata = HoodieReplaceCommitMetadata.fromBytes(
          metaClient.getActiveTimeline().getInstantDetails(instant).get(),
          HoodieReplaceCommitMetadata.class);

      return new ArrayList<>(metadata.getPartitionToReplaceFileIds().keySet());
    } catch (IOException e) {
      throw new HoodieCommitException("Cannot delete replace files", e);
    }
  }

  private int getReplacedFiles(HoodieInstant instant) {
    try {
      HoodieReplaceCommitMetadata metadata = HoodieReplaceCommitMetadata.fromBytes(
          metaClient.getActiveTimeline().getInstantDetails(instant).get(),
          HoodieReplaceCommitMetadata.class);
      int fileNumbers = 0;
      for (List<String> files: metadata.getPartitionToReplaceFileIds().values()) {
        fileNumbers += files.size();
      }
      return fileNumbers;
    } catch (IOException e) {
      throw new HoodieCommitException("Cannot delete replace files", e);
    }
  }

  public static boolean deleteReplacedFileGroups(HoodieEngineContext context, HoodieTableMetaClient metaClient,
                                                 TableFileSystemView fileSystemView,
                                                 HoodieInstant instant, List<String> replacedPartitions, int parallelism) {
    context.setJobStatus(ReplaceArchivalHelper.class.getSimpleName(), "Delete replaced file groups");

    context.flatMapAndForeach(replacedPartitions, partition -> {
      return fileSystemView.getReplacedFileGroupsBeforeOrOn(instant.getTimestamp(), partition)
          .flatMap(HoodieFileGroup::getAllRawFileSlices);
    }, fileSlice -> deleteFileSlice(fileSlice, metaClient, instant), replacedPartitions.size(), parallelism);
    return true;
  }

  private static boolean deleteFileSlice(FileSlice fileSlice, HoodieTableMetaClient metaClient, HoodieInstant instant) {
    boolean baseFileDeleteSuccess = fileSlice.getBaseFile().map(baseFile ->
        deletePath(new Path(baseFile.getPath()), metaClient, instant)).orElse(true);

    boolean logFileSuccess = fileSlice.getLogFiles().map(logFile ->
        deletePath(logFile.getPath(), metaClient, instant)).allMatch(x -> x);
    return baseFileDeleteSuccess & logFileSuccess;
  }

  private static boolean deletePath(Path path, HoodieTableMetaClient metaClient, HoodieInstant instant) {
    try {
      LOG.info("Deleting " + path);
      metaClient.getFs().delete(path);
      return true;
    } catch (IOException e) {
      LOG.error("unable to delete file groups that are replaced", e);
      return false;
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
    props.put(DataSourceWriteOptions.PARTITIONS_TO_DELETE().key(), cfg.partitions);
    props.put(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS().key(), cfg.partitionValueExtractorClass);
    props.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), cfg.hivePartitionsField);

    return DataSourceUtils.buildHiveSyncConfig(props, cfg.basePath, "PARQUET");
  }

  private void verifyHiveConfigs() {
    ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(cfg.hiveDataBase), "Hive database name couldn't be null or empty when enable sync meta, please set --hive-database/-db.");
    ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(cfg.hiveTableName), "Hive table name couldn't be null or empty when enable sync meta, please set --hive-table-name/-tn.");
  }

  private void syncHive(HiveSyncConfig hiveSyncConfig) {
    LOG.info("Syncing target hoodie table with hive table("
        + hiveSyncConfig.tableName
        + "). Hive metastore URL :"
        + hiveSyncConfig.jdbcUrl
        + ", basePath :" + cfg.basePath);
    LOG.info("Hive Sync Conf => " + hiveSyncConfig.toString());
    FileSystem fs = FSUtils.getFs(cfg.basePath, jsc.hadoopConfiguration());
    HiveConf hiveConf = new HiveConf();
    if(!StringUtils.isNullOrEmpty(cfg.hiveHMSUris)) {
      hiveConf.set("hive.metastore.uris", cfg.hiveHMSUris);
    }
    hiveConf.addResource(fs.getConf());
    LOG.info("Hive Conf => " + hiveConf.getAllProperties().toString());
    new HiveSyncTool(hiveSyncConfig, hiveConf, fs).syncHoodieTable();
  }

  /**
   * Prints the delete data files info.
   *
   * @param partitionToReplaceFileIds
   */
  private void printDeleteFilesInfo(Map<String, List<String>> partitionToReplaceFileIds) {
    LOG.info("Data files and partitions to delete : ");
    for (Map.Entry<String, List<String>> entry  : partitionToReplaceFileIds.entrySet()) {
      LOG.info(String.format("Partitions : %s, corresponding data file IDs : $%s", entry.getKey(), entry.getValue()));
    }
  }
}
