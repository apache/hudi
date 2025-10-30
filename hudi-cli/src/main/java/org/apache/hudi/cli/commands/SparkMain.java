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

package org.apache.hudi.cli.commands;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.cli.ArchiveExecutorUtils;
import org.apache.hudi.cli.utils.SparkUtil;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.PartitionPathEncodeUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieBootstrapConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.exception.HoodieSavepointException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.action.compact.strategy.UnBoundedCompactionStrategy;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.table.upgrade.SparkUpgradeDowngradeHelper;
import org.apache.hudi.table.upgrade.UpgradeDowngrade;
import org.apache.hudi.utilities.HoodieCleaner;
import org.apache.hudi.utilities.HoodieClusteringJob;
import org.apache.hudi.utilities.HoodieCompactionAdminTool;
import org.apache.hudi.utilities.HoodieCompactionAdminTool.Operation;
import org.apache.hudi.utilities.HoodieCompactor;
import org.apache.hudi.utilities.streamer.BootstrapExecutor;
import org.apache.hudi.utilities.streamer.HoodieStreamer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.hudi.DeDupeType;
import org.apache.spark.sql.hudi.DedupeSparkJob;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;
import static org.apache.hudi.utilities.UtilHelpers.EXECUTE;
import static org.apache.hudi.utilities.UtilHelpers.SCHEDULE;
import static org.apache.hudi.utilities.UtilHelpers.SCHEDULE_AND_EXECUTE;
import static org.apache.hudi.utilities.UtilHelpers.buildProperties;
import static org.apache.hudi.utilities.UtilHelpers.readConfig;

/**
 * This class deals with initializing spark context based on command entered to hudi-cli.
 */
public class SparkMain {

  private static final Logger LOG = LoggerFactory.getLogger(SparkMain.class);

  /**
   * Commands.
   */
  enum SparkCommand {
    BOOTSTRAP(21), ROLLBACK(6), DEDUPLICATE(8), ROLLBACK_TO_SAVEPOINT(6), SAVEPOINT(7),
    IMPORT(13), UPSERT(13), COMPACT_SCHEDULE(6), COMPACT_RUN(10), COMPACT_SCHEDULE_AND_EXECUTE(9),
    COMPACT_UNSCHEDULE_PLAN(9), COMPACT_UNSCHEDULE_FILE(10), COMPACT_VALIDATE(7), COMPACT_REPAIR(8),
    CLUSTERING_SCHEDULE(6), CLUSTERING_RUN(9), CLUSTERING_SCHEDULE_AND_EXECUTE(8), CLEAN(5),
    DELETE_MARKER(5), DELETE_SAVEPOINT(5), UPGRADE(5), DOWNGRADE(5),
    REPAIR_DEPRECATED_PARTITION(4), RENAME_PARTITION(6), ARCHIVE(8);

    private final int minArgsCount;

    SparkCommand(int minArgsCount) {
      this.minArgsCount = minArgsCount;
    }

    void assertEq(int factArgsCount) {
      ValidationUtils.checkArgument(factArgsCount == minArgsCount);
    }

    void assertGtEq(int factArgsCount) {
      ValidationUtils.checkArgument(factArgsCount >= minArgsCount);
    }

    List<String> makeConfigs(String[] args) {
      List<String> configs = new ArrayList<>();
      if (args.length > minArgsCount) {
        configs.addAll(Arrays.asList(args).subList(minArgsCount, args.length));
      }
      return configs;
    }

    String getPropsFilePath(String[] args) {
      return (args.length >= minArgsCount && !isNullOrEmpty(args[minArgsCount - 1]))
          ? args[minArgsCount - 1] : null;
    }
  }

  public static void addAppArgs(SparkLauncher sparkLauncher, SparkMain.SparkCommand cmd, String... args) {
    //cmd is going to be the first arg so that is why it is minArgsCount - 1
    ValidationUtils.checkArgument(args.length == cmd.minArgsCount - 1, "For developers only: App args does not match minArgsCount");
    sparkLauncher.addAppArgs(cmd.toString());
    sparkLauncher.addAppArgs(args);
  }

  public static void main(String[] args) {
    ValidationUtils.checkArgument(args.length >= 4);
    final String commandString = args[0];
    LOG.info("Invoking SparkMain: {}", commandString);
    final SparkCommand cmd = SparkCommand.valueOf(commandString);

    JavaSparkContext jsc = SparkUtil.initJavaSparkContext("hoodie-cli-" + commandString,
        Option.of(args[1]), Option.of(args[2]));

    int returnCode = 0;
    try {
      cmd.assertGtEq(args.length);
      List<String> configs = cmd.makeConfigs(args);
      String propsFilePath = cmd.getPropsFilePath(args);
      switch (cmd) {
        case ROLLBACK:
          cmd.assertEq(args.length);
          returnCode = rollback(jsc, args[3], args[4], Boolean.parseBoolean(args[5]));
          break;
        case DEDUPLICATE:
          cmd.assertEq(args.length);
          returnCode = deduplicatePartitionPath(jsc, args[3], args[4], args[5], Boolean.parseBoolean(args[6]), args[7]);
          break;
        case ROLLBACK_TO_SAVEPOINT:
          cmd.assertEq(args.length);
          returnCode = rollbackToSavepoint(jsc, args[3], args[4], Boolean.parseBoolean(args[5]));
          break;
        case IMPORT:
        case UPSERT:
          throw new HoodieNotSupportedException("This command is no longer supported. Use HoodieStreamer utility instead.");
        case COMPACT_RUN:
          returnCode = compact(jsc, args[3], args[4], args[5], Integer.parseInt(args[6]), args[7],
              Integer.parseInt(args[8]), HoodieCompactor.EXECUTE, propsFilePath, configs);
          break;
        case COMPACT_SCHEDULE_AND_EXECUTE:
          returnCode = compact(jsc, args[3], args[4], null, Integer.parseInt(args[5]), args[6],
              Integer.parseInt(args[7]), HoodieCompactor.SCHEDULE_AND_EXECUTE, propsFilePath, configs);
          break;
        case COMPACT_SCHEDULE:
          returnCode = compact(jsc, args[3], args[4], null, 1, "", 0, HoodieCompactor.SCHEDULE, propsFilePath, configs);
          break;
        case COMPACT_VALIDATE:
          cmd.assertEq(args.length);
          doCompactValidate(jsc, args[3], args[4], args[5], Integer.parseInt(args[6]));
          returnCode = 0;
          break;
        case COMPACT_REPAIR:
          cmd.assertEq(args.length);
          doCompactRepair(jsc, args[3], args[4], args[5], Integer.parseInt(args[6]), Boolean.parseBoolean(args[7]));
          returnCode = 0;
          break;
        case COMPACT_UNSCHEDULE_FILE:
          cmd.assertEq(args.length);
          doCompactUnscheduleFile(jsc, args[3], args[4], args[5], args[6], Integer.parseInt(args[7]),
              Boolean.parseBoolean(args[8]), Boolean.parseBoolean(args[9]));
          returnCode = 0;
          break;
        case COMPACT_UNSCHEDULE_PLAN:
          cmd.assertEq(args.length);
          doCompactUnschedule(jsc, args[3], args[4], args[5], Integer.parseInt(args[6]),
              Boolean.parseBoolean(args[7]), Boolean.parseBoolean(args[8]));
          returnCode = 0;
          break;
        case CLUSTERING_RUN:
          returnCode = cluster(jsc, args[3], args[4], args[5], Integer.parseInt(args[6]), args[2],
              Integer.parseInt(args[7]), EXECUTE, propsFilePath, configs);
          break;
        case CLUSTERING_SCHEDULE_AND_EXECUTE:
          returnCode = cluster(jsc, args[3], args[4], null, Integer.parseInt(args[5]), args[2],
              Integer.parseInt(args[6]), SCHEDULE_AND_EXECUTE, propsFilePath, configs);
          break;
        case CLUSTERING_SCHEDULE:
          returnCode = cluster(jsc, args[3], args[4], null, 1, args[2], 0, SCHEDULE, propsFilePath, configs);
          break;
        case CLEAN:
          clean(jsc, args[3], propsFilePath, configs);
          break;
        case SAVEPOINT:
          cmd.assertEq(args.length);
          returnCode = createSavepoint(jsc, args[3], args[4], args[5], args[6]);
          break;
        case DELETE_MARKER:
          cmd.assertEq(args.length);
          returnCode = deleteMarker(jsc, args[3], args[4]);
          break;
        case DELETE_SAVEPOINT:
          cmd.assertEq(args.length);
          returnCode = deleteSavepoint(jsc, args[3], args[4]);
          break;
        case BOOTSTRAP:
          returnCode = doBootstrap(jsc, args[3], args[4], args[5], args[6], args[7], args[8], args[9], args[10],
              args[11], args[12], args[13], args[14], args[15], args[16], args[17], args[18], args[19], propsFilePath, configs);
          break;
        case UPGRADE:
        case DOWNGRADE:
          cmd.assertEq(args.length);
          returnCode = upgradeOrDowngradeTable(jsc, args[3], args[4]);
          break;
        case REPAIR_DEPRECATED_PARTITION:
          cmd.assertEq(args.length);
          returnCode = repairDeprecatedPartition(jsc, args[3]);
          break;
        case RENAME_PARTITION:
          cmd.assertEq(args.length);
          returnCode = renamePartition(jsc, args[3], args[4], args[5]);
          break;
        case ARCHIVE:
          cmd.assertEq(args.length);
          returnCode = archive(jsc, Integer.parseInt(args[3]), Integer.parseInt(args[4]), Integer.parseInt(args[5]), Boolean.parseBoolean(args[6]), args[7]);
          break;
        default:
          break;
      }
    } catch (Exception exception) {
      LOG.error("Fail to execute commandString", exception);
      returnCode = -1;
    } finally {
      jsc.stop();
    }
    System.exit(returnCode);
  }

  protected static void clean(JavaSparkContext jsc, String basePath, String propsFilePath,
                              List<String> configs) {
    HoodieCleaner.Config cfg = new HoodieCleaner.Config();
    cfg.basePath = basePath;
    cfg.propsFilePath = propsFilePath;
    cfg.configs = configs;
    new HoodieCleaner(cfg, jsc).run();
  }

  protected static int deleteMarker(JavaSparkContext jsc, String instantTime, String basePath) {
    try (SparkRDDWriteClient client = createHoodieClient(jsc, basePath, false)) {
      HoodieWriteConfig config = client.getConfig();
      HoodieEngineContext context = client.getEngineContext();
      HoodieSparkTable table = HoodieSparkTable.create(config, context);
      client.validateAgainstTableProperties(table.getMetaClient().getTableConfig(), config);
      WriteMarkersFactory.get(config.getMarkersType(), table, instantTime)
          .quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());
      return 0;
    } catch (Exception e) {
      LOG.warn(String.format("Failed: Could not clean marker instantTime: \"%s\".", instantTime), e);
      return -1;
    }
  }

  private static void doCompactValidate(JavaSparkContext jsc, String basePath, String compactionInstant,
                                        String outputPath, int parallelism) throws Exception {
    HoodieCompactionAdminTool.Config cfg = new HoodieCompactionAdminTool.Config();
    cfg.basePath = basePath;
    cfg.operation = Operation.VALIDATE;
    cfg.outputPath = outputPath;
    cfg.compactionInstantTime = compactionInstant;
    cfg.parallelism = parallelism;
    new HoodieCompactionAdminTool(cfg).run(jsc);
  }

  private static void doCompactRepair(JavaSparkContext jsc, String basePath, String compactionInstant,
                                      String outputPath, int parallelism, boolean dryRun) throws Exception {
    HoodieCompactionAdminTool.Config cfg = new HoodieCompactionAdminTool.Config();
    cfg.basePath = basePath;
    cfg.operation = Operation.REPAIR;
    cfg.outputPath = outputPath;
    cfg.compactionInstantTime = compactionInstant;
    cfg.parallelism = parallelism;
    cfg.dryRun = dryRun;
    new HoodieCompactionAdminTool(cfg).run(jsc);
  }

  private static void doCompactUnschedule(JavaSparkContext jsc, String basePath, String compactionInstant,
                                          String outputPath, int parallelism, boolean skipValidation, boolean dryRun) throws Exception {
    HoodieCompactionAdminTool.Config cfg = new HoodieCompactionAdminTool.Config();
    cfg.basePath = basePath;
    cfg.operation = Operation.UNSCHEDULE_PLAN;
    cfg.outputPath = outputPath;
    cfg.compactionInstantTime = compactionInstant;
    cfg.parallelism = parallelism;
    cfg.dryRun = dryRun;
    cfg.skipValidation = skipValidation;
    new HoodieCompactionAdminTool(cfg).run(jsc);
  }

  private static void doCompactUnscheduleFile(JavaSparkContext jsc, String basePath, String fileId, String partitionPath,
                                              String outputPath, int parallelism, boolean skipValidation, boolean dryRun)
      throws Exception {
    HoodieCompactionAdminTool.Config cfg = new HoodieCompactionAdminTool.Config();
    cfg.basePath = basePath;
    cfg.operation = Operation.UNSCHEDULE_FILE;
    cfg.outputPath = outputPath;
    cfg.partitionPath = partitionPath;
    cfg.fileId = fileId;
    cfg.parallelism = parallelism;
    cfg.dryRun = dryRun;
    cfg.skipValidation = skipValidation;
    new HoodieCompactionAdminTool(cfg).run(jsc);
  }

  private static int compact(JavaSparkContext jsc, String basePath, String tableName, String compactionInstant,
                             int parallelism, String schemaFile, int retry, String mode, String propsFilePath,
                             List<String> configs) {
    HoodieCompactor.Config cfg = new HoodieCompactor.Config();
    cfg.basePath = basePath;
    cfg.tableName = tableName;
    cfg.compactionInstantTime = compactionInstant;
    // TODO: Make this configurable along with strategy specific config - For now, this is a generic enough strategy
    cfg.strategyClassName = UnBoundedCompactionStrategy.class.getCanonicalName();
    cfg.parallelism = parallelism;
    cfg.schemaFile = schemaFile;
    cfg.runningMode = mode;
    cfg.propsFilePath = propsFilePath;
    cfg.configs = configs;
    return new HoodieCompactor(jsc, cfg).compact(retry);
  }

  private static int cluster(JavaSparkContext jsc, String basePath, String tableName, String clusteringInstant,
                             int parallelism, String sparkMemory, int retry, String runningMode, String propsFilePath, List<String> configs) {
    HoodieClusteringJob.Config cfg = new HoodieClusteringJob.Config();
    cfg.basePath = basePath;
    cfg.tableName = tableName;
    cfg.clusteringInstantTime = clusteringInstant;
    cfg.parallelism = parallelism;
    cfg.runningMode = runningMode;
    cfg.propsFilePath = propsFilePath;
    cfg.configs = configs;
    jsc.getConf().set("spark.executor.memory", sparkMemory);
    return new HoodieClusteringJob(jsc, cfg).cluster(retry);
  }

  private static int deduplicatePartitionPath(JavaSparkContext jsc, String duplicatedPartitionPath,
                                              String repairedOutputPath, String basePath, boolean dryRun, String dedupeType) {
    DedupeSparkJob job = new DedupeSparkJob(basePath, duplicatedPartitionPath, repairedOutputPath,
        SQLContext.getOrCreate(jsc.sc()),
        HoodieStorageUtils.getStorage(basePath, HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration())),
        DeDupeType.withName(dedupeType));
    job.fixDuplicates(dryRun);
    return 0;
  }

  public static int repairDeprecatedPartition(JavaSparkContext jsc, String basePath) {
    SQLContext sqlContext = SQLContext.getOrCreate(jsc.sc());
    Dataset<Row> recordsToRewrite = getRecordsToRewrite(basePath, PartitionPathEncodeUtils.DEPRECATED_DEFAULT_PARTITION_PATH, sqlContext);

    if (!recordsToRewrite.isEmpty()) {
      recordsToRewrite.cache();
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
          .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()))
          .setBasePath(basePath).build();
      Map<String, String> propsMap = getPropsForRewrite(metaClient);
      rewriteRecordsToNewPartition(basePath, PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH, recordsToRewrite, metaClient, propsMap);
      // after re-writing, we can safely delete older data.
      deleteOlderPartition(basePath, PartitionPathEncodeUtils.DEPRECATED_DEFAULT_PARTITION_PATH, recordsToRewrite, propsMap);
    }
    return 0;
  }

  public static int renamePartition(JavaSparkContext jsc, String basePath, String oldPartition, String newPartition) {
    SQLContext sqlContext = SQLContext.getOrCreate(jsc.sc());
    Dataset<Row> recordsToRewrite = getRecordsToRewrite(basePath, oldPartition, sqlContext);

    if (!recordsToRewrite.isEmpty()) {
      recordsToRewrite.cache();
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
          .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()))
          .setBasePath(basePath).build();
      Map<String, String> propsMap = getPropsForRewrite(metaClient);
      rewriteRecordsToNewPartition(basePath, newPartition, recordsToRewrite, metaClient, propsMap);
      // after re-writing, we can safely delete older partition.
      deleteOlderPartition(basePath, oldPartition, recordsToRewrite, propsMap);
      // also, we can physically delete the old partition.
      FileSystem fs = HadoopFSUtils.getFs(new Path(basePath), metaClient.getStorageConf());
      try {
        fs.delete(new Path(basePath, oldPartition), true);
      } catch (IOException e) {
        LOG.warn("Failed to delete older partition {}", basePath);
      }
    }
    return 0;
  }

  private static void deleteOlderPartition(String basePath, String oldPartition, Dataset<Row> recordsToRewrite, Map<String, String> propsMap) {
    propsMap.put("hoodie.datasource.write.partitions.to.delete", oldPartition);
    recordsToRewrite.write()
        .options(propsMap)
        .option("hoodie.datasource.write.operation", WriteOperationType.DELETE_PARTITION.value())
        .format("hudi")
        .mode("Append")
        .save(basePath);
  }

  private static void rewriteRecordsToNewPartition(String basePath, String newPartition, Dataset<Row> recordsToRewrite, HoodieTableMetaClient metaClient, Map<String, String> propsMap) {
    String partitionFieldProp = metaClient.getTableConfig().getPartitionFieldProp();
    StructType structType = recordsToRewrite.schema();
    int partitionIndex = structType.fieldIndex(partitionFieldProp);

    recordsToRewrite.withColumn(metaClient.getTableConfig().getPartitionFieldProp(), functions.lit(newPartition).cast(structType.apply(partitionIndex).dataType()))
        .write()
        .options(propsMap)
        .option("hoodie.datasource.write.operation", WriteOperationType.BULK_INSERT.value())
        .format("hudi")
        .mode("Append")
        .save(basePath);
  }

  private static Dataset<Row> getRecordsToRewrite(String basePath, String oldPartition, SQLContext sqlContext) {
    return sqlContext.read()
        .format("hudi")
        .load(basePath + "/" + oldPartition)
        .drop(HoodieRecord.RECORD_KEY_METADATA_FIELD)
        .drop(HoodieRecord.PARTITION_PATH_METADATA_FIELD)
        .drop(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD)
        .drop(HoodieRecord.FILENAME_METADATA_FIELD)
        .drop(HoodieRecord.COMMIT_TIME_METADATA_FIELD);
  }

  private static Map<String, String> getPropsForRewrite(HoodieTableMetaClient metaClient) {
    Map<String, String> propsMap = new HashMap<>();
    metaClient.getTableConfig().getProps().forEach((k, v) -> propsMap.put(k.toString(), v.toString()));
    propsMap.put(HoodieWriteConfig.SKIP_DEFAULT_PARTITION_VALIDATION.key(), "true");
    propsMap.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), metaClient.getTableConfig().getRecordKeyFieldProp());
    propsMap.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), HoodieTableConfig.getPartitionFieldPropForKeyGenerator(metaClient.getTableConfig()).orElse(""));
    propsMap.put(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME().key(), metaClient.getTableConfig().getKeyGeneratorClassName());
    return propsMap;
  }

  private static int doBootstrap(JavaSparkContext jsc, String tableName, String tableType, String basePath,
                                 String sourcePath, String recordKeyCols, String partitionFields, String parallelism, String schemaProviderClass,
                                 String bootstrapIndexClass, String selectorClass, String keyGenerator, String fullBootstrapInputProvider,
                                 String recordMergeMode, String payloadClassName, String recordMergeStrategyId, String recordMergeImplClasses,
                                 String enableHiveSync, String propsFilePath, List<String> configs) throws IOException {

    TypedProperties properties = propsFilePath == null ? buildProperties(configs)
        : readConfig(jsc.hadoopConfiguration(), new Path(propsFilePath), configs).getProps(true);

    properties.setProperty(HoodieBootstrapConfig.BASE_PATH.key(), sourcePath);

    if (!isNullOrEmpty(keyGenerator) && KeyGeneratorType.getNames().contains(keyGenerator.toUpperCase(Locale.ROOT))) {
      properties.setProperty(HoodieWriteConfig.KEYGENERATOR_TYPE.key(), keyGenerator.toUpperCase(Locale.ROOT));
    } else {
      properties.setProperty(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME().key(), keyGenerator);
    }

    properties.setProperty(HoodieBootstrapConfig.FULL_BOOTSTRAP_INPUT_PROVIDER_CLASS_NAME.key(), fullBootstrapInputProvider);
    properties.setProperty(HoodieBootstrapConfig.PARALLELISM_VALUE.key(), parallelism);
    properties.setProperty(HoodieBootstrapConfig.MODE_SELECTOR_CLASS_NAME.key(), selectorClass);
    properties.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD().key(), recordKeyCols);
    properties.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), partitionFields);

    HoodieStreamer.Config cfg = new HoodieStreamer.Config();
    cfg.targetTableName = tableName;
    cfg.targetBasePath = basePath;
    cfg.tableType = tableType;
    cfg.schemaProviderClassName = schemaProviderClass;
    cfg.bootstrapIndexClass = bootstrapIndexClass;
    cfg.payloadClassName = payloadClassName;
    cfg.recordMergeMode = RecordMergeMode.getValue(recordMergeMode);
    cfg.recordMergeStrategyId = recordMergeStrategyId;
    cfg.recordMergeImplClasses = recordMergeImplClasses;
    cfg.enableHiveSync = Boolean.valueOf(enableHiveSync);

    new BootstrapExecutor(cfg, jsc, HadoopFSUtils.getFs(basePath, jsc.hadoopConfiguration()),
        jsc.hadoopConfiguration(), properties).execute();
    return 0;
  }

  private static int rollback(JavaSparkContext jsc, String instantTime, String basePath, Boolean rollbackUsingMarkers) throws Exception {
    SparkRDDWriteClient client = createHoodieClient(jsc, basePath, rollbackUsingMarkers, false);
    if (client.rollback(instantTime)) {
      LOG.info("The commit \"{}\" rolled back.", instantTime);
      return 0;
    } else {
      LOG.warn("The commit \"{}\" failed to roll back.", instantTime);
      return -1;
    }
  }

  private static int createSavepoint(JavaSparkContext jsc, String commitTime, String user,
                                     String comments, String basePath) throws Exception {
    try (SparkRDDWriteClient client = createHoodieClient(jsc, basePath, false)) {
      client.savepoint(commitTime, user, comments);
      LOG.info("The commit \"{}\" has been savepointed.", commitTime);
      return 0;
    } catch (HoodieSavepointException se) {
      LOG.warn("Failed: Could not create savepoint \"{}\".", commitTime);
      return -1;
    }
  }

  private static int rollbackToSavepoint(JavaSparkContext jsc, String savepointTime, String basePath, boolean lazyCleanPolicy) throws Exception {
    try (SparkRDDWriteClient client = createHoodieClient(jsc, basePath, lazyCleanPolicy)) {
      client.restoreToSavepoint(savepointTime);
      LOG.info("The commit \"{}\" rolled back.", savepointTime);
      return 0;
    } catch (Exception e) {
      LOG.warn(String.format("The commit \"%s\" failed to roll back.", savepointTime), e);
      return -1;
    }
  }

  private static int deleteSavepoint(JavaSparkContext jsc, String savepointTime, String basePath) throws Exception {
    try (SparkRDDWriteClient client = createHoodieClient(jsc, basePath, false)) {
      client.deleteSavepoint(savepointTime);
      LOG.info("Savepoint \"{}\" deleted.", savepointTime);
      return 0;
    } catch (Exception e) {
      LOG.warn(String.format("Failed: Could not delete savepoint \"%s\".", savepointTime), e);
      return -1;
    }
  }

  /**
   * Upgrade or downgrade table.
   *
   * @param jsc       instance of {@link JavaSparkContext} to use.
   * @param basePath  base path of the dataset.
   * @param toVersion version to which upgrade/downgrade to be done.
   * @return 0 if success, else -1.
   * @throws Exception
   */
  protected static int upgradeOrDowngradeTable(JavaSparkContext jsc, String basePath, String toVersion) {
    HoodieWriteConfig config = getWriteConfig(basePath, Boolean.parseBoolean(HoodieWriteConfig.ROLLBACK_USING_MARKERS_ENABLE.defaultValue()),
        false);
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder()
            .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()))
            .setBasePath(config.getBasePath())
            .setLoadActiveTimelineOnLoad(false).setConsistencyGuardConfig(config.getConsistencyGuardConfig())
            .setLayoutVersion(Option.of(new TimelineLayoutVersion(config.getTimelineLayoutVersion())))
            .setFileSystemRetryConfig(config.getFileSystemRetryConfig()).build();
    HoodieWriteConfig updatedConfig = HoodieWriteConfig.newBuilder().withProps(config.getProps())
        .forTable(metaClient.getTableConfig().getTableName()).build();
    try {
      new UpgradeDowngrade(metaClient, updatedConfig, new HoodieSparkEngineContext(jsc), SparkUpgradeDowngradeHelper.getInstance())
          .run(HoodieTableVersion.valueOf(toVersion), null);
      LOG.info("Table at \"{}\" upgraded / downgraded to version \"{}\".", basePath, toVersion);
      return 0;
    } catch (Exception e) {
      LOG.warn(String.format("Failed: Could not upgrade/downgrade table at \"%s\" to version \"%s\".", basePath, toVersion), e);
      return -1;
    }
  }

  private static SparkRDDWriteClient createHoodieClient(JavaSparkContext jsc, String basePath, Boolean rollbackUsingMarkers, boolean lazyCleanPolicy) throws Exception {
    HoodieWriteConfig config = getWriteConfig(basePath, rollbackUsingMarkers, lazyCleanPolicy);
    return new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), config, true);
  }

  private static SparkRDDWriteClient createHoodieClient(JavaSparkContext jsc, String basePath, boolean lazyCleanPolicy) throws Exception {
    return createHoodieClient(jsc, basePath, Boolean.parseBoolean(HoodieWriteConfig.ROLLBACK_USING_MARKERS_ENABLE.defaultValue()), lazyCleanPolicy);
  }

  private static HoodieWriteConfig getWriteConfig(String basePath, Boolean rollbackUsingMarkers, boolean lazyCleanPolicy) {
    return HoodieWriteConfig.newBuilder().withPath(basePath)
        .withRollbackUsingMarkers(rollbackUsingMarkers)
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(lazyCleanPolicy ? HoodieFailedWritesCleaningPolicy.LAZY :
            HoodieFailedWritesCleaningPolicy.EAGER).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build()).build();
  }

  private static int archive(JavaSparkContext jsc, int minCommits, int maxCommits, int commitsRetained, boolean enableMetadata, String basePath) {
    try {
      return ArchiveExecutorUtils.archive(jsc, minCommits, maxCommits, commitsRetained, enableMetadata, basePath);
    } catch (IOException ex) {
      return -1;
    }
  }
}
