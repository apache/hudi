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
import org.apache.hudi.cli.DeDupeType;
import org.apache.hudi.cli.DedupeSparkJob;
import org.apache.hudi.cli.utils.SparkUtil;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieBootstrapConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieSavepointException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.table.action.compact.strategy.UnBoundedCompactionStrategy;
import org.apache.hudi.table.upgrade.SparkUpgradeDowngradeHelper;
import org.apache.hudi.table.upgrade.UpgradeDowngrade;
import org.apache.hudi.utilities.HDFSParquetImporter;
import org.apache.hudi.utilities.HDFSParquetImporter.Config;
import org.apache.hudi.utilities.HoodieCleaner;
import org.apache.hudi.utilities.HoodieClusteringJob;
import org.apache.hudi.utilities.HoodieCompactionAdminTool;
import org.apache.hudi.utilities.HoodieCompactionAdminTool.Operation;
import org.apache.hudi.utilities.HoodieCompactor;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.deltastreamer.BootstrapExecutor;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * This class deals with initializing spark context based on command entered to hudi-cli.
 */
public class SparkMain {

  private static final Logger LOG = Logger.getLogger(SparkMain.class);

  /**
   * Commands.
   */
  enum SparkCommand {
    BOOTSTRAP, ROLLBACK, DEDUPLICATE, ROLLBACK_TO_SAVEPOINT, SAVEPOINT, IMPORT, UPSERT, COMPACT_SCHEDULE, COMPACT_RUN,
    COMPACT_UNSCHEDULE_PLAN, COMPACT_UNSCHEDULE_FILE, COMPACT_VALIDATE, COMPACT_REPAIR, CLUSTERING_SCHEDULE,
    CLUSTERING_RUN, CLEAN, DELETE_SAVEPOINT, UPGRADE, DOWNGRADE
  }

  public static void main(String[] args) throws Exception {
    String command = args[0];
    LOG.info("Invoking SparkMain:" + command);

    SparkCommand cmd = SparkCommand.valueOf(command);

    JavaSparkContext jsc = SparkUtil.initJavaSparkConf("hoodie-cli-" + command, Option.of(args[1]), Option.of(args[2]));
    int returnCode = 0;
    try {
      switch (cmd) {
        case ROLLBACK:
          assert (args.length == 5);
          returnCode = rollback(jsc, args[3], args[4]);
          break;
        case DEDUPLICATE:
          assert (args.length == 8);
          returnCode = deduplicatePartitionPath(jsc, args[3], args[4], args[5], Boolean.parseBoolean(args[6]), args[7]);
          break;
        case ROLLBACK_TO_SAVEPOINT:
          assert (args.length == 5);
          returnCode = rollbackToSavepoint(jsc, args[3], args[4]);
          break;
        case IMPORT:
        case UPSERT:
          assert (args.length >= 13);
          String propsFilePath = null;
          if (!StringUtils.isNullOrEmpty(args[12])) {
            propsFilePath = args[12];
          }
          List<String> configs = new ArrayList<>();
          if (args.length > 13) {
            configs.addAll(Arrays.asList(args).subList(13, args.length));
          }
          returnCode = dataLoad(jsc, command, args[3], args[4], args[5], args[6], args[7], args[8],
                  Integer.parseInt(args[9]), args[10], Integer.parseInt(args[11]), propsFilePath, configs);
          break;
        case COMPACT_RUN:
          assert (args.length >= 10);
          propsFilePath = null;
          if (!StringUtils.isNullOrEmpty(args[9])) {
            propsFilePath = args[9];
          }
          configs = new ArrayList<>();
          if (args.length > 10) {
            configs.addAll(Arrays.asList(args).subList(9, args.length));
          }
          returnCode = compact(jsc, args[3], args[4], args[5], Integer.parseInt(args[6]), args[7],
              Integer.parseInt(args[8]), false, propsFilePath, configs);
          break;
        case COMPACT_SCHEDULE:
          assert (args.length >= 7);
          propsFilePath = null;
          if (!StringUtils.isNullOrEmpty(args[6])) {
            propsFilePath = args[6];
          }
          configs = new ArrayList<>();
          if (args.length > 7) {
            configs.addAll(Arrays.asList(args).subList(7, args.length));
          }
          returnCode = compact(jsc, args[3], args[4], args[5], 1, "", 0, true, propsFilePath, configs);
          break;
        case COMPACT_VALIDATE:
          assert (args.length == 7);
          doCompactValidate(jsc, args[3], args[4], args[5], Integer.parseInt(args[6]));
          returnCode = 0;
          break;
        case COMPACT_REPAIR:
          assert (args.length == 8);
          doCompactRepair(jsc, args[3], args[4], args[5], Integer.parseInt(args[6]),
                  Boolean.parseBoolean(args[7]));
          returnCode = 0;
          break;
        case COMPACT_UNSCHEDULE_FILE:
          assert (args.length == 10);
          doCompactUnscheduleFile(jsc, args[3], args[4], args[5], args[6], Integer.parseInt(args[7]),
              Boolean.parseBoolean(args[8]), Boolean.parseBoolean(args[9]));
          returnCode = 0;
          break;
        case COMPACT_UNSCHEDULE_PLAN:
          assert (args.length == 9);
          doCompactUnschedule(jsc, args[3], args[4], args[5], Integer.parseInt(args[6]),
                  Boolean.parseBoolean(args[7]), Boolean.parseBoolean(args[8]));
          returnCode = 0;
          break;
        case CLUSTERING_RUN:
          assert (args.length >= 8);
          propsFilePath = null;
          if (!StringUtils.isNullOrEmpty(args[7])) {
            propsFilePath = args[7];
          }
          configs = new ArrayList<>();
          if (args.length > 8) {
            configs.addAll(Arrays.asList(args).subList(8, args.length));
          }
          returnCode = cluster(jsc, args[1], args[2], args[3], Integer.parseInt(args[4]), args[5],
              Integer.parseInt(args[6]), false, propsFilePath, configs);
          break;
        case CLUSTERING_SCHEDULE:
          assert (args.length >= 6);
          propsFilePath = null;
          if (!StringUtils.isNullOrEmpty(args[5])) {
            propsFilePath = args[5];
          }
          configs = new ArrayList<>();
          if (args.length > 6) {
            configs.addAll(Arrays.asList(args).subList(6, args.length));
          }
          returnCode = cluster(jsc, args[1], args[2], args[3], 1, args[4], 0, true, propsFilePath, configs);
          break;
        case CLEAN:
          assert (args.length >= 5);
          propsFilePath = null;
          if (!StringUtils.isNullOrEmpty(args[4])) {
            propsFilePath = args[4];
          }
          configs = new ArrayList<>();
          if (args.length > 5) {
            configs.addAll(Arrays.asList(args).subList(5, args.length));
          }
          clean(jsc, args[3], propsFilePath, configs);
          break;
        case SAVEPOINT:
          assert (args.length == 7);
          returnCode = createSavepoint(jsc, args[3], args[4], args[5], args[6]);
          break;
        case DELETE_SAVEPOINT:
          assert (args.length == 5);
          returnCode = deleteSavepoint(jsc, args[3], args[4]);
          break;
        case BOOTSTRAP:
          assert (args.length >= 18);
          propsFilePath = null;
          if (!StringUtils.isNullOrEmpty(args[17])) {
            propsFilePath = args[17];
          }
          configs = new ArrayList<>();
          if (args.length > 18) {
            configs.addAll(Arrays.asList(args).subList(18, args.length));
          }
          returnCode = doBootstrap(jsc, args[3], args[4], args[5], args[6], args[7], args[8], args[9], args[10],
                  args[11], args[12], args[13], args[14], args[15], args[16], propsFilePath, configs);
          break;
        case UPGRADE:
        case DOWNGRADE:
          assert (args.length == 5);
          returnCode = upgradeOrDowngradeTable(jsc, args[3], args[4]);
          break;
        default:
          break;
      }
    } catch (Throwable throwable) {
      LOG.error("Fail to execute command", throwable);
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

  private static int dataLoad(JavaSparkContext jsc, String command, String srcPath, String targetPath, String tableName,
      String tableType, String rowKey, String partitionKey, int parallelism, String schemaFile,
      int retry, String propsFilePath, List<String> configs) {
    Config cfg = new Config();
    cfg.command = command;
    cfg.srcPath = srcPath;
    cfg.targetPath = targetPath;
    cfg.tableName = tableName;
    cfg.tableType = tableType;
    cfg.rowKey = rowKey;
    cfg.partitionKey = partitionKey;
    cfg.parallelism = parallelism;
    cfg.schemaFile = schemaFile;
    cfg.propsFilePath = propsFilePath;
    cfg.configs = configs;
    return new HDFSParquetImporter(cfg).dataImport(jsc, retry);
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
      int parallelism, String schemaFile, int retry, boolean schedule, String propsFilePath,
      List<String> configs) {
    HoodieCompactor.Config cfg = new HoodieCompactor.Config();
    cfg.basePath = basePath;
    cfg.tableName = tableName;
    cfg.compactionInstantTime = compactionInstant;
    // TODO: Make this configurable along with strategy specific config - For now, this is a generic enough strategy
    cfg.strategyClassName = UnBoundedCompactionStrategy.class.getCanonicalName();
    cfg.parallelism = parallelism;
    cfg.schemaFile = schemaFile;
    cfg.runSchedule = schedule;
    cfg.propsFilePath = propsFilePath;
    cfg.configs = configs;
    return new HoodieCompactor(jsc, cfg).compact(retry);
  }

  private static int cluster(JavaSparkContext jsc, String basePath, String tableName, String clusteringInstant,
      int parallelism, String sparkMemory, int retry, boolean schedule, String propsFilePath, List<String> configs) {
    HoodieClusteringJob.Config cfg = new HoodieClusteringJob.Config();
    cfg.basePath = basePath;
    cfg.tableName = tableName;
    cfg.clusteringInstantTime = clusteringInstant;
    cfg.parallelism = parallelism;
    cfg.runSchedule = schedule;
    cfg.propsFilePath = propsFilePath;
    cfg.configs = configs;
    jsc.getConf().set("spark.executor.memory", sparkMemory);
    return new HoodieClusteringJob(jsc, cfg).cluster(retry);
  }

  private static int deduplicatePartitionPath(JavaSparkContext jsc, String duplicatedPartitionPath,
      String repairedOutputPath, String basePath, boolean dryRun, String dedupeType) {
    DedupeSparkJob job = new DedupeSparkJob(basePath, duplicatedPartitionPath, repairedOutputPath, new SQLContext(jsc),
        FSUtils.getFs(basePath, jsc.hadoopConfiguration()), DeDupeType.withName(dedupeType));
    job.fixDuplicates(dryRun);
    return 0;
  }

  private static int doBootstrap(JavaSparkContext jsc, String tableName, String tableType, String basePath,
      String sourcePath, String recordKeyCols, String partitionFields, String parallelism, String schemaProviderClass,
      String bootstrapIndexClass, String selectorClass, String keyGenerator, String fullBootstrapInputProvider,
      String payloadClassName, String enableHiveSync, String propsFilePath, List<String> configs) throws IOException {

    TypedProperties properties = propsFilePath == null ? UtilHelpers.buildProperties(configs)
        : UtilHelpers.readConfig(FSUtils.getFs(propsFilePath, jsc.hadoopConfiguration()), new Path(propsFilePath), configs).getConfig();

    properties.setProperty(HoodieBootstrapConfig.BASE_PATH.key(), sourcePath);

    if (!StringUtils.isNullOrEmpty(keyGenerator) && KeyGeneratorType.getNames().contains(keyGenerator.toUpperCase(Locale.ROOT))) {
      properties.setProperty(HoodieBootstrapConfig.KEYGEN_TYPE.key(), keyGenerator.toUpperCase(Locale.ROOT));
    } else {
      properties.setProperty(HoodieBootstrapConfig.KEYGEN_CLASS_NAME.key(), keyGenerator);
    }

    properties.setProperty(HoodieBootstrapConfig.FULL_BOOTSTRAP_INPUT_PROVIDER_CLASS_NAME.key(), fullBootstrapInputProvider);
    properties.setProperty(HoodieBootstrapConfig.PARALLELISM_VALUE.key(), parallelism);
    properties.setProperty(HoodieBootstrapConfig.MODE_SELECTOR_CLASS_NAME.key(), selectorClass);
    properties.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD().key(), recordKeyCols);
    properties.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), partitionFields);

    HoodieDeltaStreamer.Config cfg = new HoodieDeltaStreamer.Config();
    cfg.targetTableName = tableName;
    cfg.targetBasePath = basePath;
    cfg.tableType = tableType;
    cfg.schemaProviderClassName = schemaProviderClass;
    cfg.bootstrapIndexClass = bootstrapIndexClass;
    cfg.payloadClassName = payloadClassName;
    cfg.enableHiveSync = Boolean.valueOf(enableHiveSync);

    new BootstrapExecutor(cfg, jsc, FSUtils.getFs(basePath, jsc.hadoopConfiguration()),
        jsc.hadoopConfiguration(), properties).execute();
    return 0;
  }

  private static int rollback(JavaSparkContext jsc, String instantTime, String basePath) throws Exception {
    SparkRDDWriteClient client = createHoodieClient(jsc, basePath);
    if (client.rollback(instantTime)) {
      LOG.info(String.format("The commit \"%s\" rolled back.", instantTime));
      return 0;
    } else {
      LOG.warn(String.format("The commit \"%s\" failed to roll back.", instantTime));
      return -1;
    }
  }

  private static int createSavepoint(JavaSparkContext jsc, String commitTime, String user,
      String comments, String basePath) throws Exception {
    SparkRDDWriteClient client = createHoodieClient(jsc, basePath);
    try {
      client.savepoint(commitTime, user, comments);
      LOG.info(String.format("The commit \"%s\" has been savepointed.", commitTime));
      return 0;
    } catch (HoodieSavepointException se) {
      LOG.warn(String.format("Failed: Could not create savepoint \"%s\".", commitTime));
      return -1;
    }
  }

  private static int rollbackToSavepoint(JavaSparkContext jsc, String savepointTime, String basePath) throws Exception {
    SparkRDDWriteClient client = createHoodieClient(jsc, basePath);
    try {
      client.restoreToSavepoint(savepointTime);
      LOG.info(String.format("The commit \"%s\" rolled back.", savepointTime));
      return 0;
    } catch (Exception e) {
      LOG.warn(String.format("The commit \"%s\" failed to roll back.", savepointTime));
      return -1;
    }
  }

  private static int deleteSavepoint(JavaSparkContext jsc, String savepointTime, String basePath) throws Exception {
    SparkRDDWriteClient client = createHoodieClient(jsc, basePath);
    try {
      client.deleteSavepoint(savepointTime);
      LOG.info(String.format("Savepoint \"%s\" deleted.", savepointTime));
      return 0;
    } catch (Exception e) {
      LOG.warn(String.format("Failed: Could not delete savepoint \"%s\".", savepointTime));
      return -1;
    }
  }

  /**
   * Upgrade or downgrade table.
   *
   * @param jsc instance of {@link JavaSparkContext} to use.
   * @param basePath base path of the dataset.
   * @param toVersion version to which upgrade/downgrade to be done.
   * @return 0 if success, else -1.
   * @throws Exception
   */
  protected static int upgradeOrDowngradeTable(JavaSparkContext jsc, String basePath, String toVersion) {
    HoodieWriteConfig config = getWriteConfig(basePath);
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setConf(jsc.hadoopConfiguration()).setBasePath(config.getBasePath())
            .setLoadActiveTimelineOnLoad(false).setConsistencyGuardConfig(config.getConsistencyGuardConfig())
            .setLayoutVersion(Option.of(new TimelineLayoutVersion(config.getTimelineLayoutVersion()))).build();
    try {
      new UpgradeDowngrade(metaClient, config, new HoodieSparkEngineContext(jsc), SparkUpgradeDowngradeHelper.getInstance())
          .run(HoodieTableVersion.valueOf(toVersion), null);
      LOG.info(String.format("Table at \"%s\" upgraded / downgraded to version \"%s\".", basePath, toVersion));
      return 0;
    } catch (Exception e) {
      LOG.warn(String.format("Failed: Could not upgrade/downgrade table at \"%s\" to version \"%s\".", basePath, toVersion), e);
      return -1;
    }
  }

  private static SparkRDDWriteClient createHoodieClient(JavaSparkContext jsc, String basePath) throws Exception {
    HoodieWriteConfig config = getWriteConfig(basePath);
    return new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), config);
  }

  private static HoodieWriteConfig getWriteConfig(String basePath) {
    return HoodieWriteConfig.newBuilder().withPath(basePath)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build()).build();
  }
}
