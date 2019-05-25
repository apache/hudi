/*
 * Copyright (c) 2016,2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.cli.commands;

import com.beust.jcommander.Parameter;
import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.cli.DedupeSparkJob;
import com.uber.hoodie.cli.utils.SparkUtil;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.AbstractCommandConfig;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.InvalidCommandConfigException;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.utilities.HDFSParquetImporter;
import com.uber.hoodie.utilities.HoodieCompactionAdminTool;
import com.uber.hoodie.utilities.HoodieCompactor;
import java.util.Arrays;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

public class SparkMain {

  private static final Logger LOG = Logger.getLogger(SparkMain.class);

  /**
   * Commands
   */
  enum SparkCommand {
    ROLLBACK, DEDUPLICATE, ROLLBACK_TO_SAVEPOINT, SAVEPOINT, IMPORT, UPSERT, COMPACT_SCHEDULE, COMPACT_RUN,
    COMPACT_UNSCHEDULE_PLAN, COMPACT_UNSCHEDULE_FILE, COMPACT_VALIDATE, COMPACT_REPAIR;
  }

  static class HoodieRollbackCommandConfig extends AbstractCommandConfig {

    @Parameter(names = {"--commit-time",
            "-sp"}, description = "Commit time for rollback")
    public String commitTime = null;

    @Parameter(names = {"--savepoint-time",
            "-sp"}, description = "Savepoint time for rollback")
    public String savepointTime = null;

    @Parameter(names = {"--base-path",
            "-bp"}, description = "Base path for the hoodie dataset", required = true)
    public String basePath = null;
  }

  static class HoodieDeduplicatePartitionCommandConfig extends AbstractCommandConfig {

    @Parameter(names = {"--duplicated-partition-path",
            "-dpp"}, description = "Duplicated partition path for deduplication", required = true)
    public String duplicatedPartitionPath = null;

    @Parameter(names = {"--repaired-output-path",
            "-rop"}, description = "Repaired output path for deduplication", required = true)
    public String repairedOutputPath = null;

    @Parameter(names = {"--base-path",
            "-bp"}, description = "Base path for the hoodie dataset", required = true)
    public String basePath = null;
  }

  public static void main(String[] args) throws Exception {
    String command = args[0];
    LOG.info("Invoking SparkMain:" + command);

    SparkCommand cmd = SparkCommand.valueOf(command);
    String[] commandConfigs = Arrays.copyOfRange(args, 1, args.length);

    JavaSparkContext jsc = SparkUtil.initJavaSparkConf("hoodie-cli-" + command);
    int returnCode = 0;
    switch (cmd) {
      case ROLLBACK:
        returnCode = rollback(jsc, getConfig(HoodieRollbackCommandConfig.class, commandConfigs));
        break;
      case DEDUPLICATE:
        returnCode = deduplicatePartitionPath(jsc,
                getConfig(HoodieDeduplicatePartitionCommandConfig.class, commandConfigs));
        break;
      case ROLLBACK_TO_SAVEPOINT:
        returnCode = rollbackToSavepoint(jsc, getConfig(HoodieRollbackCommandConfig.class, commandConfigs));
        break;
      case IMPORT:
      case UPSERT:
        returnCode = dataLoad(jsc, getConfig(HDFSParquetImporter.Config.class, commandConfigs));
        break;
      case COMPACT_SCHEDULE:
      case COMPACT_RUN:
        returnCode = compact(jsc, getConfig(HoodieCompactor.Config.class, commandConfigs));
        break;
      case COMPACT_VALIDATE:
      case COMPACT_REPAIR:
      case COMPACT_UNSCHEDULE_FILE:
      case COMPACT_UNSCHEDULE_PLAN:
        doCompactOperation(jsc, getConfig(HoodieCompactionAdminTool.Config.class, commandConfigs));
        returnCode = 0;
        break;
      default:
        break;
    }
    System.exit(returnCode);
  }

  private static <T extends AbstractCommandConfig> T getConfig(Class<T> configClass, String[] configs) {
    try {
      T configObject = configClass.newInstance();
      configObject.parseCommandConfig(configs);
      return configObject;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new InvalidCommandConfigException("Unable to instantiate command config class ", e);
    }
  }

  private static int dataLoad(JavaSparkContext jsc, HDFSParquetImporter.Config config) throws Exception {
    LOG.info("Command config: " + config.toString());
    return new HDFSParquetImporter(config).dataImport(jsc, config.retry);
  }

  private static void doCompactOperation(JavaSparkContext jsc, HoodieCompactionAdminTool.Config config)
      throws Exception {
    LOG.info("Command config: " + config.toString());
    if ((null != config.sparkMaster) && (!config.sparkMaster.isEmpty())) {
      jsc.getConf().setMaster(config.sparkMaster);
    }
    jsc.getConf().set("spark.executor.memory", config.sparkMemory);
    new HoodieCompactionAdminTool(config).run(jsc);
  }

  private static int compact(JavaSparkContext jsc, HoodieCompactor.Config config) {
    LOG.info("Command config: " + config.toString());
    jsc.getConf().set("spark.executor.memory", config.sparkMemory);
    return new HoodieCompactor(config).compact(jsc, config.retry);
  }

  private static int deduplicatePartitionPath(JavaSparkContext jsc,
      HoodieDeduplicatePartitionCommandConfig config) {
    LOG.info("Command config: " + config.toString());
    DedupeSparkJob job = new DedupeSparkJob(config.basePath, config.duplicatedPartitionPath, config.repairedOutputPath,
        new SQLContext(jsc),
        FSUtils.getFs(config.basePath, jsc.hadoopConfiguration()));
    job.fixDuplicates(true);
    return 0;
  }

  private static int rollback(JavaSparkContext jsc, HoodieRollbackCommandConfig config) throws Exception {
    LOG.info("Command config: " + config.toString());
    HoodieWriteClient client = createHoodieClient(jsc, config.basePath);
    if (client.rollback(config.commitTime)) {
      LOG.info(String.format("The commit \"%s\" rolled back.", config.commitTime));
      return 0;
    } else {
      LOG.info(String.format("The commit \"%s\" failed to roll back.", config.commitTime));
      return -1;
    }
  }

  private static int rollbackToSavepoint(JavaSparkContext jsc, HoodieRollbackCommandConfig config)
      throws Exception {
    LOG.info("Command config: " + config.toString());
    HoodieWriteClient client = createHoodieClient(jsc, config.basePath);
    if (client.rollbackToSavepoint(config.savepointTime)) {
      LOG.info(String.format("The commit \"%s\" rolled back.", config.savepointTime));
      return 0;
    } else {
      LOG.info(String.format("The commit \"%s\" failed to roll back.", config.savepointTime));
      return -1;
    }
  }

  private static HoodieWriteClient createHoodieClient(JavaSparkContext jsc, String basePath) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).withIndexConfig(
        HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build()).build();
    return new HoodieWriteClient(jsc, config);
  }
}
