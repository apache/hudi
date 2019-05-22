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

import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.cli.DedupeSparkJob;
import com.uber.hoodie.cli.utils.SparkUtil;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.configs.AbstractCommandConfig;
import com.uber.hoodie.configs.HDFSParquetImporterJobConfig;
import com.uber.hoodie.configs.HoodieCommitRollbackJobConfig;
import com.uber.hoodie.configs.HoodieCompactionAdminToolJobConfig;
import com.uber.hoodie.configs.HoodieCompactorJobConfig;
import com.uber.hoodie.configs.HoodieDeduplicatePartitionJobConfig;
import com.uber.hoodie.configs.HoodieRollbackToSavePointJobConfig;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.utilities.HDFSParquetImporter;
import com.uber.hoodie.utilities.HoodieCompactionAdminTool;
import com.uber.hoodie.utilities.HoodieCompactor;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

public class SparkMain extends AbstractCommandConfig {

  private static final Logger LOG = Logger.getLogger(SparkMain.class);

  /**
   * Commands
   */
  enum SparkCommand {
    ROLLBACK, DEDUPLICATE, ROLLBACK_TO_SAVEPOINT, SAVEPOINT, IMPORT, UPSERT, COMPACT_SCHEDULE, COMPACT_RUN,
    COMPACT_UNSCHEDULE_PLAN, COMPACT_UNSCHEDULE_FILE, COMPACT_VALIDATE, COMPACT_REPAIR;
  }

  private Map<String, AbstractCommandConfig> commandConfigMap = new HashMap<>();

  {
    commandConfigMap.put(SparkCommand.ROLLBACK.name(), new HoodieCommitRollbackJobConfig());
    commandConfigMap.put(SparkCommand.DEDUPLICATE.name(), new HoodieDeduplicatePartitionJobConfig());
    commandConfigMap.put(SparkCommand.ROLLBACK_TO_SAVEPOINT.name(), new HoodieRollbackToSavePointJobConfig());

    commandConfigMap.put(SparkCommand.IMPORT.name(), new HDFSParquetImporterJobConfig());
    commandConfigMap.put(SparkCommand.UPSERT.name(), new HDFSParquetImporterJobConfig());

    commandConfigMap.put(SparkCommand.COMPACT_RUN.name(), new HoodieCompactorJobConfig());
    commandConfigMap.put(SparkCommand.COMPACT_SCHEDULE.name(), new HoodieCompactorJobConfig());

    commandConfigMap.put(SparkCommand.COMPACT_REPAIR.name(), new HoodieCompactionAdminToolJobConfig());
    commandConfigMap.put(SparkCommand.COMPACT_VALIDATE.name(), new HoodieCompactionAdminToolJobConfig());
    commandConfigMap.put(SparkCommand.COMPACT_UNSCHEDULE_FILE.name(), new HoodieCompactionAdminToolJobConfig());
    commandConfigMap.put(SparkCommand.COMPACT_UNSCHEDULE_PLAN.name(), new HoodieCompactionAdminToolJobConfig());
  }

  public static void main(String[] args) throws Exception {
    String command = args[0];
    LOG.info("Invoking SparkMain:" + command);

    SparkCommand cmd = SparkCommand.valueOf(command);

    SparkMain mainRunner = new SparkMain();
    mainRunner.parseJobConfig(args, mainRunner.commandConfigMap);
    AbstractCommandConfig config = mainRunner.commandConfigMap.get(cmd.name());

    JavaSparkContext jsc = SparkUtil.initJavaSparkConf("hoodie-cli-" + command);
    int returnCode = 0;
    switch (cmd) {
      case ROLLBACK:
        returnCode = rollback(jsc, (HoodieCommitRollbackJobConfig) config);
        break;
      case DEDUPLICATE:
        returnCode = deduplicatePartitionPath(jsc, (HoodieDeduplicatePartitionJobConfig) config);
        break;
      case ROLLBACK_TO_SAVEPOINT:
        returnCode = rollbackToSavepoint(jsc, (HoodieRollbackToSavePointJobConfig) config);
        break;
      case IMPORT:
      case UPSERT:
        returnCode = dataLoad(jsc, (HDFSParquetImporterJobConfig) config);
        break;
      case COMPACT_SCHEDULE:
      case COMPACT_RUN:
        returnCode = compact(jsc, (HoodieCompactorJobConfig) config);
        break;
      case COMPACT_VALIDATE:
      case COMPACT_REPAIR:
      case COMPACT_UNSCHEDULE_FILE:
      case COMPACT_UNSCHEDULE_PLAN:
        doCompactOperation(jsc, (HoodieCompactionAdminToolJobConfig) config);
        returnCode = 0;
        break;
      default:
        break;
    }
    System.exit(returnCode);
  }

  private static int dataLoad(JavaSparkContext jsc, HDFSParquetImporterJobConfig config) throws Exception {
    LOG.info("Job config: " + config.toString());
    return new HDFSParquetImporter(config).dataImport(jsc, config.retry);
  }

  private static void doCompactOperation(JavaSparkContext jsc, HoodieCompactionAdminToolJobConfig config)
      throws Exception {
    LOG.info("Job config: " + config.toString());
    if ((null != config.sparkMaster) && (!config.sparkMaster.isEmpty())) {
      jsc.getConf().setMaster(config.sparkMaster);
    }
    jsc.getConf().set("spark.executor.memory", config.sparkMemory);
    new HoodieCompactionAdminTool(config).run(jsc);
  }

  private static int compact(JavaSparkContext jsc, HoodieCompactorJobConfig config) {
    LOG.info("Job config: " + config.toString());
    jsc.getConf().set("spark.executor.memory", config.sparkMemory);
    return new HoodieCompactor(config).compact(jsc, config.retry);
  }

  private static int deduplicatePartitionPath(JavaSparkContext jsc,
      HoodieDeduplicatePartitionJobConfig config) {
    LOG.info("Job config: " + config.toString());
    DedupeSparkJob job = new DedupeSparkJob(config.basePath, config.duplicatedPartitionPath, config.repairedOutputPath,
        new SQLContext(jsc),
        FSUtils.getFs(config.basePath, jsc.hadoopConfiguration()));
    job.fixDuplicates(true);
    return 0;
  }

  private static int rollback(JavaSparkContext jsc, HoodieCommitRollbackJobConfig config) throws Exception {
    LOG.info("Job config: " + config.toString());
    HoodieWriteClient client = createHoodieClient(jsc, config.basePath);
    if (client.rollback(config.commitTime)) {
      LOG.info(String.format("The commit \"%s\" rolled back.", config.commitTime));
      return 0;
    } else {
      LOG.info(String.format("The commit \"%s\" failed to roll back.", config.commitTime));
      return -1;
    }
  }

  private static int rollbackToSavepoint(JavaSparkContext jsc, HoodieRollbackToSavePointJobConfig config)
      throws Exception {
    LOG.info("Job config: " + config.toString());
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
