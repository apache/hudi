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

import org.apache.hudi.cli.DedupeConfig;
import org.apache.hudi.cli.DedupeSparkJob;
import org.apache.hudi.cli.utils.SparkUtil;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.HDFSParquetImporter;
import org.apache.hudi.utilities.HoodieCleaner;
import org.apache.hudi.utilities.HoodieCompactionAdminTool;
import org.apache.hudi.utilities.HoodieCompactor;
import org.apache.hudi.utilities.HoodieRollback;
import org.apache.hudi.utilities.config.AbstractCommandConfig;
import org.apache.hudi.utilities.exception.InvalidCommandConfigException;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import java.util.Arrays;
import java.util.List;

/**
 * This class deals with initializing spark context based on command entered to hudi-cli.
 */
public class SparkMain {

  private static final Logger LOG = Logger.getLogger(SparkMain.class);

  /**
   * Commands.
   */
  enum SparkCommand {
    ROLLBACK, DEDUPLICATE, ROLLBACK_TO_SAVEPOINT, SAVEPOINT, IMPORT, UPSERT, COMPACT_SCHEDULE, COMPACT_RUN, COMPACT_UNSCHEDULE_PLAN, COMPACT_UNSCHEDULE_FILE, COMPACT_VALIDATE, COMPACT_REPAIR, CLEAN
  }

  public static void main(String[] args) throws Exception {
    String command = args[0];
    LOG.info("Invoking SparkMain:" + command);

    SparkCommand cmd = SparkCommand.valueOf(command);
    String[] commandConfigs = Arrays.copyOfRange(args, 1, args.length);

    JavaSparkContext jsc = sparkMasterContained(cmd)
        ? SparkUtil.initJavaSparkConf("hoodie-cli-" + command, Option.of(args[1]), Option.of(args[2]))
        : SparkUtil.initJavaSparkConf("hoodie-cli-" + command);
    int returnCode = 0;
    switch (cmd) {
      case ROLLBACK:
        returnCode = rollback(jsc, getConfig(HoodieRollback.Config.class, commandConfigs));
        break;
      case DEDUPLICATE:
        returnCode = deduplicatePartitionPath(jsc, getConfig(DedupeConfig.class, commandConfigs));
        break;
      case ROLLBACK_TO_SAVEPOINT:
        returnCode = rollbackToSavepoint(jsc, getConfig(HoodieRollback.Config.class, commandConfigs));
        break;
      case IMPORT:
      case UPSERT:
        returnCode = dataLoad(jsc, getConfig(HDFSParquetImporter.Config.class, commandConfigs));
        break;
      case COMPACT_RUN:
      case COMPACT_SCHEDULE:
        LOG.info("now came to SparkMain for compact_schedule run.. ");
        returnCode = compact(jsc, getConfig(HoodieCompactor.Config.class, commandConfigs));
        break;
      case COMPACT_VALIDATE:
      case COMPACT_REPAIR:
      case COMPACT_UNSCHEDULE_FILE:
      case COMPACT_UNSCHEDULE_PLAN:
        doCompactOperation(jsc, getConfig(HoodieCompactionAdminTool.Config.class, commandConfigs));
        returnCode = 0;
        break;
      case CLEAN:
        clean(jsc, getConfig(HoodieCleaner.Config.class, commandConfigs));
        returnCode = 0;
        break;
      default:
        break;
    }
    System.exit(returnCode);
  }

  private static <T extends AbstractCommandConfig> T getConfig(Class<T> configClass, String[] configs) {
    try {
      LOG.info("getting config now for config class: " + configClass);
      T configObject = configClass.newInstance();
      configObject.parseCommandConfig(configs);
      return configObject;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new InvalidCommandConfigException("Unable to instantiate command config class ", e);
    }
  }

  private static void clean(JavaSparkContext jsc, HoodieCleaner.Config config) {
    LOG.info("Command config: " + config.toString());
    new HoodieCleaner(config, jsc).run();
  }

  private static int dataLoad(JavaSparkContext jsc, HDFSParquetImporter.Config config) {
    LOG.info("Command config: " + config.toString());
    return new HDFSParquetImporter(config).dataImport(jsc, config.retry);
  }

  private static void doCompactOperation(JavaSparkContext jsc, HoodieCompactionAdminTool.Config config) throws Exception {
    LOG.info("Command config: " + config.toString());
    new HoodieCompactionAdminTool(config).run(jsc);
  }

  private static int compact(JavaSparkContext jsc, HoodieCompactor.Config config) {
    LOG.info("Command config for HoodieCompactor: " + config.toString());
    jsc.getConf().set("spark.executor.memory", config.sparkMemory);
    return new HoodieCompactor(config).compact(jsc, config.retry);
  }

  private static boolean sparkMasterContained(SparkCommand command) {
    List<SparkCommand> masterContained = Arrays.asList(SparkCommand.COMPACT_VALIDATE, SparkCommand.COMPACT_REPAIR,
        SparkCommand.COMPACT_UNSCHEDULE_PLAN, SparkCommand.COMPACT_UNSCHEDULE_FILE, SparkCommand.CLEAN);
    return masterContained.contains(command);
  }

  private static int deduplicatePartitionPath(JavaSparkContext jsc, DedupeConfig config) {
    LOG.info("Command config: " + config.toString());
    DedupeSparkJob job = new DedupeSparkJob(config,
        new SQLContext(jsc),
        FSUtils.getFs(config.basePath(), jsc.hadoopConfiguration()));
    job.fixDuplicates(true);
    return 0;
  }

  private static int rollback(JavaSparkContext jsc, HoodieRollback.Config config) throws Exception {
    LOG.info("Command config: " + config.toString());
    return new HoodieRollback(config).rollback(jsc);
  }

  private static int rollbackToSavepoint(JavaSparkContext jsc, HoodieRollback.Config config) throws Exception {
    LOG.info("Command config: " + config.toString());
    return new HoodieRollback(config).rollbackToSavepoint(jsc);
  }
}
