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

import org.apache.hudi.HoodieWriteClient;
import org.apache.hudi.cli.DedupeSparkJob;
import org.apache.hudi.cli.utils.SparkUtil;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.io.compact.strategy.UnBoundedCompactionStrategy;
import org.apache.hudi.utilities.HDFSParquetImporter;
import org.apache.hudi.utilities.HDFSParquetImporter.Config;
import org.apache.hudi.utilities.HoodieCompactionAdminTool;
import org.apache.hudi.utilities.HoodieCompactionAdminTool.Operation;
import org.apache.hudi.utilities.HoodieCompactor;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * This class deals with initializing spark context based on command entered to hudi-cli.
 */
public class SparkMain {

  protected static final Logger LOG = Logger.getLogger(SparkMain.class);

  /**
   * Commands.
   */
  enum SparkCommand {
    ROLLBACK, DEDUPLICATE, ROLLBACK_TO_SAVEPOINT, SAVEPOINT, IMPORT, UPSERT, COMPACT_SCHEDULE, COMPACT_RUN, COMPACT_UNSCHEDULE_PLAN, COMPACT_UNSCHEDULE_FILE, COMPACT_VALIDATE, COMPACT_REPAIR
  }

  public static void main(String[] args) throws Exception {
    String command = args[0];
    LOG.info("Invoking SparkMain:" + command);

    SparkCommand cmd = SparkCommand.valueOf(command);

    JavaSparkContext jsc = SparkUtil.initJavaSparkConf("hoodie-cli-" + command);
    int returnCode = 0;
    switch (cmd) {
      case ROLLBACK:
        assert (args.length == 3);
        returnCode = rollback(jsc, args[1], args[2]);
        break;
      case DEDUPLICATE:
        assert (args.length == 4);
        returnCode = deduplicatePartitionPath(jsc, args[1], args[2], args[3]);
        break;
      case ROLLBACK_TO_SAVEPOINT:
        assert (args.length == 3);
        returnCode = rollbackToSavepoint(jsc, args[1], args[2]);
        break;
      case IMPORT:
      case UPSERT:
        assert (args.length == 11);
        returnCode = dataLoad(jsc, command, args[1], args[2], args[3], args[4], args[5], args[6],
            Integer.parseInt(args[7]), args[8], SparkUtil.DEFUALT_SPARK_MASTER, args[9], Integer.parseInt(args[10]));
        break;
      case COMPACT_RUN:
        assert (args.length == 8);
        returnCode = compact(jsc, args[1], args[2], args[3], Integer.parseInt(args[4]), args[5], args[6],
            Integer.parseInt(args[7]), false);
        break;
      case COMPACT_SCHEDULE:
        assert (args.length == 5);
        returnCode = compact(jsc, args[1], args[2], args[3], 1, "", args[4], 0, true);
        break;
      case COMPACT_VALIDATE:
        assert (args.length == 7);
        doCompactValidate(jsc, args[1], args[2], args[3], Integer.parseInt(args[4]), args[5], args[6]);
        returnCode = 0;
        break;
      case COMPACT_REPAIR:
        assert (args.length == 8);
        doCompactRepair(jsc, args[1], args[2], args[3], Integer.parseInt(args[4]), args[5], args[6],
            Boolean.valueOf(args[7]));
        returnCode = 0;
        break;
      case COMPACT_UNSCHEDULE_FILE:
        assert (args.length == 9);
        doCompactUnscheduleFile(jsc, args[1], args[2], args[3], Integer.parseInt(args[4]), args[5], args[6],
            Boolean.valueOf(args[7]), Boolean.valueOf(args[8]));
        returnCode = 0;
        break;
      case COMPACT_UNSCHEDULE_PLAN:
        assert (args.length == 9);
        doCompactUnschedule(jsc, args[1], args[2], args[3], Integer.parseInt(args[4]), args[5], args[6],
            Boolean.valueOf(args[7]), Boolean.valueOf(args[8]));
        returnCode = 0;
        break;
      default:
        break;
    }
    System.exit(returnCode);
  }

  private static int dataLoad(JavaSparkContext jsc, String command, String srcPath, String targetPath, String tableName,
      String tableType, String rowKey, String partitionKey, int parallelism, String schemaFile, String sparkMaster,
      String sparkMemory, int retry) throws Exception {
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
    jsc.getConf().set("spark.executor.memory", sparkMemory);
    return new HDFSParquetImporter(cfg).dataImport(jsc, retry);
  }

  private static void doCompactValidate(JavaSparkContext jsc, String basePath, String compactionInstant,
      String outputPath, int parallelism, String sparkMaster, String sparkMemory) throws Exception {
    HoodieCompactionAdminTool.Config cfg = new HoodieCompactionAdminTool.Config();
    cfg.basePath = basePath;
    cfg.operation = Operation.VALIDATE;
    cfg.outputPath = outputPath;
    cfg.compactionInstantTime = compactionInstant;
    cfg.parallelism = parallelism;
    if ((null != sparkMaster) && (!sparkMaster.isEmpty())) {
      jsc.getConf().setMaster(sparkMaster);
    }
    jsc.getConf().set("spark.executor.memory", sparkMemory);
    new HoodieCompactionAdminTool(cfg).run(jsc);
  }

  private static void doCompactRepair(JavaSparkContext jsc, String basePath, String compactionInstant,
      String outputPath, int parallelism, String sparkMaster, String sparkMemory, boolean dryRun) throws Exception {
    HoodieCompactionAdminTool.Config cfg = new HoodieCompactionAdminTool.Config();
    cfg.basePath = basePath;
    cfg.operation = Operation.REPAIR;
    cfg.outputPath = outputPath;
    cfg.compactionInstantTime = compactionInstant;
    cfg.parallelism = parallelism;
    cfg.dryRun = dryRun;
    if ((null != sparkMaster) && (!sparkMaster.isEmpty())) {
      jsc.getConf().setMaster(sparkMaster);
    }
    jsc.getConf().set("spark.executor.memory", sparkMemory);
    new HoodieCompactionAdminTool(cfg).run(jsc);
  }

  private static void doCompactUnschedule(JavaSparkContext jsc, String basePath, String compactionInstant,
      String outputPath, int parallelism, String sparkMaster, String sparkMemory, boolean skipValidation,
      boolean dryRun) throws Exception {
    HoodieCompactionAdminTool.Config cfg = new HoodieCompactionAdminTool.Config();
    cfg.basePath = basePath;
    cfg.operation = Operation.UNSCHEDULE_PLAN;
    cfg.outputPath = outputPath;
    cfg.compactionInstantTime = compactionInstant;
    cfg.parallelism = parallelism;
    cfg.dryRun = dryRun;
    cfg.skipValidation = skipValidation;
    if ((null != sparkMaster) && (!sparkMaster.isEmpty())) {
      jsc.getConf().setMaster(sparkMaster);
    }
    jsc.getConf().set("spark.executor.memory", sparkMemory);
    new HoodieCompactionAdminTool(cfg).run(jsc);
  }

  private static void doCompactUnscheduleFile(JavaSparkContext jsc, String basePath, String fileId, String outputPath,
      int parallelism, String sparkMaster, String sparkMemory, boolean skipValidation, boolean dryRun)
      throws Exception {
    HoodieCompactionAdminTool.Config cfg = new HoodieCompactionAdminTool.Config();
    cfg.basePath = basePath;
    cfg.operation = Operation.UNSCHEDULE_FILE;
    cfg.outputPath = outputPath;
    cfg.fileId = fileId;
    cfg.parallelism = parallelism;
    cfg.dryRun = dryRun;
    cfg.skipValidation = skipValidation;
    if ((null != sparkMaster) && (!sparkMaster.isEmpty())) {
      jsc.getConf().setMaster(sparkMaster);
    }
    jsc.getConf().set("spark.executor.memory", sparkMemory);
    new HoodieCompactionAdminTool(cfg).run(jsc);
  }

  private static int compact(JavaSparkContext jsc, String basePath, String tableName, String compactionInstant,
      int parallelism, String schemaFile, String sparkMemory, int retry, boolean schedule) throws Exception {
    HoodieCompactor.Config cfg = new HoodieCompactor.Config();
    cfg.basePath = basePath;
    cfg.tableName = tableName;
    cfg.compactionInstantTime = compactionInstant;
    // TODO: Make this configurable along with strategy specific config - For now, this is a generic enough strategy
    cfg.strategyClassName = UnBoundedCompactionStrategy.class.getCanonicalName();
    cfg.parallelism = parallelism;
    cfg.schemaFile = schemaFile;
    cfg.runSchedule = schedule;
    jsc.getConf().set("spark.executor.memory", sparkMemory);
    return new HoodieCompactor(cfg).compact(jsc, retry);
  }

  private static int deduplicatePartitionPath(JavaSparkContext jsc, String duplicatedPartitionPath,
      String repairedOutputPath, String basePath) throws Exception {
    DedupeSparkJob job = new DedupeSparkJob(basePath, duplicatedPartitionPath, repairedOutputPath, new SQLContext(jsc),
        FSUtils.getFs(basePath, jsc.hadoopConfiguration()));
    job.fixDuplicates(true);
    return 0;
  }

  private static int rollback(JavaSparkContext jsc, String commitTime, String basePath) throws Exception {
    HoodieWriteClient client = createHoodieClient(jsc, basePath);
    if (client.rollback(commitTime)) {
      LOG.info(String.format("The commit \"%s\" rolled back.", commitTime));
      return 0;
    } else {
      LOG.info(String.format("The commit \"%s\" failed to roll back.", commitTime));
      return -1;
    }
  }

  private static int rollbackToSavepoint(JavaSparkContext jsc, String savepointTime, String basePath) throws Exception {
    HoodieWriteClient client = createHoodieClient(jsc, basePath);
    if (client.rollbackToSavepoint(savepointTime)) {
      LOG.info(String.format("The commit \"%s\" rolled back.", savepointTime));
      return 0;
    } else {
      LOG.info(String.format("The commit \"%s\" failed to roll back.", savepointTime));
      return -1;
    }
  }

  private static HoodieWriteClient createHoodieClient(JavaSparkContext jsc, String basePath) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build()).build();
    return new HoodieWriteClient(jsc, config);
  }
}
