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
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.utilities.HDFSParquetImporter;
import com.uber.hoodie.utilities.HoodieCompactor;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

public class SparkMain {

  protected static final Logger LOG = Logger.getLogger(SparkMain.class);

  /**
   * Commands
   */
  enum SparkCommand {
    ROLLBACK, DEDUPLICATE, ROLLBACK_TO_SAVEPOINT, SAVEPOINT, IMPORT, UPSERT, COMPACT_SCHEDULE, COMPACT_RUN,
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
        assert (args.length == 9);
        returnCode = compact(jsc, args[1], args[2], args[3], args[4], args[5], Integer.parseInt(args[6]),
            args[7], args[8], Integer.parseInt(args[9]), false);
        break;
      case COMPACT_SCHEDULE:
        assert (args.length == 10);
        returnCode = compact(jsc, args[1], args[2], args[3], args[4], args[5], Integer.parseInt(args[6]),
            args[7], args[8], Integer.parseInt(args[9]), true);
        break;
      default:
        break;
    }

    System.exit(returnCode);
  }

  private static int dataLoad(JavaSparkContext jsc, String command,
      String srcPath, String targetPath, String tableName,
      String tableType, String rowKey, String partitionKey, int parallelism, String schemaFile, String sparkMaster,
      String sparkMemory, int retry) throws Exception {
    HDFSParquetImporter.Config cfg = new HDFSParquetImporter.Config();
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

  private static int compact(JavaSparkContext jsc, String basePath, String tableName, String compactionInstant,
      String rowKey, String partitionKey, int parallelism, String schemaFile,
      String sparkMemory, int retry, boolean schedule) throws Exception {
    HoodieCompactor.Config cfg = new HoodieCompactor.Config();
    cfg.basePath = basePath;
    cfg.tableName = tableName;
    cfg.compactionInstantTime = compactionInstant;
    cfg.rowKey = rowKey;
    cfg.partitionKey = partitionKey;
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
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).withIndexConfig(
        HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build()).build();
    return new HoodieWriteClient(jsc, config);
  }
}
