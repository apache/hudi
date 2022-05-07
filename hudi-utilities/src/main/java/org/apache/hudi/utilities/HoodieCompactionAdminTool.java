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

package org.apache.hudi.utilities;

import org.apache.hudi.client.CompactionAdminClient;
import org.apache.hudi.client.CompactionAdminClient.RenameOpResult;
import org.apache.hudi.client.CompactionAdminClient.ValidationOpResult;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;

public class HoodieCompactionAdminTool {

  private final Config cfg;

  public HoodieCompactionAdminTool(Config cfg) {
    this.cfg = cfg;
  }

  public static void main(String[] args) throws Exception {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    HoodieCompactionAdminTool admin = new HoodieCompactionAdminTool(cfg);
    admin.run(UtilHelpers.buildSparkContext("admin-compactor", cfg.sparkMaster, cfg.sparkMemory));
  }

  /**
   * Executes one of compaction admin operations.
   */
  public void run(JavaSparkContext jsc) throws Exception {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(jsc.hadoopConfiguration()).setBasePath(cfg.basePath).build();
    try (CompactionAdminClient admin = new CompactionAdminClient(new HoodieSparkEngineContext(jsc), cfg.basePath)) {
      final FileSystem fs = FSUtils.getFs(cfg.basePath, jsc.hadoopConfiguration());
      if (cfg.outputPath != null && fs.exists(new Path(cfg.outputPath))) {
        throw new IllegalStateException("Output File Path already exists");
      }
      switch (cfg.operation) {
        case VALIDATE:
          List<ValidationOpResult> res =
              admin.validateCompactionPlan(metaClient, cfg.compactionInstantTime, cfg.parallelism);
          if (cfg.printOutput) {
            printOperationResult("Result of Validation Operation :", res);
          }
          serializeOperationResult(fs, res);
          break;
        case UNSCHEDULE_FILE:
          List<RenameOpResult> r = admin.unscheduleCompactionFileId(
              new HoodieFileGroupId(cfg.partitionPath, cfg.fileId), cfg.skipValidation, cfg.dryRun);
          if (cfg.printOutput) {
            System.out.println(r);
          }
          serializeOperationResult(fs, r);
          break;
        case UNSCHEDULE_PLAN:
          List<RenameOpResult> r2 = admin.unscheduleCompactionPlan(cfg.compactionInstantTime, cfg.skipValidation,
              cfg.parallelism, cfg.dryRun);
          if (cfg.printOutput) {
            printOperationResult("Result of Unscheduling Compaction Plan :", r2);
          }
          serializeOperationResult(fs, r2);
          break;
        case REPAIR:
          List<RenameOpResult> r3 = admin.repairCompaction(cfg.compactionInstantTime, cfg.parallelism, cfg.dryRun);
          if (cfg.printOutput) {
            printOperationResult("Result of Repair Operation :", r3);
          }
          serializeOperationResult(fs, r3);
          break;
        default:
          throw new IllegalStateException("Not yet implemented !!");
      }
    }
  }

  private <T> void serializeOperationResult(FileSystem fs, T result) throws Exception {
    if ((cfg.outputPath != null) && (result != null)) {
      Path outputPath = new Path(cfg.outputPath);
      FSDataOutputStream fsout = fs.create(outputPath, true);
      ObjectOutputStream out = new ObjectOutputStream(fsout);
      out.writeObject(result);
      out.close();
      fsout.close();
    }
  }

  /**
   * Print Operation Result.
   *
   * @param initialLine Initial Line
   * @param result Result
   */
  private <T> void printOperationResult(String initialLine, List<T> result) {
    System.out.println(initialLine);
    for (T r : result) {
      System.out.print(r);
    }
  }

  /**
   * Operation Types.
   */
  public enum Operation {
    VALIDATE, UNSCHEDULE_PLAN, UNSCHEDULE_FILE, REPAIR
  }

  /**
   * Admin Configuration Options.
   */
  public static class Config implements Serializable {

    @Parameter(names = {"--operation", "-op"}, description = "Operation", required = true)
    public Operation operation = Operation.VALIDATE;
    @Parameter(names = {"--base-path", "-bp"}, description = "Base path for the table", required = true)
    public String basePath = null;
    @Parameter(names = {"--instant-time", "-in"}, description = "Compaction Instant time", required = false)
    public String compactionInstantTime = null;
    @Parameter(names = {"--partition-path", "-pp"}, description = "Partition Path", required = false)
    public String partitionPath = null;
    @Parameter(names = {"--file-id", "-id"}, description = "File Id", required = false)
    public String fileId = null;
    @Parameter(names = {"--parallelism", "-pl"}, description = "Parallelism for hoodie insert", required = false)
    public int parallelism = 3;
    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = true)
    public String sparkMaster = null;
    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = true)
    public String sparkMemory = null;
    @Parameter(names = {"--dry-run", "-dr"}, description = "Dry Run Mode", required = false)
    public boolean dryRun = false;
    @Parameter(names = {"--skip-validation", "-sv"}, description = "Skip Validation", required = false)
    public boolean skipValidation = false;
    @Parameter(names = {"--output-path", "-ot"}, description = "Output Path", required = false)
    public String outputPath = null;
    @Parameter(names = {"--print-output", "-pt"}, description = "Print Output", required = false)
    public boolean printOutput = true;
    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;
  }
}
