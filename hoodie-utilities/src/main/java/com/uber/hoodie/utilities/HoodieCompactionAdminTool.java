package com.uber.hoodie.utilities;

import com.uber.hoodie.CompactionAdminClient;
import com.uber.hoodie.CompactionAdminClient.RenameOpResult;
import com.uber.hoodie.CompactionAdminClient.ValidationOpResult;
import com.uber.hoodie.common.model.HoodieFileGroupId;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.configs.HoodieCompactionAdminToolJobConfig;
import java.io.ObjectOutputStream;
import java.util.List;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;

public class HoodieCompactionAdminTool {

  private final HoodieCompactionAdminToolJobConfig cfg;

  public HoodieCompactionAdminTool(HoodieCompactionAdminToolJobConfig cfg) {
    this.cfg = cfg;
  }

  /**
   *
   */
  public static void main(String[] args) throws Exception {
    final HoodieCompactionAdminToolJobConfig cfg = new HoodieCompactionAdminToolJobConfig();
    cfg.parseJobConfig(args, true);

    HoodieCompactionAdminTool admin = new HoodieCompactionAdminTool(cfg);
    admin.run(UtilHelpers.buildSparkContext("admin-compactor", cfg.sparkMaster, cfg.sparkMemory));
  }

  /**
   * Executes one of compaction admin operations
   */
  public void run(JavaSparkContext jsc) throws Exception {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), cfg.basePath);
    CompactionAdminClient admin = new CompactionAdminClient(jsc, cfg.basePath);
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
        List<RenameOpResult> r =
            admin.unscheduleCompactionFileId(new HoodieFileGroupId(cfg.partitionPath, cfg.fileId),
                cfg.skipValidation, cfg.dryRun);
        if (cfg.printOutput) {
          System.out.println(r);
        }
        serializeOperationResult(fs, r);
        break;
      case UNSCHEDULE_PLAN:
        List<RenameOpResult> r2 =
            admin.unscheduleCompactionPlan(cfg.compactionInstantTime, cfg.skipValidation, cfg.parallelism, cfg.dryRun);
        if (cfg.printOutput) {
          printOperationResult("Result of Unscheduling Compaction Plan :", r2);
        }
        serializeOperationResult(fs, r2);
        break;
      case REPAIR:
        List<RenameOpResult> r3 =
            admin.repairCompaction(cfg.compactionInstantTime, cfg.parallelism, cfg.dryRun);
        if (cfg.printOutput) {
          printOperationResult("Result of Repair Operation :", r3);
        }
        serializeOperationResult(fs, r3);
        break;
      default:
        throw new IllegalStateException("Not yet implemented !!");
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
   * Print Operation Result
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
}
