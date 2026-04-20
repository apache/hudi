/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities;

import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.validator.DefaultStashPartitionRenameHelper;
import org.apache.hudi.client.validator.StashPartitionRenameHelper;
import org.apache.hudi.client.validator.StashPartitionsPreCommitValidator;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodiePreCommitValidatorConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A spark-submit tool for stashing (backing up and removing) Hudi table partitions.
 *
 * <p>This tool supports three modes:
 * <ul>
 *   <li><b>stash</b> — Moves data files from specified partitions to a stash location
 *       and issues a deletePartitions operation on the Hudi table. Uses a custom
 *       {@link StashPartitionsPreCommitValidator} to move files atomically before commit.</li>
 *   <li><b>rollback_stash</b> — Copies any data found in the stash location back to the
 *       original partition paths. Used when the user no longer wants to proceed with stashing.</li>
 *   <li><b>dry_run</b> — Shows which partitions and file groups would be affected.</li>
 * </ul>
 *
 * <h3>Why a pre-commit validator instead of moving files in the tool?</h3>
 *
 * <p>A naive approach would be to first run {@code deletePartitions} in the tool, and then
 * move the data files to the stash location as a separate step. However, this is unsafe:
 * if the tool crashes or fails after {@code deletePartitions} succeeds but before the data
 * is moved to the stash location, the data is at risk. Once the replace commit from
 * {@code deletePartitions} lands on the timeline, the Hudi cleaner will eventually delete
 * the physical data files for the replaced file groups based on its configured retention
 * policy (e.g., retain last N commits). If the cleaner runs before the tool is retried,
 * the data intended for stashing is permanently lost.
 *
 * <p>By using a {@link StashPartitionsPreCommitValidator}, the file move happens
 * <b>before</b> the {@code deletePartitions} commit is finalized. If the move fails, the
 * commit does not land, and the table remains in its original state — the cleaner has
 * nothing to clean. This ensures that data is never in a state where it has been marked
 * as deleted (via a replace commit) but has not yet been safely copied to the stash location.
 *
 * <p>For restoring stashed partitions back into the Hudi table, use the standard
 * {@code insert_overwrite} write operation reading from the stash location.
 *
 * <p>Example usage:
 * <pre>
 * spark-submit \
 *   --class org.apache.hudi.utilities.HoodieStashPartitionsTool \
 *   --master local[*] \
 *   --driver-memory 1g \
 *   --executor-memory 1g \
 *   hudi-utilities-bundle.jar \
 *   --base-path /path/to/hudi/table \
 *   --table-name myTable \
 *   --stash-path /path/to/stash/folder \
 *   --partitions datestr=2023-01-01,datestr=2023-01-02 \
 *   --mode stash
 * </pre>
 */
public class HoodieStashPartitionsTool implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(HoodieStashPartitionsTool.class);

  private final transient JavaSparkContext jsc;
  private final Config cfg;
  private final TypedProperties props;
  private final HoodieTableMetaClient metaClient;

  public HoodieStashPartitionsTool(JavaSparkContext jsc, Config cfg) {
    this.jsc = jsc;
    this.cfg = cfg;
    this.props = cfg.propsFilePath == null
        ? UtilHelpers.buildProperties(cfg.configs)
        : readConfigFromFileSystem(jsc, cfg);
    this.metaClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()))
        .setBasePath(cfg.basePath)
        .setLoadActiveTimelineOnLoad(true)
        .build();
  }

  public enum Mode {
    STASH,
    ROLLBACK_STASH,
    DRY_RUN
  }

  public static class Config implements Serializable {
    @Parameter(names = {"--base-path", "-sp"}, description = "Base path for the Hudi table.", required = true)
    public String basePath = null;

    @Parameter(names = {"--table-name", "-tn"}, description = "Table name.", required = true)
    public String tableName = null;

    @Parameter(names = {"--stash-path"}, description = "Target path for stashing partition data.", required = true)
    public String stashPath = null;

    @Parameter(names = {"--partitions", "-p"}, description = "Comma separated list of partitions to stash.", required = true)
    public String partitions = null;

    @Parameter(names = {"--mode", "-m"}, description = "Set job mode: "
        + "\"stash\" to move partition data to stash location and delete partitions; "
        + "\"rollback_stash\" to copy stashed data back to table and abort stashing; "
        + "\"dry_run\" to preview which partitions and files would be affected.", required = true)
    public String runningMode = null;

    @Parameter(names = {"--parallelism", "-pl"}, description = "Parallelism for hoodie operations.", required = false)
    public int parallelism = 1500;

    @Parameter(names = {"--rename-helper-class"}, description = "Fully qualified class name of a custom "
        + "StashPartitionRenameHelper implementation. Defaults to DefaultStashPartitionRenameHelper.", required = false)
    public String renameHelperClass = null;

    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master.", required = false)
    public String sparkMaster = null;

    @Parameter(names = {"--spark-memory", "-sm"}, description = "Spark memory to use.", required = false)
    public String sparkMemory = "1g";

    @Parameter(names = {"--props"}, description = "Path to properties file on localfs or dfs, with configurations for "
        + "hoodie client for stashing partitions.")
    public String propsFilePath = null;

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed via command line using this parameter. This can be repeated.",
        splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Override
    public String toString() {
      return "HoodieStashPartitionsToolConfig {\n"
          + "   --base-path " + basePath + ", \n"
          + "   --table-name " + tableName + ", \n"
          + "   --stash-path " + stashPath + ", \n"
          + "   --partitions " + partitions + ", \n"
          + "   --mode " + runningMode + ", \n"
          + "   --parallelism " + parallelism + ", \n"
          + "   --rename-helper-class " + renameHelperClass + ", \n"
          + "   --spark-master " + sparkMaster + ", \n"
          + "   --spark-memory " + sparkMemory + ", \n"
          + "   --props " + propsFilePath + ", \n"
          + "   --hoodie-conf " + configs
          + "\n}";
    }
  }

  public static void main(String[] args) {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    Map<String, String> sparkConfigMap = new HashMap<>();
    sparkConfigMap.put("spark.executor.memory", cfg.sparkMemory);
    JavaSparkContext jsc = UtilHelpers.buildSparkContext("Hoodie-Stash-Partitions",
        cfg.sparkMaster, false, sparkConfigMap);

    HoodieStashPartitionsTool tool = new HoodieStashPartitionsTool(jsc, cfg);
    try {
      tool.run();
    } catch (Throwable throwable) {
      LOG.error("Fail to run stash partitions for " + cfg, throwable);
    } finally {
      jsc.stop();
    }
  }

  public void run() {
    try {
      LOG.info(cfg.toString());
      Mode mode = Mode.valueOf(cfg.runningMode.toUpperCase());
      switch (mode) {
        case STASH:
          LOG.info(" ****** The Hoodie Stash Partitions Tool is in stash mode ****** ");
          doStash();
          break;
        case ROLLBACK_STASH:
          LOG.info(" ****** The Hoodie Stash Partitions Tool is in rollback_stash mode ****** ");
          doRollbackStash();
          break;
        case DRY_RUN:
          LOG.info(" ****** The Hoodie Stash Partitions Tool is in dry-run mode ****** ");
          doDryRun();
          break;
        default:
          LOG.info("Unsupported running mode [" + cfg.runningMode + "], quit the job directly");
      }
    } catch (Exception e) {
      throw new HoodieException("Unable to stash partitions in " + cfg.basePath, e);
    }
  }

  /**
   * Stash partitions: pre-check to recover any partial stash from a prior failed attempt,
   * then issue deletePartitions with custom pre-commit validator. Hudi internally handles
   * rollback of any prior failed instants. The pre-commit validator handles already-stashed
   * partitions gracefully (source empty -> skip).
   *
   * <p>The pre-check runs regardless of whether MDT is enabled. Even with MDT, a prior failed
   * attempt may have had its pre-commit validator move files to the stash location before
   * crashing. Those files need to be moved back so that the subsequent deletePartitions
   * sees the complete partition state.
   */
  private void doStash() {
    List<String> partitionsToStash = Arrays.asList(cfg.partitions.split(","));

    preCheckAndRecoverPartialStash(partitionsToStash);

    // Set up props with stash-specific configs for the pre-commit validator
    TypedProperties stashProps = new TypedProperties();
    stashProps.putAll(props);
    stashProps.setProperty(HoodiePreCommitValidatorConfig.VALIDATOR_CLASS_NAMES.key(),
        StashPartitionsPreCommitValidator.class.getName());
    stashProps.setProperty(StashPartitionsPreCommitValidator.STASH_PATH_CONFIG, cfg.stashPath);
    if (!StringUtils.isNullOrEmpty(cfg.renameHelperClass)) {
      stashProps.setProperty(StashPartitionsPreCommitValidator.RENAME_HELPER_CLASS_CONFIG, cfg.renameHelperClass);
    }

    try (SparkRDDWriteClient<HoodieRecordPayload> client =
             UtilHelpers.createHoodieClient(jsc, cfg.basePath, "", cfg.parallelism, Option.empty(), stashProps)) {
      String instantTime = client.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION);
      LOG.info("Starting deletePartitions with instant {} for partitions: {}", instantTime, partitionsToStash);
      HoodieWriteResult result = client.deletePartitions(partitionsToStash, instantTime);
      client.commit(instantTime, result.getWriteStatuses(), Option.empty(), HoodieTimeline.REPLACE_COMMIT_ACTION,
          result.getPartitionToReplaceFileIds(), Option.empty());
      LOG.info("Successfully stashed partitions: {}", partitionsToStash);
    }
  }

  /**
   * Pre-check: if files exist in the stash location from a prior failed attempt,
   * copy them back to source before retrying deletePartitions. This ensures the
   * subsequent deletePartitions sees the complete partition state and creates
   * a correct replace commit. Runs for both MDT and non-MDT tables, since the
   * pre-commit validator may have moved files before crashing.
   */
  private void preCheckAndRecoverPartialStash(List<String> partitions) {
    LOG.info("Running pre-check for partial stash recovery (non-MDT table).");
    HoodieStorage storage = HoodieStorageUtils.getStorage(
        cfg.basePath, metaClient.getStorageConf());
    StashPartitionRenameHelper renameHelper = createRenameHelper();
    StoragePath basePath = metaClient.getBasePath();

    for (String partition : partitions) {
      StoragePath stashPartitionPath = new StoragePath(cfg.stashPath, partition);
      StoragePath sourcePartitionPath = new StoragePath(basePath, partition);

      try {
        if (!storage.exists(stashPartitionPath) || isDirectoryEmpty(storage, stashPartitionPath)) {
          LOG.info("Partition {} has no stashed data from prior attempt, skipping pre-check.", partition);
          continue;
        }
        // Files exist in stash — copy them back to source to ensure deletePartitions
        // sees all files and creates a complete replace commit.
        LOG.info("Found stashed files for partition {} from prior attempt. Moving back to source.", partition);
        renameHelper.movePartitionFiles(storage, stashPartitionPath, sourcePartitionPath);
        LOG.info("Recovered partition {} back to source.", partition);
      } catch (IOException e) {
        throw new HoodieException("Failed to recover partition " + partition + " from stash during pre-check.", e);
      }
    }
  }

  /**
   * Rollback stash: copy any data from stash location back to original partition paths.
   * This is for when the user no longer wants to proceed with stashing.
   */
  private void doRollbackStash() {
    List<String> partitions = Arrays.asList(cfg.partitions.split(","));
    HoodieStorage storage = HoodieStorageUtils.getStorage(
        cfg.basePath, metaClient.getStorageConf());
    StashPartitionRenameHelper renameHelper = createRenameHelper();
    StoragePath basePath = metaClient.getBasePath();

    for (String partition : partitions) {
      StoragePath stashPartitionPath = new StoragePath(cfg.stashPath, partition);
      StoragePath sourcePartitionPath = new StoragePath(basePath, partition);

      try {
        if (!storage.exists(stashPartitionPath) || isDirectoryEmpty(storage, stashPartitionPath)) {
          LOG.info("No stashed data found for partition {}, skipping.", partition);
          continue;
        }
        LOG.info("Rolling back stash for partition {}: moving from {} to {}", partition, stashPartitionPath, sourcePartitionPath);
        renameHelper.movePartitionFiles(storage, stashPartitionPath, sourcePartitionPath);
        LOG.info("Successfully rolled back stash for partition {}.", partition);
      } catch (IOException e) {
        throw new HoodieException("Failed to rollback stash for partition: " + partition, e);
      }
    }
    LOG.info("Stash rollback complete.");
  }

  /**
   * Dry run: show partitions and file groups that would be affected.
   */
  private void doDryRun() {
    try (SparkRDDWriteClient<HoodieRecordPayload> client =
             UtilHelpers.createHoodieClient(jsc, cfg.basePath, "", cfg.parallelism, Option.empty(), props)) {
      org.apache.hudi.table.HoodieSparkTable<HoodieRecordPayload> table =
          org.apache.hudi.table.HoodieSparkTable.create(client.getConfig(), client.getEngineContext());
      List<String> parts = Arrays.asList(cfg.partitions.split(","));
      HoodieStorage storage = HoodieStorageUtils.getStorage(
          cfg.basePath, metaClient.getStorageConf());
      StoragePath basePath = metaClient.getBasePath();

      LOG.info("=== Dry Run: Partitions and files that would be stashed ===");
      for (String partition : parts) {
        StoragePath sourcePartitionPath = new StoragePath(basePath, partition);
        StoragePath stashPartitionPath = new StoragePath(cfg.stashPath, partition);

        List<String> fileIds = table.getSliceView()
            .getLatestFileSlices(partition)
            .map(fs -> fs.getFileId())
            .distinct()
            .collect(Collectors.toList());

        boolean sourceExists;
        boolean stashExists;
        try {
          sourceExists = storage.exists(sourcePartitionPath) && !isDirectoryEmpty(storage, sourcePartitionPath);
          stashExists = storage.exists(stashPartitionPath) && !isDirectoryEmpty(storage, stashPartitionPath);
        } catch (IOException e) {
          throw new HoodieException("Failed to check paths for partition: " + partition, e);
        }

        LOG.info("Partition: {}", partition);
        LOG.info("  Source path: {} (exists={})", sourcePartitionPath, sourceExists);
        LOG.info("  Stash path: {} (exists={})", stashPartitionPath, stashExists);
        LOG.info("  File group IDs: {}", fileIds);
      }
    }
  }

  private boolean isDirectoryEmpty(HoodieStorage storage, StoragePath path) throws IOException {
    List<StoragePathInfo> entries = storage.listDirectEntries(path);
    return entries.isEmpty();
  }

  private StashPartitionRenameHelper createRenameHelper() {
    if (!StringUtils.isNullOrEmpty(cfg.renameHelperClass)) {
      LOG.info("Using custom rename helper: {}", cfg.renameHelperClass);
      return (StashPartitionRenameHelper) ReflectionUtils.loadClass(cfg.renameHelperClass);
    }
    return new DefaultStashPartitionRenameHelper();
  }

  private TypedProperties readConfigFromFileSystem(JavaSparkContext jsc, Config cfg) {
    return UtilHelpers.readConfig(jsc.hadoopConfiguration(), new Path(cfg.propsFilePath), cfg.configs)
        .getProps(true);
  }
}
