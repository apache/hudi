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
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodiePreCommitValidatorConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieSparkTable;

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
import java.util.LinkedHashMap;
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
      throw new HoodieException("Fail to run stash partitions for " + cfg, throwable);
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
   * Stash partitions: validates partition state using table metadata, recovers any partial
   * stash from a prior failed attempt, then issues deletePartitions with a custom pre-commit
   * validator.
   *
   * <p>The pre-check uses {@link HoodieTableMetadata} (MDT-backed when available) to determine
   * whether each partition still has active files. This allows the tool to distinguish between:
   * <ul>
   *   <li>A prior successful stash (partition deleted + data in stash) → no-op, return early</li>
   *   <li>A partial prior attempt (partition has files + data in stash) → recover from stash, retry</li>
   *   <li>A fresh run (partition has files + no stash data) → proceed normally</li>
   * </ul>
   */
  private void doStash() {
    List<String> partitionsToStash = Arrays.asList(cfg.partitions.split(","));

    if (!preCheckAndRecoverPartialStash(partitionsToStash)) {
      // All partitions already stashed successfully — nothing to do.
      return;
    }

    // Validate that all partitions are in a clean, compacted state before stashing.
    validatePartitionsForStash(partitionsToStash);

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
   * Pre-check: evaluates partition states and recovers any partial stash from a prior
   * failed attempt. For partitions with a partial stash (active files + stash data),
   * restores files from stash back to source so deletePartitions sees the complete state.
   *
   * @return true if deletePartitions should proceed, false if all partitions are already stashed
   */
  private boolean preCheckAndRecoverPartialStash(List<String> partitions) {
    LOG.info("Running pre-check: validating partition state via table metadata.");
    Map<String, PartitionStashState> stateMap = evaluatePartitionStates(partitions);
    HoodieStorage storage = HoodieStorageUtils.getStorage(cfg.basePath, metaClient.getStorageConf());
    StashPartitionRenameHelper renameHelper = createRenameHelper();
    StoragePath basePath = metaClient.getBasePath();
    int alreadyStashedCount = 0;

    for (Map.Entry<String, PartitionStashState> entry : stateMap.entrySet()) {
      String partition = entry.getKey();
      PartitionStashState state = entry.getValue();

      switch (state) {
        case ALREADY_STASHED:
          LOG.info("Partition {} already stashed (no active files in table, data exists in stash). Skipping.", partition);
          alreadyStashedCount++;
          break;
        case PARTIAL_STASH:
          LOG.info("Partition {} has active files and stash data from a prior partial attempt. "
              + "Restoring stash data back to source before retry.", partition);
          restorePartitionFromStash(renameHelper, storage, partition, basePath);
          break;
        case NOT_STASHED:
          LOG.info("Partition {} has active files and no prior stash data. Will proceed with stash.", partition);
          break;
        case NO_DATA:
          LOG.warn("Partition {} has no active files and no stash data. "
              + "It may have been deleted by another operation.", partition);
          break;
        default:
          break;
      }
    }

    if (alreadyStashedCount == partitions.size()) {
      LOG.info("All {} partition(s) are already stashed successfully. Nothing to do.", partitions.size());
      return false;
    }
    return true;
  }

  /**
   * Rollback stash: evaluates partition states and restores stashed data back to the
   * original partition paths only if the stash was partial (replace commit never landed).
   * If the stash fully completed (replace commit landed), rollback is not applicable —
   * the user must run an {@code insert_overwrite} from the stash location.
   */
  private void doRollbackStash() {
    List<String> partitions = Arrays.asList(cfg.partitions.split(","));
    Map<String, PartitionStashState> stateMap = evaluatePartitionStates(partitions);
    HoodieStorage storage = HoodieStorageUtils.getStorage(cfg.basePath, metaClient.getStorageConf());
    StashPartitionRenameHelper renameHelper = createRenameHelper();
    StoragePath basePath = metaClient.getBasePath();

    for (Map.Entry<String, PartitionStashState> entry : stateMap.entrySet()) {
      String partition = entry.getKey();
      PartitionStashState state = entry.getValue();
      StoragePath stashPartitionPath = new StoragePath(cfg.stashPath, partition);

      switch (state) {
        case ALREADY_STASHED:
          LOG.warn("Partition {} stash completed (replace commit landed). Rollback is not applicable. "
              + "To restore this partition, run an insert_overwrite from the stash location: {}", partition, stashPartitionPath);
          break;
        case PARTIAL_STASH:
          LOG.info("Partition {} has active files and stash data from a partial attempt. Restoring stash data to source.", partition);
          restorePartitionFromStash(renameHelper, storage, partition, basePath);
          LOG.info("Successfully rolled back partial stash for partition {}.", partition);
          break;
        case NOT_STASHED:
          LOG.info("No stash data found for partition {} and partition has active files. Nothing to rollback.", partition);
          break;
        case NO_DATA:
          LOG.warn("Partition {} has no active files and no stash data. Nothing to rollback.", partition);
          break;
        default:
          break;
      }
    }
    LOG.info("Stash rollback complete.");
  }

  // ---- Partition state evaluation ----

  /**
   * Classifies the stash state of each partition by checking active files via
   * {@link HoodieTableMetadata} and data in the stash location.
   */
  enum PartitionStashState {
    /** No active files in table, data exists in stash — stash fully completed. */
    ALREADY_STASHED,
    /** Active files in table and data in stash — partial prior attempt. */
    PARTIAL_STASH,
    /** Active files in table, no stash data — fresh partition, not yet stashed. */
    NOT_STASHED,
    /** No active files and no stash data — partition empty or deleted elsewhere. */
    NO_DATA
  }

  /**
   * Evaluates the stash state for each partition using {@link HoodieTableMetadata}
   * (MDT-backed when available, otherwise filesystem-based).
   */
  private Map<String, PartitionStashState> evaluatePartitionStates(List<String> partitions) {
    HoodieStorage storage = HoodieStorageUtils.getStorage(cfg.basePath, metaClient.getStorageConf());
    StoragePath basePath = metaClient.getBasePath();

    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(metaClient.getTableConfig().isMetadataTableAvailable())
        .build();

    Map<String, PartitionStashState> stateMap = new LinkedHashMap<>();

    try (HoodieTableMetadata tableMetadata = metaClient.getTableFormat().getMetadataFactory()
        .create(new HoodieLocalEngineContext(metaClient.getStorageConf()),
            metaClient.getStorage(), metadataConfig, basePath.toString(), true)) {

      for (String partition : partitions) {
        StoragePath sourcePartitionPath = new StoragePath(basePath, partition);
        StoragePath stashPartitionPath = new StoragePath(cfg.stashPath, partition);

        boolean partitionHasFiles = hasFilesInPartition(tableMetadata, sourcePartitionPath);
        boolean stashHasData = hasStashData(storage, stashPartitionPath);

        if (!partitionHasFiles && stashHasData) {
          stateMap.put(partition, PartitionStashState.ALREADY_STASHED);
        } else if (partitionHasFiles && stashHasData) {
          stateMap.put(partition, PartitionStashState.PARTIAL_STASH);
        } else if (partitionHasFiles) {
          stateMap.put(partition, PartitionStashState.NOT_STASHED);
        } else {
          stateMap.put(partition, PartitionStashState.NO_DATA);
        }
      }
    } catch (Exception e) {
      throw new HoodieException("Failed to evaluate partition stash states.", e);
    }

    return stateMap;
  }

  private void restorePartitionFromStash(StashPartitionRenameHelper renameHelper, HoodieStorage storage,
                                          String partition, StoragePath basePath) {
    StoragePath stashPartitionPath = new StoragePath(cfg.stashPath, partition);
    StoragePath sourcePartitionPath = new StoragePath(basePath, partition);
    try {
      renameHelper.restorePartitionFiles(storage, stashPartitionPath, sourcePartitionPath);
      LOG.info("Recovered partition {} back to source.", partition);
    } catch (IOException e) {
      throw new HoodieException("Failed to restore partition " + partition + " from stash.", e);
    }
  }

  /**
   * Validates that all partitions are in a clean, compacted state suitable for stashing.
   * Builds a file system view from {@link HoodieTableMetadata} and delegates to
   * {@link #validateFileGroupsForStash(HoodieTableFileSystemView, List)}.
   */
  private void validatePartitionsForStash(List<String> partitions) {
    LOG.info("Validating partitions are in a clean state for stashing.");
    StoragePath basePath = metaClient.getBasePath();

    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(metaClient.getTableConfig().isMetadataTableAvailable())
        .build();

    try (HoodieTableMetadata tableMetadata = metaClient.getTableFormat().getMetadataFactory()
        .create(new HoodieLocalEngineContext(metaClient.getStorageConf()),
            metaClient.getStorage(), metadataConfig, basePath.toString(), true)) {

      // Collect files across all partitions to build a single FSV
      List<StoragePathInfo> allFiles = new ArrayList<>();
      for (String partition : partitions) {
        StoragePath partitionPath = new StoragePath(basePath, partition);
        try {
          allFiles.addAll(tableMetadata.getAllFilesInPartition(partitionPath));
        } catch (IOException e) {
          throw new HoodieException("Failed to list files for partition: " + partition, e);
        }
      }

      try (HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(
          metaClient, metaClient.getActiveTimeline(), allFiles)) {
        validateFileGroupsForStash(fsView, partitions);
      }
    } catch (HoodieValidationException e) {
      throw e;
    } catch (Exception e) {
      throw new HoodieException("Failed to validate partitions for stash.", e);
    }

    LOG.info("All partitions validated successfully for stashing.");
  }

  /**
   * Validates that all file groups in the given partitions are in a clean state for stashing.
   * For each file group, checks that:
   * <ul>
   *   <li>There is exactly one file slice — no pending compaction or older slices.</li>
   *   <li>The file slice contains a base file.</li>
   *   <li>The file slice has no log files — no uncompacted delta data.</li>
   * </ul>
   *
   * @param fsView     the file system view to query
   * @param partitions the partitions to validate
   * @throws HoodieValidationException if any file group violates the conditions
   */
  static void validateFileGroupsForStash(HoodieTableFileSystemView fsView, List<String> partitions) {
    for (String partition : partitions) {
      List<HoodieFileGroup> fileGroups = fsView.getAllFileGroups(partition)
          .collect(Collectors.toList());

      for (HoodieFileGroup fileGroup : fileGroups) {
        List<FileSlice> allSlices = fileGroup.getAllFileSlices().collect(Collectors.toList());

        if (allSlices.size() != 1) {
          throw new HoodieValidationException(
              "Partition " + partition + " file group " + fileGroup.getFileGroupId()
                  + " has " + allSlices.size() + " file slice(s), expected exactly 1. "
                  + "Ensure all compactions are complete before stashing.");
        }

        FileSlice slice = allSlices.get(0);
        if (!slice.getBaseFile().isPresent()) {
          throw new HoodieValidationException(
              "Partition " + partition + " file group " + fileGroup.getFileGroupId()
                  + " has a file slice with no base file. "
                  + "Ensure the partition is in a valid state before stashing.");
        }

        if (slice.getLogFiles().findAny().isPresent()) {
          throw new HoodieValidationException(
              "Partition " + partition + " file group " + fileGroup.getFileGroupId()
                  + " has log files in its file slice. "
                  + "Ensure all compactions are complete before stashing.");
        }
      }
    }
  }

  private boolean hasFilesInPartition(HoodieTableMetadata tableMetadata, StoragePath partitionPath) {
    HoodieTableFileSystemView fileSystemView = null;
    try {
      List<StoragePathInfo> files = tableMetadata.getAllFilesInPartition(partitionPath);

      fileSystemView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline(), files);
      return fileSystemView.getLatestFileSlices(FSUtils.getRelativePartitionPath(metaClient.getBasePath(), partitionPath)).findFirst().isPresent();
    } catch (IOException e) {
      throw new HoodieException("Failed to list files in partition: " + partitionPath, e);
    } finally {
      if (fileSystemView != null) {
        fileSystemView.close();
      }
    }
  }

  private boolean hasStashData(HoodieStorage storage, StoragePath stashPartitionPath) {
    try {
      return storage.exists(stashPartitionPath) && !isDirectoryEmpty(storage, stashPartitionPath);
    } catch (IOException e) {
      throw new HoodieException("Failed to check stash path: " + stashPartitionPath, e);
    }
  }

  /**
   * Dry run: show partitions and file groups that would be affected.
   */
  private void doDryRun() {
    try (SparkRDDWriteClient<HoodieRecordPayload> client =
             UtilHelpers.createHoodieClient(jsc, cfg.basePath, "", cfg.parallelism, Option.empty(), props)) {
      HoodieSparkTable<HoodieRecordPayload> table =
          HoodieSparkTable.create(client.getConfig(), client.getEngineContext());
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
