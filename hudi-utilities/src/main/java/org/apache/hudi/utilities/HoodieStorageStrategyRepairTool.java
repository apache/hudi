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

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.CachingPath;
import org.apache.hudi.hadoop.SerializablePath;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.api.java.JavaSparkContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;


/**
 * A tool with spark-submit to repair Hudi table by finding and deleting dangling
 * base and log files.
 * <p>
 * You can run this tool with the following command:
 * ```
 * spark-submit \
 * --class org.apache.hudi.utilities.HoodieStorageStrategyRepairTool \
 * --driver-memory 4g \
 * --executor-memory 1g \
 * --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
 * --conf spark.sql.catalogImplementation=hive \
 * --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
 * $HUDI_DIR/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-0.11.0-SNAPSHOT.jar \
 * --mode dry_run \
 * --base-path base_path \
 * --storage-path storage_path
 * ```
 * <p>
 * You can specify the running mode of the tool through `--mode`.
 * There are three modes of the {@link HoodieStorageStrategyRepairTool}:
 * - REPAIR ("repair"): repairs the table by removing dangling data and log files not belonging to any commit.
 * The removed files are going to be backed up at the backup path provided, in case recovery is needed.
 * In this mode, backup path is required through `--backup-path`.  You can also provide a range for repairing
 * only the instants within the range, through `--start-instant-time` and `--end-instant-time`.  You can also
 * specify only one of them. If no range is provided, all instants are going to be repaired.
 * <p>
 * Example command:
 * ```
 * spark-submit \
 * --class org.apache.hudi.utilities.HoodieStorageStrategyRepairTool \
 * --driver-memory 4g \
 * --executor-memory 1g \
 * --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
 * --conf spark.sql.catalogImplementation=hive \
 * --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
 * $HUDI_DIR/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-0.11.0-SNAPSHOT.jar \
 * --mode repair \
 * --base-path base_path \
 * --backup-path backup_path
 * ```
 * <p>
 * - DRY_RUN ("dry_run"): only looks for dangling data and log files. You can also provide a range for looking
 * at only the instants within the range, through `--start-instant-time` and `--end-instant-time`.  You can also
 * specify only one of them.  If no range is provided, all instants are going to be scanned.
 * <p>
 * Example command:
 * ```
 * spark-submit \
 * --class org.apache.hudi.utilities.HoodieStorageStrategyRepairToolHoodieStorageStrategyRepairTool \
 * --driver-memory 4g \
 * --executor-memory 1g \
 * --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
 * --conf spark.sql.catalogImplementation=hive \
 * --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
 * $HUDI_DIR/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-0.11.0-SNAPSHOT.jar \
 * --mode dry_run \
 * --base-path base_path \
 * --storage-path storage_path
 * ```
 * <p>
 * - UNDO ("undo"): undoes the repair by copying back the files from backup directory to the table base path.
 * In this mode, backup path is required through `--backup-path`.
 * <p>
 * Example command:
 * ```
 * spark-submit \
 * --class org.apache.hudi.utilities.HoodieStorageStrategyRepairTool \
 * --driver-memory 4g \
 * --executor-memory 1g \
 * --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
 * --conf spark.sql.catalogImplementation=hive \
 * --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
 * $HUDI_DIR/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-0.11.0-SNAPSHOT.jar \
 * --mode undo \
 * --base-path base_path \
 * --storage-path storage_path \
 * --backup-path backup_path
 * ```
 */
public class HoodieStorageStrategyRepairTool implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieStorageStrategyRepairTool.class);
  private static final String BACKUP_DIR_PREFIX = "hoodie_storage_repair_backup_";
  // Repair config
  private final Config cfg;
  // Properties with source, hoodie client, key generator etc.
  private TypedProperties props;
  // Spark context
  private final HoodieEngineContext context;
  private final HoodieTableMetaClient metaClient;
  private HoodieWriteConfig writeConfig;

  public HoodieStorageStrategyRepairTool(JavaSparkContext jsc, Config cfg) {
    if (cfg.propsFilePath != null) {
      cfg.propsFilePath = FSUtils.addSchemeIfLocalPath(cfg.propsFilePath).toString();
    }
    this.context = new HoodieSparkEngineContext(jsc);
    this.cfg = cfg;
    this.metaClient = HoodieTableMetaClient.builder()
        .setConf(jsc.hadoopConfiguration()).setBasePath(cfg.basePath)
        .setLoadActiveTimelineOnLoad(true)
        .build();
    this.props = cfg.propsFilePath == null
        ? UtilHelpers.buildProperties(cfg.configs)
        : readConfigFromFileSystem(jsc, cfg);
    props.putAll(metaClient.getTableConfig().propsMap());
    this.writeConfig = HoodieWriteConfig.newBuilder()
        .withProps(props)
        .build();
  }

  public boolean run() {
    try {
      Mode mode = Mode.valueOf(cfg.runningMode.toUpperCase());
      switch (mode) {
        case REPAIR:
          LOG.info(" ****** The repair tool is in REPAIR mode, existing metadata table "
              + "is going to be MOVED to backup path ******");
          if (checkBackupPathForRepair() < 0) {
            LOG.error("Backup path check failed.");
            return false;
          }
          if (checkStoragePath() < 0) {
            LOG.error("Storage path check failed");
            return false;
          }
          return doRepair(false);
        case DRY_RUN:
          LOG.info(" ****** The repair tool is in DRY_RUN mode, "
              + "only listing files under storage path ******");
          if (checkStoragePath() < 0) {
            LOG.error("Storage path check failed");
            return false;
          }
          return doRepair(true);
        case UNDO:
          if (checkBackupPathAgainstBasePath() < 0) {
            LOG.error("Backup path check failed.");
            return false;
          }
          return undoRepair();
        default:
          LOG.info("Unsupported running mode [" + cfg.runningMode + "], quit the job directly");
          return false;
      }
    } catch (IOException e) {
      throw new HoodieIOException("Unable to repair table in " + cfg.basePath, e);
    }
  }

  public static void main(String[] args) {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    final JavaSparkContext jsc = UtilHelpers.buildSparkContext("hudi-table-repair", cfg.sparkMaster, cfg.sparkMemory);
    try {
      new HoodieStorageStrategyRepairTool(jsc, cfg).run();
    } catch (Throwable throwable) {
      LOG.error("Fail to run table repair for " + cfg.basePath, throwable);
    } finally {
      jsc.stop();
    }
  }

  protected boolean doRepair(boolean dryRun) {
    if (dryRun) {
      try {
        // list storage path recursively
        Map<String, Map<String, Long>> partitionToFileInfo = listFilesUnderStoragePath();
        LOG.info("Dry run completed successfully!");
      } catch (Exception e) {
        LOG.error("Dry run failed due to:" + e);
        return false;
      }
    } else {
      try {
        // back up metadata table
        if (!backupMetadataTable()) {
          return false;
        }

        // rewrite metadata table by listing storage path recursively
        new StorageRepairMetadataWriter(context.getHadoopConf().get(), writeConfig, context);
        LOG.info("Repair completed successfully!");
      } catch (Exception e) {
        LOG.error("Failed to repair metadata table due to: " + e);
      }
    }

    return true;
  }

  protected boolean undoRepair() {
    Path src = new Path(cfg.backupPath + "/metadata/");
    Path dst = new Path(cfg.basePath + "/.hoodie/metadata");

    return moveMetadataTable(src, dst);
  }

  boolean backupMetadataTable() throws IOException {
    Path src = new Path(cfg.basePath + "/.hoodie/metadata/");
    FileSystem fs = FSUtils.getFs(src, context.getHadoopConf().get());
    if (!fs.exists(src)) {
      LOG.info("Metadata table doesn't exist, path: " + src + ", skipping backup");
      return true;
    }
    Path dst = new Path(cfg.backupPath + "/metadata");

    return moveMetadataTable(src, dst);
  }

  boolean moveMetadataTable(Path src, Path dst) {
    FileSystem fs = FSUtils.getFs(dst, context.getHadoopConf().get());
    try {
      LOG.info("Moving existing metadata table to " + dst);
      return fs.rename(src, dst);
    } catch (IOException ioe) {
      LOG.error("Failed to move metadata table! " + ioe);
      return false;
    }
  }

  /**
   * Verifies the backup path for repair.
   * If there is no backup path configured, creates a new one in temp folder.
   * If the backup path already has files, throws an error to the user.
   * If the backup path is within the table base path, throws an error too.
   *
   * @return {@code 0} if successful; {@code -1} otherwise.
   * @throws IOException upon errors.
   */
  int checkBackupPathForRepair() throws IOException {
    if (cfg.backupPath == null) {
      SecureRandom random = new SecureRandom();
      long randomLong = random.nextLong();
      cfg.backupPath = "/tmp/" + BACKUP_DIR_PREFIX + randomLong;
      LOG.warn(String.format("Backup path is null! Using %s as backup path", cfg.backupPath));
    }

    Path backupPath = new Path(cfg.backupPath);
    if (metaClient.getRawFs().exists(backupPath)
        && metaClient.getRawFs().listStatus(backupPath).length > 0) {
      LOG.error(String.format("Cannot use backup path %s: it is not empty", cfg.backupPath));
      return -1;
    }

    return checkBackupPathAgainstBasePath();
  }

  /**
   * Verifies the backup path against table base path.
   * If the backup path is within the table base path, throws an error.
   *
   * @return {@code 0} if successful; {@code -1} otherwise.
   */
  int checkBackupPathAgainstBasePath() {
    if (cfg.backupPath == null) {
      LOG.error("Backup path is not configured");
      return -1;
    }

    if (cfg.backupPath.contains(cfg.basePath)) {
      LOG.error(String.format("Cannot use backup path %s: it resides in the base path %s",
          cfg.backupPath, cfg.basePath));
      return -1;
    }
    return 0;
  }

  int checkStoragePath() {
    if (cfg.storagePath == null) {
      cfg.storagePath = writeConfig.getStoragePath();
      try {
        ValidationUtils.checkArgument(cfg.storagePath != null);
      } catch (IllegalArgumentException iae) {
        LOG.error("Storage path can't be null, please check your hoodie.properties");
        return -1;
      }
    }
    return 0;
  }

  /**
   *
   * @return map of partition ID to a map of file name to file's size
   * */
  protected Map<String, Map<String, Long>> listFilesUnderStoragePath() {
    HoodieTimer timer = HoodieTimer.start();
    Map<String, Map<String, Long>> partitionIdToFileInfo = new HashMap<>();
    String tableName = metaClient.getTableConfig().getTableName();
    String storagePath = cfg.storagePath;
    LOG.info("Listing storage path: " + storagePath);

    SerializableConfiguration hadoopConf = context.getHadoopConf();

    final int fileListingParallelism = Math.max(1, props.getInteger(HoodieMetadataConfig.FILE_LISTING_PARALLELISM_VALUE.key(), 200));
    List<SerializablePath> pathsToList = new LinkedList<>();
    pathsToList.add(new SerializablePath(new CachingPath(storagePath)));

    while (!pathsToList.isEmpty()) {
      int numPathsToList = Math.min(fileListingParallelism, pathsToList.size());

      List<StorageDirectoryInfo> processedDirs = context.map(pathsToList.subList(0, numPathsToList), path -> {
        Path curPath = path.get();
        FileSystem fs = curPath.getFileSystem(hadoopConf.get());
        return new StorageDirectoryInfo(tableName, curPath, fs.listStatus(curPath));
      }, numPathsToList);

      pathsToList = new LinkedList<>(pathsToList.subList(numPathsToList, pathsToList.size()));

      for (StorageDirectoryInfo dirInfo : processedDirs) {
        if (!StringUtils.isNullOrEmpty(dirInfo.getPartitionId())) {
          // merge file info map to existing partitionToFileInfo
          if (partitionIdToFileInfo.containsKey(dirInfo.getPartitionId())) {
            partitionIdToFileInfo.get(dirInfo.getPartitionId()).putAll(dirInfo.filenameToSizeMap);
          } else {
            partitionIdToFileInfo.put(dirInfo.getPartitionId(), new HashMap<>(dirInfo.filenameToSizeMap));
          }
        } else {
          pathsToList.addAll(dirInfo.getSubDirectories().stream()
              .map(path -> new SerializablePath(new CachingPath(path.toUri())))
              .collect(Collectors.toList()));
        }
      }
    }

    long numOfFiles = partitionIdToFileInfo.values().stream()
        .flatMap(fileToSize -> fileToSize.keySet().stream())
        .count();
    LOG.info("Time spent on listing storage path: " + timer.endTimer() + " ms");
    LOG.info("Listed " + partitionIdToFileInfo.keySet().size() + " partitions");
    LOG.info("Listed files num: " + numOfFiles);

    return partitionIdToFileInfo;
  }

  static class StorageDirectoryInfo implements Serializable {
    // Relative path of the directory (relative to the base directory)
    private final Path path;
    // Map of filenames within this partition to their respective sizes
    private final Map<String, Long> filenameToSizeMap;
    // List of directories within this partition
    private final List<Path> subDirectories = new ArrayList<>();

    private String partitionId;

    StorageDirectoryInfo(String tableName, Path path, FileStatus[] children) {
      this.path = path;
      this.filenameToSizeMap = new HashMap<>(children.length);

      for (FileStatus child : children) {
        Path childPath = child.getPath();
        if (child.isDirectory() && !childPath.getName().equals(HoodieTableMetaClient.METAFOLDER_NAME)) {
          subDirectories.add(childPath);
        } else if (FSUtils.isDataFile(childPath)) {
          filenameToSizeMap.put(childPath.getName(), child.getLen());
          if (StringUtils.isNullOrEmpty(partitionId)) {
            Path fullPartitionPath = CachingPath.getPathWithoutSchemeAndAuthority(path);
            String fullPartitionPathStr = fullPartitionPath.toString();
            int tableNameStartIndex = fullPartitionPathStr.lastIndexOf(tableName);
            if (tableNameStartIndex == -1) {
              // this data file/path doesn't belong to the table being repaired!
              LOG.warn("Found data file: " + childPath + " doesn't belong to table: " + tableName + ", skipping this path");
              return;
            }

            // Partition-Path could be empty for non-partitioned tables
            String partitionName = tableNameStartIndex + tableName.length() == fullPartitionPathStr.length()
                ? ""
                : fullPartitionPathStr.substring(tableNameStartIndex + tableName.length() + 1);
            partitionId = HoodieTableMetadataUtil.getPartitionIdentifier(partitionName);
          }
        }
      }
    }

    Path getPath() {
      return path;
    }

    Map<String, Long> getFilenameToSizeMap() {
      return filenameToSizeMap;
    }

    List<Path> getSubDirectories() {
      return subDirectories;
    }

    String getPartitionId() {
      return partitionId;
    }
  }

  /**
   * Reads config from the file system.
   *
   * @param jsc {@link JavaSparkContext} instance.
   * @param cfg {@link Config} instance.
   * @return the {@link TypedProperties} instance.
   */
  private TypedProperties readConfigFromFileSystem(JavaSparkContext jsc, Config cfg) {
    return UtilHelpers.readConfig(jsc.hadoopConfiguration(), new Path(cfg.propsFilePath), cfg.configs)
        .getProps(true);
  }

  /**
   * Initialize metadata writer, metadata table would be initialized when initializing metadata writer
   *
   *
   */
  public class StorageRepairMetadataWriter extends SparkHoodieBackedTableMetadataWriter {
    <T extends SpecificRecordBase> StorageRepairMetadataWriter(Configuration hadoopConf,
                                                               HoodieWriteConfig writeConfig,
                                                               HoodieEngineContext engineContext,
                                                               Option<T> actionMetadata,
                                                               Option<String> inflightInstantTimestamp) {
      super(hadoopConf, writeConfig, HoodieFailedWritesCleaningPolicy.EAGER, engineContext,
          actionMetadata, inflightInstantTimestamp);
    }

    public StorageRepairMetadataWriter(Configuration hadoopConf,
                                       HoodieWriteConfig writeConfig,
                                       HoodieEngineContext engineContext) {
      this(hadoopConf, writeConfig, engineContext, Option.empty(), Option.empty());
    }

    @Override
    protected Map<String, Map<String, Long>> getPartitionToFilesMap() {
      return listFilesUnderStoragePath();
    }
  }

  public static class Config implements Serializable {
    @Parameter(names = {"--base-path", "-tp"}, description = "Base path for the table", required = true)
    public String basePath = null;
    @Parameter(names = {"--storage-path", "-sp"}, description = "Storage path for the table, if set to null then"
        + "repair tool would read it from table's hoodie.properties")
    public String storagePath = null;
    @Parameter(names = {"--mode", "-m"}, description = "Set job mode: Set \"repair\" means repairing the metadata "
        + "table by listing all files under storage path; "
        + "Set \"dry_run\" means only listing all files under storage path; "
        + "Set \"undo\" means undoing the repair by copying back the metadata table from backup directory", required = true)
    public String runningMode = null;
    @Parameter(names = {"--backup-path", "-bp"}, description = "Backup path for storing existing metadata table", required = false)
    public String backupPath = null;
    @Parameter(names = {"--parallelism", "-pl"}, description = "Parallelism for repair", required = false)
    public int parallelism = 2;
    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = false)
    public String sparkMaster = null;
    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = false)
    public String sparkMemory = "1g";
    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;
    @Parameter(names = {"--props-file"}, description = "path to properties file on localfs or dfs, with configurations for "
        + "hoodie client for table repair")
    public String propsFilePath = null;
    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated",
        splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();
  }

  public enum Mode {
    // Repairs the table by removing dangling data and log files not belonging to any commit
    REPAIR,
    // Dry run by only looking for dangling data and log files
    DRY_RUN,
    // Undoes the repair by copying back the files from backup directory
    UNDO
  }
}
