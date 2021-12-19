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
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metadata.FileSystemBackedTableMetadata;
import org.apache.hudi.table.repair.RepairUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import scala.Tuple2;

/**
 * A tool with spark-submit to repair Hudi table by finding and deleting dangling
 * base and log files.
 * <p>
 * You can run this tool with the following command:
 * ```
 * spark-submit \
 * --class org.apache.hudi.utilities.HoodieRepairTool \
 * --driver-memory 4g \
 * --executor-memory 1g \
 * --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
 * --conf spark.sql.catalogImplementation=hive \
 * --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
 * --packages org.apache.spark:spark-avro_2.12:3.1.2 \
 * $HUDI_DIR/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-0.11.0-SNAPSHOT.jar \
 * --mode dry_run \
 * --base-path base_path \
 * --assume-date-partitioning
 * ```
 * <p>
 * You can specify the running mode of the tool through `--mode`.
 * There are three modes of the {@link HoodieRepairTool}:
 * - REPAIR ("repair"): repairs the table by removing dangling data and log files not belonging to any commit.
 * The removed files are going to be backed up at the backup path provided, in case recovery is needed.
 * In this mode, backup path is required through `--backup-path`.  You can also provide a range for repairing
 * only the instants within the range, through `--start-instant-time` and `--end-instant-time`.  You can also
 * specify only one of them. If no range is provided, all instants are going to be repaired.
 * <p>
 * Example command:
 * ```
 * spark-submit \
 * --class org.apache.hudi.utilities.HoodieRepairTool \
 * --driver-memory 4g \
 * --executor-memory 1g \
 * --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
 * --conf spark.sql.catalogImplementation=hive \
 * --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
 * --packages org.apache.spark:spark-avro_2.12:3.1.2 \
 * $HUDI_DIR/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-0.11.0-SNAPSHOT.jar \
 * --mode repair \
 * --base-path base_path \
 * --backup-path backup_path \
 * --start-instant-time ts1 \
 * --end-instant-time ts2 \
 * --assume-date-partitioning
 * ```
 * <p>
 * - DRY_RUN ("dry_run"): only looks for dangling data and log files. You can also provide a range for looking
 * at only the instants within the range, through `--start-instant-time` and `--end-instant-time`.  You can also
 * specify only one of them.  If no range is provided, all instants are going to be scanned.
 * <p>
 * Example command:
 * ```
 * spark-submit \
 * --class org.apache.hudi.utilities.HoodieRepairTool \
 * --driver-memory 4g \
 * --executor-memory 1g \
 * --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
 * --conf spark.sql.catalogImplementation=hive \
 * --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
 * --packages org.apache.spark:spark-avro_2.12:3.1.2 \
 * $HUDI_DIR/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-0.11.0-SNAPSHOT.jar \
 * --mode dry_run \
 * --base-path base_path \
 * --start-instant-time ts1 \
 * --end-instant-time ts2 \
 * --assume-date-partitioning
 * ```
 * <p>
 * - UNDO ("undo"): undoes the repair by copying back the files from backup directory to the table base path.
 * In this mode, backup path is required through `--backup-path`.
 * <p>
 * Example command:
 * ```
 * spark-submit \
 * --class org.apache.hudi.utilities.HoodieRepairTool \
 * --driver-memory 4g \
 * --executor-memory 1g \
 * --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
 * --conf spark.sql.catalogImplementation=hive \
 * --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
 * --packages org.apache.spark:spark-avro_2.12:3.1.2 \
 * $HUDI_DIR/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-0.11.0-SNAPSHOT.jar \
 * --mode undo \
 * --base-path base_path \
 * --backup-path backup_path
 * ```
 */
public class HoodieRepairTool {

  private static final Logger LOG = LogManager.getLogger(HoodieRepairTool.class);
  private static final String BACKUP_DIR_PREFIX = "hoodie_repair_backup_";
  // Repair config
  private final Config cfg;
  // Properties with source, hoodie client, key generator etc.
  private TypedProperties props;
  // Spark context
  private final JavaSparkContext jsc;
  private final HoodieTableMetaClient metaClient;
  private final FileSystemBackedTableMetadata tableMetadata;

  public HoodieRepairTool(JavaSparkContext jsc, Config cfg) {
    if (cfg.propsFilePath != null) {
      cfg.propsFilePath = FSUtils.addSchemeIfLocalPath(cfg.propsFilePath).toString();
    }
    this.jsc = jsc;
    this.cfg = cfg;
    this.props = cfg.propsFilePath == null
        ? UtilHelpers.buildProperties(cfg.configs)
        : readConfigFromFileSystem(jsc, cfg);
    this.metaClient = HoodieTableMetaClient.builder()
        .setConf(jsc.hadoopConfiguration()).setBasePath(cfg.basePath)
        .setLoadActiveTimelineOnLoad(true)
        .build();
    this.tableMetadata = new FileSystemBackedTableMetadata(
        new HoodieSparkEngineContext(jsc),
        new SerializableConfiguration(jsc.hadoopConfiguration()),
        cfg.basePath, cfg.assumeDatePartitioning);
  }

  public void run() {
    Option<String> startingInstantOption = Option.ofNullable(cfg.startingInstantTime);
    Option<String> endingInstantOption = Option.ofNullable(cfg.endingInstantTime);

    if (startingInstantOption.isPresent() && endingInstantOption.isPresent()) {
      LOG.info(String.format("Start repairing completed instants between %s and %s (inclusive)",
          startingInstantOption.get(), endingInstantOption.get()));
    } else if (startingInstantOption.isPresent()) {
      LOG.info(String.format("Start repairing completed instants from %s (inclusive)",
          startingInstantOption.get()));
    } else if (endingInstantOption.isPresent()) {
      LOG.info(String.format("Start repairing completed instants till %s (inclusive)",
          endingInstantOption.get()));
    } else {
      LOG.info("Start repairing all completed instants");
    }

    try {
      Mode mode = Mode.valueOf(cfg.runningMode.toUpperCase());
      switch (mode) {
        case REPAIR:
          LOG.info(" ****** The repair tool is in REPAIR mode, dangling data and logs files "
              + "not belonging to any commit are going to be DELETED from the table ******");
          if (checkBackupPathForRepair() < 0) {
            LOG.error("Backup path check failed.");
            break;
          }
          doRepair(startingInstantOption, endingInstantOption, false);
          break;
        case DRY_RUN:
          LOG.info(" ****** The repair tool is in DRY_RUN mode, "
              + "only LOOKING FOR dangling data and log files from the table ******");
          doRepair(startingInstantOption, endingInstantOption, true);
          break;
        case UNDO:
          if (checkBackupPathAgainstBasePath() < 0) {
            LOG.error("Backup path check failed.");
            break;
          }
          undoRepair();
          break;
        default:
          LOG.info("Unsupported running mode [" + cfg.runningMode + "], quit the job directly");
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
      new HoodieRepairTool(jsc, cfg).run();
    } catch (Throwable throwable) {
      LOG.error("Fail to run table repair for " + cfg.basePath, throwable);
    } finally {
      jsc.stop();
    }
  }

  /**
   * Copies the list of files from source base path to destination base path.
   * The destination file path (base + relative) should not already exist.
   *
   * @param jsc               {@link JavaSparkContext} instance.
   * @param relativeFilePaths A {@link List} of relative file paths for copying.
   * @param sourceBasePath    Source base path.
   * @param destBasePath      Destination base path.
   * @param parallelism       Parallelism.
   * @return {@code true} if all successful; {@code false} otherwise.
   */
  static boolean copyFiles(
      JavaSparkContext jsc, List<String> relativeFilePaths, String sourceBasePath,
      String destBasePath, int parallelism) {
    SerializableConfiguration conf = new SerializableConfiguration(jsc.hadoopConfiguration());
    List<Boolean> allResults = jsc.parallelize(relativeFilePaths, parallelism)
        .mapPartitions(iterator -> {
          List<Boolean> results = new ArrayList<>();
          FileSystem fs = FSUtils.getFs(destBasePath, conf.get());
          iterator.forEachRemaining(filePath -> {
            boolean success = false;
            Path destPath = new Path(destBasePath, filePath);
            try {
              if (!fs.exists(destPath)) {
                FileIOUtils.copy(fs, new Path(sourceBasePath, filePath), destPath);
                success = true;
              }
            } catch (IOException e) {
              // Copy Fail
            } finally {
              results.add(success);
            }
          });
          return results.iterator();
        })
        .collect();
    return allResults.stream().reduce((r1, r2) -> r1 && r2).orElse(false);
  }

  /**
   * Lists all Hoodie files from the table base path.
   *
   * @param basePathStr Table base path.
   * @param conf        {@link Configuration} instance.
   * @return An array of {@link FileStatus} of all Hoodie files.
   * @throws IOException upon errors.
   */
  static FileStatus[] listFilesFromBasePath(String basePathStr, Configuration conf) throws IOException {
    final Set<String> validFileExtensions = Arrays.stream(HoodieFileFormat.values())
        .map(HoodieFileFormat::getFileExtension).collect(Collectors.toCollection(HashSet::new));
    final String logFileExtension = HoodieFileFormat.HOODIE_LOG.getFileExtension();
    FileSystem fs = FSUtils.getFs(basePathStr, conf);
    Path basePath = new Path(basePathStr);

    try {
      return Arrays.stream(fs.listStatus(basePath, path -> {
        String extension = FSUtils.getFileExtension(path.getName());
        return validFileExtensions.contains(extension) || path.getName().contains(logFileExtension);
      })).filter(FileStatus::isFile).toArray(FileStatus[]::new);
    } catch (IOException e) {
      // return empty FileStatus if partition does not exist already
      if (!fs.exists(basePath)) {
        return new FileStatus[0];
      } else {
        throw e;
      }
    }
  }

  /**
   * Does repair, either in REPAIR or DRY_RUN mode.
   *
   * @param startingInstantOption {@link Option} of starting instant for scanning, can be empty.
   * @param endingInstantOption   {@link Option} of ending instant for scanning, can be empty.
   * @param isDryRun              Is dry run.
   * @throws IOException upon errors.
   */
  void doRepair(
      Option<String> startingInstantOption, Option<String> endingInstantOption, boolean isDryRun) throws IOException {
    // Scans all partitions to find base and log files in the base path
    List<Path> allFilesInPartitions = getBaseAndLogFilePathsFromFileSystem();
    // Buckets the files based on instant time
    // instant time -> relative paths of base and log files to base path
    Map<String, List<String>> instantToFilesMap = RepairUtils.tagInstantsOfBaseAndLogFiles(
        metaClient.getBasePath(),
        metaClient.getTableConfig().getBaseFileFormat().getFileExtension(), allFilesInPartitions);
    List<String> instantTimesToRepair = instantToFilesMap.keySet().stream()
        .filter(instant -> (!startingInstantOption.isPresent()
            || instant.compareTo(startingInstantOption.get()) >= 0)
            && (!endingInstantOption.isPresent()
            || instant.compareTo(endingInstantOption.get()) <= 0)
        ).collect(Collectors.toList());

    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieArchivedTimeline archivedTimeline = metaClient.getArchivedTimeline();
    // This assumes that the archived timeline only has completed instants so this is safe
    archivedTimeline.loadCompletedInstantDetailsInMemory();

    int parallelism = Math.max(Math.min(instantTimesToRepair.size(), cfg.parallelism), 1);
    List<Tuple2<String, List<String>>> instantFilesToRemove =
        jsc.parallelize(instantTimesToRepair, parallelism)
            .mapToPair(instantToRepair ->
                new Tuple2<>(instantToRepair, RepairUtils.findInstantFilesToRemove(instantToRepair,
                    instantToFilesMap.get(instantToRepair), activeTimeline, archivedTimeline)))
            .collect();

    List<Tuple2<String, List<String>>> instantsWithDanglingFiles =
        instantFilesToRemove.stream().filter(e -> !e._2.isEmpty()).collect(Collectors.toList());
    printRepairInfo(instantTimesToRepair, instantsWithDanglingFiles);
    if (!isDryRun) {
      List<String> relativeFilePathsToDelete =
          instantsWithDanglingFiles.stream().flatMap(e -> e._2.stream()).collect(Collectors.toList());
      if (relativeFilePathsToDelete.size() > 0) {
        parallelism = Math.max(Math.min(relativeFilePathsToDelete.size(), cfg.parallelism), 1);
        if (!backupFiles(relativeFilePathsToDelete, parallelism)) {
          LOG.error("Error backing up dangling files. Exiting...");
          return;
        }
        deleteFiles(relativeFilePathsToDelete, parallelism);
      }
      LOG.info(String.format("Table repair on %s is successful", cfg.basePath));
    }
  }

  /**
   * @return All hoodie files of the table from the file system.
   * @throws IOException upon errors.
   */
  List<Path> getBaseAndLogFilePathsFromFileSystem() throws IOException {
    List<String> allPartitionPaths = tableMetadata.getAllPartitionPaths()
        .stream().map(partitionPath ->
            FSUtils.getPartitionPath(cfg.basePath, partitionPath).toString())
        .collect(Collectors.toList());
    return tableMetadata.getAllFilesInPartitions(allPartitionPaths).values().stream()
        .map(fileStatuses ->
            Arrays.stream(fileStatuses).map(fileStatus -> fileStatus.getPath()).collect(Collectors.toList()))
        .flatMap(list -> list.stream())
        .collect(Collectors.toList());
  }

  /**
   * Undoes repair for UNDO mode.
   *
   * @throws IOException upon errors.
   */
  void undoRepair() throws IOException {
    FileSystem fs = metaClient.getFs();
    String backupPathStr = cfg.backupPath;
    Path backupPath = new Path(backupPathStr);
    if (!fs.exists(backupPath)) {
      LOG.error("Cannot find backup path: " + backupPath);
      return;
    }

    List<String> relativeFilePaths = Arrays.stream(
            listFilesFromBasePath(backupPathStr, jsc.hadoopConfiguration()))
        .map(fileStatus ->
            FSUtils.getPartitionPath(backupPathStr, fileStatus.getPath().toString()).toString())
        .collect(Collectors.toList());
    int parallelism = Math.max(Math.min(relativeFilePaths.size(), cfg.parallelism), 1);
    restoreFiles(relativeFilePaths, parallelism);
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
    }

    Path backupPath = new Path(cfg.backupPath);
    if (metaClient.getFs().exists(backupPath)
        && metaClient.getFs().listStatus(backupPath).length > 0) {
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

  /**
   * Backs up dangling files from table base path to backup path.
   *
   * @param relativeFilePaths A {@link List} of relative file paths for backup.
   * @param parallelism       Parallelism for copying.
   * @return {@code true} if all successful; {@code false} otherwise.
   */
  boolean backupFiles(List<String> relativeFilePaths, int parallelism) {
    return copyFiles(jsc, relativeFilePaths, cfg.basePath, cfg.backupPath, parallelism);
  }

  /**
   * Restores dangling files from backup path to table base path.
   *
   * @param relativeFilePaths A {@link List} of relative file paths for restoring.
   * @param parallelism       Parallelism for copying.
   * @return {@code true} if all successful; {@code false} otherwise.
   */
  boolean restoreFiles(List<String> relativeFilePaths, int parallelism) {
    return copyFiles(jsc, relativeFilePaths, cfg.backupPath, cfg.basePath, parallelism);
  }

  /**
   * Deletes files from table base path.
   *
   * @param relativeFilePaths A {@link List} of relative file paths for deleting.
   * @param parallelism       Parallelism for deleting.
   */
  void deleteFiles(List<String> relativeFilePaths, int parallelism) {
    jsc.parallelize(relativeFilePaths, parallelism)
        .mapPartitions(iterator -> {
          FileSystem fs = metaClient.getFs();
          List<Boolean> results = new ArrayList<>();
          iterator.forEachRemaining(filePath -> {
            boolean success = false;
            try {
              success = fs.delete(new Path(filePath), false);
            } catch (IOException e) {
              LOG.warn("Failed to delete file " + filePath);
            } finally {
              results.add(success);
            }
          });
          return results.iterator();
        })
        .collect();
  }

  /**
   * Prints the repair info.
   *
   * @param instantTimesToRepair      A list instant times in consideration for repair
   * @param instantsWithDanglingFiles A list of instants with dangling files.
   */
  private void printRepairInfo(
      List<String> instantTimesToRepair, List<Tuple2<String, List<String>>> instantsWithDanglingFiles) {
    int numInstantsToRepair = instantsWithDanglingFiles.size();
    LOG.warn("Number of instants verified based on the base and log files: "
        + instantTimesToRepair.size());
    LOG.warn("Instant timestamps: " + instantTimesToRepair);
    LOG.warn("Number of instants to repair: " + numInstantsToRepair);
    if (numInstantsToRepair > 0) {
      instantsWithDanglingFiles.forEach(e -> {
        LOG.warn(" -> Instant " + numInstantsToRepair);
        LOG.warn("   ** Removing files: " + e._2);
      });
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

  public static class Config implements Serializable {
    @Parameter(names = {"--base-path", "-sp"}, description = "Base path for the table", required = true)
    public String basePath = null;
    @Parameter(names = {"--mode", "-m"}, description = "Set job mode: Set \"repair\" means repairing the table "
        + "by removing dangling data and log files not belonging to any commit; "
        + "Set \"dry_run\" means only looking for dangling data and log files; "
        + "Set \"undo\" means undoing the repair by copying back the files from backup directory", required = true)
    public String runningMode = null;
    @Parameter(names = {"--start-instant-time", "-si"}, description = "Starting Instant time "
        + "for repair (inclusive)", required = false)
    public String startingInstantTime = null;
    @Parameter(names = {"--end-instant-time", "-ei"}, description = "Ending Instant time "
        + "for repair (inclusive)", required = false)
    public String endingInstantTime = null;
    @Parameter(names = {"--backup-path", "-bp"}, description = "Backup path for storing dangling data "
        + "and log files from the table", required = false)
    public String backupPath = null;
    @Parameter(names = {"--parallelism", "-pl"}, description = "Parallelism for repair", required = false)
    public int parallelism = 2;
    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = false)
    public String sparkMaster = null;
    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = false)
    public String sparkMemory = "1g";
    @Parameter(names = {"--assume-date-partitioning", "-dp"}, description = "whether the partition path "
        + "is date with three levels", required = false)
    public Boolean assumeDatePartitioning = false;
    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
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
