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

package org.apache.hudi.common.fs;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.storage.HoodieStorageStrategy;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.exception.InvalidHoodieFileNameException;
import org.apache.hudi.exception.InvalidHoodiePathException;
import org.apache.hudi.hadoop.CachingPath;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.shaded.com.jd.chubaofs.hadoop.ChubaoFSFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.HoodieTableConfig.HOODIE_BASE_PATH_KEY;
import static org.apache.hudi.common.table.HoodieTableConfig.HOODIE_STORAGE_CHUBAO_FS_LOG_DIR;
import static org.apache.hudi.common.table.HoodieTableConfig.HOODIE_STORAGE_CHUBAO_FS_LOG_LEVEL;
import static org.apache.hudi.common.table.HoodieTableConfig.HOODIE_STORAGE_CHUBAO_FS_OWNER;
import static org.apache.hudi.common.table.HoodieTableConfig.HOODIE_STORAGE_PATH;
import static org.apache.hudi.common.table.HoodieTableConfig.HOODIE_STORAGE_STRATEGY_CLASS_NAME;
import static org.apache.hudi.hadoop.CachingPath.getPathWithoutSchemeAndAuthority;

/**
 * Utility functions related to accessing the file storage.
 */
public class FSUtils {

  private static final Logger LOG = LogManager.getLogger(FSUtils.class);
  // Log files are of this pattern - .b5068208-e1a4-11e6-bf01-fe55135034f3_20170101134598.log.1_1-0-1
  // Archive log files are of this pattern - .commits_.archive.1_1-0-1
  public static final Pattern LOG_FILE_PATTERN =
      Pattern.compile("^\\.(.+)_(.*)\\.(log|archive)\\.(\\d+)(_((\\d+)-(\\d+)-(\\d+))(.cdc)?)?");

  // 00000000_063f8be4-49d3-43fd-bc74-ee37b8329ceb_0_0_3-1-0_20250409161256974.parquet
  // 8个group
  public static final Pattern LSM_LOG_FILE_PATTERN =
      Pattern.compile("^(\\d+)_([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})_(\\d+)_(\\d+)_(\\d+)-(\\d+)-(\\d+)_(.*)\\.(parquet)$");
  public static final Pattern PREFIX_BY_FILE_ID_PATTERN = Pattern.compile("^(.+)-(\\d+)");
  private static final int MAX_ATTEMPTS_RECOVER_LEASE = 10;
  private static final long MIN_CLEAN_TO_KEEP = 10;
  private static final long MIN_ROLLBACK_TO_KEEP = 10;
  private static final String HOODIE_ENV_PROPS_PREFIX = "HOODIE_ENV_";

  private static final PathFilter ALLOW_ALL_FILTER = file -> true;
  public static final String LSM_TEMP_FILE_SUFFIX = ".temp";

  public static Configuration prepareHadoopConf(Configuration conf) {
    // look for all properties, prefixed to be picked up
    for (Entry<String, String> prop : System.getenv().entrySet()) {
      if (prop.getKey().startsWith(HOODIE_ENV_PROPS_PREFIX)) {
        LOG.info("Picking up value for hoodie env var :" + prop.getKey());
        conf.set(prop.getKey().replace(HOODIE_ENV_PROPS_PREFIX, "").replaceAll("_DOT_", "."), prop.getValue());
      }
    }
    return conf;
  }

  /**
   * TODO zhangyue
   * 待定 HDFSParquetImporterUtils
   */
  public static FileSystem getFs(String pathStr, Configuration conf) {
    return getFs(new Path(pathStr), conf);
  }

  public static FileSystem getFs(Path path, Configuration conf) {
    FileSystem fs;
    prepareHadoopConf(conf);
    try {
      fs = path.getFileSystem(conf);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to get instance of " + FileSystem.class.getName(), e);
    }
    return fs;
  }

  public static FileSystem getFs(String pathStr, Configuration conf, boolean localByDefault) {
    if (localByDefault) {
      return getFs(addSchemeIfLocalPath(pathStr), conf);
    }
    return getFs(pathStr, conf);
  }

  /**
   * use this api carefully, because it will load table config from base path
   * @param basePath
   * @param conf
   * @return
   */
  public static HoodieWrapperFileSystem getHoodieWrapperFileSystem(String basePath, Configuration conf) {
    FileSystem fileSystem = FSUtils.getFs(basePath, conf);
    HoodieTableConfig tableConfig = HoodieTableConfig.fromBasePath(basePath, conf);
    return new HoodieWrapperFileSystem(fileSystem, conf, tableConfig);
  }

  public static HoodieWrapperFileSystem getHoodieWrapperFileSystem(String basePath, Configuration conf, HoodieTableConfig tableConfig) {
    FileSystem fileSystem = FSUtils.getFs(basePath, conf);
    if (tableConfig == null) {
      return new HoodieWrapperFileSystem(fileSystem, conf, HoodieTableConfig.fromBasePath(basePath, conf));
    } else {
      return new HoodieWrapperFileSystem(fileSystem, conf, tableConfig);
    }
  }

  public static void addChubaoFsConfig2HadoopConf(Configuration configuration, HoodieTableConfig tableConfig) {
    if (StringUtils.nonEmpty(tableConfig.getChubaoFsOwner())) {
      LOG.info("ChubaoFs configs are set, adding them to hadoop configuration");
      configuration.set("fs.chubaofs.impl", ChubaoFSFileSystem.class.getName());
      configuration.set("cfs.log.dir", tableConfig.getChubaoFsLogDir());
      configuration.set("cfs.log.level", tableConfig.getChubaoFsLogLevel());
      String chubaoFsOwner = tableConfig.getChubaoFsOwner();
      StringUtils.splitKeyValues(chubaoFsOwner, ",", ":")
          .forEach((key, value) -> {
            configuration.set("cfs.owner." + key, value);
          });
    }
  }

  /**
   * Check if table already exists in the given path.
   * @param path base path of the table.
   * @param fs instance of {@link FileSystem}.
   * @return {@code true} if table exists. {@code false} otherwise.
   */
  public static boolean isTableExists(String path, FileSystem fs) throws IOException {
    return fs.exists(new Path(path + "/" + HoodieTableMetaClient.METAFOLDER_NAME));
  }

  public static Path addSchemeIfLocalPath(String path) {
    Path providedPath = new Path(path);
    File localFile = new File(path);
    if (!providedPath.isAbsolute() && localFile.exists()) {
      Path resolvedPath = new Path("file://" + localFile.getAbsolutePath());
      LOG.info("Resolving file " + path + " to be a local file.");
      return resolvedPath;
    }
    LOG.info("Resolving file " + path + "to be a remote file.");
    return providedPath;
  }

  /**
   * Makes path qualified w/ {@link FileSystem}'s URI
   *
   * @param fs instance of {@link FileSystem} path belongs to
   * @param path path to be qualified
   * @return qualified path, prefixed w/ the URI of the target FS object provided
   */
  public static Path makeQualified(FileSystem fs, Path path) {
    return path.makeQualified(fs.getUri(), fs.getWorkingDirectory());
  }

  /**
   * A write token uniquely identifies an attempt at one of the IOHandle operations (Merge/Create/Append).
   */
  public static String makeWriteToken(int taskPartitionId, int stageId, long taskAttemptId) {
    return String.format("%d-%d-%d", taskPartitionId, stageId, taskAttemptId);
  }

  // TODO: this should be removed
  public static String makeBaseFileName(String instantTime, String writeToken, String fileId) {
    return String.format("%s_%s_%s%s", fileId, writeToken, instantTime,
        HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().getFileExtension());
  }

  public static String makeBaseFileName(String instantTime, String writeToken, String fileId, String fileExtension) {
    return String.format("%s_%s_%s%s", fileId, writeToken, instantTime, fileExtension);
  }

  // bucketNumber_UUID_version_levelNumber_partitionId-stageId-attemptId_writeInstant.parquet.temp
  public static String makeLSMFileNameWithSuffix(String fileID, String writeInstant, String version,
                                                 String levelNumber, String writeToken, String fileExtension) {
    String uuid = UUID.randomUUID().toString();
    return String.format("%s_%s_%s_%s_%s_%s%s%s", fileID, uuid, version, levelNumber,
        writeToken, writeInstant, fileExtension, LSM_TEMP_FILE_SUFFIX);
  }

  // bucketNumber_UUID_version_levelNumber_partitionId-stageId-attemptId_writeInstant.parquet
  public static String makeLSMFileName(String fileID, String writeInstant, String version,
                                       String levelNumber, String writeToken, String fileExtension) {
    String uuid = UUID.randomUUID().toString();
    return String.format("%s_%s_%s_%s_%s_%s%s", fileID, uuid, version, levelNumber, writeToken, writeInstant, fileExtension);
  }

  public static String makeLSMFileName(String fileID, String uuid, String writeInstant, String version,
                                       String levelNumber, String writeToken, String fileExtension) {
    return String.format("%s_%s_%s_%s_%s_%s%s", fileID, uuid, version, levelNumber, writeToken, writeInstant, fileExtension);
  }

  public static String makeBootstrapIndexFileName(String instantTime, String fileId, String ext) {
    return String.format("%s_%s_%s%s", fileId, "1-0-1", instantTime, ext);
  }

  public static String maskWithoutFileId(String instantTime, int taskPartitionId) {
    return String.format("*_%s_%s%s", taskPartitionId, instantTime, HoodieTableConfig.BASE_FILE_FORMAT
        .defaultValue().getFileExtension());
  }

  public static String getCommitFromCommitFile(String commitFileName) {
    return commitFileName.split("\\.")[0];
  }

  public static String getCommitTime(String fullFileName) {
    if (isLogFile(fullFileName)) {
      String[] splits = fullFileName.split("_");
      if (splits.length == 6) {
        // LSM LogFile
        return splits[5].split("\\.")[0];
      } else {
        return splits[1].split("\\.")[0];
      }
    }
    return fullFileName.split("_")[2].split("\\.")[0];
  }

  public static String getCommitTimeWithFullPath(String path) {
    String fullFileName;
    if (path.contains("/")) {
      fullFileName = path.substring(path.lastIndexOf("/") + 1);
    } else {
      fullFileName = path;
    }
    return getCommitTime(fullFileName);
  }

  public static long getFileSize(FileSystem fs, Path path) throws IOException {
    return fs.exists(path) ? fs.getFileStatus(path).getLen() : 0L;
  }

  // Get FileID From BaseFileName
  public static String getFileId(String fullFileName) {
    return fullFileName.split("_")[0];
  }

  /**
   * Gets all partition paths assuming date partitioning (year, month, day) three levels down.
   */
  public static List<String> getAllPartitionFoldersThreeLevelsDown(FileSystem fs, String basePath) throws IOException {
    List<String> datePartitions = new ArrayList<>();
    // Avoid listing and including any folders under the metafolder
    PathFilter filter = getExcludeMetaPathFilter();
    FileStatus[] folders = fs.globStatus(new Path(basePath + "/*/*/*"), filter);
    for (FileStatus status : folders) {
      Path path = status.getPath();
      datePartitions.add(String.format("%s/%s/%s", path.getParent().getParent().getName(), path.getParent().getName(),
          path.getName()));
    }
    return datePartitions;
  }

  /**
   * Given a base partition and a partition path, return relative path of partition path to the base path.
   * TODO zhangyue:
   * HoodieBackedTableMetadataWriter
   * BootstrapUtils
   * BaseTableMetadata
   */
  public static String getRelativePartitionPath(Path basePath, Path fullPartitionPath) {
    basePath = getPathWithoutSchemeAndAuthority(basePath);
    fullPartitionPath = getPathWithoutSchemeAndAuthority(fullPartitionPath);

    String fullPartitionPathStr = fullPartitionPath.toString();

    if (!fullPartitionPathStr.startsWith(basePath.toString())) {
      throw new IllegalArgumentException("Partition path \"" + fullPartitionPathStr
          + "\" does not belong to base-path \"" + basePath + "\"");
    }

    int partitionStartIndex = fullPartitionPathStr.indexOf(basePath.getName(),
        basePath.getParent() == null ? 0 : basePath.getParent().toString().length());
    // Partition-Path could be empty for non-partitioned tables
    return partitionStartIndex + basePath.getName().length() == fullPartitionPathStr.length() ? ""
        : fullPartitionPathStr.substring(partitionStartIndex + basePath.getName().length() + 1);
  }

  /**
   * Obtain all the partition paths, that are present in this table, denoted by presence of
   * {@link HoodiePartitionMetadata#HOODIE_PARTITION_METAFILE_PREFIX}.
   *
   * If the basePathStr is a subdirectory of .hoodie folder then we assume that the partitions of an internal
   * table (a hoodie table within the .hoodie directory) are to be obtained.
   *
   * @param fs FileSystem instance
   * @param basePathStr base directory
   */
  public static List<String> getAllFoldersWithPartitionMetaFile(FileSystem fs, String basePathStr,
                                                                HoodieStorageStrategy hoodieStorageStrategy) throws IOException {
    // If the basePathStr is a folder within the .hoodie directory then we are listing partitions within an
    // internal table.
    final boolean isMetadataTable = HoodieTableMetadata.isMetadataTable(basePathStr);
    final Path basePath = new Path(basePathStr);
    final List<String> partitions = new ArrayList<>();
    processFiles(fs, basePathStr, (locatedFileStatus) -> {
      Path filePath = locatedFileStatus.getPath();
      if (filePath.getName().startsWith(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX)) {
        partitions.add(
            hoodieStorageStrategy.getRelativePath(filePath.getParent()));
      }
      return true;
    }, !isMetadataTable);
    return partitions;
  }

  /**
   * Recursively processes all files in the base-path. If excludeMetaFolder is set, the meta-folder and all its subdirs
   * are skipped
   *
   * @param fs File System
   * @param basePathStr Base-Path
   * @param consumer Callback for processing
   * @param excludeMetaFolder Exclude .hoodie folder
   * @throws IOException -
   */
  public static void processFiles(FileSystem fs, String basePathStr, Function<FileStatus, Boolean> consumer,
                                  boolean excludeMetaFolder) throws IOException {
    PathFilter pathFilter = excludeMetaFolder ? getExcludeMetaPathFilter() : ALLOW_ALL_FILTER;
    FileStatus[] topLevelStatuses = fs.listStatus(new Path(basePathStr));
    for (FileStatus child : topLevelStatuses) {
      if (child.isFile()) {
        boolean success = consumer.apply(child);
        if (!success) {
          throw new HoodieException("Failed to process file-status=" + child);
        }
      } else if (pathFilter.accept(child.getPath())) {
        RemoteIterator<LocatedFileStatus> itr = fs.listFiles(child.getPath(), true);
        while (itr.hasNext()) {
          FileStatus status = itr.next();
          boolean success = consumer.apply(status);
          if (!success) {
            throw new HoodieException("Failed to process file-status=" + status);
          }
        }
      }
    }
  }

  public static List<String> getAllPartitionPaths(HoodieEngineContext engineContext, String basePathStr,
                                                  boolean useFileListingFromMetadata,
                                                  boolean assumeDatePartitioning) {
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(useFileListingFromMetadata)
        .withAssumeDatePartitioning(assumeDatePartitioning)
        .build();
    try (HoodieTableMetadata tableMetadata = HoodieTableMetadata.create(engineContext, metadataConfig, basePathStr,
        FileSystemViewStorageConfig.SPILLABLE_DIR.defaultValue())) {
      return tableMetadata.getAllPartitionPaths();
    } catch (Exception e) {
      throw new HoodieException("Error fetching partition paths from metadata table", e);
    }
  }

  public static List<String> getAllPartitionPaths(HoodieEngineContext engineContext, HoodieMetadataConfig metadataConfig,
                                                  String basePathStr) {
    try (HoodieTableMetadata tableMetadata = HoodieTableMetadata.create(engineContext, metadataConfig, basePathStr,
        FileSystemViewStorageConfig.SPILLABLE_DIR.defaultValue())) {
      return tableMetadata.getAllPartitionPaths();
    } catch (Exception e) {
      throw new HoodieException("Error fetching partition paths from metadata table", e);
    }
  }

  public static Map<String, FileStatus[]> getFilesInPartitions(HoodieEngineContext engineContext,
                                                               HoodieMetadataConfig metadataConfig,
                                                               String basePathStr,
                                                               String[] partitionPaths) {
    try (HoodieTableMetadata tableMetadata = HoodieTableMetadata.create(engineContext, metadataConfig, basePathStr,
        FileSystemViewStorageConfig.SPILLABLE_DIR.defaultValue(), true)) {
      return tableMetadata.getAllFilesInPartitions(Arrays.asList(partitionPaths));
    } catch (Exception ex) {
      throw new HoodieException("Error get files in partitions: " + String.join(",", partitionPaths), ex);
    }
  }

  public static String getFileExtension(String fullName) {
    String actualName;
    if (fullName.endsWith(LSM_TEMP_FILE_SUFFIX)) {
      actualName = fullName.replace(LSM_TEMP_FILE_SUFFIX, "");
    } else {
      actualName = fullName;
    }
    Objects.requireNonNull(actualName);
    String fileName = new File(actualName).getName();
    int dotIndex = fileName.lastIndexOf('.');
    return dotIndex == -1 ? "" : fileName.substring(dotIndex);
  }

  private static PathFilter getExcludeMetaPathFilter() {
    // Avoid listing and including any folders under the metafolder
    return (path) -> !path.toString().contains(HoodieTableMetaClient.METAFOLDER_NAME);
  }

  /**
   * Returns a new unique prefix for creating a file group.
   */
  public static String createNewFileIdPfx() {
    return UUID.randomUUID().toString();
  }

  /**
   * Returns prefix for a file group from fileId.
   */
  public static String getFileIdPfxFromFileId(String fileId) {
    Matcher matcher = PREFIX_BY_FILE_ID_PATTERN.matcher(fileId);
    if (!matcher.find()) {
      throw new HoodieValidationException("Failed to get prefix from " + fileId);
    }
    return matcher.group(1);
  }

  public static String createNewFileId(String idPfx, int id) {
    return String.format("%s-%d", idPfx, id);
  }

  /**
   * Get the file extension from the log file. 前面没有'.'
   */
  public static String getFileExtensionFromLog(Path logPath) {
    String actualFilename = logPath.getName().endsWith(LSM_TEMP_FILE_SUFFIX) ? logPath.getName().replace(LSM_TEMP_FILE_SUFFIX, "") : logPath.getName();
    Option<Matcher> matchLSMLogFile = matchLSMLogFile(actualFilename);
    if (matchLSMLogFile.isPresent()) {
      return matchLSMLogFile.get().group(9);
    } else {
      Matcher matcher = LOG_FILE_PATTERN.matcher(actualFilename);
      if (!matcher.find()) {
        throw new InvalidHoodiePathException(logPath, "LogFile");
      }
      return matcher.group(3);
    }
  }

  /**
   * Get the fileId from a fileName.
   */
  public static String getFileIdFromFileName(String fileName) {
    if (FSUtils.matchCommonLogFile(fileName).isPresent()) {
      Matcher matcher = LOG_FILE_PATTERN.matcher(fileName);
      if (!matcher.find()) {
        throw new InvalidHoodieFileNameException(fileName, "LogFile");
      }
      return matcher.group(1);
    }
    if (FSUtils.matchLSMLogFile(fileName).isPresent()) {
      Matcher matcher = FSUtils.LSM_LOG_FILE_PATTERN.matcher(fileName);
      if (!matcher.find()) {
        throw new InvalidHoodieFileNameException(fileName, "LogFile");
      }
      return matcher.group(1);
    }

    return FSUtils.getFileId(fileName);
  }

  /**
   * Get the first part of the file name in the log file. That will be the fileId. Log file do not have instantTime in
   * the file name.
   */
  public static String getFileIdFromLogPath(Path path) {
    Option<Matcher> matchLSMLogFile = matchLSMLogFile(path.getName());
    if (matchLSMLogFile.isPresent()) {
      return matchLSMLogFile.get().group(1);
    } else {
      Matcher matcher = LOG_FILE_PATTERN.matcher(path.getName());
      if (!matcher.find()) {
        throw new InvalidHoodiePathException(path, "LogFile");
      }
      return matcher.group(1);
    }
  }

  /**
   * Check if the file is a base file of a log file. Then get the fileId appropriately.
   */
  public static String getFileIdFromFilePath(Path filePath) {
    Option<Matcher> matchCommonLogFile = matchCommonLogFile(filePath.getName());
    if (matchCommonLogFile.isPresent()) {
      return matchCommonLogFile.get().group(1);
    } else {
      Option<Matcher> matchLSMLogFile = matchLSMLogFile(filePath.getName());
      if (matchLSMLogFile.isPresent()) {
        return matchLSMLogFile.get().group(1);
      } else {
        return FSUtils.getFileId(filePath.getName());
      }
    }
  }

  /**
   * Get the first part of the file name in the log file. That will be the fileId. Log file do not have instantTime in
   * the file name.
   */
  public static String getBaseCommitTimeFromLogPath(Path path) {
    Option<Matcher> matchLSMLogFile = matchLSMLogFile(path.getName());
    if (matchLSMLogFile.isPresent()) {
      return matchLSMLogFile.get().group(8);
    } else {
      Matcher matcher = LOG_FILE_PATTERN.matcher(path.getName());
      if (!matcher.find()) {
        throw new InvalidHoodiePathException(path, "LogFile");
      }
      return matcher.group(2);
    }
  }

  /**
   * Get TaskPartitionId used in log-path.
   */
  public static Integer getTaskPartitionIdFromLogPath(Path path) {
    Option<Matcher> matchLSMLogFile = matchLSMLogFile(path.getName());
    if (matchLSMLogFile.isPresent()) {
      String val = matchLSMLogFile.get().group(5);
      return val == null ? null : Integer.parseInt(val);
    } else {
      Matcher matcher = LOG_FILE_PATTERN.matcher(path.getName());
      if (!matcher.find()) {
        throw new InvalidHoodiePathException(path, "LogFile");
      }
      String val = matcher.group(7);
      return val == null ? null : Integer.parseInt(val);
    }
  }

  /**
   * Get Write-Token used in log-path.
   */
  public static String getWriteTokenFromLogPath(Path path) {
    Option<Matcher> matchLSMLogFile = matchLSMLogFile(path.getName());
    if (matchLSMLogFile.isPresent()) {
      return matchLSMLogFile.get().group(5) + "-" + matchLSMLogFile.get().group(6) + "-" + matchLSMLogFile.get().group(7);
    } else {
      Matcher matcher = LOG_FILE_PATTERN.matcher(path.getName());
      if (!matcher.find()) {
        throw new InvalidHoodiePathException(path, "LogFile");
      }
      return matcher.group(6);
    }
  }

  /**
   * Get StageId used in log-path.
   */
  public static Integer getStageIdFromLogPath(Path path) {
    Option<Matcher> matchLSMLogFile = matchLSMLogFile(path.getName());
    if (matchLSMLogFile.isPresent()) {
      String val = matchLSMLogFile.get().group(6);
      return val == null ? null : Integer.parseInt(val);
    } else {
      Matcher matcher = LOG_FILE_PATTERN.matcher(path.getName());
      if (!matcher.find()) {
        throw new InvalidHoodiePathException(path, "LogFile");
      }
      String val = matcher.group(8);
      return val == null ? null : Integer.parseInt(val);
    }
  }

  /**
   * Get Task Attempt Id used in log-path.
   */
  public static Integer getTaskAttemptIdFromLogPath(Path path) {
    Option<Matcher> matchLSMLogFile = matchLSMLogFile(path.getName());
    if (matchLSMLogFile.isPresent()) {
      String val = matchLSMLogFile.get().group(7);
      return val == null ? null : Integer.parseInt(val);
    } else {
      Matcher matcher = LOG_FILE_PATTERN.matcher(path.getName());
      if (!matcher.find()) {
        throw new InvalidHoodiePathException(path, "LogFile");
      }
      String val = matcher.group(9);
      return val == null ? null : Integer.parseInt(val);
    }
  }

  /**
   * Get the last part of the file name in the log file and convert to int.
   */
  public static int getFileVersionFromLog(Path logPath) {
    return getFileVersionFromLog(logPath.getName());
  }

  public static int getLevelNumFromLog(Path logPath) {
    String logFileName = logPath.getName();
    Matcher matcher = LSM_LOG_FILE_PATTERN.matcher(logFileName);
    if (!matcher.find()) {
      throw new HoodieIOException("Invalid log file name: " + logFileName);
    }
    return Integer.parseInt(matcher.group(4));
  }

  public static String getLSMFilePrefix(Path path) {
    String lsmFileName = path.getName();
    Matcher matcher = LSM_LOG_FILE_PATTERN.matcher(lsmFileName);
    if (!matcher.find()) {
      throw new HoodieIOException("Invalid log file name: " + lsmFileName);
    }
    return matcher.group(1) + "_" + matcher.group(2);
  }

  public static String getUUIDFromLog(Path logPath) {
    String logFileName = logPath.getName();
    Matcher matcher = LSM_LOG_FILE_PATTERN.matcher(logFileName);
    if (!matcher.find()) {
      throw new HoodieIOException("Invalid log file name: " + logFileName);
    }
    return matcher.group(2);
  }

  public static int getFileVersionFromLog(String logFileName) {
    Option<Matcher> matchLSMLogFile = matchLSMLogFile(logFileName);
    if (matchLSMLogFile.isPresent()) {
      return Integer.parseInt(matchLSMLogFile.get().group(3));
    } else {
      Matcher matcher = LOG_FILE_PATTERN.matcher(logFileName);
      if (!matcher.find()) {
        throw new HoodieIOException("Invalid log file name: " + logFileName);
      }
      return Integer.parseInt(matcher.group(4));
    }
  }

  public static String getSuffixFromLogPath(Path path) {
    Matcher matcher = LOG_FILE_PATTERN.matcher(path.getName());
    if (!matcher.find()) {
      throw new InvalidHoodiePathException(path, "LogFile");
    }
    String val = matcher.group(10);
    return val == null ? "" : val;
  }

  public static String makeLogFileName(String fileId, String logFileExtension, String baseCommitTime, int version,
      String writeToken) {
    String suffix = (writeToken == null)
        ? String.format("%s_%s%s.%d", fileId, baseCommitTime, logFileExtension, version)
        : String.format("%s_%s%s.%d_%s", fileId, baseCommitTime, logFileExtension, version, writeToken);
    return HoodieLogFile.LOG_FILE_PREFIX + suffix;
  }

  public static boolean isBaseFile(Path path) {
    String extension = getFileExtension(path.getName());
    return HoodieFileFormat.BASE_FILE_EXTENSIONS.contains(extension) && !matchLSMLogFile(path.getName()).isPresent()
        && !path.getName().contains(LSM_TEMP_FILE_SUFFIX);
  }

  public static boolean isLogFile(Path logPath) {
    return isLogFile(logPath.getName());
  }

  public static boolean isLogFile(String fileName) {
    return matchCommonLogFile(fileName).isPresent() || matchLSMLogFile(fileName).isPresent();
  }

  public static Option<Matcher> matchCommonLogFile(String fileName) {
    if (StringUtils.isNullOrEmpty(fileName)) {
      return Option.empty();
    }
    if (fileName.contains("/")) {
      fileName = fileName.substring(fileName.lastIndexOf("/") + 1);
    }
    Matcher matcher = LOG_FILE_PATTERN.matcher(fileName);
    if (matcher.find() && fileName.contains(".log")) {
      return Option.of(matcher);
    } else {
      return Option.empty();
    }
  }

  public static Option<Matcher> matchLSMLogFile(String fileName) {
    if (StringUtils.isNullOrEmpty(fileName)) {
      return Option.empty();
    }
    if (fileName.contains("/")) {
      fileName = fileName.substring(fileName.lastIndexOf("/") + 1);
    }
    Matcher matcher = LSM_LOG_FILE_PATTERN.matcher(fileName);
    if (matcher.find()) {
      return Option.of(matcher);
    } else {
      return Option.empty();
    }
  }

  /**
   * Returns true if the given path is a Base file or a Log file.
   */
  public static boolean isDataFile(Path path) {
    return isBaseFile(path) || isLogFile(path);
  }

  /**
   * Get the names of all the base and log files in the given partition path.
   */
  public static FileStatus[] getAllDataFilesInPartition(FileSystem fs, Path partitionPath) throws IOException {
    final Set<String> validFileExtensions = Arrays.stream(HoodieFileFormat.values())
        .map(HoodieFileFormat::getFileExtension).collect(Collectors.toCollection(HashSet::new));
    final String logFileExtension = HoodieFileFormat.HOODIE_LOG.getFileExtension();

    try {
      return Arrays.stream(fs.listStatus(partitionPath, path -> {
        String extension = FSUtils.getFileExtension(path.getName());
        return validFileExtensions.contains(extension) || path.getName().contains(logFileExtension);
      })).filter(FileStatus::isFile).toArray(FileStatus[]::new);
    } catch (IOException e) {
      // return empty FileStatus if partition does not exist already
      if (!fs.exists(partitionPath)) {
        return new FileStatus[0];
      } else {
        throw e;
      }
    }
  }

  /**
   * Get the latest log file for the passed in file-id in the partition path
   */
  public static Option<HoodieLogFile> getLatestLogFile(FileSystem fs, Path partitionPath, String fileId,
                                                       String logFileExtension, String baseCommitTime) throws IOException {
    return getLatestLogFile(getAllLogFiles(fs, partitionPath, fileId, logFileExtension, baseCommitTime));
  }

  /**
   * Get all the log files for the passed in file-id in the partition path.
   */
  public static Stream<HoodieLogFile> getAllLogFiles(FileSystem fs, Path partitionPath, final String fileId,
      final String logFileExtension, final String baseCommitTime) throws IOException {
    try {
      PathFilter pathFilter = path -> path.getName().startsWith("." + fileId) && path.getName().contains(logFileExtension);
      return Arrays.stream(fs.listStatus(partitionPath, pathFilter))
          .map(HoodieLogFile::new)
          .filter(s -> s.getBaseCommitTime().equals(baseCommitTime));
    } catch (FileNotFoundException e) {
      return Stream.of();
    }
  }

  /**
   * Get the latest log version for the fileId in the partition path.
   */
  public static Option<Pair<Integer, String>> getLatestLogVersion(FileSystem fs, Path partitionPath,
      final String fileId, final String logFileExtension, final String baseCommitTime) throws IOException {
    Option<HoodieLogFile> latestLogFile =
        getLatestLogFile(getAllLogFiles(fs, partitionPath, fileId, logFileExtension, baseCommitTime));
    if (latestLogFile.isPresent()) {
      return Option
          .of(Pair.of(latestLogFile.get().getLogVersion(), getWriteTokenFromLogPath(latestLogFile.get().getPath())));
    }
    return Option.empty();
  }

  /**
   * computes the next log version for the specified fileId in the partition path.
   */
  public static int computeNextLogVersion(FileSystem fs, Path partitionPath, final String fileId,
      final String logFileExtension, final String baseCommitTime) throws IOException {
    Option<Pair<Integer, String>> currentVersionWithWriteToken =
        getLatestLogVersion(fs, partitionPath, fileId, logFileExtension, baseCommitTime);
    // handle potential overflow
    return (currentVersionWithWriteToken.isPresent()) ? currentVersionWithWriteToken.get().getKey() + 1
        : HoodieLogFile.LOGFILE_BASE_VERSION;
  }

  public static int getDefaultBufferSize(final FileSystem fs) {
    return fs.getConf().getInt("io.file.buffer.size", 4096);
  }

  public static Short getDefaultReplication(FileSystem fs, Path path) {
    return fs.getDefaultReplication(path);
  }

  /**
   * When a file was opened and the task died without closing the stream, another task executor cannot open because the
   * existing lease will be active. We will try to recover the lease, from HDFS. If a data node went down, it takes
   * about 10 minutes for the lease to be recovered. But if the client dies, this should be instant.
   */
  public static boolean recoverDFSFileLease(final DistributedFileSystem dfs, final Path p)
      throws IOException, InterruptedException {
    LOG.info("Recover lease on dfs file " + p);
    // initiate the recovery
    boolean recovered = false;
    for (int nbAttempt = 0; nbAttempt < MAX_ATTEMPTS_RECOVER_LEASE; nbAttempt++) {
      LOG.info("Attempt " + nbAttempt + " to recover lease on dfs file " + p);
      recovered = dfs.recoverLease(p);
      if (recovered) {
        break;
      }
      // Sleep for 1 second before trying again. Typically it takes about 2-3 seconds to recover
      // under default settings
      Thread.sleep(1000);
    }
    return recovered;
  }

  public static void createPathIfNotExists(FileSystem fs, Path partitionPath) throws IOException {
    if (!fs.exists(partitionPath)) {
      fs.mkdirs(partitionPath);
    }
  }

  public static Long getSizeInMB(long sizeInBytes) {
    return sizeInBytes / (1024 * 1024);
  }

  /**
   * TODO zhangyue need change
   * AWSGlueCatalogSyncClient
   * RepairsCommand
   * HoodieBackedTableMetadataWriter
   * CleanPlanV2MigrationHandler
   * RepairMigratePartitionMetaProcedure
   * HoodieAdbJdbcClient
   * HoodieMetadataTableValidator
   * HoodieSnapshotCopier
   * HoodieSnapshotExporter
   */
  public static Path getPartitionPath(String basePath, String partitionPath) {
    if (StringUtils.isNullOrEmpty(partitionPath)) {
      return new Path(basePath);
    }

    // NOTE: We have to chop leading "/" to make sure Hadoop does not treat it like
    //       absolute path
    String properPartitionPath = partitionPath.startsWith("/")
        ? partitionPath.substring(1)
        : partitionPath;
    return getPartitionPath(new CachingPath(basePath), properPartitionPath);
  }

  /**
   * TODO zhangyue need change
   * RepairsCommand
   * HoodieSparkConsistentBucketIndex
   * HoodieCDCExtractor -- CDC 用不了
   * CleanMetadataV1MigrationHandler
   * CompactionV1MigrationHandler
   * RepairAddpartitionmetaProcedure
   * HoodieMetadataTableValidator
   */
  public static Path getPartitionPath(Path basePath, String partitionPath) {
    // For non-partitioned table, return only base-path
    return StringUtils.isNullOrEmpty(partitionPath) ? basePath : new CachingPath(basePath, partitionPath);
  }

  /**
   * Extracts the file name from the relative path based on the table base path.  For example:
   * "/2022/07/29/file1.parquet", "/2022/07/29" -> "file1.parquet"
   * "2022/07/29/file2.parquet", "2022/07/29" -> "file2.parquet"
   * "/file3.parquet", "" -> "file3.parquet"
   * "file4.parquet", "" -> "file4.parquet"
   *
   * @param filePathWithPartition the relative file path based on the table base path.
   * @param partition             the relative partition path.  For partitioned table, `partition` contains the relative partition path;
   *                              for non-partitioned table, `partition` is empty
   * @return Extracted file name in String.
   */
  public static String getFileName(String filePathWithPartition, String partition) {
    int offset = StringUtils.isNullOrEmpty(partition)
        ? (filePathWithPartition.startsWith("/") ? 1 : 0) : partition.length() + 1;
    return filePathWithPartition.substring(offset);
  }

  /**
   * Get DFS full partition path (e.g. hdfs://ip-address:8020:/<absolute path>)
   */
  public static String getDFSFullPartitionPath(FileSystem fs, Path fullPartitionPath) {
    return fs.getUri() + fullPartitionPath.toUri().getRawPath();
  }

  /**
   * This is due to HUDI-140 GCS has a different behavior for detecting EOF during seek().
   *
   * @param fs fileSystem instance.
   * @return true if the inputstream or the wrapped one is of type GoogleHadoopFSInputStream
   */
  public static boolean isGCSFileSystem(FileSystem fs) {
    return fs.getScheme().equals(StorageSchemes.GCS.getScheme());
  }

  /**
   * Chdfs will throw {@code IOException} instead of {@code EOFException}. It will cause error in isBlockCorrupted().
   * Wrapped by {@code BoundedFsDataInputStream}, to check whether the desired offset is out of the file size in advance.
   */
  public static boolean isCHDFileSystem(FileSystem fs) {
    return StorageSchemes.CHDFS.getScheme().equals(fs.getScheme());
  }

  public static Configuration registerFileSystem(Path file, Configuration conf, HoodieConfig hoodieConfig) {
    Configuration returnConf = new Configuration(conf);
    String scheme = FSUtils.getFs(file.toString(), conf).getScheme();
    returnConf.set("fs." + HoodieWrapperFileSystem.getHoodieScheme(scheme) + ".impl",
        HoodieWrapperFileSystem.class.getName());
    if (hoodieConfig.contains(HOODIE_BASE_PATH_KEY)) {
      returnConf.set(HOODIE_BASE_PATH_KEY, hoodieConfig.getString(HOODIE_BASE_PATH_KEY));
    }

    if (hoodieConfig.contains(HOODIE_STORAGE_STRATEGY_CLASS_NAME) && hoodieConfig.contains(HOODIE_STORAGE_PATH)
        && !StringUtils.isNullOrEmpty(hoodieConfig.getString(HOODIE_STORAGE_STRATEGY_CLASS_NAME))
        && !StringUtils.isNullOrEmpty(hoodieConfig.getString(HOODIE_STORAGE_PATH))) {
      returnConf.set(HoodieTableConfig.HOODIE_STORAGE_STRATEGY_CLASS_NAME.key(), hoodieConfig.getString(HOODIE_STORAGE_STRATEGY_CLASS_NAME.key()));
      returnConf.set(HoodieTableConfig.HOODIE_STORAGE_PATH.key(), hoodieConfig.getString(HOODIE_STORAGE_PATH.key()));

      if ((hoodieConfig.contains(HOODIE_STORAGE_CHUBAO_FS_OWNER) && !StringUtils.isNullOrEmpty(hoodieConfig.getString(HOODIE_STORAGE_CHUBAO_FS_OWNER)))) {
        returnConf.set(HoodieTableConfig.HOODIE_STORAGE_CHUBAO_FS_LOG_DIR.key(), hoodieConfig.getStringOrDefault(HOODIE_STORAGE_CHUBAO_FS_LOG_DIR));
        returnConf.set(HoodieTableConfig.HOODIE_STORAGE_CHUBAO_FS_LOG_LEVEL.key(), hoodieConfig.getStringOrDefault(HOODIE_STORAGE_CHUBAO_FS_LOG_LEVEL));
        returnConf.set(HoodieTableConfig.HOODIE_STORAGE_CHUBAO_FS_OWNER.key(), hoodieConfig.getString(HOODIE_STORAGE_CHUBAO_FS_OWNER));
      }
    }
    return returnConf;
  }

  /**
   * Get the FS implementation for this table.
   * @param path  Path String
   * @param hadoopConf  Serializable Hadoop Configuration
   * @param consistencyGuardConfig Consistency Guard Config
   * @return HoodieWrapperFileSystem
   */
  public static HoodieWrapperFileSystem getFs(String path, SerializableConfiguration hadoopConf,
      ConsistencyGuardConfig consistencyGuardConfig) {
    FileSystem fileSystem = FSUtils.getFs(path, hadoopConf.newCopy());
    return new HoodieWrapperFileSystem(fileSystem,
        consistencyGuardConfig.isConsistencyCheckEnabled()
            ? new FailSafeConsistencyGuard(fileSystem, consistencyGuardConfig)
            : new NoOpConsistencyGuard());
  }

  /**
   * Helper to filter out paths under metadata folder when running fs.globStatus.
   * @param fs  File System
   * @param globPath Glob Path
   * @return the file status list of globPath exclude the meta folder
   * @throws IOException when having trouble listing the path
   */
  public static List<FileStatus> getGlobStatusExcludingMetaFolder(FileSystem fs, Path globPath) throws IOException {
    FileStatus[] statuses = fs.globStatus(globPath);
    return Arrays.stream(statuses)
        .filter(fileStatus -> !fileStatus.getPath().toString().contains(HoodieTableMetaClient.METAFOLDER_NAME))
        .collect(Collectors.toList());
  }

  /**
   * Deletes a directory by deleting sub-paths in parallel on the file system.
   *
   * @param hoodieEngineContext {@code HoodieEngineContext} instance
   * @param fs file system
   * @param dirPath directory path
   * @param parallelism parallelism to use for sub-paths
   * @return {@code true} if the directory is delete; {@code false} otherwise.
   */
  public static boolean deleteDir(
      HoodieEngineContext hoodieEngineContext, FileSystem fs, Path dirPath, int parallelism) {
    try {
      if (fs.exists(dirPath)) {
        FSUtils.parallelizeSubPathProcess(hoodieEngineContext, fs, dirPath, parallelism, e -> true,
            pairOfSubPathAndConf -> deleteSubPath(
                pairOfSubPathAndConf.getKey(), pairOfSubPathAndConf.getValue(), true)
        );
        boolean result = fs.delete(dirPath, false);
        LOG.info("Removed directory at " + dirPath);
        return result;
      }
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
    return false;
  }

  /**
   * Processes sub-path in parallel.
   *
   * @param hoodieEngineContext {@code HoodieEngineContext} instance
   * @param fs file system
   * @param dirPath directory path
   * @param parallelism parallelism to use for sub-paths
   * @param subPathPredicate predicate to use to filter sub-paths for processing
   * @param pairFunction actual processing logic for each sub-path
   * @param <T> type of result to return for each sub-path
   * @return a map of sub-path to result of the processing
   */
  public static <T> Map<String, T> parallelizeSubPathProcess(
      HoodieEngineContext hoodieEngineContext, FileSystem fs, Path dirPath, int parallelism,
      Predicate<FileStatus> subPathPredicate, SerializableFunction<Pair<String, SerializableConfiguration>, T> pairFunction) {
    Map<String, T> result = new HashMap<>();
    try {
      FileStatus[] fileStatuses = fs.listStatus(dirPath);
      List<String> subPaths = Arrays.stream(fileStatuses)
          .filter(subPathPredicate)
          .map(fileStatus -> fileStatus.getPath().toString())
          .collect(Collectors.toList());
      result = parallelizeFilesProcess(hoodieEngineContext, fs, parallelism, pairFunction, subPaths);
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
    return result;
  }

  public static <T> Map<String, T> parallelizeFilesProcess(
      HoodieEngineContext hoodieEngineContext,
      FileSystem fs,
      int parallelism,
      SerializableFunction<Pair<String, SerializableConfiguration>, T> pairFunction,
      List<String> subPaths) {
    Map<String, T> result = new HashMap<>();
    if (subPaths.size() > 0) {
      SerializableConfiguration conf = new SerializableConfiguration(fs.getConf());
      int actualParallelism = Math.min(subPaths.size(), parallelism);

      hoodieEngineContext.setJobStatus(FSUtils.class.getSimpleName(),
          "Parallel listing paths " + String.join(",", subPaths));

      result = hoodieEngineContext.mapToPair(subPaths,
          subPath -> new ImmutablePair<>(subPath, pairFunction.apply(new ImmutablePair<>(subPath, conf))),
          actualParallelism);
    }
    return result;
  }

  /**
   * Deletes a sub-path.
   *
   * @param subPathStr sub-path String
   * @param conf       serializable config
   * @param recursive  is recursive or not
   * @return {@code true} if the sub-path is deleted; {@code false} otherwise.
   */
  public static boolean deleteSubPath(String subPathStr, SerializableConfiguration conf, boolean recursive) {
    try {
      Path subPath = new Path(subPathStr);
      FileSystem fileSystem = subPath.getFileSystem(conf.get());
      return fileSystem.delete(subPath, recursive);
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }

  public static List<FileStatus> getAllDataFileStatus(FileSystem fs, Path path) throws IOException {
    List<FileStatus> statuses = new ArrayList<>();
    for (FileStatus status : fs.listStatus(path)) {
      if (!status.getPath().toString().contains(HoodieTableMetaClient.METAFOLDER_NAME)) {
        if (status.isDirectory()) {
          statuses.addAll(getAllDataFileStatus(fs, status.getPath()));
        } else {
          statuses.add(status);
        }
      }
    }
    return statuses;
  }

  /**
   * Deletes a sub-path.
   *
   * @param fs   The file system implementation for this table.
   * @param path the base path of hudi table.
   * @return     A list of file status of files under the path.
   */
  public static List<FileStatus> getAllDataFileStatusExcludingMetaFolder(FileSystem fs, Path path) throws IOException {
    List<FileStatus> statuses = new ArrayList<>();
    for (FileStatus status : fs.listStatus(path)) {
      if (!status.getPath().toString().contains(HoodieTableMetaClient.METAFOLDER_NAME)) {
        if (status.isDirectory()) {
          statuses.addAll(getAllDataFileStatusExcludingMetaFolder(fs, status.getPath()));
        } else {
          statuses.add(status);
        }
      }
    }
    return statuses;
  }

  /**
   * Lists file status at a certain level in the directory hierarchy.
   * <p>
   * E.g., given "/tmp/hoodie_table" as the rootPath, and 3 as the expected level,
   * this method gives back the {@link FileStatus} of all files under
   * "/tmp/hoodie_table/[*]/[*]/[*]/" folders.
   *
   * @param hoodieEngineContext {@link HoodieEngineContext} instance.
   * @param fs                  {@link FileSystem} instance.
   * @param rootPath            Root path for the file listing.
   * @param expectLevel         Expected level of directory hierarchy for files to be added.
   * @param parallelism         Parallelism for the file listing.
   * @return A list of file status of files at the level.
   */

  public static List<FileStatus> getFileStatusAtLevel(
      HoodieEngineContext hoodieEngineContext, FileSystem fs, Path rootPath,
      int expectLevel, int parallelism) {
    List<String> levelPaths = new ArrayList<>();
    List<FileStatus> result = new ArrayList<>();
    levelPaths.add(rootPath.toString());

    for (int i = 0; i <= expectLevel; i++) {
      result = FSUtils.parallelizeFilesProcess(hoodieEngineContext, fs, parallelism,
          pairOfSubPathAndConf -> {
            Path path = new Path(pairOfSubPathAndConf.getKey());
            try {
              FileSystem fileSystem = path.getFileSystem(pairOfSubPathAndConf.getValue().get());
              return Arrays.stream(fileSystem.listStatus(path))
                .collect(Collectors.toList());
            } catch (IOException e) {
              throw new HoodieIOException("Failed to list " + path, e);
            }
          },
          levelPaths)
          .values().stream()
          .flatMap(list -> list.stream()).collect(Collectors.toList());
      if (i < expectLevel) {
        levelPaths = result.stream()
            .filter(FileStatus::isDirectory)
            .map(fileStatus -> fileStatus.getPath().toString())
            .collect(Collectors.toList());
      }
    }
    return result;
  }

  /**
   * Serializable function interface.
   *
   * @param <T> Input value type.
   * @param <R> Output value type.
   */
  public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {
  }

  private static Option<HoodieLogFile> getLatestLogFile(Stream<HoodieLogFile> logFiles) {
    return Option.fromJavaOptional(logFiles.min(HoodieLogFile.getReverseLogFileComparator()));
  }

  /**
   * Copy from HoodieWrapperFileSystem.createImmutableFileInPath(xx)
   */
  public static void createImmutableFileInPath(FileSystem fs, Path fullPath, Option<byte[]> content, boolean needTempFile)
      throws HoodieIOException {
    FSDataOutputStream fsout = null;
    Path tmpPath = null;
    boolean isWrote = false;
    try {
      if (!content.isPresent()) {
        fsout = fs.create(fullPath, false);
      }

      if (content.isPresent() && needTempFile) {
        Path parent = fullPath.getParent();
        tmpPath = new Path(parent, fullPath.getName() + "." + UUID.randomUUID());
        fsout = fs.create(tmpPath, false);
        fsout.write(content.get());
        isWrote = true;
      }

      if (content.isPresent() && !needTempFile) {
        fsout = fs.create(fullPath, false);
        fsout.write(content.get());
      }
    } catch (IOException e) {
      String errorMsg = "Failed to create file" + (tmpPath != null ? tmpPath : fullPath);
      throw new HoodieIOException(errorMsg, e);
    } finally {
      boolean isClosed = false;
      boolean renameSuccess = false;
      try {
        if (null != fsout) {
          fsout.close();
          isClosed = true;
        }
        if (null != tmpPath && isClosed && isWrote) {
          renameSuccess = fs.rename(tmpPath, fullPath);
        }
      } catch (IOException e) {
        throw new HoodieIOException("HoodieIOE occurs, rename " + tmpPath + " to the target "
            + fullPath + ": " + renameSuccess + ", " + " closing : " + isClosed, e);
      } finally {
        if (!renameSuccess && null != tmpPath) {
          try {
            fs.delete(tmpPath);
            LOG.warn("Fail to rename " + tmpPath + " to " + fullPath
                + ", target file exists: " + fs.exists(fullPath));
          } catch (IOException e) {
            throw new HoodieIOException("Failed to delete tmp file " + tmpPath, e);
          }
        }
      }
    }
  }

  /**
   * Delete inflight and requested rollback instant file after temp file created.
   */
  public static void createImmutableRollbackFileInPath(FileSystem fs, Path fullPath, Option<byte[]> content, boolean needTempFile, Runnable deleteInflightAndRequestedInstantFunc)
          throws HoodieIOException {
    FSDataOutputStream fsout = null;
    Path tmpPath = null;
    boolean isWrote = false;
    try {
      if (content.isPresent() && needTempFile) {
        Path parent = fullPath.getParent();
        tmpPath = new Path(parent, fullPath.getName() + "." + UUID.randomUUID());
        fsout = fs.create(tmpPath, false);
        fsout.write(content.get());
        isWrote = true;
      }

      deleteInflightAndRequestedInstantFunc.run();

      if (!content.isPresent()) {
        fsout = fs.create(fullPath, false);
      }

      if (content.isPresent() && !needTempFile) {
        fsout = fs.create(fullPath, false);
        fsout.write(content.get());
      }
    } catch (IOException e) {
      String errorMsg = "Failed to create file" + (tmpPath != null ? tmpPath : fullPath);
      throw new HoodieIOException(errorMsg, e);
    } finally {
      boolean isClosed = false;
      boolean renameSuccess = false;
      try {
        if (null != fsout) {
          fsout.close();
          isClosed = true;
        }
        if (null != tmpPath && isClosed && isWrote) {
          renameSuccess = fs.rename(tmpPath, fullPath);
        }
      } catch (IOException e) {
        throw new HoodieIOException("HoodieIOE occurs, rename " + tmpPath + " to the target "
                + fullPath + ": " + renameSuccess + ", " + " closing : " + isClosed, e);
      } finally {
        if (!renameSuccess && null != tmpPath) {
          try {
            // Don't delete temp rollback file if rename failed
            LOG.warn("Fail to rename " + tmpPath + " to " + fullPath
                    + ", target file exists: " + fs.exists(fullPath));
          } catch (IOException e) {
            throw new HoodieIOException("Failed to check target file exists or not: " + fullPath, e);
          }
        }
      }
    }
  }
}