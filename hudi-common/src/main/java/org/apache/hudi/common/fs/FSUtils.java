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

package org.apache.hudi.common.fs;

import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.avro.model.HoodiePath;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.exception.InvalidHoodieFileNameException;
import org.apache.hudi.exception.InvalidHoodiePathException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathFilter;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.inline.InLineFSUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility functions related to accessing the file storage.
 */
public class FSUtils {

  private static final Logger LOG = LoggerFactory.getLogger(FSUtils.class);
  // Log files are of this pattern - .b5068208-e1a4-11e6-bf01-fe55135034f3_20170101134598.log.1_1-0-1
  // Archive log files are of this pattern - .commits_.archive.1_1-0-1
  public static final String PATH_SEPARATOR = "/";
  public static final Pattern LOG_FILE_PATTERN =
      Pattern.compile("^\\.(.+)_(.*)\\.(log|archive)\\.(\\d+)(_((\\d+)-(\\d+)-(\\d+))(.cdc)?)?");
  public static final Pattern PREFIX_BY_FILE_ID_PATTERN = Pattern.compile("^(.+)-(\\d+)");
  private static final Pattern BASE_FILE_PATTERN = Pattern.compile("[a-zA-Z0-9-]+_[a-zA-Z0-9-]+_[0-9]+\\.[a-zA-Z0-9]+");

  private static final String LOG_FILE_EXTENSION = ".log";

  private static final StoragePathFilter ALLOW_ALL_FILTER = file -> true;

  /**
   * Check if table already exists in the given path.
   *
   * @param path base path of the table.
   * @param storage   instance of {@link HoodieStorage}.
   * @return {@code true} if table exists. {@code false} otherwise.
   */
  public static boolean isTableExists(String path, HoodieStorage storage) throws IOException {
    return storage.exists(new StoragePath(path, HoodieTableMetaClient.METAFOLDER_NAME));
  }

  /**
   * Makes path qualified with {@link HoodieStorage}'s URI.
   *
   * @param storage instance of {@link HoodieStorage}.
   * @param path    to be qualified.
   * @return qualified path, prefixed with the URI of the target HoodieStorage object provided.
   */
  public static StoragePath makeQualified(HoodieStorage storage, StoragePath path) {
    return path.makeQualified(storage.getUri());
  }

  /**
   * A write token uniquely identifies an attempt at one of the IOHandle operations (Merge/Create/Append).
   */
  public static String makeWriteToken(int taskPartitionId, int stageId, long taskAttemptId) {
    return String.format("%d-%d-%d", taskPartitionId, stageId, taskAttemptId);
  }

  public static String makeBaseFileName(String instantTime, String writeToken, String fileId, String fileExtension) {
    return String.format("%s_%s_%s%s", fileId, writeToken, instantTime, fileExtension);
  }

  public static String makeBootstrapIndexFileName(String instantTime, String fileId, String ext) {
    return String.format("%s_%s_%s%s", fileId, "1-0-1", instantTime, ext);
  }

  public static String maskWithoutFileId(String instantTime, int taskPartitionId) {
    return String.format("*_%s_%s%s", taskPartitionId, instantTime, HoodieTableConfig.BASE_FILE_FORMAT
        .defaultValue().getFileExtension());
  }

  public static String getCommitTime(String fullFileName) {
    try {
      if (isLogFile(fullFileName)) {
        return fullFileName.split("_")[1].split("\\.", 2)[0];
      }
      return fullFileName.split("_")[2].split("\\.", 2)[0];
    } catch (ArrayIndexOutOfBoundsException e) {
      throw new HoodieException("Failed to get commit time from filename: " + fullFileName, e);
    }
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

  public static long getFileSize(HoodieStorage storage, StoragePath path) throws IOException {
    return storage.getPathInfo(path).getLength();
  }

  public static String getFileId(String fullFileName) {
    return fullFileName.split("_", 2)[0];
  }

  /**
   * @param filePath
   * @returns the filename from the given path. Path could be the absolute path or just partition path and file name.
   */
  public static String getFileNameFromPath(String filePath) {
    return filePath.substring(filePath.lastIndexOf("/") + 1);
  }

  /**
   * Gets all partition paths assuming date partitioning (year, month, day) three levels down.
   * TODO: (Lin) Delete this function after we remove the assume.date.partitioning config completely.
   */
  public static List<String> getAllPartitionFoldersThreeLevelsDown(HoodieStorage storage, String basePath) throws IOException {
    List<String> datePartitions = new ArrayList<>();
    // Avoid listing and including any folders under the metafolder
    StoragePathFilter filter = getExcludeMetaPathFilter();
    List<StoragePathInfo> folders = storage.globEntries(new StoragePath(basePath + "/*/*/*"), filter);
    for (StoragePathInfo pathInfo : folders) {
      StoragePath path = pathInfo.getPath();
      datePartitions.add(
          String.format("%s/%s/%s", path.getParent().getParent().getName(), path.getParent().getName(),
              path.getName()));
    }
    return datePartitions;
  }

  public static String getRelativePartitionPath(StoragePath basePath, StoragePath fullPartitionPath) {
    basePath = getPathWithoutSchemeAndAuthority(basePath);
    fullPartitionPath = getPathWithoutSchemeAndAuthority(fullPartitionPath);

    String fullPartitionPathStr = fullPartitionPath.toString();
    String basePathString = basePath.toString();

    if (!fullPartitionPathStr.startsWith(basePathString)) {
      throw new IllegalArgumentException("Partition path \"" + fullPartitionPathStr
          + "\" does not belong to base-path \"" + basePath + "\"");
    }

    // Partition-Path could be empty for non-partitioned tables
    return fullPartitionPathStr.length() == basePathString.length() ? ""
        : fullPartitionPathStr.substring(basePathString.length() + 1);
  }

  public static StoragePath getPathWithoutSchemeAndAuthority(StoragePath path) {
    return path.getPathWithoutSchemeAndAuthority();
  }

  /**
   * Recursively processes all files in the base-path. If excludeMetaFolder is set, the meta-folder and all its subdirs
   * are skipped
   *
   * @param storage           File System
   * @param basePathStr       Base-Path
   * @param consumer          Callback for processing
   * @param excludeMetaFolder Exclude .hoodie folder
   * @throws IOException -
   */
  public static void processFiles(HoodieStorage storage, String basePathStr, Function<StoragePathInfo, Boolean> consumer,
                                  boolean excludeMetaFolder) throws IOException {
    StoragePathFilter pathFilter = excludeMetaFolder ? getExcludeMetaPathFilter() : ALLOW_ALL_FILTER;
    List<StoragePathInfo> topLevelInfoList = storage.listDirectEntries(new StoragePath(basePathStr));
    for (StoragePathInfo child : topLevelInfoList) {
      if (child.isFile()) {
        boolean success = consumer.apply(child);
        if (!success) {
          throw new HoodieException("Failed to process file-status=" + child);
        }
      } else if (pathFilter.accept(child.getPath())) {
        List<StoragePathInfo> list = storage.listFiles(child.getPath());
        for (StoragePathInfo pathInfo : list) {
          boolean success = consumer.apply(pathInfo);
          if (!success) {
            throw new HoodieException("Failed to process StoragePathInfo=" + pathInfo);
          }
        }
      }
    }
  }

  public static List<String> getAllPartitionPaths(HoodieEngineContext engineContext,
                                                  HoodieStorage storage,
                                                  String basePathStr,
                                                  boolean useFileListingFromMetadata) {
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(useFileListingFromMetadata)
        .build();
    try (HoodieTableMetadata tableMetadata = HoodieTableMetadata.create(engineContext, storage, metadataConfig, basePathStr)) {
      return tableMetadata.getAllPartitionPaths();
    } catch (Exception e) {
      throw new HoodieException("Error fetching partition paths from metadata table", e);
    }
  }

  public static List<String> getAllPartitionPaths(HoodieEngineContext engineContext,
                                                  HoodieStorage storage,
                                                  HoodieMetadataConfig metadataConfig,
                                                  String basePathStr) {
    try (HoodieTableMetadata tableMetadata = HoodieTableMetadata.create(engineContext, storage, metadataConfig,
        basePathStr)) {
      return tableMetadata.getAllPartitionPaths();
    } catch (Exception e) {
      throw new HoodieException("Error fetching partition paths from metadata table", e);
    }
  }

  public static List<String> getAllPartitionPaths(HoodieEngineContext engineContext,
                                                  HoodieStorage storage,
                                                  StoragePath basePath,
                                                  boolean useFileListingFromMetadata) {
    return getAllPartitionPaths(engineContext, storage, basePath.toString(), useFileListingFromMetadata);
  }

  public static List<String> getAllPartitionPaths(HoodieEngineContext engineContext,
                                                  HoodieStorage storage,
                                                  HoodieMetadataConfig metadataConfig,
                                                  StoragePath basePath) {
    return getAllPartitionPaths(engineContext, storage, metadataConfig, basePath.toString());
  }

  public static Map<String, List<StoragePathInfo>> getFilesInPartitions(HoodieEngineContext engineContext,
                                                                        HoodieStorage storage,
                                                                        HoodieMetadataConfig metadataConfig,
                                                                        String basePathStr,
                                                                        String[] partitionPaths) {
    try (HoodieTableMetadata tableMetadata = HoodieTableMetadata.create(engineContext, storage, metadataConfig,
        basePathStr)) {
      return tableMetadata.getAllFilesInPartitions(Arrays.asList(partitionPaths));
    } catch (Exception ex) {
      throw new HoodieException("Error get files in partitions: " + String.join(",", partitionPaths), ex);
    }
  }

  public static String getFileExtension(String fullName) {
    Objects.requireNonNull(fullName);
    String fileName = new File(fullName).getName();
    int dotIndex = fileName.lastIndexOf('.');
    return dotIndex == -1 ? "" : fileName.substring(dotIndex);
  }

  private static StoragePathFilter getExcludeMetaPathFilter() {
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
    // format: {idPrefix}-{id}
    return new StringBuilder()
        .append(idPfx)
        .append('-')
        .append(id)
        .toString();
  }

  /**
   * Get the file extension from the log file.
   */
  public static String getFileExtensionFromLog(StoragePath logPath) {
    Matcher matcher = LOG_FILE_PATTERN.matcher(logPath.getName());
    if (!matcher.find()) {
      throw new InvalidHoodiePathException(logPath.toString(), "LogFile");
    }
    return matcher.group(3);
  }

  public static String getFileIdFromFileName(String fileName) {
    if (FSUtils.isLogFile(fileName)) {
      Matcher matcher = LOG_FILE_PATTERN.matcher(fileName);
      if (!matcher.find()) {
        throw new InvalidHoodieFileNameException(fileName, "LogFile");
      }
      return matcher.group(1);
    }
    return FSUtils.getFileId(fileName);
  }

  public static String getFileIdFromLogPath(StoragePath path) {
    Matcher matcher = LOG_FILE_PATTERN.matcher(path.getName());
    if (!matcher.find()) {
      throw new InvalidHoodiePathException(path, "LogFile");
    }
    return matcher.group(1);
  }

  public static String getFileIdFromFilePath(StoragePath filePath) {
    if (FSUtils.isLogFile(filePath)) {
      return FSUtils.getFileIdFromLogPath(filePath);
    }
    return FSUtils.getFileId(filePath.getName());
  }

  /**
   * Get the second part of the file name in the log file. That will be the delta commit time.
   */
  public static String getDeltaCommitTimeFromLogPath(StoragePath path) {
    Matcher matcher = LOG_FILE_PATTERN.matcher(path.getName());
    if (!matcher.find()) {
      throw new InvalidHoodiePathException(path.toString(), "LogFile");
    }
    return matcher.group(2);
  }

  /**
   * Get TaskPartitionId used in log-path.
   */
  public static Integer getTaskPartitionIdFromLogPath(StoragePath path) {
    Matcher matcher = LOG_FILE_PATTERN.matcher(path.getName());
    if (!matcher.find()) {
      throw new InvalidHoodiePathException(path.toString(), "LogFile");
    }
    String val = matcher.group(7);
    return val == null ? null : Integer.parseInt(val);
  }

  /**
   * Get Write-Token used in log-path.
   */
  public static String getWriteTokenFromLogPath(StoragePath path) {
    Matcher matcher = LOG_FILE_PATTERN.matcher(path.getName());
    if (!matcher.find()) {
      throw new InvalidHoodiePathException(path.toString(), "LogFile");
    }
    return matcher.group(6);
  }

  /**
   * Get StageId used in log-path.
   */
  public static Integer getStageIdFromLogPath(StoragePath path) {
    Matcher matcher = LOG_FILE_PATTERN.matcher(path.getName());
    if (!matcher.find()) {
      throw new InvalidHoodiePathException(path.toString(), "LogFile");
    }
    String val = matcher.group(8);
    return val == null ? null : Integer.parseInt(val);
  }

  /**
   * Get Task Attempt Id used in log-path.
   */
  public static Integer getTaskAttemptIdFromLogPath(StoragePath path) {
    Matcher matcher = LOG_FILE_PATTERN.matcher(path.getName());
    if (!matcher.find()) {
      throw new InvalidHoodiePathException(path.toString(), "LogFile");
    }
    String val = matcher.group(9);
    return val == null ? null : Integer.parseInt(val);
  }

  /**
   * Get the last part of the file name in the log file and convert to int.
   */
  public static int getFileVersionFromLog(StoragePath logPath) {
    return getFileVersionFromLog(logPath.getName());
  }

  public static int getFileVersionFromLog(String logFileName) {
    Matcher matcher = LOG_FILE_PATTERN.matcher(logFileName);
    if (!matcher.find()) {
      throw new HoodieIOException("Invalid log file name: " + logFileName);
    }
    return Integer.parseInt(matcher.group(4));
  }

  public static String makeLogFileName(String fileId, String logFileExtension, String deltaCommitTime, int version,
      String writeToken) {
    String suffix = (writeToken == null)
        ? String.format("%s_%s%s.%d", fileId, deltaCommitTime, logFileExtension, version)
        : String.format("%s_%s%s.%d_%s", fileId, deltaCommitTime, logFileExtension, version, writeToken);
    return HoodieLogFile.LOG_FILE_PREFIX + suffix;
  }

  public static boolean isBaseFile(StoragePath path) {
    String extension = getFileExtension(path.getName());
    if (HoodieFileFormat.BASE_FILE_EXTENSIONS.contains(extension)) {
      return BASE_FILE_PATTERN.matcher(path.getName()).matches();
    }
    return false;
  }

  public static boolean isLogFile(StoragePath logPath) {
    String scheme = logPath.toUri().getScheme();
    return isLogFile(InLineFSUtils.SCHEME.equals(scheme)
        ? InLineFSUtils.getOuterFilePathFromInlinePath(logPath).getName() : logPath.getName());
  }

  public static boolean isLogFile(String fileName) {
    if (fileName.contains(LOG_FILE_EXTENSION)) {
      Matcher matcher = LOG_FILE_PATTERN.matcher(fileName);
      return matcher.find();
    }
    return false;
  }

  public static boolean isDataFile(StoragePath path) {
    return isBaseFile(path) || isLogFile(path);
  }

  public static List<StoragePathInfo> getAllDataFilesInPartition(HoodieStorage storage,
                                                                 StoragePath partitionPath)
      throws IOException {
    final Set<String> validFileExtensions = Arrays.stream(HoodieFileFormat.values())
        .map(HoodieFileFormat::getFileExtension).collect(Collectors.toCollection(HashSet::new));
    final String logFileExtension = HoodieFileFormat.HOODIE_LOG.getFileExtension();

    try {
      return storage.listDirectEntries(partitionPath, path -> {
        String extension = FSUtils.getFileExtension(path.getName());
        return validFileExtensions.contains(extension) || path.getName().contains(logFileExtension);
      }).stream().filter(StoragePathInfo::isFile).collect(Collectors.toList());
    } catch (FileNotFoundException ex) {
      // return empty FileStatus if partition does not exist already
      return Collections.emptyList();
    }
  }

  /**
   * Get the latest log file for the passed in file-id in the partition path
   */
  public static Option<HoodieLogFile> getLatestLogFile(HoodieStorage storage, StoragePath partitionPath, String fileId,
                                                       String logFileExtension, String deltaCommitTime) throws IOException {
    return getLatestLogFile(getAllLogFiles(storage, partitionPath, fileId, logFileExtension, deltaCommitTime));
  }

  /**
   * Get all the log files for the passed in file-id in the partition path.
   */
  public static Stream<HoodieLogFile> getAllLogFiles(HoodieStorage storage, StoragePath partitionPath, final String fileId,
                                                     final String logFileExtension, final String deltaCommitTime) throws IOException {
    try {
      StoragePathFilter pathFilter = path -> path.getName().startsWith("." + fileId) && path.getName().contains(logFileExtension);
      return storage.listDirectEntries(partitionPath, pathFilter).stream()
          .map(HoodieLogFile::new)
          .filter(s -> s.getDeltaCommitTime().equals(deltaCommitTime));
    } catch (FileNotFoundException e) {
      return Stream.of();
    }
  }

  /**
   * Get the latest log version for the fileId in the partition path.
   */
  public static Option<Pair<Integer, String>> getLatestLogVersion(HoodieStorage storage, StoragePath partitionPath,
                                                                  final String fileId, final String logFileExtension, final String deltaCommitTime) throws IOException {
    Option<HoodieLogFile> latestLogFile =
        getLatestLogFile(getAllLogFiles(storage, partitionPath, fileId, logFileExtension, deltaCommitTime));
    if (latestLogFile.isPresent()) {
      return Option
          .of(Pair.of(latestLogFile.get().getLogVersion(), latestLogFile.get().getLogWriteToken()));
    }
    return Option.empty();
  }

  public static void createPathIfNotExists(HoodieStorage storage, StoragePath partitionPath)
      throws IOException {
    if (!storage.exists(partitionPath)) {
      storage.createDirectory(partitionPath);
    }
  }

  public static Long getSizeInMB(long sizeInBytes) {
    return sizeInBytes / (1024 * 1024);
  }

  public static StoragePath constructAbsolutePath(String basePath, String relativePartitionPath) {
    if (StringUtils.isNullOrEmpty(relativePartitionPath)) {
      return new StoragePath(basePath);
    }

    // NOTE: We have to chop leading "/" to make sure Hadoop does not treat it like
    //       absolute path
    String properPartitionPath = relativePartitionPath.startsWith(PATH_SEPARATOR)
        ? relativePartitionPath.substring(1)
        : relativePartitionPath;
    return constructAbsolutePath(new StoragePath(basePath), properPartitionPath);
  }

  public static StoragePath constructAbsolutePath(StoragePath basePath, String relativePartitionPath) {
    // For non-partitioned table, return only base-path
    return StringUtils.isNullOrEmpty(relativePartitionPath) ? basePath : new StoragePath(basePath, relativePartitionPath);
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
   * Helper to filter out paths under metadata folder when running fs.globStatus.
   *
   * @param storage  {@link HoodieStorage} instance.
   * @param globPath Glob Path
   * @return the file status list of globPath exclude the meta folder
   * @throws IOException when having trouble listing the path
   */
  public static List<StoragePathInfo> getGlobStatusExcludingMetaFolder(HoodieStorage storage,
                                                                       StoragePath globPath)
      throws IOException {
    List<StoragePathInfo> statuses = storage.globEntries(globPath);
    return statuses.stream()
        .filter(fileStatus -> !fileStatus.getPath().toString()
            .contains(HoodieTableMetaClient.METAFOLDER_NAME))
        .collect(Collectors.toList());
  }

  /**
   * Deletes a directory by deleting sub-paths in parallel on the file system.
   *
   * @param hoodieEngineContext {@code HoodieEngineContext} instance
   * @param storage             {@link HoodieStorage} instance.
   * @param dirPath             directory path.
   * @param parallelism         parallelism to use for sub-paths
   * @return {@code true} if the directory is delete; {@code false} otherwise.
   */
  public static boolean deleteDir(
      HoodieEngineContext hoodieEngineContext, HoodieStorage storage, StoragePath dirPath, int parallelism) {
    try {
      if (storage.exists(dirPath)) {
        FSUtils.parallelizeSubPathProcess(hoodieEngineContext, storage, dirPath, parallelism, e -> true,
            pairOfSubPathAndConf -> deleteSubPath(
                pairOfSubPathAndConf.getKey(), pairOfSubPathAndConf.getValue(), true)
        );
        boolean result = storage.deleteDirectory(dirPath);
        LOG.info("Removed directory at {}", dirPath);
        return result;
      }
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
    return false;
  }

  public static HoodiePath fromStoragePath(StoragePath path) {
    if (null == path) {
      return null;
    }
    return HoodiePath.newBuilder().setUri(path.toString()).build();
  }

  public static HoodieFileStatus fromPathInfo(StoragePathInfo pathInfo) {
    if (null == pathInfo) {
      return null;
    }

    HoodieFileStatus fStatus = new HoodieFileStatus();

    fStatus.setPath(fromStoragePath(pathInfo.getPath()));
    fStatus.setLength(pathInfo.getLength());
    fStatus.setIsDir(pathInfo.isDirectory());
    fStatus.setBlockReplication((int) pathInfo.getBlockReplication());
    fStatus.setBlockSize(pathInfo.getBlockSize());
    fStatus.setModificationTime(pathInfo.getModificationTime());
    fStatus.setAccessTime(pathInfo.getModificationTime());

    return fStatus;
  }

  /**
   * Processes sub-path in parallel.
   *
   * @param hoodieEngineContext {@link HoodieEngineContext} instance
   * @param storage             {@link HoodieStorage} instance
   * @param dirPath             directory path
   * @param parallelism         parallelism to use for sub-paths
   * @param subPathPredicate    predicate to use to filter sub-paths for processing
   * @param pairFunction        actual processing logic for each sub-path
   * @param <T>                 type of result to return for each sub-path
   * @return a map of sub-path to result of the processing
   */
  public static <T> Map<String, T> parallelizeSubPathProcess(
      HoodieEngineContext hoodieEngineContext, HoodieStorage storage, StoragePath dirPath, int parallelism,
      Predicate<StoragePathInfo> subPathPredicate, SerializableFunction<Pair<String, StorageConfiguration<?>>, T> pairFunction) {
    try {
      List<StoragePathInfo> pathInfoList = storage.listDirectEntries(dirPath);
      List<String> subPaths = pathInfoList.stream()
          .filter(subPathPredicate)
          .map(fileStatus -> fileStatus.getPath().toString())
          .collect(Collectors.toList());
      return parallelizeFilesProcess(hoodieEngineContext, storage, parallelism, pairFunction, subPaths);
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  public static <T> Map<String, T> parallelizeFilesProcess(
      HoodieEngineContext hoodieEngineContext,
      HoodieStorage storage,
      int parallelism,
      SerializableFunction<Pair<String, StorageConfiguration<?>>, T> pairFunction,
      List<String> subPaths) {
    Map<String, T> result = new HashMap<>();
    if (subPaths.size() > 0) {
      StorageConfiguration<?> storageConf = storage.getConf();
      int actualParallelism = Math.min(subPaths.size(), parallelism);

      hoodieEngineContext.setJobStatus(FSUtils.class.getSimpleName(),
          "Parallel listing paths " + String.join(",", subPaths));

      result = hoodieEngineContext.mapToPair(subPaths,
          subPath -> new ImmutablePair<>(subPath, pairFunction.apply(new ImmutablePair<>(subPath, storageConf))),
          actualParallelism);
    }
    return result;
  }

  /**
   * Deletes a sub-path.
   *
   * @param subPathStr sub-path String
   * @param conf       storage config
   * @param recursive  is recursive or not
   * @return {@code true} if the sub-path is deleted; {@code false} otherwise.
   */
  public static boolean deleteSubPath(String subPathStr, StorageConfiguration<?> conf, boolean recursive) {
    try {
      StoragePath subPath = new StoragePath(subPathStr);
      HoodieStorage storage = HoodieStorageUtils.getStorage(subPath, conf);
      if (recursive) {
        return storage.deleteDirectory(subPath);
      }
      return storage.deleteFile(subPath);
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }

  public static List<StoragePathInfo> getAllDataPathInfo(HoodieStorage storage, StoragePath path)
      throws IOException {
    List<StoragePathInfo> pathInfoList = new ArrayList<>();
    for (StoragePathInfo pathInfo : storage.listDirectEntries(path)) {
      if (!pathInfo.getPath().toString().contains(HoodieTableMetaClient.METAFOLDER_NAME)) {
        if (pathInfo.isDirectory()) {
          pathInfoList.addAll(getAllDataPathInfo(storage, pathInfo.getPath()));
        } else {
          pathInfoList.add(pathInfo);
        }
      }
    }
    return pathInfoList;
  }

  public static boolean comparePathsWithoutScheme(String pathStr1, String pathStr2) {
    StoragePath pathWithoutScheme1 = getPathWithoutScheme(new StoragePath(pathStr1));
    StoragePath pathWithoutScheme2 = getPathWithoutScheme(new StoragePath(pathStr2));
    return pathWithoutScheme1.equals(pathWithoutScheme2);
  }

  public static StoragePath getPathWithoutScheme(StoragePath path) {
    return path.isAbsolute()
        ? new StoragePath(path.toUri().getRawSchemeSpecificPart()) : path;
  }

  // Converts s3a to s3a
  public static String s3aToS3(String s3aUrl) {
    return s3aUrl.replaceFirst("(?i)^s3a://", "s3://");
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
}
