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

package org.apache.hudi.common.util;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.InvalidHoodiePathException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Utility functions related to accessing the file storage.
 */
public class FSUtils {

  private static final Logger LOG = LoggerFactory.getLogger(FSUtils.class);
  // Log files are of this pattern - .b5068208-e1a4-11e6-bf01-fe55135034f3_20170101134598.log.1
  private static final Pattern LOG_FILE_PATTERN =
      Pattern.compile("\\.(.*)_(.*)\\.(.*)\\.([0-9]*)(_(([0-9]*)-([0-9]*)-([0-9]*)))?");
  private static final String LOG_FILE_PREFIX = ".";
  private static final int MAX_ATTEMPTS_RECOVER_LEASE = 10;
  private static final long MIN_CLEAN_TO_KEEP = 10;
  private static final long MIN_ROLLBACK_TO_KEEP = 10;
  private static final String HOODIE_ENV_PROPS_PREFIX = "HOODIE_ENV_";

  private static final PathFilter ALLOW_ALL_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path file) {
      return true;
    }
  };

  public static Configuration prepareHadoopConf(Configuration conf) {
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

    // look for all properties, prefixed to be picked up
    for (Entry<String, String> prop : System.getenv().entrySet()) {
      if (prop.getKey().startsWith(HOODIE_ENV_PROPS_PREFIX)) {
        LOG.info("Picking up value for hoodie env var :{}", prop.getKey());
        conf.set(prop.getKey().replace(HOODIE_ENV_PROPS_PREFIX, "").replaceAll("_DOT_", "."), prop.getValue());
      }
    }
    return conf;
  }

  public static FileSystem getFs(String path, Configuration conf) {
    FileSystem fs;
    conf = prepareHadoopConf(conf);
    try {
      fs = new Path(path).getFileSystem(conf);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to get instance of " + FileSystem.class.getName(), e);
    }
    LOG.info(String.format("Hadoop Configuration: fs.defaultFS: [%s], Config:[%s], FileSystem: [%s]",
        conf.getRaw("fs.defaultFS"), conf.toString(), fs.toString()));
    return fs;
  }

  /**
   * A write token uniquely identifies an attempt at one of the IOHandle operations (Merge/Create/Append).
   */
  public static String makeWriteToken(int taskPartitionId, int stageId, long taskAttemptId) {
    return String.format("%d-%d-%d", taskPartitionId, stageId, taskAttemptId);
  }

  public static String makeDataFileName(String commitTime, String writeToken, String fileId) {
    return String.format("%s_%s_%s.parquet", fileId, writeToken, commitTime);
  }

  public static String makeMarkerFile(String commitTime, String writeToken, String fileId) {
    return String.format("%s_%s_%s%s", fileId, writeToken, commitTime, HoodieTableMetaClient.MARKER_EXTN);
  }

  public static String translateMarkerToDataPath(String basePath, String markerPath, String instantTs) {
    Preconditions.checkArgument(markerPath.endsWith(HoodieTableMetaClient.MARKER_EXTN));
    String markerRootPath = Path.getPathWithoutSchemeAndAuthority(
        new Path(String.format("%s/%s/%s", basePath, HoodieTableMetaClient.TEMPFOLDER_NAME, instantTs))).toString();
    int begin = markerPath.indexOf(markerRootPath);
    Preconditions.checkArgument(begin >= 0,
        "Not in marker dir. Marker Path=" + markerPath + ", Expected Marker Root=" + markerRootPath);
    String rPath = markerPath.substring(begin + markerRootPath.length() + 1);
    return String.format("%s/%s%s", basePath, rPath.replace(HoodieTableMetaClient.MARKER_EXTN, ""),
        HoodieFileFormat.PARQUET.getFileExtension());
  }

  public static String maskWithoutFileId(String commitTime, int taskPartitionId) {
    return String.format("*_%s_%s%s", taskPartitionId, commitTime, HoodieFileFormat.PARQUET.getFileExtension());
  }

  public static String getCommitFromCommitFile(String commitFileName) {
    return commitFileName.split("\\.")[0];
  }

  public static String getCommitTime(String fullFileName) {
    return fullFileName.split("_")[2].split("\\.")[0];
  }

  public static long getFileSize(FileSystem fs, Path path) throws IOException {
    return fs.getFileStatus(path).getLen();
  }

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
   */
  public static String getRelativePartitionPath(Path basePath, Path partitionPath) {
    basePath = Path.getPathWithoutSchemeAndAuthority(basePath);
    partitionPath = Path.getPathWithoutSchemeAndAuthority(partitionPath);
    String partitionFullPath = partitionPath.toString();
    int partitionStartIndex = partitionFullPath.indexOf(basePath.getName(),
        basePath.getParent() == null ? 0 : basePath.getParent().toString().length());
    // Partition-Path could be empty for non-partitioned tables
    return partitionStartIndex + basePath.getName().length() == partitionFullPath.length() ? ""
        : partitionFullPath.substring(partitionStartIndex + basePath.getName().length() + 1);
  }

  /**
   * Obtain all the partition paths, that are present in this table, denoted by presence of
   * {@link HoodiePartitionMetadata#HOODIE_PARTITION_METAFILE}.
   */
  public static List<String> getAllFoldersWithPartitionMetaFile(FileSystem fs, String basePathStr) throws IOException {
    final Path basePath = new Path(basePathStr);
    final List<String> partitions = new ArrayList<>();
    processFiles(fs, basePathStr, (locatedFileStatus) -> {
      Path filePath = locatedFileStatus.getPath();
      if (filePath.getName().equals(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE)) {
        partitions.add(getRelativePartitionPath(basePath, filePath.getParent()));
      }
      return true;
    }, true);
    return partitions;
  }

  public static final List<String> getAllDataFilesForMarkers(FileSystem fs, String basePath, String instantTs,
      String markerDir) throws IOException {
    List<String> dataFiles = new LinkedList<>();
    processFiles(fs, markerDir, (status) -> {
      String pathStr = status.getPath().toString();
      if (pathStr.endsWith(HoodieTableMetaClient.MARKER_EXTN)) {
        dataFiles.add(FSUtils.translateMarkerToDataPath(basePath, pathStr, instantTs));
      }
      return true;
    }, false);
    return dataFiles;
  }

  /**
   * Recursively processes all files in the base-path. If excludeMetaFolder is set, the meta-folder and all its subdirs
   * are skipped
   * 
   * @param fs File System
   * @param basePathStr Base-Path
   * @param consumer Callback for processing
   * @param excludeMetaFolder Exclude .hoodie folder
   * @throws IOException
   */
  @VisibleForTesting
  static void processFiles(FileSystem fs, String basePathStr, Function<FileStatus, Boolean> consumer,
      boolean excludeMetaFolder) throws IOException {
    PathFilter pathFilter = excludeMetaFolder ? getExcludeMetaPathFilter() : ALLOW_ALL_FILTER;
    FileStatus[] topLevelStatuses = fs.listStatus(new Path(basePathStr));
    for (int i = 0; i < topLevelStatuses.length; i++) {
      FileStatus child = topLevelStatuses[i];
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

  public static List<String> getAllPartitionPaths(FileSystem fs, String basePathStr, boolean assumeDatePartitioning)
      throws IOException {
    if (assumeDatePartitioning) {
      return getAllPartitionFoldersThreeLevelsDown(fs, basePathStr);
    } else {
      return getAllFoldersWithPartitionMetaFile(fs, basePathStr);
    }
  }

  public static String getFileExtension(String fullName) {
    Preconditions.checkNotNull(fullName);
    String fileName = (new File(fullName)).getName();
    int dotIndex = fileName.indexOf('.');
    return dotIndex == -1 ? "" : fileName.substring(dotIndex);
  }

  private static PathFilter getExcludeMetaPathFilter() {
    // Avoid listing and including any folders under the metafolder
    return (path) -> {
      if (path.toString().contains(HoodieTableMetaClient.METAFOLDER_NAME)) {
        return false;
      }
      return true;
    };
  }

  public static String getInstantTime(String name) {
    return name.replace(getFileExtension(name), "");
  }

  /**
   * Returns a new unique prefix for creating a file group.
   */
  public static String createNewFileIdPfx() {
    return UUID.randomUUID().toString();
  }

  /**
   * Get the file extension from the log file.
   */
  public static String getFileExtensionFromLog(Path logPath) {
    Matcher matcher = LOG_FILE_PATTERN.matcher(logPath.getName());
    if (!matcher.find()) {
      throw new InvalidHoodiePathException(logPath, "LogFile");
    }
    return matcher.group(3);
  }

  /**
   * Get the first part of the file name in the log file. That will be the fileId. Log file do not have commitTime in
   * the file name.
   */
  public static String getFileIdFromLogPath(Path path) {
    Matcher matcher = LOG_FILE_PATTERN.matcher(path.getName());
    if (!matcher.find()) {
      throw new InvalidHoodiePathException(path, "LogFile");
    }
    return matcher.group(1);
  }

  /**
   * Check if the file is a parquet file of a log file. Then get the fileId appropriately.
   */
  public static String getFileIdFromFilePath(Path filePath) {
    if (FSUtils.isLogFile(filePath)) {
      return FSUtils.getFileIdFromLogPath(filePath);
    }
    return FSUtils.getFileId(filePath.getName());
  }

  /**
   * Get the first part of the file name in the log file. That will be the fileId. Log file do not have commitTime in
   * the file name.
   */
  public static String getBaseCommitTimeFromLogPath(Path path) {
    Matcher matcher = LOG_FILE_PATTERN.matcher(path.getName());
    if (!matcher.find()) {
      throw new InvalidHoodiePathException(path, "LogFile");
    }
    return matcher.group(2);
  }

  /**
   * Get TaskPartitionId used in log-path.
   */
  public static Integer getTaskPartitionIdFromLogPath(Path path) {
    Matcher matcher = LOG_FILE_PATTERN.matcher(path.getName());
    if (!matcher.find()) {
      throw new InvalidHoodiePathException(path, "LogFile");
    }
    String val = matcher.group(7);
    return val == null ? null : Integer.parseInt(val);
  }

  /**
   * Get Write-Token used in log-path.
   */
  public static String getWriteTokenFromLogPath(Path path) {
    Matcher matcher = LOG_FILE_PATTERN.matcher(path.getName());
    if (!matcher.find()) {
      throw new InvalidHoodiePathException(path, "LogFile");
    }
    return matcher.group(6);
  }

  /**
   * Get StageId used in log-path.
   */
  public static Integer getStageIdFromLogPath(Path path) {
    Matcher matcher = LOG_FILE_PATTERN.matcher(path.getName());
    if (!matcher.find()) {
      throw new InvalidHoodiePathException(path, "LogFile");
    }
    String val = matcher.group(8);
    return val == null ? null : Integer.parseInt(val);
  }

  /**
   * Get Task Attempt Id used in log-path.
   */
  public static Integer getTaskAttemptIdFromLogPath(Path path) {
    Matcher matcher = LOG_FILE_PATTERN.matcher(path.getName());
    if (!matcher.find()) {
      throw new InvalidHoodiePathException(path, "LogFile");
    }
    String val = matcher.group(9);
    return val == null ? null : Integer.parseInt(val);
  }

  /**
   * Get the last part of the file name in the log file and convert to int.
   */
  public static int getFileVersionFromLog(Path logPath) {
    Matcher matcher = LOG_FILE_PATTERN.matcher(logPath.getName());
    if (!matcher.find()) {
      throw new InvalidHoodiePathException(logPath, "LogFile");
    }
    return Integer.parseInt(matcher.group(4));
  }

  public static String makeLogFileName(String fileId, String logFileExtension, String baseCommitTime, int version,
      String writeToken) {
    String suffix =
        (writeToken == null) ? String.format("%s_%s%s.%d", fileId, baseCommitTime, logFileExtension, version)
            : String.format("%s_%s%s.%d_%s", fileId, baseCommitTime, logFileExtension, version, writeToken);
    return LOG_FILE_PREFIX + suffix;
  }

  public static boolean isLogFile(Path logPath) {
    Matcher matcher = LOG_FILE_PATTERN.matcher(logPath.getName());
    if (!matcher.find()) {
      return false;
    }
    return true;
  }

  /**
   * Get the latest log file written from the list of log files passed in.
   */
  public static Option<HoodieLogFile> getLatestLogFile(Stream<HoodieLogFile> logFiles) {
    return Option.fromJavaOptional(logFiles.sorted(HoodieLogFile.getReverseLogFileComparator()).findFirst());
  }

  /**
   * Get all the log files for the passed in FileId in the partition path.
   */
  public static Stream<HoodieLogFile> getAllLogFiles(FileSystem fs, Path partitionPath, final String fileId,
      final String logFileExtension, final String baseCommitTime) throws IOException {
    return Arrays
        .stream(fs.listStatus(partitionPath,
            path -> path.getName().startsWith("." + fileId) && path.getName().contains(logFileExtension)))
        .map(HoodieLogFile::new).filter(s -> s.getBaseCommitTime().equals(baseCommitTime));
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
   * about 10 minutes for the lease to be rocovered. But if the client dies, this should be instant.
   */
  public static boolean recoverDFSFileLease(final DistributedFileSystem dfs, final Path p)
      throws IOException, InterruptedException {
    LOG.info("Recover lease on dfs file {}", p);
    // initiate the recovery
    boolean recovered = false;
    for (int nbAttempt = 0; nbAttempt < MAX_ATTEMPTS_RECOVER_LEASE; nbAttempt++) {
      LOG.info("Attempt {} to recover lease on dfs file {}", nbAttempt, p);
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

  public static void deleteOlderCleanMetaFiles(FileSystem fs, String metaPath, Stream<HoodieInstant> instants) {
    // TODO - this should be archived when archival is made general for all meta-data
    // skip MIN_CLEAN_TO_KEEP and delete rest
    instants.skip(MIN_CLEAN_TO_KEEP).map(s -> {
      try {
        return fs.delete(new Path(metaPath, s.getFileName()), false);
      } catch (IOException e) {
        throw new HoodieIOException("Could not delete clean meta files" + s.getFileName(), e);
      }
    });
  }

  public static void deleteOlderRollbackMetaFiles(FileSystem fs, String metaPath, Stream<HoodieInstant> instants) {
    // TODO - this should be archived when archival is made general for all meta-data
    // skip MIN_ROLLBACK_TO_KEEP and delete rest
    instants.skip(MIN_ROLLBACK_TO_KEEP).map(s -> {
      try {
        return fs.delete(new Path(metaPath, s.getFileName()), false);
      } catch (IOException e) {
        throw new HoodieIOException("Could not delete rollback meta files " + s.getFileName(), e);
      }
    });
  }

  public static void deleteOlderRestoreMetaFiles(FileSystem fs, String metaPath, Stream<HoodieInstant> instants) {
    // TODO - this should be archived when archival is made general for all meta-data
    // skip MIN_ROLLBACK_TO_KEEP and delete rest
    instants.skip(MIN_ROLLBACK_TO_KEEP).map(s -> {
      try {
        return fs.delete(new Path(metaPath, s.getFileName()), false);
      } catch (IOException e) {
        throw new HoodieIOException("Could not delete restore meta files " + s.getFileName(), e);
      }
    });
  }

  public static void createPathIfNotExists(FileSystem fs, Path partitionPath) throws IOException {
    if (!fs.exists(partitionPath)) {
      fs.mkdirs(partitionPath);
    }
  }

  public static Long getSizeInMB(long sizeInBytes) {
    return sizeInBytes / (1024 * 1024);
  }

  public static Path getPartitionPath(String basePath, String partitionPath) {
    return getPartitionPath(new Path(basePath), partitionPath);
  }

  public static Path getPartitionPath(Path basePath, String partitionPath) {
    // FOr non-partitioned table, return only base-path
    return ((partitionPath == null) || (partitionPath.isEmpty())) ? basePath : new Path(basePath, partitionPath);
  }

  /**
   * Get DFS full partition path (e.g. hdfs://ip-address:8020:/<absolute path>)
   */
  public static String getDFSFullPartitionPath(FileSystem fs, Path partitionPath) {
    return fs.getUri() + partitionPath.toUri().getRawPath();
  }

  /**
   * This is due to HUDI-140 GCS has a different behavior for detecting EOF during seek().
   * 
   * @param inputStream FSDataInputStream
   * @return true if the inputstream or the wrapped one is of type GoogleHadoopFSInputStream
   */
  public static boolean isGCSInputStream(FSDataInputStream inputStream) {
    return inputStream.getClass().getCanonicalName().equals("com.google.cloud.hadoop.fs.gcs.GoogleHadoopFSInputStream")
        || inputStream.getWrappedStream().getClass().getCanonicalName()
            .equals("com.google.cloud.hadoop.fs.gcs.GoogleHadoopFSInputStream");
  }
}
