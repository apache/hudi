/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
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

package com.uber.hoodie.common.util;

import com.google.common.base.Preconditions;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.model.HoodiePartitionMetadata;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.exception.InvalidHoodiePathException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Utility functions related to accessing the file storage
 */
public class FSUtils {

  private static final Logger LOG = LogManager.getLogger(FSUtils.class);
  // Log files are of this pattern - .b5068208-e1a4-11e6-bf01-fe55135034f3_20170101134598.log.1
  private static final Pattern LOG_FILE_PATTERN = Pattern.compile("\\.(.*)_(.*)\\.(.*)\\.([0-9]*)");
  private static final String LOG_FILE_PREFIX = ".";
  private static final int MAX_ATTEMPTS_RECOVER_LEASE = 10;
  private static final long MIN_CLEAN_TO_KEEP = 10;
  private static final long MIN_ROLLBACK_TO_KEEP = 10;
  private static final String HOODIE_ENV_PROPS_PREFIX = "HOODIE_ENV_";

  public static Configuration prepareHadoopConf(Configuration conf) {
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

    // look for all properties, prefixed to be picked up
    for (Entry<String, String> prop : System.getenv().entrySet()) {
      if (prop.getKey().startsWith(HOODIE_ENV_PROPS_PREFIX)) {
        LOG.info("Picking up value for hoodie env var :" + prop.getKey());
        conf.set(prop.getKey()
                .replace(HOODIE_ENV_PROPS_PREFIX, "")
                .replaceAll("_DOT_", "."),
            prop.getValue());
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
      throw new HoodieIOException("Failed to get instance of " + FileSystem.class.getName(),
          e);
    }
    LOG.info(
        String.format("Hadoop Configuration: fs.defaultFS: [%s], Config:[%s], FileSystem: [%s]",
            conf.getRaw("fs.defaultFS"), conf.toString(), fs.toString()));
    return fs;
  }

  public static String makeDataFileName(String commitTime, int taskPartitionId, String fileId) {
    return String.format("%s_%d_%s.parquet", fileId, taskPartitionId, commitTime);
  }

  public static String makeTempDataFileName(String partitionPath, String commitTime,
      int taskPartitionId, String fileId, int stageId, long taskAttemptId) {
    return String.format("%s_%s_%d_%s_%d_%d.parquet", partitionPath.replace("/", "-"), fileId,
        taskPartitionId, commitTime, stageId, taskAttemptId);
  }

  public static String maskWithoutFileId(String commitTime, int taskPartitionId) {
    return String.format("*_%s_%s.parquet", taskPartitionId, commitTime);
  }

  public static String maskWithoutTaskPartitionId(String commitTime, String fileId) {
    return String.format("%s_*_%s.parquet", fileId, commitTime);
  }

  public static String maskWithOnlyCommitTime(String commitTime) {
    return String.format("*_*_%s.parquet", commitTime);
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
  public static List<String> getAllFoldersThreeLevelsDown(FileSystem fs, String basePath)
      throws IOException {
    List<String> datePartitions = new ArrayList<>();
    FileStatus[] folders = fs.globStatus(new Path(basePath + "/*/*/*"));
    for (FileStatus status : folders) {
      Path path = status.getPath();
      datePartitions.add(String.format("%s/%s/%s", path.getParent().getParent().getName(),
          path.getParent().getName(), path.getName()));
    }
    return datePartitions;
  }

  public static String getRelativePartitionPath(Path basePath, Path partitionPath) {
    String partitionFullPath = partitionPath.toString();
    int partitionStartIndex = partitionFullPath.lastIndexOf(basePath.getName());
    return partitionFullPath.substring(partitionStartIndex + basePath.getName().length() + 1);
  }

  /**
   * Obtain all the partition paths, that are present in this table, denoted by presence of {@link
   * com.uber.hoodie.common.model.HoodiePartitionMetadata#HOODIE_PARTITION_METAFILE}
   */
  public static List<String> getAllFoldersWithPartitionMetaFile(FileSystem fs, String basePathStr)
      throws IOException {
    List<String> partitions = new ArrayList<>();
    Path basePath = new Path(basePathStr);
    RemoteIterator<LocatedFileStatus> allFiles = fs.listFiles(new Path(basePathStr), true);
    while (allFiles.hasNext()) {
      Path filePath = allFiles.next().getPath();
      if (filePath.getName().equals(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE)) {
        partitions.add(getRelativePartitionPath(basePath, filePath.getParent()));
      }
    }
    return partitions;
  }

  public static List<String> getAllPartitionPaths(FileSystem fs, String basePathStr,
      boolean assumeDatePartitioning)
      throws IOException {
    if (assumeDatePartitioning) {
      return getAllFoldersThreeLevelsDown(fs, basePathStr);
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

  public static String getInstantTime(String name) {
    return name.replace(getFileExtension(name), "");
  }


  /**
   * Get the file extension from the log file
   */
  public static String getFileExtensionFromLog(Path logPath) {
    Matcher matcher = LOG_FILE_PATTERN.matcher(logPath.getName());
    if (!matcher.find()) {
      throw new InvalidHoodiePathException(logPath, "LogFile");
    }
    return matcher.group(3);
  }

  /**
   * Get the first part of the file name in the log file. That will be the fileId. Log file do not
   * have commitTime in the file name.
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
   * Get the first part of the file name in the log file. That will be the fileId. Log file do not
   * have commitTime in the file name.
   */
  public static String getBaseCommitTimeFromLogPath(Path path) {
    Matcher matcher = LOG_FILE_PATTERN.matcher(path.getName());
    if (!matcher.find()) {
      throw new InvalidHoodiePathException(path, "LogFile");
    }
    return matcher.group(2);
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

  /**
   * Get the last part of the file name in the log file and convert to int.
   */
  public static String getPartitionPathFromFilePath(Path filePath) {
    String [] pathElements = filePath.toUri().toString().split("/");
    return String.format("%s/%s/%s", pathElements[pathElements.length - 4], pathElements[pathElements.length - 3],
        pathElements[pathElements.length - 2]);
  }

  public static String makeLogFileName(String fileId, String logFileExtension,
      String baseCommitTime, int version) {
    return LOG_FILE_PREFIX + String
        .format("%s_%s%s.%d", fileId, baseCommitTime, logFileExtension, version);
  }

  public static String maskWithoutLogVersion(String commitTime, String fileId,
      String logFileExtension) {
    return LOG_FILE_PREFIX + String.format("%s_%s%s*", fileId, commitTime, logFileExtension);
  }

  public static boolean isLogFile(Path logPath) {
    Matcher matcher = LOG_FILE_PATTERN.matcher(logPath.getName());
    if (!matcher.find()) {
      return false;
    }
    return true;
  }

  /**
   * Get the latest log file written from the list of log files passed in
   */
  public static Optional<HoodieLogFile> getLatestLogFile(Stream<HoodieLogFile> logFiles) {
    return logFiles.sorted(Comparator
        .comparing(s -> s.getLogVersion(),
            Comparator.reverseOrder())).findFirst();
  }

  /**
   * Get all the log files for the passed in FileId in the partition path
   */
  public static Stream<HoodieLogFile> getAllLogFiles(FileSystem fs, Path partitionPath,
      final String fileId, final String logFileExtension, final String baseCommitTime)
      throws IOException {
    return Arrays.stream(fs.listStatus(partitionPath,
        path -> path.getName().startsWith("." + fileId) && path.getName()
            .contains(logFileExtension)))
        .map(HoodieLogFile::new).filter(s -> s.getBaseCommitTime().equals(baseCommitTime));
  }

  /**
   * Get the latest log version for the fileId in the partition path
   */
  public static Optional<Integer> getLatestLogVersion(FileSystem fs, Path partitionPath,
      final String fileId, final String logFileExtension, final String baseCommitTime)
      throws IOException {
    Optional<HoodieLogFile> latestLogFile =
        getLatestLogFile(
            getAllLogFiles(fs, partitionPath, fileId, logFileExtension, baseCommitTime));
    if (latestLogFile.isPresent()) {
      return Optional.of(latestLogFile.get().getLogVersion());
    }
    return Optional.empty();
  }

  public static int getCurrentLogVersion(FileSystem fs, Path partitionPath,
      final String fileId, final String logFileExtension, final String baseCommitTime)
      throws IOException {
    Optional<Integer> currentVersion =
        getLatestLogVersion(fs, partitionPath, fileId, logFileExtension, baseCommitTime);
    // handle potential overflow
    return (currentVersion.isPresent()) ? currentVersion.get() : HoodieLogFile.LOGFILE_BASE_VERSION;
  }

  /**
   * computes the next log version for the specified fileId in the partition path
   */
  public static int computeNextLogVersion(FileSystem fs, Path partitionPath, final String fileId,
      final String logFileExtension, final String baseCommitTime) throws IOException {
    Optional<Integer> currentVersion =
        getLatestLogVersion(fs, partitionPath, fileId, logFileExtension, baseCommitTime);
    // handle potential overflow
    return (currentVersion.isPresent()) ? currentVersion.get() + 1
        : HoodieLogFile.LOGFILE_BASE_VERSION;
  }

  public static int getDefaultBufferSize(final FileSystem fs) {
    return fs.getConf().getInt("io.file.buffer.size", 4096);
  }

  public static Short getDefaultReplication(FileSystem fs, Path path) {
    return fs.getDefaultReplication(path);
  }

  public static Long getDefaultBlockSize(FileSystem fs, Path path) {
    return fs.getDefaultBlockSize(path);
  }

  /**
   * When a file was opened and the task died without closing the stream, another task executor
   * cannot open because the existing lease will be active. We will try to recover the lease, from
   * HDFS. If a data node went down, it takes about 10 minutes for the lease to be rocovered. But if
   * the client dies, this should be instant.
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

  public static void deleteOlderCleanMetaFiles(FileSystem fs, String metaPath,
      Stream<HoodieInstant> instants) {
    //TODO - this should be archived when archival is made general for all meta-data
    // skip MIN_CLEAN_TO_KEEP and delete rest
    instants.skip(MIN_CLEAN_TO_KEEP).map(s -> {
      try {
        return fs.delete(new Path(metaPath, s.getFileName()), false);
      } catch (IOException e) {
        throw new HoodieIOException("Could not delete clean meta files" + s.getFileName(),
            e);
      }
    });
  }

  public static void deleteOlderRollbackMetaFiles(FileSystem fs, String metaPath,
      Stream<HoodieInstant> instants) {
    //TODO - this should be archived when archival is made general for all meta-data
    // skip MIN_ROLLBACK_TO_KEEP and delete rest
    instants.skip(MIN_ROLLBACK_TO_KEEP).map(s -> {
      try {
        return fs.delete(new Path(metaPath, s.getFileName()), false);
      } catch (IOException e) {
        throw new HoodieIOException(
            "Could not delete rollback meta files " + s.getFileName(), e);
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
}
