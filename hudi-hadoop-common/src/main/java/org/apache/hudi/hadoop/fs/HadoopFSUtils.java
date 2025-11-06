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

package org.apache.hudi.hadoop.fs;

import org.apache.hudi.avro.model.HoodieFSPermission;
import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.avro.model.HoodiePath;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.InvalidHoodiePathException;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.StorageSchemes;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static org.apache.hudi.common.fs.FSUtils.LOG_FILE_PATTERN;

/**
 * Utility functions related to accessing the file storage on Hadoop.
 */
public class HadoopFSUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopFSUtils.class);
  private static final String HOODIE_ENV_PROPS_PREFIX = "HOODIE_ENV_";
  private static final int MAX_ATTEMPTS_RECOVER_LEASE = 10;

  public static Configuration prepareHadoopConf(Configuration conf) {
    // look for all properties, prefixed to be picked up
    for (Map.Entry<String, String> prop : System.getenv().entrySet()) {
      if (prop.getKey().startsWith(HOODIE_ENV_PROPS_PREFIX)) {
        LOG.info("Picking up value for hoodie env var : {}", prop.getKey());
        conf.set(prop.getKey().replace(HOODIE_ENV_PROPS_PREFIX, "").replaceAll("_DOT_", "."), prop.getValue());
      }
    }
    return conf;
  }

  public static StorageConfiguration<Configuration> getStorageConf(Configuration conf) {
    return getStorageConf(conf, false);
  }

  public static StorageConfiguration<Configuration> getStorageConf() {
    return getStorageConf(prepareHadoopConf(new Configuration()), false);
  }

  public static StorageConfiguration<Configuration> getStorageConfWithCopy(Configuration conf) {
    return getStorageConf(conf, true);
  }

  public static <T> FileSystem getFs(String pathStr, StorageConfiguration<T> storageConf) {
    return getFs(new Path(pathStr), storageConf);
  }

  public static <T> FileSystem getFs(String pathStr, StorageConfiguration<T> storageConf, boolean newCopy) {
    return getFs(new Path(pathStr), storageConf, newCopy);
  }

  public static <T> FileSystem getFs(Path path, StorageConfiguration<T> storageConf) {
    return getFs(path, storageConf, false);
  }

  public static <T> FileSystem getFs(Path path, StorageConfiguration<T> storageConf, boolean newCopy) {
    Configuration conf = newCopy ? storageConf.unwrapCopyAs(Configuration.class) : storageConf.unwrapAs(Configuration.class);
    return getFs(path, conf);
  }

  public static FileSystem getFs(String pathStr, Configuration conf) {
    return getFs(new Path(pathStr), conf);
  }

  public static FileSystem getFs(StoragePath path, Configuration conf) {
    return getFs(convertToHadoopPath(path), conf);
  }

  public static FileSystem getFs(Path path, Configuration conf) {
    FileSystem fs;
    prepareHadoopConf(conf);
    try {
      fs = path.getFileSystem(conf);
    } catch (IOException e) {
      throw new HoodieIOException(String.format("Failed to get instance of %s", FileSystem.class.getName()), e);
    }
    return fs;
  }

  public static FileSystem getFs(String pathStr, Configuration conf, boolean localByDefault) {
    if (localByDefault) {
      return getFs(addSchemeIfLocalPath(pathStr), conf);
    }
    return getFs(pathStr, conf);
  }

  public static Path addSchemeIfLocalPath(String path) {
    Path providedPath = new Path(path);
    File localFile = new File(path);
    if (!providedPath.isAbsolute() && localFile.exists()) {
      Path resolvedPath = new Path("file://" + localFile.getAbsolutePath());
      LOG.info("Resolving file {} to be a local file.", path);
      return resolvedPath;
    }
    LOG.info("Resolving file {} to be a remote file.", path);
    return providedPath;
  }

  /**
   * @param path {@link StoragePath} instance.
   * @return the Hadoop {@link Path} instance after conversion.
   */
  public static Path convertToHadoopPath(StoragePath path) {
    return new Path(path.toUri());
  }

  /**
   * @param path Hadoop {@link Path} instance.
   * @return the {@link StoragePath} instance after conversion.
   */
  public static StoragePath convertToStoragePath(Path path) {
    return new StoragePath(path.toUri());
  }

  /**
   * @param fileStatus Hadoop {@link FileStatus} instance.
   * @return the {@link StoragePathInfo} instance after conversion.
   */
  public static StoragePathInfo convertToStoragePathInfo(FileStatus fileStatus) {
    return new StoragePathInfo(
        convertToStoragePath(fileStatus.getPath()),
        fileStatus.getLen(),
        fileStatus.isDirectory(),
        fileStatus.getReplication(),
        fileStatus.getBlockSize(),
        fileStatus.getModificationTime());
  }

  public static StoragePathInfo convertToStoragePathInfo(FileStatus fileStatus, String[] locations) {
    return new StoragePathInfo(
        convertToStoragePath(fileStatus.getPath()),
        fileStatus.getLen(),
        fileStatus.isDirectory(),
        fileStatus.getReplication(),
        fileStatus.getBlockSize(),
        fileStatus.getModificationTime(),
        locations);
  }

  /**
   * @param pathInfo {@link StoragePathInfo} instance.
   * @return the {@link FileStatus} instance after conversion.
   */
  public static FileStatus convertToHadoopFileStatus(StoragePathInfo pathInfo) {
    return new FileStatus(
        pathInfo.getLength(),
        pathInfo.isDirectory(),
        pathInfo.getBlockReplication(),
        pathInfo.getBlockSize(),
        pathInfo.getModificationTime(),
        convertToHadoopPath(pathInfo.getPath()));
  }

  /**
   * Fetch the right {@link FSDataInputStream} to be used by wrapping with required input streams.
   *
   * @param fs         instance of {@link FileSystem} in use.
   * @param filePath   path of the file.
   * @param bufferSize buffer size to be used.
   * @param wrapStream if false, don't attempt to wrap the stream
   * @return the right {@link FSDataInputStream} as required.
   */
  public static FSDataInputStream getFSDataInputStream(FileSystem fs,
                                                       StoragePath filePath,
                                                       int bufferSize,
                                                       boolean wrapStream) {
    FSDataInputStream fsDataInputStream = null;
    try {
      fsDataInputStream = fs.open(convertToHadoopPath(filePath), bufferSize);
    } catch (IOException e) {
      throw new HoodieIOException(String.format("Exception creating input stream from file: %s", filePath), e);
    }

    if (!wrapStream) {
      return fsDataInputStream;
    }

    if (isGCSFileSystem(fs)) {
      // in GCS FS, we might need to interceptor seek offsets as we might get EOF exception
      return new SchemeAwareFSDataInputStream(getFSDataInputStreamForGCS(fsDataInputStream, filePath, bufferSize), true);
    }

    if (isCHDFileSystem(fs)) {
      return new BoundedFsDataInputStream(fs, convertToHadoopPath(filePath), fsDataInputStream);
    }

    if (fsDataInputStream.getWrappedStream() instanceof FSInputStream) {
      return new TimedFSDataInputStream(convertToHadoopPath(filePath), new FSDataInputStream(
          new BufferedFSInputStream((FSInputStream) fsDataInputStream.getWrappedStream(), bufferSize)));
    }

    // fsDataInputStream.getWrappedStream() maybe a BufferedFSInputStream
    // need to wrap in another BufferedFSInputStream the make bufferSize work?
    return fsDataInputStream;
  }

  /**
   * GCS FileSystem needs some special handling for seek and hence this method assists to fetch the right {@link FSDataInputStream} to be
   * used by wrapping with required input streams.
   *
   * @param fsDataInputStream original instance of {@link FSDataInputStream}.
   * @param filePath          path of the file.
   * @param bufferSize        buffer size to be used.
   * @return the right {@link FSDataInputStream} as required.
   */
  private static FSDataInputStream getFSDataInputStreamForGCS(FSDataInputStream fsDataInputStream,
                                                              StoragePath filePath,
                                                              int bufferSize) {
    // in case of GCS FS, there are two flows.
    // a. fsDataInputStream.getWrappedStream() instanceof FSInputStream
    // b. fsDataInputStream.getWrappedStream() not an instanceof FSInputStream, but an instance of FSDataInputStream.
    // (a) is handled in the first if block and (b) is handled in the second if block. If not, we fallback to original fsDataInputStream
    if (fsDataInputStream.getWrappedStream() instanceof FSInputStream) {
      return new TimedFSDataInputStream(convertToHadoopPath(filePath), new FSDataInputStream(
          new BufferedFSInputStream((FSInputStream) fsDataInputStream.getWrappedStream(), bufferSize)));
    }

    if (fsDataInputStream.getWrappedStream() instanceof FSDataInputStream
        && ((FSDataInputStream) fsDataInputStream.getWrappedStream()).getWrappedStream() instanceof FSInputStream) {
      FSInputStream inputStream = (FSInputStream) ((FSDataInputStream) fsDataInputStream.getWrappedStream()).getWrappedStream();
      return new TimedFSDataInputStream(convertToHadoopPath(filePath),
          new FSDataInputStream(new BufferedFSInputStream(inputStream, bufferSize)));
    }

    return fsDataInputStream;
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

  private static StorageConfiguration<Configuration> getStorageConf(Configuration conf, boolean copy) {
    return new HadoopStorageConfiguration(conf, copy);
  }

  public static Configuration registerFileSystem(StoragePath file, Configuration conf) {
    Configuration returnConf = new Configuration(conf);
    String scheme = HadoopFSUtils.getFs(file.toString(), conf).getScheme();
    returnConf.set("fs." + HoodieWrapperFileSystem.getHoodieScheme(scheme) + ".impl",
        HoodieWrapperFileSystem.class.getName());
    return returnConf;
  }

  public static Path toPath(HoodiePath path) {
    if (null == path) {
      return null;
    }
    return new Path(path.getUri());
  }

  public static HoodiePath fromPath(Path path) {
    if (null == path) {
      return null;
    }
    return HoodiePath.newBuilder().setUri(path.toString()).build();
  }

  public static FsPermission toFSPermission(HoodieFSPermission fsPermission) {
    if (null == fsPermission) {
      return null;
    }
    FsAction userAction = fsPermission.getUserAction() != null ? FsAction.valueOf(fsPermission.getUserAction()) : null;
    FsAction grpAction = fsPermission.getGroupAction() != null ? FsAction.valueOf(fsPermission.getGroupAction()) : null;
    FsAction otherAction =
        fsPermission.getOtherAction() != null ? FsAction.valueOf(fsPermission.getOtherAction()) : null;
    boolean stickyBit = fsPermission.getStickyBit() != null ? fsPermission.getStickyBit() : false;
    return new FsPermission(userAction, grpAction, otherAction, stickyBit);
  }

  public static HoodieFSPermission fromFSPermission(FsPermission fsPermission) {
    if (null == fsPermission) {
      return null;
    }
    String userAction = fsPermission.getUserAction() != null ? fsPermission.getUserAction().name() : null;
    String grpAction = fsPermission.getGroupAction() != null ? fsPermission.getGroupAction().name() : null;
    String otherAction = fsPermission.getOtherAction() != null ? fsPermission.getOtherAction().name() : null;
    return HoodieFSPermission.newBuilder().setUserAction(userAction).setGroupAction(grpAction)
        .setOtherAction(otherAction).setStickyBit(fsPermission.getStickyBit()).build();
  }

  public static HoodieFileStatus fromFileStatus(FileStatus fileStatus) {
    if (null == fileStatus) {
      return null;
    }

    HoodieFileStatus fStatus = new HoodieFileStatus();
    try {
      fStatus.setPath(fromPath(fileStatus.getPath()));
      fStatus.setLength(fileStatus.getLen());
      fStatus.setIsDir(fileStatus.isDirectory());
      fStatus.setBlockReplication((int) fileStatus.getReplication());
      fStatus.setBlockSize(fileStatus.getBlockSize());
      fStatus.setModificationTime(fileStatus.getModificationTime());
      fStatus.setAccessTime(fileStatus.getModificationTime());
      fStatus.setSymlink(fileStatus.isSymlink() ? fromPath(fileStatus.getSymlink()) : null);
      safeReadAndSetMetadata(fStatus, fileStatus);
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
    return fStatus;
  }

  /**
   * Used to safely handle FileStatus calls which might fail on some FileSystem implementation.
   * (DeprecatedLocalFileSystem)
   */
  private static void safeReadAndSetMetadata(HoodieFileStatus fStatus, FileStatus fileStatus) {
    try {
      fStatus.setOwner(fileStatus.getOwner());
      fStatus.setGroup(fileStatus.getGroup());
      fStatus.setPermission(fromFSPermission(fileStatus.getPermission()));
    } catch (IllegalArgumentException ie) {
      // Deprecated File System (testing) does not work well with this call
      // skipping
    }
  }

  public static long getFileSize(FileSystem fs, Path path) throws IOException {
    return fs.getFileStatus(path).getLen();
  }

  /**
   * Given a base partition and a partition path, return relative path of partition path to the base path.
   */
  public static String getRelativePartitionPath(Path basePath, Path fullPartitionPath) {
    return FSUtils.getRelativePartitionPath(new StoragePath(basePath.toUri()), new StoragePath(fullPartitionPath.toUri()));
  }

  /**
   * Get the first part of the file name in the log file. That will be the fileId. Log file do not have instantTime in
   * the file name.
   */
  public static String getFileIdFromLogPath(Path path) {
    Matcher matcher = LOG_FILE_PATTERN.matcher(path.getName());
    if (!matcher.matches()) {
      throw new InvalidHoodiePathException(path.toString(), "LogFile");
    }
    return matcher.group(1);
  }

  /**
   * Get the second part of the file name in the log file. That will be the delta commit time.
   */
  public static String getDeltaCommitTimeFromLogPath(Path path) {
    Matcher matcher = LOG_FILE_PATTERN.matcher(path.getName());
    if (!matcher.matches()) {
      throw new InvalidHoodiePathException(path.toString(), "LogFile");
    }
    return matcher.group(2);
  }

  /**
   * Check if the file is a base file of a log file. Then get the fileId appropriately.
   */
  public static String getFileIdFromFilePath(Path filePath) {
    if (isLogFile(filePath)) {
      return getFileIdFromLogPath(filePath);
    }
    return FSUtils.getFileIdFromFileName(filePath.getName());
  }

  public static boolean isBaseFile(Path path) {
    String extension = FSUtils.getFileExtension(path.getName());
    return HoodieFileFormat.BASE_FILE_EXTENSIONS.contains(extension);
  }

  public static boolean isLogFile(Path logPath) {
    return FSUtils.isLogFile(new StoragePath(logPath.getName()));
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
   * When a file was opened and the task died without closing the stream, another task executor cannot open because the
   * existing lease will be active. We will try to recover the lease, from HDFS. If a data node went down, it takes
   * about 10 minutes for the lease to be recovered. But if the client dies, this should be instant.
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

  public static Path constructAbsolutePathInHadoopPath(String basePath, String relativePartitionPath) {
    return new Path(FSUtils.constructAbsolutePath(basePath, relativePartitionPath).toUri());
  }

  /**
   * Get DFS full partition path (e.g. hdfs://ip-address:8020:/<absolute path>)
   */
  public static String getDFSFullPartitionPath(FileSystem fs, Path fullPartitionPath) {
    return fs.getUri() + fullPartitionPath.toUri().getRawPath();
  }

  public static <T> Map<String, T> parallelizeFilesProcess(
      HoodieEngineContext hoodieEngineContext,
      FileSystem fs,
      int parallelism,
      FSUtils.SerializableFunction<Pair<String, StorageConfiguration<Configuration>>, T> pairFunction,
      List<String> subPaths) {
    Map<String, T> result = new HashMap<>();
    if (subPaths.size() > 0) {
      StorageConfiguration<Configuration> conf = new HadoopStorageConfiguration(fs.getConf(), true);
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
      result = parallelizeFilesProcess(hoodieEngineContext, fs, parallelism,
          pairOfSubPathAndConf -> {
            Path path = new Path(pairOfSubPathAndConf.getKey());
            try {
              FileSystem fileSystem = path.getFileSystem(pairOfSubPathAndConf.getValue().unwrap());
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
}
