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

import org.apache.hudi.exception.HoodieIOException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Utility functions related to accessing the file storage on Hadoop.
 */
public class HadoopFSUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopFSUtils.class);
  private static final String HOODIE_ENV_PROPS_PREFIX = "HOODIE_ENV_";

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

  public static StorageConfiguration<Configuration> getStorageConfWithCopy(Configuration conf) {
    return getStorageConf(conf, true);
  }

  public static <T> FileSystem getFs(String pathStr, StorageConfiguration<T> storageConf) {
    return getFs(new Path(pathStr), storageConf);
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
    return getFs(new Path(path.toUri()), conf);
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
   * @return the right {@link FSDataInputStream} as required.
   */
  public static FSDataInputStream getFSDataInputStream(FileSystem fs,
                                                       StoragePath filePath,
                                                       int bufferSize) {
    FSDataInputStream fsDataInputStream = null;
    try {
      fsDataInputStream = fs.open(convertToHadoopPath(filePath), bufferSize);
    } catch (IOException e) {
      throw new HoodieIOException(String.format("Exception creating input stream from file: %s", filePath), e);
    }

    if (isGCSFileSystem(fs)) {
      // in GCS FS, we might need to interceptor seek offsets as we might get EOF exception
      return new SchemeAwareFSDataInputStream(getFSDataInputStreamForGCS(fsDataInputStream, filePath, bufferSize), true);
    }

    if (isCHDFileSystem(fs)) {
      return new BoundedFsDataInputStream(fs, convertToHadoopPath(filePath), fsDataInputStream);
    }

    /*
    if (fsDataInputStream.getWrappedStream() instanceof FSInputStream) {
      return new TimedFSDataInputStream(convertToHadoopPath(filePath), new FSDataInputStream(
          new BufferedFSInputStream((FSInputStream) fsDataInputStream.getWrappedStream(), bufferSize)));
    }
     */

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
}
