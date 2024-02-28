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

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.apache.hadoop.conf.Configuration;
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
        LOG.info("Picking up value for hoodie env var :" + prop.getKey());
        conf.set(prop.getKey().replace(HOODIE_ENV_PROPS_PREFIX, "").replaceAll("_DOT_", "."), prop.getValue());
      }
    }
    return conf;
  }

  public static StorageConfiguration<Configuration> getStorageConf(Configuration conf) {
    return getStorageConf(conf, false);
  }

  public static StorageConfiguration<Configuration> getStorageConf(Configuration conf, boolean copy) {
    return new HadoopStorageConfiguration(conf, copy);
  }

  public static <T> FileSystem getFs(String pathStr, StorageConfiguration<T> storageConf) {
    return getFs(new Path(pathStr), storageConf);
  }

  public static <T> FileSystem getFs(Path path, StorageConfiguration<T> storageConf) {
    return getFs(path, storageConf, false);
  }

  public static <T> FileSystem getFs(Path path, StorageConfiguration<T> storageConf, boolean newCopy) {
    T conf = newCopy ? storageConf.newCopy() : storageConf.get();
    ValidationUtils.checkArgument(conf instanceof Configuration);
    return getFs(path, (Configuration) conf);
  }

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
}
