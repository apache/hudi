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

package org.apache.hudi.table.marker;

import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Operates on marker files for a given write action (commit, delta commit, compaction).
 */
public class DirectMarkerFiles extends MarkerFiles {

  private static final Logger LOG = LogManager.getLogger(DirectMarkerFiles.class);
  private final transient FileSystem fs;

  public DirectMarkerFiles(FileSystem fs, String basePath, String markerFolderPath, String instantTime) {
    super(basePath, markerFolderPath, instantTime);
    this.fs = fs;
  }

  public DirectMarkerFiles(HoodieTable table, String instantTime) {
    this(table.getMetaClient().getFs(),
        table.getMetaClient().getBasePath(),
        table.getMetaClient().getMarkerFolderPath(instantTime),
        instantTime);
  }

  /**
   * Delete Marker directory corresponding to an instant.
   *
   * @param context HoodieEngineContext.
   * @param parallelism parallelism for deletion.
   */
  public boolean deleteMarkerDir(HoodieEngineContext context, int parallelism) {
    try {
      if (fs.exists(markerDirPath)) {
        FileStatus[] fileStatuses = fs.listStatus(markerDirPath);
        List<String> markerDirSubPaths = Arrays.stream(fileStatuses)
                .map(fileStatus -> fileStatus.getPath().toString())
                .collect(Collectors.toList());

        if (markerDirSubPaths.size() > 0) {
          SerializableConfiguration conf = new SerializableConfiguration(fs.getConf());
          parallelism = Math.min(markerDirSubPaths.size(), parallelism);
          context.foreach(markerDirSubPaths, subPathStr -> {
            Path subPath = new Path(subPathStr);
            FileSystem fileSystem = subPath.getFileSystem(conf.get());
            fileSystem.delete(subPath, true);
          }, parallelism);
        }

        boolean result = fs.delete(markerDirPath, true);
        LOG.info("Removing marker directory at " + markerDirPath);
        return result;
      }
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
    return false;
  }

  public boolean doesMarkerDirExist() throws IOException {
    return fs.exists(markerDirPath);
  }

  public Set<String> createdAndMergedDataPaths(HoodieEngineContext context, int parallelism) throws IOException {
    Set<String> dataFiles = new HashSet<>();

    FileStatus[] topLevelStatuses = fs.listStatus(markerDirPath);
    List<String> subDirectories = new ArrayList<>();
    for (FileStatus topLevelStatus: topLevelStatuses) {
      if (topLevelStatus.isFile()) {
        String pathStr = topLevelStatus.getPath().toString();
        if (pathStr.contains(HoodieTableMetaClient.MARKER_EXTN) && !pathStr.endsWith(IOType.APPEND.name())) {
          dataFiles.add(translateMarkerToDataPath(pathStr));
        }
      } else {
        subDirectories.add(topLevelStatus.getPath().toString());
      }
    }

    if (subDirectories.size() > 0) {
      parallelism = Math.min(subDirectories.size(), parallelism);
      SerializableConfiguration serializedConf = new SerializableConfiguration(fs.getConf());
      context.setJobStatus(this.getClass().getSimpleName(), "Obtaining marker files for all created, merged paths");
      dataFiles.addAll(context.flatMap(subDirectories, directory -> {
        Path path = new Path(directory);
        FileSystem fileSystem = path.getFileSystem(serializedConf.get());
        RemoteIterator<LocatedFileStatus> itr = fileSystem.listFiles(path, true);
        List<String> result = new ArrayList<>();
        while (itr.hasNext()) {
          FileStatus status = itr.next();
          String pathStr = status.getPath().toString();
          if (pathStr.contains(HoodieTableMetaClient.MARKER_EXTN) && !pathStr.endsWith(IOType.APPEND.name())) {
            result.add(translateMarkerToDataPath(pathStr));
          }
        }
        return result.stream();
      }, parallelism));
    }

    return dataFiles;
  }

  private String translateMarkerToDataPath(String markerPath) {
    String rPath = stripMarkerFolderPrefix(markerPath);
    return stripMarkerSuffix(rPath);
  }

  public Set<String> allMarkerFilePaths() throws IOException {
    Set<String> markerFiles = new HashSet<>();
    if (doesMarkerDirExist()) {
      FSUtils.processFiles(fs, markerDirPath.toString(), fileStatus -> {
        markerFiles.add(stripMarkerFolderPrefix(fileStatus.getPath().toString()));
        return true;
      }, false);
    }
    return markerFiles;
  }

  protected Path create(String partitionPath, String dataFileName, IOType type, boolean checkIfExists) {
    LOG.info("^^^ [direct] Create marker file : " + partitionPath + " " + dataFileName);
    long startTimeMs = System.currentTimeMillis();
    Path markerPath = getMarkerPath(partitionPath, dataFileName, type);
    Path dirPath = markerPath.getParent();
    try {
      if (!fs.exists(dirPath)) {
        fs.mkdirs(dirPath); // create a new partition as needed.
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to make dir " + dirPath, e);
    }
    try {
      if (checkIfExists && fs.exists(markerPath)) {
        LOG.warn("Marker Path=" + markerPath + " already exists, cancel creation");
        return null;
      }
      LOG.info("Creating Marker Path=" + markerPath);
      fs.create(markerPath, false).close();
    } catch (IOException e) {
      throw new HoodieException("Failed to create marker file " + markerPath, e);
    }
    LOG.info("&&& [direct] Created marker file in " + (System.currentTimeMillis() - startTimeMs) + " ms");
    return markerPath;
  }
}
