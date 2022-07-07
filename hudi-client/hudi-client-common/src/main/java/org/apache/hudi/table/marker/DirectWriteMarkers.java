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
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.MarkerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
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
import java.util.stream.Stream;

/**
 * Marker operations of directly accessing the file system to create and delete
 * marker files.  Each data file has a corresponding marker file.
 */
public class DirectWriteMarkers extends WriteMarkers {

  private static final Logger LOG = LogManager.getLogger(DirectWriteMarkers.class);
  private final transient FileSystem fs;

  public DirectWriteMarkers(FileSystem fs, String basePath, String markerFolderPath, String instantTime) {
    super(basePath, markerFolderPath, instantTime);
    this.fs = fs;
  }

  public DirectWriteMarkers(HoodieTable table, String instantTime) {
    this(table.getMetaClient().getFs(),
        table.getMetaClient().getBasePath(),
        table.getMetaClient().getMarkerFolderPath(instantTime),
        instantTime);
  }

  /**
   * Deletes Marker directory corresponding to an instant.
   *
   * @param context HoodieEngineContext.
   * @param parallelism parallelism for deletion.
   */
  public boolean deleteMarkerDir(HoodieEngineContext context, int parallelism) {
    return FSUtils.deleteDir(context, fs, markerDirPath, parallelism);
  }

  /**
   * @return {@code true} if marker directory exists; {@code false} otherwise.
   * @throws IOException
   */
  public boolean doesMarkerDirExist() throws IOException {
    return fs.exists(markerDirPath);
  }

  @Override
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
    String rPath = MarkerUtils.stripMarkerFolderPrefix(markerPath, basePath, instantTime);
    return stripMarkerSuffix(rPath);
  }

  @Override
  public Set<String> allMarkerFilePaths() throws IOException {
    Set<String> markerFiles = new HashSet<>();
    if (doesMarkerDirExist()) {
      FSUtils.processFiles(fs, markerDirPath.toString(), fileStatus -> {
        markerFiles.add(MarkerUtils.stripMarkerFolderPrefix(fileStatus.getPath().toString(), basePath, instantTime));
        return true;
      }, false);
    }
    return markerFiles;
  }

  /**
   * We need to do list operation here.
   * In order to reduce the list pressure as much as possible, first we build path prefix in advance:  '$base_path/.temp/instant_time/partition_path',
   * and only list these specific partition_paths we need instead of list all the '$base_path/.temp/'
   * @param config
   * @param partitionPath
   * @param fileId
   * @return true if current fileID is already existed under .temp/instant_time/partition_path/..
   * @throws IOException
   */
  @Override
  public boolean hasMarkerConflict(HoodieWriteConfig config, String partitionPath, String fileId) throws IOException {
    String tempFolderPath = getTempFolderPath();
    long res = Arrays.stream(fs.listStatus(new Path(tempFolderPath)))
        .parallel()
        .map(FileStatus::getPath)
        .filter(markerPath -> {
          return !markerPath.getName().equalsIgnoreCase(instantTime);})
        .flatMap(currentMarkerDirPath -> {
          try {
            Path markerPartitionPath;
            if (StringUtils.isNullOrEmpty(partitionPath)) {
              markerPartitionPath = currentMarkerDirPath;
            } else {
              markerPartitionPath = new Path(currentMarkerDirPath, partitionPath);
            }

            if (!StringUtils.isNullOrEmpty(partitionPath) && !fs.exists(markerPartitionPath)) {
              return Stream.empty();
            } else {
              return Arrays.stream(fs.listStatus(markerPartitionPath)).parallel()
                  .filter((path) -> path.toString().contains(fileId));
            }
          } catch (IOException e) {
            throw new HoodieIOException("IOException occurs during checking marker file conflict");
          }
        }).count();

    if (res != 0L) {
      LOG.warn("Detected conflict marker files: " + partitionPath + "/" + fileId + " for " + instantTime);
      return true;
    }
    return false;
  }

  private String getTempFolderPath() {
    return basePath + Path.SEPARATOR + HoodieTableMetaClient.TEMPFOLDER_NAME;
  }

  /**
   * Creates a marker file based on the full marker name excluding the base path and instant.
   *
   * @param markerName the full marker name, e.g., "2021/08/13/file1.marker.CREATE"
   * @return path of the marker file
   */
  public Option<Path> create(String markerName) {
    return create(new Path(markerDirPath, markerName), true);
  }

  @Override
  protected Option<Path> create(String partitionPath, String dataFileName, IOType type, boolean checkIfExists) {
    return create(getMarkerPath(partitionPath, dataFileName, type), checkIfExists);
  }

  private Option<Path> create(Path markerPath, boolean checkIfExists) {
    HoodieTimer timer = new HoodieTimer().startTimer();
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
        return Option.empty();
      }
      LOG.info("Creating Marker Path=" + markerPath);
      fs.create(markerPath, false).close();
    } catch (IOException e) {
      throw new HoodieException("Failed to create marker file " + markerPath, e);
    }
    LOG.info("[direct] Created marker file " + markerPath.toString()
        + " in " + timer.endTimer() + " ms");
    return Option.of(markerPath);
  }
}
