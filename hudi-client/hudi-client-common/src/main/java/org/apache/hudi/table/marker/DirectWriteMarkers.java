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

import org.apache.hudi.common.conflict.detection.DirectMarkerBasedDetectionStrategy;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.MarkerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.function.Predicate;

import static org.apache.hudi.table.marker.ConflictDetectionUtils.getDefaultEarlyConflictDetectionStrategy;

/**
 * Marker operations of directly accessing the file system to create and delete
 * marker files.  Each data file has a corresponding marker file.
 */
public class DirectWriteMarkers extends WriteMarkers {

  private static final Logger LOG = LoggerFactory.getLogger(DirectWriteMarkers.class);
  private static final Predicate<String> APPEND_MARKER_PREDICATE = pathStr -> pathStr.contains(HoodieTableMetaClient.MARKER_EXTN) && pathStr.endsWith(IOType.APPEND.name());
  private static final Predicate<String> NOT_APPEND_MARKER_PREDICATE = pathStr -> pathStr.contains(HoodieTableMetaClient.MARKER_EXTN) && !pathStr.endsWith(IOType.APPEND.name());

  private final transient HoodieStorage storage;

  public DirectWriteMarkers(HoodieStorage storage, String basePath, String markerFolderPath, String instantTime) {
    super(basePath, markerFolderPath, instantTime);
    this.storage = storage;
  }

  public DirectWriteMarkers(HoodieTable table, String instantTime) {
    this(table.getStorage(),
        table.getMetaClient().getBasePath().toString(),
        table.getMetaClient().getMarkerFolderPath(instantTime),
        instantTime);
  }

  /**
   * Deletes Marker directory corresponding to an instant.
   *
   * @param context     HoodieEngineContext.
   * @param parallelism parallelism for deletion.
   */
  public boolean deleteMarkerDir(HoodieEngineContext context, int parallelism) {
    return FSUtils.deleteDir(context, storage, markerDirPath, parallelism);
  }

  /**
   * @return {@code true} if marker directory exists; {@code false} otherwise.
   * @throws IOException
   */
  public boolean doesMarkerDirExist() throws IOException {
    return storage.exists(markerDirPath);
  }

  @Override
  public Set<String> createdAndMergedDataPaths(HoodieEngineContext context, int parallelism) throws IOException {
    Set<String> dataFiles = new HashSet<>();

    List<StoragePathInfo> topLevelInfoList = storage.listDirectEntries(markerDirPath);
    List<String> subDirectories = new ArrayList<>();
    for (StoragePathInfo topLevelInfo: topLevelInfoList) {
      if (topLevelInfo.isFile()) {
        String pathStr = topLevelInfo.getPath().toString();
        if (pathStr.contains(HoodieTableMetaClient.MARKER_EXTN) && !pathStr.endsWith(IOType.APPEND.name())) {
          dataFiles.add(translateMarkerToDataPath(pathStr));
        }
      } else {
        subDirectories.add(topLevelInfo.getPath().toString());
      }
    }

    if (subDirectories.size() > 0) {
      parallelism = Math.min(subDirectories.size(), parallelism);
      StorageConfiguration<?> storageConf = storage.getConf();
      context.setJobStatus(this.getClass().getSimpleName(), "Obtaining marker files for all created, merged paths");
      dataFiles.addAll(context.flatMap(subDirectories, directory -> {
        Path path = new Path(directory);
        FileSystem fileSystem = HadoopFSUtils.getFs(path, storageConf.unwrapAs(Configuration.class));
        RemoteIterator<LocatedFileStatus> itr = fileSystem.listFiles(path, true);
        List<String> result = new ArrayList<>();
        while (itr.hasNext()) {
          FileStatus status = itr.next();
          String pathStr = status.getPath().toString();
          if (NOT_APPEND_MARKER_PREDICATE.test(pathStr)) {
            result.add(translateMarkerToDataPath(pathStr));
          }
        }
        return result.stream();
      }, parallelism));
    }

    return dataFiles;
  }

  public Set<String> getAppendedLogPaths(HoodieEngineContext context, int parallelism) throws IOException {
    Set<String> logFiles = new HashSet<>();
    List<String> subDirectories = getSubDirectoriesByMarkerCondition(storage.listDirectEntries(markerDirPath), logFiles, APPEND_MARKER_PREDICATE);

    if (subDirectories.size() > 0) {
      parallelism = Math.min(subDirectories.size(), parallelism);
      StorageConfiguration<Configuration> storageConf = new HadoopStorageConfiguration((Configuration) storage.getConf().unwrap(), true);
      context.setJobStatus(this.getClass().getSimpleName(), "Obtaining marker files for all created, merged paths");
      logFiles.addAll(context.flatMap(subDirectories, directory -> {
        Queue<Path> candidatesDirs = new LinkedList<>();
        candidatesDirs.add(new Path(directory));
        List<String> result = new ArrayList<>();
        while (!candidatesDirs.isEmpty()) {
          Path path = candidatesDirs.remove();
          FileSystem fileSystem = HadoopFSUtils.getFs(path, storageConf);
          RemoteIterator<FileStatus> itr = fileSystem.listStatusIterator(path);
          while (itr.hasNext()) {
            FileStatus status = itr.next();
            if (status.isDirectory()) {
              candidatesDirs.add(status.getPath());
            } else {
              String pathStr = status.getPath().toString();
              if (APPEND_MARKER_PREDICATE.test(pathStr)) {
                result.add(translateMarkerToDataPath(pathStr));
              }
            }
          }
        }
        return result.stream();
      }, parallelism));
    }

    return logFiles;
  }

  private List<String> getSubDirectoriesByMarkerCondition(List<StoragePathInfo> topLevelInfoList, Set<String> dataFiles, Predicate<String> pathCondition) {
    List<String> subDirectories = new ArrayList<>();
    for (StoragePathInfo topLevelInfo : topLevelInfoList) {
      if (topLevelInfo.isFile()) {
        String pathStr = topLevelInfo.getPath().toString();
        if (pathCondition.test(pathStr)) {
          dataFiles.add(translateMarkerToDataPath(pathStr));
        }
      } else {
        subDirectories.add(topLevelInfo.getPath().toString());
      }
    }
    return subDirectories;
  }

  private String translateMarkerToDataPath(String markerPath) {
    String rPath = MarkerUtils.stripMarkerFolderPrefix(markerPath, basePath, instantTime);
    return stripMarkerSuffix(rPath);
  }

  @Override
  public Set<String> allMarkerFilePaths() throws IOException {
    Set<String> markerFiles = new HashSet<>();
    if (doesMarkerDirExist()) {
      FSUtils.processFiles(storage, markerDirPath.toString(), fileStatus -> {
        markerFiles.add(MarkerUtils.stripMarkerFolderPrefix(fileStatus.getPath().toString(), basePath, instantTime));
        return true;
      }, false);
    }
    return markerFiles;
  }

  /**
   * Creates a marker file based on the full marker name excluding the base path and instant.
   *
   * @param markerName the full marker name, e.g., "2021/08/13/file1.marker.CREATE"
   * @return path of the marker file
   */
  public Option<StoragePath> create(String markerName) {
    return create(new StoragePath(markerDirPath, markerName), true);
  }

  @Override
  protected Option<StoragePath> create(String partitionPath, String fileName, IOType type, boolean checkIfExists) {
    return create(getMarkerPath(partitionPath, fileName, type), checkIfExists);
  }

  @Override
  public Option<StoragePath> createWithEarlyConflictDetection(String partitionPath, String dataFileName, IOType type, boolean checkIfExists,
                                                              HoodieWriteConfig config, String fileId, HoodieActiveTimeline activeTimeline) {
    String strategyClassName = config.getEarlyConflictDetectionStrategyClassName();
    if (!ReflectionUtils.isSubClass(strategyClassName, DirectMarkerBasedDetectionStrategy.class)) {
      LOG.warn("Cannot use " + strategyClassName + " for direct markers.");
      strategyClassName = getDefaultEarlyConflictDetectionStrategy(MarkerType.DIRECT);
      LOG.warn("Falling back to " + strategyClassName);
    }
    DirectMarkerBasedDetectionStrategy strategy =
        (DirectMarkerBasedDetectionStrategy) ReflectionUtils.loadClass(strategyClassName,
            new Class<?>[] {HoodieStorage.class, String.class, String.class, String.class,
                HoodieActiveTimeline.class, HoodieWriteConfig.class},
            storage, partitionPath, fileId, instantTime, activeTimeline, config);

    strategy.detectAndResolveConflictIfNecessary();
    return create(getMarkerPath(partitionPath, dataFileName, type), checkIfExists);
  }

  private Option<StoragePath> create(StoragePath markerPath, boolean checkIfExists) {
    HoodieTimer timer = HoodieTimer.start();
    StoragePath dirPath = markerPath.getParent();
    try {
      if (!storage.exists(dirPath)) {
        storage.createDirectory(dirPath); // create a new partition as needed.
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to make dir " + dirPath, e);
    }
    try {
      if (checkIfExists && storage.exists(markerPath)) {
        LOG.warn("Marker Path=" + markerPath + " already exists, cancel creation");
        return Option.empty();
      }
      LOG.info("Creating Marker Path=" + markerPath);
      storage.create(markerPath, false).close();
    } catch (IOException e) {
      throw new HoodieException("Failed to create marker file " + markerPath, e);
    }
    LOG.info("[direct] Created marker file " + markerPath
        + " in " + timer.endTimer() + " ms");
    return Option.of(markerPath);
  }
}
