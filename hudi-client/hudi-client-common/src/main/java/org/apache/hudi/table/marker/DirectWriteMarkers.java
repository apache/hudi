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
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static org.apache.hudi.table.marker.ConflictDetectionUtils.getDefaultEarlyConflictDetectionStrategy;

/**
 * Marker operations of directly accessing the file system to create and delete
 * marker files for table version 8 and above.
 * Each data file has a corresponding marker file.
 */
public class DirectWriteMarkers extends WriteMarkers {

  private static final Logger LOG = LoggerFactory.getLogger(DirectWriteMarkers.class);
  private static final Predicate<String> NOT_APPEND_MARKER_PREDICATE = pathStr -> pathStr.contains(HoodieTableMetaClient.MARKER_EXTN) && !pathStr.endsWith(IOType.APPEND.name());

  protected final transient HoodieStorage storage;

  DirectWriteMarkers(HoodieStorage storage,
                     String basePath,
                     String markerFolderPath,
                     String instantTime) {
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
    Pair<List<String>, Set<String>> subDirectoriesAndDataFiles = getSubDirectoriesByMarkerCondition(storage.listDirectEntries(markerDirPath), NOT_APPEND_MARKER_PREDICATE);
    List<String> subDirectories = subDirectoriesAndDataFiles.getLeft();
    Set<String> dataFiles = subDirectoriesAndDataFiles.getRight();
    if (subDirectories.size() > 0) {
      parallelism = Math.min(subDirectories.size(), parallelism);
      StorageConfiguration<?> storageConf = storage.getConf();
      context.setJobStatus(this.getClass().getSimpleName(), "Obtaining marker files for all created, merged paths");
      dataFiles.addAll(context.flatMap(subDirectories, directory -> {
        StoragePath path = new StoragePath(directory);
        HoodieStorage storage = HoodieStorageUtils.getStorage(path, storageConf);
        return storage.listFiles(path).stream()
            .map(pathInfo -> pathInfo.getPath().toString())
            .filter(pathStr -> NOT_APPEND_MARKER_PREDICATE.test(pathStr))
            .map(this::translateMarkerToDataPath);
      }, parallelism));
    }

    return dataFiles;
  }

  protected Pair<List<String>, Set<String>> getSubDirectoriesByMarkerCondition(List<StoragePathInfo> topLevelInfoList, Predicate<String> pathCondition) {
    Set<String> dataFiles = new HashSet<>();
    List<String> subDirectories = new ArrayList<>();
    for (StoragePathInfo topLevelInfo: topLevelInfoList) {
      if (topLevelInfo.isFile()) {
        String pathStr = topLevelInfo.getPath().toString();
        if (pathCondition.test(pathStr)) {
          dataFiles.add(translateMarkerToDataPath(pathStr));
        }
      } else {
        subDirectories.add(topLevelInfo.getPath().toString());
      }
    }

    return Pair.of(subDirectories, dataFiles);
  }

  protected String translateMarkerToDataPath(String markerPath) {
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
      LOG.warn("Cannot use {} for direct markers.", strategyClassName);
      strategyClassName = getDefaultEarlyConflictDetectionStrategy(MarkerType.DIRECT);
      LOG.warn("Falling back to {}", strategyClassName);
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
    try {
      if (checkIfExists && storage.exists(markerPath)) {
        LOG.info("Marker Path={} already exists, cancel creation", markerPath);
        return Option.empty();
      }
      LOG.debug("Creating Marker Path={}", markerPath);
      storage.create(markerPath, false).close();
    } catch (IOException e) {
      throw new HoodieException("Failed to create marker file " + markerPath, e);
    }
    LOG.info("[direct] Created marker file {} in {} ms", markerPath, timer.endTimer());
    return Option.of(markerPath);
  }
}
