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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.conflict.detection.DirectMarkerBasedDetectionStrategy;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
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
import org.apache.hudi.table.HoodieTable;

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
import java.util.List;
import java.util.Set;
import java.util.zip.CRC32;

import static org.apache.hudi.table.marker.ConflictDetectionUtils.getDefaultEarlyConflictDetectionStrategy;

/**
 * Marker operations of directly accessing the file system to create and delete
 * marker files.  Each data file has a corresponding marker file.
 */
public class DirectWriteMarkers extends WriteMarkers {

  private static final Logger LOG = LoggerFactory.getLogger(DirectWriteMarkers.class);
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
   * Creates marker directory.
   * @throws HoodieIOException
   */
  @Override
  public void createMarkerDir() throws HoodieIOException {
    try {
      FSUtils.createPathIfNotExists(fs, markerDirPath);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to create marker folder " + markerDirPath, e);
    }
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
   */
  @Override
  public boolean doesMarkerDirExist() {
    return FSUtils.pathExists(markerDirPath, fs);
  }

  @Override
  public Set<String> createdAndMergedDataPaths(HoodieEngineContext context, int parallelism) throws IOException {
    Set<String> dataFiles = new HashSet<>();

    FileStatus[] topLevelStatuses = fs.listStatus(markerDirPath);
    List<String> subDirectories = new ArrayList<>();
    for (FileStatus topLevelStatus: topLevelStatuses) {
      if (topLevelStatus.isFile()) {
        String pathStr = topLevelStatus.getPath().toString();
        if (pathStr.contains(HoodieTableMetaClient.INPROGRESS_MARKER_EXTN) && !pathStr.endsWith(IOType.APPEND.name())) {
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
        FileSystem fileSystem = FSUtils.getFs(path, serializedConf.get());
        RemoteIterator<LocatedFileStatus> itr = fileSystem.listFiles(path, true);
        List<String> result = new ArrayList<>();
        while (itr.hasNext()) {
          FileStatus status = itr.next();
          String pathStr = status.getPath().toString();
          if (pathStr.contains(HoodieTableMetaClient.INPROGRESS_MARKER_EXTN) && !pathStr.endsWith(IOType.APPEND.name())) {
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

  public static String stripMarkerSuffix(String path) {
    return path.substring(0, path.indexOf(HoodieTableMetaClient.INPROGRESS_MARKER_EXTN));
  }

  public static String stripOldStyleMarkerSuffix(String path) {
    // marker file was created by older version of Hudi, with INPROGRESS_MARKER_EXTN (f1_w1_c1.marker).
    // Rename to data file by replacing .marker with .parquet.
    return String.format("%s%s", path.substring(0, path.indexOf(HoodieTableMetaClient.INPROGRESS_MARKER_EXTN)),
        HoodieFileFormat.PARQUET.getFileExtension());
  }

  @Override
  public Set<String> allMarkerFilePaths() throws IOException {
    Set<String> markerFiles = new HashSet<>();
    if (doesMarkerDirExist()) {
      FSUtils.processFiles(fs, markerDirPath.toString(), fileStatus -> {
        // Only the inprogress markerFiles are to be included here
        if (fileStatus.getPath().toString().contains(HoodieTableMetaClient.INPROGRESS_MARKER_EXTN)) {
          markerFiles.add(MarkerUtils.stripMarkerFolderPrefix(fileStatus.getPath().toString(), basePath, instantTime));
        }
        return true;
      }, false);
    }
    return markerFiles;
  }

  public boolean markerExists(Path markerPath) {
    return FSUtils.pathExists(markerPath, fs);
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
  protected Option<Path> create(String partitionPath, String fileName, IOType type, boolean checkIfExists) {
    return create(getMarkerPath(partitionPath, fileName, type), checkIfExists);
  }

  @Override
  public Option<Path> createWithEarlyConflictDetection(String partitionPath, String dataFileName, IOType type, boolean checkIfExists,
                                                       HoodieWriteConfig config, String fileId, HoodieActiveTimeline activeTimeline) {
    String strategyClassName = config.getEarlyConflictDetectionStrategyClassName();
    if (!ReflectionUtils.isSubClass(strategyClassName, DirectMarkerBasedDetectionStrategy.class)) {
      LOG.warn("Cannot use " + strategyClassName + " for direct markers.");
      strategyClassName = getDefaultEarlyConflictDetectionStrategy(MarkerType.DIRECT);
      LOG.warn("Falling back to " + strategyClassName);
    }
    DirectMarkerBasedDetectionStrategy strategy =
        (DirectMarkerBasedDetectionStrategy) ReflectionUtils.loadClass(strategyClassName,
            fs, partitionPath, fileId, instantTime, activeTimeline, config);

    strategy.detectAndResolveConflictIfNecessary();
    return create(getMarkerPath(partitionPath, dataFileName, type), checkIfExists);
  }

  private Option<Path> create(Path markerPath, boolean checkIfExists) {
    HoodieTimer timer = HoodieTimer.start();
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

  @Override
  public Option<Path> createCompletionMarker(String partitionPath, String fileId, String instantTime, IOType type,
                                                boolean ignoreExisting, Option<byte[]> serializedData)
      throws HoodieException, IOException {
    Path markerPath = getCompletionMarkerPath(partitionPath, fileId, instantTime, type);
    if (!fs.exists(markerPath.getParent())) {
      throw new HoodieException("Marker directory is absent." + markerPath.getParent());
    }
    try {
      FSDataOutputStream outputStream = fs.create(markerPath, ignoreExisting);
      if (serializedData.isPresent()) {
        outputStream.writeLong(serializedData.get().length);
        outputStream.write(serializedData.get());
        outputStream.writeLong(generateChecksum(serializedData.get()));
      }
      outputStream.close();
    } catch (IOException e) {
      throw new HoodieException("Failed to create completed marker " + markerPath);
    }
    return Option.of(markerPath);
  }

  public static long generateChecksum(byte[] data) {
    CRC32 crc = new CRC32();
    crc.update(data);
    return crc.getValue();
  }

  public Option<byte[]> getContentsOfCompletionMarker(String partitionPath, String fileId, String instantTime, IOType type) throws HoodieException, IOException {
    Path markerPath = getCompletionMarkerPath(partitionPath, fileId, instantTime, type);
    if (!fs.exists(markerPath)) {
      throw new HoodieException("Marker File is absent." + markerPath);
    }
    FSDataInputStream inputStream = fs.open(markerPath);
    long length = inputStream.readLong();
    byte[] content = new byte[(int)length];
    inputStream.read(content);
    long crc = inputStream.readLong();
    if (crc != generateChecksum(content)) {
      throw new HoodieException("Content checksum match failed for " + markerPath);
    }
    return Option.of(content);
  }

}
