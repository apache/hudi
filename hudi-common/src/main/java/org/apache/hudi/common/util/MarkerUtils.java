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

package org.apache.hudi.common.util;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.heartbeat.HoodieHeartbeatUtils.isHeartbeatExpired;
import static org.apache.hudi.common.util.FileIOUtils.closeQuietly;

/**
 * A utility class for marker related operations.
 */
public class MarkerUtils {
  public static final String MARKERS_FILENAME_PREFIX = "MARKERS";
  public static final String MARKER_TYPE_FILENAME = MARKERS_FILENAME_PREFIX + ".type";
  private static final Logger LOG = LoggerFactory.getLogger(MarkerUtils.class);

  /**
   * Strips the folder prefix of the marker file path corresponding to a data file.
   *
   * @param fullMarkerPath the full path of the marker file
   * @param basePath       the base path
   * @param instantTime    instant of interest
   * @return marker file name
   */
  public static String stripMarkerFolderPrefix(String fullMarkerPath, String basePath, String instantTime) {
    ValidationUtils.checkArgument(fullMarkerPath.contains(HoodieTableMetaClient.MARKER_EXTN),
        String.format("Using DIRECT markers but marker path does not contain extension: %s",
            HoodieTableMetaClient.MARKER_EXTN));
    String markerRootPath = new StoragePath(
        String.format("%s/%s/%s", basePath, HoodieTableMetaClient.TEMPFOLDER_NAME, instantTime))
        .getPathWithoutSchemeAndAuthority().toString();
    return stripMarkerFolderPrefix(fullMarkerPath, markerRootPath);
  }

  /**
   * Strips the marker folder prefix of any file path under the marker directory.
   *
   * @param fullMarkerPath the full path of the file
   * @param markerDir      marker directory
   * @return file name
   */
  public static String stripMarkerFolderPrefix(String fullMarkerPath, String markerDir) {
    int begin = fullMarkerPath.indexOf(markerDir);
    ValidationUtils.checkArgument(begin >= 0,
        "Not in marker dir. Marker Path=" + fullMarkerPath + ", Expected Marker Root=" + markerDir);
    return fullMarkerPath.substring(begin + markerDir.length() + 1);
  }

  /**
   * @param storage   {@link HoodieStorage} to use.
   * @param markerDir marker directory.
   * @return {@code true} if the MARKERS.type file exists; {@code false} otherwise.
   */
  public static boolean doesMarkerTypeFileExist(HoodieStorage storage, String markerDir) throws IOException {
    return storage.exists(new StoragePath(markerDir, MARKER_TYPE_FILENAME));
  }

  /**
   * Reads the marker type from `MARKERS.type` file.
   *
   * @param storage   {@link HoodieStorage} to use.
   * @param markerDir marker directory.
   * @return the marker type, or empty if the marker type file does not exist.
   */
  public static Option<MarkerType> readMarkerType(HoodieStorage storage, String markerDir) {
    StoragePath markerTypeFilePath = new StoragePath(markerDir, MARKER_TYPE_FILENAME);
    InputStream inputStream = null;
    Option<MarkerType> content = Option.empty();
    try {
      if (!doesMarkerTypeFileExist(storage, markerDir)) {
        return Option.empty();
      }
      inputStream = storage.open(markerTypeFilePath);
      String markerType = FileIOUtils.readAsUTFString(inputStream);
      if (StringUtils.isNullOrEmpty(markerType)) {
        return Option.empty();
      }
      content = Option.of(MarkerType.valueOf(markerType));
    } catch (IOException e) {
      throw new HoodieIOException("Cannot read marker type file " + markerTypeFilePath
          + "; " + e.getMessage(), e);
    } finally {
      closeQuietly(inputStream);
    }
    return content;
  }

  /**
   * Writes the marker type to the file `MARKERS.type`.
   *
   * @param markerType marker type.
   * @param storage    {@link HoodieStorage} to use.
   * @param markerDir  marker directory.
   */
  public static void writeMarkerTypeToFile(MarkerType markerType, HoodieStorage storage, String markerDir) {
    StoragePath markerTypeFilePath = new StoragePath(markerDir, MARKER_TYPE_FILENAME);
    OutputStream outputStream = null;
    BufferedWriter bufferedWriter = null;
    try {
      outputStream = storage.create(markerTypeFilePath, false);
      bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
      bufferedWriter.write(markerType.toString());
    } catch (IOException e) {
      throw new HoodieException("Failed to create marker type file " + markerTypeFilePath
          + "; " + e.getMessage(), e);
    } finally {
      closeQuietly(bufferedWriter);
      closeQuietly(outputStream);
    }
  }

  /**
   * Deletes `MARKERS.type` file.
   *
   * @param storage   {@link HoodieStorage} to use.
   * @param markerDir marker directory.
   */
  public static void deleteMarkerTypeFile(HoodieStorage storage, String markerDir) {
    StoragePath markerTypeFilePath = new StoragePath(markerDir, MARKER_TYPE_FILENAME);
    try {
      storage.deleteFile(markerTypeFilePath);
    } catch (IOException e) {
      throw new HoodieIOException("Cannot delete marker type file " + markerTypeFilePath
          + "; " + e.getMessage(), e);
    }
  }

  /**
   * Reads files containing the markers written by timeline-server-based marker mechanism.
   *
   * @param markerDir   marker directory.
   * @param storage     file system to use.
   * @param context     instance of {@link HoodieEngineContext} to use
   * @param parallelism parallelism to use
   * @return A {@code Map} of file name to the set of markers stored in the file.
   */
  public static Map<String, Set<String>> readTimelineServerBasedMarkersFromFileSystem(
      String markerDir, HoodieStorage storage, HoodieEngineContext context, int parallelism) {
    StoragePath dirPath = new StoragePath(markerDir);
    try {
      if (storage.exists(dirPath)) {
        Predicate<StoragePathInfo> prefixFilter = pathInfo ->
            pathInfo.getPath().getName().startsWith(MARKERS_FILENAME_PREFIX);
        Predicate<StoragePathInfo> markerTypeFilter = pathInfo ->
            !pathInfo.getPath().getName().equals(MARKER_TYPE_FILENAME);
        return FSUtils.parallelizeSubPathProcess(
            context, storage, dirPath, parallelism, prefixFilter.and(markerTypeFilter),
            pairOfSubPathAndConf -> {
              String markersFilePathStr = pairOfSubPathAndConf.getKey();
              StorageConfiguration<?> conf = pairOfSubPathAndConf.getValue();
              return readMarkersFromFile(new StoragePath(markersFilePathStr), conf);
            });
      }
      return new HashMap<>();
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  /**
   * Reads the markers stored in the underlying file.
   *
   * @param markersFilePath file path for the markers
   * @param conf            storage config
   * @return markers in a {@code Set} of String.
   */
  public static Set<String> readMarkersFromFile(StoragePath markersFilePath, StorageConfiguration<?> conf) {
    return readMarkersFromFile(markersFilePath, conf, false);
  }

  /**
   * Reads the markers stored in the underlying file.
   *
   * @param markersFilePath File path for the markers.
   * @param conf            storage config.
   * @param ignoreException Whether to ignore IOException.
   * @return Markers in a {@code Set} of String.
   */
  public static Set<String> readMarkersFromFile(StoragePath markersFilePath,
                                                StorageConfiguration<?> conf,
                                                boolean ignoreException) {
    InputStream inputStream = null;
    Set<String> markers = new HashSet<>();
    try {
      LOG.debug("Read marker file: " + markersFilePath);
      HoodieStorage storage = HoodieStorageUtils.getStorage(markersFilePath, conf);
      inputStream = storage.open(markersFilePath);
      markers = new HashSet<>(FileIOUtils.readAsUTFStringLines(inputStream));
    } catch (IOException e) {
      String errorMessage = "Failed to read MARKERS file " + markersFilePath;
      if (ignoreException) {
        LOG.warn(errorMessage + ". Ignoring the exception and continue.", e);
      } else {
        throw new HoodieIOException(errorMessage, e);
      }
    } finally {
      closeQuietly(inputStream);
    }
    return markers;
  }

  /**
   * Gets all marker directories.
   *
   * @param tempPath Temporary folder under .hoodie.
   * @param storage  File system to use.
   * @return All marker directories.
   * @throws IOException upon error.
   */
  public static List<StoragePath> getAllMarkerDir(StoragePath tempPath,
                                                  HoodieStorage storage) throws IOException {
    return storage.listDirectEntries(tempPath).stream().map(StoragePathInfo::getPath).collect(Collectors.toList());
  }

  /**
   * Whether there is write conflict with completed commit among multiple writers.
   *
   * @param activeTimeline          Active timeline.
   * @param currentFileIDs          Current set of file IDs.
   * @param completedCommitInstants Completed commits.
   * @return {@code true} if the conflict is detected; {@code false} otherwise.
   */
  public static boolean hasCommitConflict(HoodieActiveTimeline activeTimeline, Set<String> currentFileIDs, Set<HoodieInstant> completedCommitInstants) {

    Set<HoodieInstant> currentInstants = new HashSet<>(
        activeTimeline.reload().getCommitsTimeline().filterCompletedInstants().getInstants());

    currentInstants.removeAll(completedCommitInstants);
    Set<String> missingFileIDs = currentInstants.stream().flatMap(instant -> {
      try {
        return HoodieCommitMetadata.fromBytes(activeTimeline.getInstantDetails(instant).get(), HoodieCommitMetadata.class)
            .getFileIdAndRelativePaths().keySet().stream();
      } catch (Exception e) {
        return Stream.empty();
      }
    }).collect(Collectors.toSet());
    currentFileIDs.retainAll(missingFileIDs);
    return !currentFileIDs.isEmpty();
  }

  /**
   * Get Candidate Instant to do conflict checking:
   * 1. Skip current writer related instant(currentInstantTime)
   * 2. Skip all instants after currentInstantTime
   * 3. Skip dead writers related instants based on heart-beat
   * 4. Skip pending compaction instant (For now we don' do early conflict check with compact action)
   * Because we don't want to let pending compaction block common writer.
   *
   * @param instants
   * @return
   */
  public static List<String> getCandidateInstants(HoodieActiveTimeline activeTimeline,
                                                  List<StoragePath> instants, String currentInstantTime,
                                                  long maxAllowableHeartbeatIntervalInMs,
                                                  HoodieStorage storage, String basePath) {

    return instants.stream().map(StoragePath::toString).filter(instantPath -> {
      String instantTime = markerDirToInstantTime(instantPath);
      return instantTime.compareToIgnoreCase(currentInstantTime) < 0
          && !activeTimeline.filterPendingCompactionTimeline().containsInstant(instantTime)
          && !activeTimeline.filterPendingReplaceTimeline().containsInstant(instantTime);
    }).filter(instantPath -> {
      try {
        return !isHeartbeatExpired(markerDirToInstantTime(instantPath),
            maxAllowableHeartbeatIntervalInMs, storage, basePath);
      } catch (IOException e) {
        return false;
      }
    }).collect(Collectors.toList());
  }

  /**
   * Get fileID from full marker path, for example:
   * 20210623/0/20210825/932a86d9-5c1d-44c7-ac99-cb88b8ef8478-0_85-15-1390_20220620181735781.parquet.marker.MERGE
   *    ==> get 20210623/0/20210825/932a86d9-5c1d-44c7-ac99-cb88b8ef8478-0
   * @param marker
   * @return
   */
  public static String makerToPartitionAndFileID(String marker) {
    String[] ele = marker.split("_");
    return ele[0];
  }

  /**
   * Get instantTime from full marker path, for example:
   * /var/folders/t3/th1dw75d0yz2x2k2qt6ys9zh0000gp/T/junit6502909693741900820/dataset/.hoodie/.temp/003
   *    ==> 003
   * @param marker
   * @return
   */
  public static String markerDirToInstantTime(String marker) {
    String[] ele = marker.split("/");
    return ele[ele.length - 1];
  }
}