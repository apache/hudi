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

import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.apache.hudi.common.util.FileIOUtils.closeQuietly;

/**
 * A utility class for marker related operations.
 */
public class MarkerUtils {
  public static final String MARKERS_FILENAME_PREFIX = "MARKERS";
  public static final String MARKER_TYPE_FILENAME = MARKERS_FILENAME_PREFIX + ".type";
  private static final Logger LOG = LogManager.getLogger(MarkerUtils.class);

  /**
   * Strips the folder prefix of the marker file path corresponding to a data file.
   *
   * @param fullMarkerPath the full path of the marker file
   * @param basePath       the base path
   * @param instantTime    instant of interest
   * @return marker file name
   */
  public static String stripMarkerFolderPrefix(String fullMarkerPath, String basePath, String instantTime) {
    ValidationUtils.checkArgument(fullMarkerPath.contains(HoodieTableMetaClient.MARKER_EXTN));
    String markerRootPath = Path.getPathWithoutSchemeAndAuthority(
        new Path(String.format("%s/%s/%s", basePath, HoodieTableMetaClient.TEMPFOLDER_NAME, instantTime))).toString();
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
   * @param fileSystem file system to use.
   * @param markerDir  marker directory.
   * @return {@code true} if the MARKERS.type file exists; {@code false} otherwise.
   */
  public static boolean doesMarkerTypeFileExist(FileSystem fileSystem, String markerDir) throws IOException {
    return fileSystem.exists(new Path(markerDir, MARKER_TYPE_FILENAME));
  }

  /**
   * Reads the marker type from `MARKERS.type` file.
   *
   * @param fileSystem file system to use.
   * @param markerDir  marker directory.
   * @return the marker type, or empty if the marker type file does not exist.
   */
  public static Option<MarkerType> readMarkerType(FileSystem fileSystem, String markerDir) {
    Path markerTypeFilePath = new Path(markerDir, MARKER_TYPE_FILENAME);
    FSDataInputStream fsDataInputStream = null;
    Option<MarkerType> content = Option.empty();
    try {
      if (!doesMarkerTypeFileExist(fileSystem, markerDir)) {
        return Option.empty();
      }
      fsDataInputStream = fileSystem.open(markerTypeFilePath);
      content = Option.of(MarkerType.valueOf(FileIOUtils.readAsUTFString(fsDataInputStream)));
    } catch (IOException e) {
      throw new HoodieIOException("Cannot read marker type file " + markerTypeFilePath.toString()
          + "; " + e.getMessage(), e);
    } finally {
      closeQuietly(fsDataInputStream);
    }
    return content;
  }

  /**
   * Writes the marker type to the file `MARKERS.type`.
   *
   * @param markerType marker type.
   * @param fileSystem file system to use.
   * @param markerDir  marker directory.
   */
  public static void writeMarkerTypeToFile(MarkerType markerType, FileSystem fileSystem, String markerDir) {
    Path markerTypeFilePath = new Path(markerDir, MARKER_TYPE_FILENAME);
    FSDataOutputStream fsDataOutputStream = null;
    BufferedWriter bufferedWriter = null;
    try {
      fsDataOutputStream = fileSystem.create(markerTypeFilePath, false);
      bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
      bufferedWriter.write(markerType.toString());
    } catch (IOException e) {
      throw new HoodieException("Failed to create marker type file " + markerTypeFilePath.toString()
          + "; " + e.getMessage(), e);
    } finally {
      closeQuietly(bufferedWriter);
      closeQuietly(fsDataOutputStream);
    }
  }

  /**
   * Deletes `MARKERS.type` file.
   *
   * @param fileSystem file system to use.
   * @param markerDir  marker directory.
   */
  public static void deleteMarkerTypeFile(FileSystem fileSystem, String markerDir) {
    Path markerTypeFilePath = new Path(markerDir, MARKER_TYPE_FILENAME);
    try {
      fileSystem.delete(markerTypeFilePath, false);
    } catch (IOException e) {
      throw new HoodieIOException("Cannot delete marker type file " + markerTypeFilePath.toString()
          + "; " + e.getMessage(), e);
    }
  }

  /**
   * Reads files containing the markers written by timeline-server-based marker mechanism.
   *
   * @param markerDir   marker directory.
   * @param fileSystem  file system to use.
   * @param context     instance of {@link HoodieEngineContext} to use
   * @param parallelism parallelism to use
   * @return A {@code Map} of file name to the set of markers stored in the file.
   */
  public static Map<String, Set<String>> readTimelineServerBasedMarkersFromFileSystem(
      String markerDir, FileSystem fileSystem, HoodieEngineContext context, int parallelism) {
    return readTimelineServerBasedMarkersFromFileSystem(markerDir, fileSystem, context, parallelism, false);
  }

  public static Map<String, Set<String>> readTimelineServerBasedMarkersFromFileSystem(
      String markerDir, FileSystem fileSystem, HoodieEngineContext context, int parallelism, boolean ignoreException) {
    Path dirPath = new Path(markerDir);
    try {
      if (fileSystem.exists(dirPath)) {
        Predicate<FileStatus> prefixFilter = fileStatus ->
            fileStatus.getPath().getName().startsWith(MARKERS_FILENAME_PREFIX);
        Predicate<FileStatus> markerTypeFilter = fileStatus ->
            !fileStatus.getPath().getName().equals(MARKER_TYPE_FILENAME);
        return FSUtils.parallelizeSubPathProcess(
            context, fileSystem, dirPath, parallelism, prefixFilter.and(markerTypeFilter),
            pairOfSubPathAndConf -> {
              String markersFilePathStr = pairOfSubPathAndConf.getKey();
              SerializableConfiguration conf = pairOfSubPathAndConf.getValue();
              return readMarkersFromFile(new Path(markersFilePathStr), conf, ignoreException);
            });
      }
      return new HashMap<>();
    } catch (IOException ioe) {
      if (ignoreException) {
        LOG.warn("IOException occurs during read TimelineServer based markers from fileSystem", ioe);
        return new HashMap<>();
      }

      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  /**
   * Reads the markers stored in the underlying file.
   *
   * @param markersFilePath file path for the markers
   * @param conf            serializable config
   * @return markers in a {@code Set} of String.
   */
  public static Set<String> readMarkersFromFile(Path markersFilePath, SerializableConfiguration conf) {
    return readMarkersFromFile(markersFilePath, conf, false);
  }

  public static Set<String> readMarkersFromFile(Path markersFilePath, SerializableConfiguration conf, boolean ignoreException) {
    FSDataInputStream fsDataInputStream = null;
    Set<String> markers = new HashSet<>();
    try {
      LOG.debug("Read marker file: " + markersFilePath);
      FileSystem fs = markersFilePath.getFileSystem(conf.get());
      fsDataInputStream = fs.open(markersFilePath);
      markers = new HashSet<>(FileIOUtils.readAsUTFStringLines(fsDataInputStream));
    } catch (IOException e) {
      if (ignoreException) {
        LOG.warn("IOException occurs during read MARKERS file, ", e);
      } else {
        throw new HoodieIOException("Failed to read MARKERS file " + markersFilePath, e);
      }
    } finally {
      closeQuietly(fsDataInputStream);
    }
    return markers;
  }
}