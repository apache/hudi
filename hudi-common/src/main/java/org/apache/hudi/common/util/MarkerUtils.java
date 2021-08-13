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
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.FileIOUtils.closeQuietly;

/**
 * A utility class for marker related operations.
 */
public class MarkerUtils {
  public static final String MARKERS_FILENAME_PREFIX = "MARKERS";
  public static final String MARKER_TYPE_FILENAME = MARKERS_FILENAME_PREFIX + ".type";
  private static final Logger LOG = LogManager.getLogger(MarkerUtils.class);

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
    String content = null;
    try {
      if (!fileSystem.exists(markerTypeFilePath)) {
        return Option.empty();
      }
      fsDataInputStream = fileSystem.open(markerTypeFilePath);
      content = FileIOUtils.readAsUTFString(fsDataInputStream);
    } catch (IOException e) {
      throw new HoodieIOException("Cannot read marker type file " + markerTypeFilePath.toString()
          + "; " + e.getMessage(), e);
    } finally {
      closeQuietly(fsDataInputStream);
    }

    if (content != null) {
      return Option.of(MarkerType.valueOf(content));
    }
    return Option.empty();
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
    Path dirPath = new Path(markerDir);
    try {
      if (fileSystem.exists(dirPath)) {
        FileStatus[] fileStatuses = fileSystem.listStatus(dirPath);
        Predicate<String> prefixFilter = pathStr -> pathStr.contains(MARKERS_FILENAME_PREFIX);
        Predicate<String> filenameFilter = pathStr -> !pathStr.equals(MARKER_TYPE_FILENAME);
        List<String> markerDirSubPaths = Arrays.stream(fileStatuses)
            .map(fileStatus -> fileStatus.getPath().toString())
            .filter(prefixFilter.and(filenameFilter))
            .collect(Collectors.toList());

        if (markerDirSubPaths.size() > 0) {
          SerializableConfiguration conf = new SerializableConfiguration(fileSystem.getConf());
          int actualParallelism = Math.min(markerDirSubPaths.size(), parallelism);
          return context.mapToPair(markerDirSubPaths, markersFilePathStr -> {
            Path markersFilePath = new Path(markersFilePathStr);
            FileSystem fs = markersFilePath.getFileSystem(conf.get());
            FSDataInputStream fsDataInputStream = null;
            BufferedReader bufferedReader = null;
            Set<String> markers = new HashSet<>();
            try {
              LOG.debug("Read marker file: " + markersFilePathStr);
              fsDataInputStream = fs.open(markersFilePath);
              bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream, StandardCharsets.UTF_8));
              markers = bufferedReader.lines().collect(Collectors.toSet());
            } catch (IOException e) {
              throw new HoodieIOException("Failed to read file " + markersFilePathStr, e);
            } finally {
              closeQuietly(bufferedReader);
              closeQuietly(fsDataInputStream);
            }
            return new ImmutablePair<>(markersFilePathStr, markers);
          }, actualParallelism);
        }
      }
      return new HashMap<>();
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }
}
