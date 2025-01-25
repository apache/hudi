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

package org.apache.hudi.common.testutils;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Utils for creating dummy Hudi files in testing.
 */
public class FileCreateUtilsBase {

  protected static final Logger LOG = LoggerFactory.getLogger(FileCreateUtilsBase.class);

  protected static final String WRITE_TOKEN = "1-0-1";
  protected static final String BASE_FILE_EXTENSION = HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().getFileExtension();
  /** An empty byte array */
  public static final byte[] EMPTY_BYTES = new byte[0];

  public static String baseFileName(String instantTime, String fileId) {
    return baseFileName(instantTime, fileId, BASE_FILE_EXTENSION);
  }

  public static String baseFileName(String instantTime, String fileId, String fileExtension) {
    return FSUtils.makeBaseFileName(instantTime, WRITE_TOKEN, fileId, fileExtension);
  }

  public static String logFileName(String instantTime, String fileId, int version) {
    return logFileName(instantTime, fileId, version,
        HoodieFileFormat.HOODIE_LOG.getFileExtension());
  }

  public static String logFileName(String instantTime, String fileId, int version,
                                   String fileExtension) {
    return FSUtils.makeLogFileName(fileId, fileExtension, instantTime, version, WRITE_TOKEN);
  }

  public static String markerFileName(String fileName, IOType ioType) {
    return String.format("%s%s.%s", fileName, HoodieTableMetaClient.MARKER_EXTN, ioType.name());
  }

  public static String markerFileName(String instantTime, String fileId, IOType ioType,
                                      String fileExtension) {
    return markerFileName(instantTime, fileId, ioType, fileExtension, WRITE_TOKEN);
  }

  public static String markerFileName(String instantTime, String fileId, IOType ioType,
                                      String fileExtension, String writeToken) {
    return String.format("%s_%s_%s%s%s.%s", fileId, writeToken, instantTime, fileExtension,
        HoodieTableMetaClient.MARKER_EXTN, ioType);
  }

  public static boolean isBaseOrLogFilename(String filename) {
    for (HoodieFileFormat format : HoodieFileFormat.values()) {
      if (filename.contains(format.getFileExtension())) {
        return true;
      }
    }
    return false;
  }

  public static URI createPartitionMetaFile(String basePath, String partitionPath) throws IOException {
    Path metaFilePath;
    try {
      Path parentPath = Paths.get(new URI(basePath).getPath(), partitionPath);
      Files.createDirectories(parentPath);
      metaFilePath = parentPath.resolve(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX);
      if (Files.notExists(metaFilePath)) {
        Files.createFile(metaFilePath);
      }
      return metaFilePath.toUri();
    } catch (URISyntaxException e) {
      throw new HoodieException("Error creating partition meta file", e);
    }
  }

  protected static void createMetaFileInMetaPath(StoragePath metaPath, String instantTime, String suffix,
                                                 HoodieStorage storage, boolean preTableVersion8) throws IOException {
    if (!storage.exists(metaPath)) {
      storage.create(metaPath).close();
    }

    if (suffix.contains(HoodieTimeline.INFLIGHT_EXTENSION)
        || suffix.contains(HoodieTimeline.REQUESTED_EXTENSION)) {
      StoragePath metaFilePath = new StoragePath(metaPath, instantTime + suffix);
      if (!storage.exists(metaFilePath)) {
        storage.create(metaFilePath).close();
      }
    } else {
      String instantTimeWithCompletionTime =
          preTableVersion8 ? instantTime : instantTime + "_" + InProcessTimeGenerator.createNewInstantTime();
      storage.create(new StoragePath(metaPath, instantTimeWithCompletionTime + suffix))
          .close();
    }
  }

  protected static void removeMetaFileInTimelinePath(String timelinePath, String instantTime, String suffix) throws IOException {
    try {
      Path parentPath = Paths.get(new StoragePath(timelinePath).makeQualified(new URI("file:///")).toUri());

      if (suffix.contains(HoodieTimeline.INFLIGHT_EXTENSION) || suffix.contains(HoodieTimeline.REQUESTED_EXTENSION)) {
        Path metaFilePath = parentPath.resolve(instantTime + suffix);
        if (Files.exists(metaFilePath)) {
          Files.delete(metaFilePath);
        }
      } else {
        if (Files.exists(parentPath)) {
          try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(parentPath, instantTime + "*" + suffix)) {
            Iterator<Path> iterator = dirStream.iterator();
            // The instant file exists
            if (iterator.hasNext()) {
              // doesn't contains completion time
              Files.delete(iterator.next());
            }
          }
        }
      }
    } catch (Exception ex) {
      throw new HoodieException(ex);
    }
  }

  protected static void deleteMetaFileInTimeline(String timelinePath, String instantTime, String suffix,
                                                 HoodieStorage storage) throws IOException {
    StoragePath parentPath = new StoragePath(timelinePath);

    if (suffix.contains(HoodieTimeline.INFLIGHT_EXTENSION)
        || suffix.contains(HoodieTimeline.REQUESTED_EXTENSION)) {
      StoragePath metaFilePath = new StoragePath(parentPath, instantTime + suffix);
      if (storage.exists(metaFilePath)) {
        storage.deleteFile(metaFilePath);
      }
    } else {
      StoragePath metaFilePath =
          new StoragePath(parentPath, instantTime + "*" + suffix);
      List<StoragePathInfo> pathInfoList = storage.globEntries(metaFilePath);
      if (pathInfoList.size() != 0) {
        storage.deleteFile(pathInfoList.get(0).getPath());
      }
    }
  }

  /**
   * Find total basefiles for passed in paths.
   */
  public static Map<String, Long> getBaseFileCountsForPaths(String basePath, HoodieStorage storage,
                                                            String... paths) {
    Map<String, Long> toReturn = new HashMap<>();
    try {
      HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(
          storage.getConf(), basePath);
      for (String path : paths) {
        TableFileSystemView.BaseFileOnlyView fileSystemView =
            new HoodieTableFileSystemView(metaClient,
                metaClient.getCommitsTimeline().filterCompletedInstants(),
                storage.globEntries(new StoragePath(path)));
        toReturn.put(path, fileSystemView.getLatestBaseFiles().count());
      }
      return toReturn;
    } catch (Exception e) {
      throw new HoodieException("Error reading hoodie table as a dataframe", e);
    }
  }
}
