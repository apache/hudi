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

package org.apache.hudi;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.hudi.common.table.log.HoodieLogFormat.DEFAULT_WRITE_TOKEN;
import static org.apache.hudi.common.testutils.HoodieTestUtils.COMMIT_METADATA_SER_DE;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR;

@Slf4j
public class HoodieTestCommitGenerator {
  public static final String BASE_FILE_WRITE_TOKEN = "1-0-1";
  public static final String LOG_FILE_WRITE_TOKEN = DEFAULT_WRITE_TOKEN;

  public static void initCommitInfoForRepairTests(
      Map<String, List<Pair<String, String>>> baseFileInfo,
      Map<String, List<Pair<String, String>>> logFileInfo) {
    baseFileInfo.clear();
    logFileInfo.clear();
    baseFileInfo.put("000", CollectionUtils.createImmutableList(
        new ImmutablePair<>("2022/01/01", UUID.randomUUID().toString()),
        new ImmutablePair<>("2022/01/02", UUID.randomUUID().toString()),
        new ImmutablePair<>("2022/01/03", UUID.randomUUID().toString())
    ));
    baseFileInfo.put("001", CollectionUtils.createImmutableList(
        new ImmutablePair<>("2022/01/04", UUID.randomUUID().toString()),
        new ImmutablePair<>("2022/01/05", UUID.randomUUID().toString())
    ));
    baseFileInfo.put("002", CollectionUtils.createImmutableList(
        new ImmutablePair<>("2022/01/06", UUID.randomUUID().toString())
    ));
    logFileInfo.put("001", CollectionUtils.createImmutableList(
        new ImmutablePair<>("2022/01/03", UUID.randomUUID().toString()),
        new ImmutablePair<>("2022/01/06", UUID.randomUUID().toString())
    ));
  }

  public static void setupTimelineInFS(
      String basePath,
      Map<String, List<Pair<String, String>>> baseFileInfo,
      Map<String, List<Pair<String, String>>> logFileInfo,
      Map<String, Map<String, List<Pair<String, String>>>> instantInfoMap) throws IOException {
    instantInfoMap.clear();
    for (String instantTime : baseFileInfo.keySet()) {
      Map<String, List<Pair<String, String>>> partitionPathToFileIdAndNameMap = new HashMap<>();
      baseFileInfo.getOrDefault(instantTime, new ArrayList<>())
          .forEach(e -> {
            List<Pair<String, String>> fileInfoList = partitionPathToFileIdAndNameMap
                .computeIfAbsent(e.getKey(), k -> new ArrayList<>());
            String fileId = e.getValue();
            fileInfoList.add(new ImmutablePair<>(fileId, getBaseFilename(instantTime, fileId)));
          });
      logFileInfo.getOrDefault(instantTime, new ArrayList<>())
          .forEach(e -> {
            List<Pair<String, String>> fileInfoList = partitionPathToFileIdAndNameMap
                .computeIfAbsent(e.getKey(), k -> new ArrayList<>());
            String fileId = e.getValue();
            fileInfoList.add(new ImmutablePair<>(fileId, getLogFilename(instantTime, fileId)));
          });
      createCommitAndDataFiles(basePath, instantTime, partitionPathToFileIdAndNameMap);
      instantInfoMap.put(instantTime, partitionPathToFileIdAndNameMap);
    }
  }

  public static String getBaseFilename(String instantTime, String fileId) {
    return FSUtils.makeBaseFileName(instantTime, BASE_FILE_WRITE_TOKEN, fileId, HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().getFileExtension());
  }

  public static String getLogFilename(String instantTime, String fileId) {
    return FSUtils.makeLogFileName(
        fileId, HoodieFileFormat.HOODIE_LOG.getFileExtension(), instantTime, 1, LOG_FILE_WRITE_TOKEN);
  }

  public static void createCommitAndDataFiles(
      String basePath, String instantTime,
      Map<String, List<Pair<String, String>>> partitionPathToFileIdAndNameMap) throws IOException {
    String commitFilename = INSTANT_FILE_NAME_GENERATOR.makeCommitFileName(instantTime + "_" + InProcessTimeGenerator.createNewInstantTime());
    HoodieCommitMetadata commitMetadata =
        generateCommitMetadata(partitionPathToFileIdAndNameMap, Collections.emptyMap());
    createCommitFileWithMetadata(basePath, new HadoopStorageConfiguration(true), commitFilename, commitMetadata);
    for (String partitionPath : partitionPathToFileIdAndNameMap.keySet()) {
      createPartitionMetaFile(basePath, partitionPath);
      partitionPathToFileIdAndNameMap.get(partitionPath)
          .forEach(fileInfo -> {
            String filename = fileInfo.getValue();
            try {
              createDataFile(basePath, new HadoopStorageConfiguration(true), partitionPath, filename);
            } catch (IOException e) {
              log.error(String.format("Failed to create data file: %s/%s/%s",
                  basePath, partitionPath, filename));
            }
          });
    }
  }

  public static HoodieCommitMetadata generateCommitMetadata(
      Map<String, List<Pair<String, String>>> partitionPathToFileIdAndNameMap,
      Map<String, String> extraMetadata) {
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    for (Map.Entry<String, String> entry : extraMetadata.entrySet()) {
      metadata.addMetadata(entry.getKey(), entry.getValue());
    }
    partitionPathToFileIdAndNameMap.forEach((partitionPath, fileInfoList) ->
        fileInfoList.forEach(fileInfo -> {
          HoodieWriteStat writeStat = new HoodieWriteStat();
          writeStat.setPartitionPath(partitionPath);
          writeStat.setPath(new StoragePath(partitionPath, fileInfo.getValue()).toString());
          writeStat.setFileId(fileInfo.getKey());
          // Below are dummy values
          writeStat.setTotalWriteBytes(10000);
          writeStat.setPrevCommit("000");
          writeStat.setNumWrites(10);
          writeStat.setNumUpdateWrites(15);
          writeStat.setTotalLogBlocks(2);
          writeStat.setTotalLogRecords(100);
          metadata.addWriteStat(partitionPath, writeStat);
        }));
    return metadata;
  }

  public static <T> void createCommitFileWithMetadata(
      String basePath, StorageConfiguration<?> storageConf, String filename, T metadata) throws IOException {
    HoodieStorage storage = HoodieStorageUtils.getStorage(basePath, storageConf);
    StoragePath commitFilePath = new StoragePath(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + HoodieTableMetaClient.TIMELINEFOLDER_NAME + "/" + filename);
    try (OutputStream os = storage.create(commitFilePath, true)) {
      COMMIT_METADATA_SER_DE.getInstantWriter(metadata).get().writeToStream(os);
    }
  }

  public static void createDataFile(
      String basePath, StorageConfiguration<?> storageConf,
      String partitionPath, String filename) throws IOException {
    HoodieStorage storage = HoodieStorageUtils.getStorage(basePath, storageConf);
    StoragePath filePath = new StoragePath(new StoragePath(basePath, partitionPath), filename);
    StoragePath parent = filePath.getParent();
    if (!storage.exists(parent)) {
      storage.createDirectory(parent);
    }
    if (!storage.exists(filePath)) {
      storage.create(filePath);
    }
  }

  public static void createPartitionMetaFile(String basePath, String partitionPath) throws IOException {
    java.nio.file.Path metaFilePath;
    try {
      java.nio.file.Path parentPath = Paths.get(new URI(basePath).getPath(), partitionPath);
      Files.createDirectories(parentPath);
      metaFilePath = parentPath.resolve(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX);
      if (Files.notExists(metaFilePath)) {
        Files.createFile(metaFilePath);
      }
    } catch (URISyntaxException e) {
      throw new HoodieException("Error creating partition meta file", e);
    }
  }
}
