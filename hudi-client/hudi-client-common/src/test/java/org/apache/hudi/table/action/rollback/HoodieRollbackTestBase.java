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

package org.apache.hudi.table.action.rollback;

import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.table.HoodieTableMetaClient.TEMPFOLDER_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class HoodieRollbackTestBase {
  @TempDir
  java.nio.file.Path tmpDir;
  protected StoragePath basePath;
  protected HoodieStorage storage;
  @Mock
  protected HoodieTable table;
  @Mock
  protected HoodieTableConfig tableConfig;
  @Mock
  protected HoodieTableMetaClient metaClient;
  @Mock
  protected HoodieActiveTimeline timeline;
  @Mock
  protected HoodieWriteConfig config;

  @BeforeEach
  void setup() throws IOException {
    MockitoAnnotations.openMocks(this);
    when(table.getMetaClient()).thenReturn(metaClient);
    basePath = new StoragePath(tmpDir.toString(), UUID.randomUUID().toString());
    storage = HoodieTestUtils.getStorage(basePath);
    when(table.getStorage()).thenReturn(storage);
    when(metaClient.getBasePath()).thenReturn(basePath);
    when(metaClient.getTempFolderPath())
        .thenReturn(new StoragePath(basePath, TEMPFOLDER_NAME).toString());
    when(metaClient.getMarkerFolderPath(any()))
        .thenReturn(basePath + Path.SEPARATOR + TEMPFOLDER_NAME);
    when(metaClient.getStorage()).thenReturn(storage);
    when(metaClient.getActiveTimeline()).thenReturn(timeline);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(config.getMarkersType()).thenReturn(MarkerType.DIRECT);
    Properties props = new Properties();
    props.put("hoodie.table.name", "test_table");
    props.put(HoodieTableConfig.TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
    HoodieTableMetaClient.newTableBuilder()
        .fromProperties(props)
        .initTable(storage.getConf(), metaClient.getBasePath());
  }

  protected void prepareMetaClient(HoodieTableVersion tableVersion) {
    when(tableConfig.getTableVersion()).thenReturn(tableVersion);
    when(table.version()).thenReturn(tableVersion);
    if (tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
      when(metaClient.getTimelinePath()).thenReturn(
          new StoragePath(basePath, HoodieTableMetaClient.METAFOLDER_NAME));
    } else {
      when(metaClient.getTimelinePath()).thenReturn(new StoragePath(
          new StoragePath(basePath, HoodieTableMetaClient.METAFOLDER_NAME), "timeline"));
    }
  }

  protected StoragePath createBaseFileToRollback(String partition,
                                                 String fileId,
                                                 String instantTime) throws IOException {
    StoragePath baseFilePath = new StoragePath(new StoragePath(basePath, partition),
        FileCreateUtils.baseFileName(instantTime, fileId));
    if (!storage.exists(baseFilePath.getParent())) {
      storage.createDirectory(baseFilePath.getParent());
      // Add partition metafile so partition path listing works
      storage.create(new StoragePath(baseFilePath.getParent(),
          HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX));
    }
    storage.create(baseFilePath).close();
    return baseFilePath;
  }

  protected Map<String, Long> createLogFilesToRollback(String partition,
                                                       String fileId,
                                                       String instantTime,
                                                       IntStream logVersions,
                                                       long size) {
    return logVersions.boxed()
        .map(version -> {
          String logFileName = FileCreateUtils.logFileName(instantTime, fileId, version);
          try {
            storage.create(new StoragePath(new StoragePath(basePath, partition), logFileName)).close();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          return logFileName;
        })
        .collect(Collectors.toMap(Function.identity(), e -> size));
  }
}

