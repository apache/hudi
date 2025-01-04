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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.fs.NoOpConsistencyGuard;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;
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
  protected Path basePath;
  protected HoodieWrapperFileSystem fs;
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
    basePath = new Path(tmpDir.toString(), UUID.randomUUID().toString());
    fs = new HoodieWrapperFileSystem(FSUtils.getFs(
        basePath, HoodieTestUtils.getDefaultHadoopConf()), new NoOpConsistencyGuard());
    when(metaClient.getBasePath()).thenReturn(basePath.toString());
    when(metaClient.getBasePathV2()).thenReturn(basePath);
    when(metaClient.getMarkerFolderPath(any()))
        .thenReturn(basePath + Path.SEPARATOR + TEMPFOLDER_NAME);
    when(metaClient.getFs()).thenReturn(fs);
    when(metaClient.getActiveTimeline()).thenReturn(timeline);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(config.getMarkersType()).thenReturn(MarkerType.DIRECT);
    Properties props = new Properties();
    props.put("hoodie.table.name", "test_table");
    HoodieTableMetaClient.initTableAndGetMetaClient(
        HoodieTestUtils.getDefaultHadoopConf(), metaClient.getBasePathV2().toString(), props);
  }

  protected Path createBaseFileToRollback(String partition,
                                          String fileId,
                                          String instantTime) throws IOException {
    Path baseFilePath = new Path(new Path(basePath, partition),
        FileCreateUtils.baseFileName(instantTime, fileId));
    if (!fs.exists(baseFilePath.getParent())) {
      fs.mkdirs(baseFilePath.getParent());
      // Add partition metafile so partition path listing works
      fs.create(new Path(baseFilePath.getParent(),
          HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX));
    }
    fs.create(baseFilePath).close();
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
            fs.create(new Path(new Path(basePath, partition), logFileName)).close();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          return logFileName;
        })
        .collect(Collectors.toMap(Function.identity(), e -> size));
  }
}
