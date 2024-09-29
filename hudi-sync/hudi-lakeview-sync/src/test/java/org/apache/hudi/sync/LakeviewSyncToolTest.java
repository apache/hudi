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

package org.apache.hudi.sync;

import ai.onehouse.config.Config;
import ai.onehouse.config.models.configv1.Database;
import ai.onehouse.config.models.configv1.ParserConfig;
import ai.onehouse.metadata_extractor.TableDiscoveryAndUploadJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedConstruction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class LakeviewSyncToolTest {

  private static final String BASE_PATH = "/tmp/test";

  private Configuration hadoopConf;

  @BeforeEach
  public void setUp() {
    FileSystem fileSystem = HadoopFSUtils.getFs(BASE_PATH, new Configuration());
    hadoopConf = fileSystem.getConf();
  }

  private static Stream<Arguments> getArguments() {
    return Stream.of(Arguments.of("s3", false),
        Arguments.of("s3", true),
        Arguments.of("gcs", false),
        Arguments.of("gcs", true));
  }

  @ParameterizedTest
  @MethodSource("getArguments")
  void testSyncTool(String fileSystem, boolean failSyncing) throws IOException {
    List<ParserConfig> expectedParserConfigs = new ArrayList<>();
    expectedParserConfigs.add(ParserConfig.builder()
        .lake("lake-1")
        .databases(Arrays.asList(Database.builder()
                .name("database-1")
                .basePaths(Arrays.asList(fileSystem + "://user-bucket/lake-1/database-1/table-1",
                    fileSystem + "://user-bucket/lake-1/database-1/table-2"))
                .build(),
            Database.builder()
                .name("database-2")
                .basePaths(Arrays.asList(fileSystem + "://user-bucket/lake-1/database-2/table-1",
                    fileSystem + "://user-bucket/lake-1/database-2/table-2"))
                .build()))
        .build());
    expectedParserConfigs.add(ParserConfig.builder()
        .lake("lake-2")
        .databases(Collections.singletonList(Database.builder()
            .name("database-1")
            .basePaths(Arrays.asList(fileSystem + "://user-bucket/lake-2/database-1/table-1",
                fileSystem + "://user-bucket/lake-2/database-1/table-2"))
            .build()))
        .build());

    Properties properties = new Properties();
    properties.load(this.getClass().getResourceAsStream(String.format("/lakeview-sync-%s.properties", fileSystem)));
    try (MockedConstruction<TableDiscoveryAndUploadJob> mockedConstruction =
             mockConstruction(TableDiscoveryAndUploadJob.class, (tableDiscoveryAndUploadJob, context) -> {
               if (failSyncing) {
                 doThrow(new RuntimeException()).when(tableDiscoveryAndUploadJob).runOnce();
               } else {
                 doNothing().when(tableDiscoveryAndUploadJob).runOnce();
               }
             });
         LakeviewSyncTool lakeviewSyncTool = new LakeviewSyncTool(properties, hadoopConf)) {
      Config config = lakeviewSyncTool.getConfig();
      assertNotNull(config);
      assertEquals(new HashSet<>(expectedParserConfigs),
          new HashSet<>(config.getMetadataExtractorConfig().getParserConfig()));
      Optional<List<String>> pathExclusionPatterns = config.getMetadataExtractorConfig().getPathExclusionPatterns();
      assertTrue(pathExclusionPatterns.isPresent());
      assertEquals(2, pathExclusionPatterns.get().size());
      if (fileSystem.equals("s3")) {
        assertNotNull(config.getFileSystemConfiguration().getS3Config());
      } else {
        assertNotNull(config.getFileSystemConfiguration().getGcsConfig());
      }

      assertDoesNotThrow(lakeviewSyncTool::syncHoodieTable);

      List<TableDiscoveryAndUploadJob> constructedObjects = mockedConstruction.constructed();
      assertEquals(1, constructedObjects.size());
      TableDiscoveryAndUploadJob tableDiscoveryAndUploadJob = constructedObjects.get(0);
      verify(tableDiscoveryAndUploadJob, times(1)).runOnce();
    }
  }

  @Test
  void testSyncToolDisabled() {
    Properties properties = new Properties();
    try (LakeviewSyncTool lakeviewSyncTool = new LakeviewSyncTool(properties, hadoopConf)) {
      assertNull(lakeviewSyncTool.getConfig());
      // no explicit mocks needed as the syncing is disabled by default
      lakeviewSyncTool.syncHoodieTable();
    }
  }

  @Test
  void testSyncToolInvalidConfig() throws IOException {
    Properties properties = new Properties();
    properties.load(this.getClass().getResourceAsStream("/lakeview-sync-no-fs.properties"));
    try (LakeviewSyncTool ignored = new LakeviewSyncTool(properties, hadoopConf)) {
      fail("Exception is expected");
    } catch (IllegalArgumentException e) {
      assertEquals("Couldn't find any properties related to file system", e.getMessage());
    }
  }
}