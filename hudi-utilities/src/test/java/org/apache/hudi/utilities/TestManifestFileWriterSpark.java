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

package org.apache.hudi.utilities;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieMetadataTestTable;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.sync.common.util.ManifestFileWriter;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.hudi.common.model.WriteOperationType.UPSERT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestManifestFileWriterSpark extends HoodieSparkClientTestHarness {

  protected HoodieTableType tableType;

  @BeforeEach
  public void setUp() throws IOException {
    this.tableType = HoodieTableType.COPY_ON_WRITE;
    initPath();
    initSparkContexts("TestHoodieMetadata");
    initHoodieStorage();
    initMetaClient(tableType);
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupResources();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testCreateManifestFile(boolean enableMetadata) throws Exception {
    HoodieWriteConfig writeConfig = getWriteConfig(basePath, enableMetadata);

    // Generate data files for 3 partitions.
    createTestDataForPartitionedTable(metaClient, enableMetadata, context, context.getStorageConf(), writeConfig);
    ManifestFileWriter manifestFileWriter = ManifestFileWriter.builder().setMetaClient(metaClient).build();
    manifestFileWriter.writeManifestFile(false);
    StoragePath manifestFilePath = manifestFileWriter.getManifestFilePath(false);
    try (InputStream is = metaClient.getStorage().open(manifestFilePath)) {
      List<String> expectedLines = FileIOUtils.readAsUTFStringLines(is);
      assertEquals(9, expectedLines.size(), "there should be 9 base files in total; 3 per partition.");
      expectedLines.forEach(line -> assertFalse(line.contains(basePath)));
    }
  }

  private static void createTestDataForPartitionedTable(HoodieTableMetaClient metaClient,
                                                        boolean enableMetadata, HoodieEngineContext context, StorageConfiguration storageConfiguration,
                                                        HoodieWriteConfig writeConfig) throws Exception {
    final String instantTime = "100";
    HoodieTestTable testTable = null;
    if (enableMetadata) {
      HoodieTableMetadataWriter metadataWriter = SparkHoodieBackedTableMetadataWriter.create(storageConfiguration, writeConfig, context);
      // reload because table configs could have been updated
      metaClient = HoodieTableMetaClient.reload(metaClient);
      testTable = HoodieMetadataTestTable.of(metaClient, metadataWriter, Option.of(context));
    } else {
      testTable = HoodieTestTable.of(metaClient);
    }
    doWriteOperation(testTable, instantTime);
  }

  private HoodieWriteConfig getWriteConfig(String basePath, boolean enableMetadata) {
    return HoodieWriteConfig.newBuilder().withPath(basePath)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(enableMetadata).build()).build();
  }

  protected static void doWriteOperation(HoodieTestTable testTable, String commitTime) throws Exception {
    doWriteOperation(testTable, commitTime, UPSERT);
  }

  protected static void doWriteOperation(HoodieTestTable testTable, String commitTime, WriteOperationType operationType) throws Exception {
    testTable.withPartitionMetaFiles("p1", "p2", "p3");
    testTable.doWriteOperation(commitTime, operationType, emptyList(), asList("p1", "p2", "p3"), 3);
  }
}
