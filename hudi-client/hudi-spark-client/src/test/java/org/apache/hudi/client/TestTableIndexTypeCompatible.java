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

package org.apache.hudi.client;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion.VERSION_1;

public class TestTableIndexTypeCompatible extends HoodieClientTestBase {

  private void createTableWithPersisIndexType(String indexType) throws IOException {
    // Create the table
    HoodieTableMetaClient.initTableType(metaClient.getHadoopConf(), metaClient.getBasePath(),
        HoodieTableType.MERGE_ON_READ, metaClient.getTableConfig().getTableName(),
        metaClient.getArchivePath(), metaClient.getTableConfig().getPayloadClass(), VERSION_1, indexType);
  }

  private SparkRDDWriteClient getWriteClientWithIndexType(IndexType indexType) {
    HoodieWriteConfig hoodieWriteConfig = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType).build())
        .build();
    SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig, false);
    return client;
  }

  private static Iterable<Object[]> indexTypeCompatibleParameter() {
    return Arrays.asList(new Object[][] { { "BLOOM", "BLOOM" }, { "GLOBAL_BLOOM", "SIMPLE" }, { "GLOBAL_BLOOM", "BLOOM" } });
  }

  private static Iterable<Object[]> indexTypeNotCompatibleParameter() {
    return Arrays.asList(new Object[][] { { "SIMPLE", "BLOOM"}, { "SIMPLE", "GLOBAL_BLOOM"},
        { "BLOOM", "GLOBAL_BLOOM"}});
  }

  @ParameterizedTest
  @MethodSource("indexTypeCompatibleParameter")
  public void testTableIndexTypeCompatible(String persistIndexType, String writeIndexType) throws Exception {
    createTableWithPersisIndexType(persistIndexType);
    SparkRDDWriteClient client = getWriteClientWithIndexType(IndexType.valueOf(writeIndexType));
    assertDoesNotThrow(() -> {
      client.createMetaClient(true);
    }, "");
  }

  @ParameterizedTest
  @MethodSource("indexTypeNotCompatibleParameter")
  public void testTableIndexTypeNotCompatible(String persistIndexType, String writeIndexType) throws Exception {
    createTableWithPersisIndexType(persistIndexType);
    SparkRDDWriteClient client = getWriteClientWithIndexType(IndexType.valueOf(writeIndexType));
    assertThrows(HoodieException.class, () -> {
      client.createMetaClient(true);
    }, "");
  }
}
