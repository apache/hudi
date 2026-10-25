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

package org.apache.hudi.common.bootstrap.index;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieTableQueryType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.core.read.BaseHoodieTableFileIndex;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.hudi.hadoop.fs.HadoopFSUtils.getStorageConf;

@ExtendWith(MockitoExtension.class)
class TestBaseHoodieTableFileIndex extends HoodieCommonTestHarness {

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testGetFileSlicesCount(boolean useSpillableMap) throws IOException {
    initMetaClient();
    TypedProperties properties = new TypedProperties();
    if (useSpillableMap) {
      properties.put(HoodieCommonConfig.HOODIE_FILE_INDEX_USE_SPILLABLE_MAP.key(), true);
    }
    BaseHoodieTableFileIndex baseHoodieTableFileIndex = new TestLocalIndex(
        new HoodieLocalEngineContext(getStorageConf()),
        metaClient,
        properties,
        HoodieTableQueryType.READ_OPTIMIZED,
        Collections.emptyList(),
        Option.empty(),
        false,
        false,
        true,
        null,
        true,
        Option.empty(),
        Option.empty()
    );
    Assertions.assertTrue(baseHoodieTableFileIndex.getFileSlicesCount() == 0);
  }

  /**
   * The metadata config the index builds is gated on a three-way conjunction, and both of the
   * added conjuncts came from behavior fixes: {@code isFilesPartitionAvailable} from HUDI-5403
   * (#7488, a Trino listing regression) and {@code useLatestBaseFilesPathFilterForListing} from
   * #18136. Nothing else in the repo asserts how that conjunction resolves.
   */
  @ParameterizedTest
  @CsvSource({
      "true,  true,  false, true",
      "true,  true,  true,  false",
      "true,  false, false, false",
      "true,  false, true,  false",
      "false, true,  false, false",
      "false, true,  true,  false",
      "false, false, false, false",
      "false, false, true,  false"
  })
  void testMetadataConfigIsEnabledOnlyWhenEveryConjunctHolds(boolean metadataEnabled,
                                                             boolean filesPartitionAvailable,
                                                             boolean useLatestBaseFilesPathFilterForListing,
                                                             boolean expectedEnabled) throws IOException {
    initMetaClient();
    if (filesPartitionAvailable) {
      metaClient.getTableConfig().setValue(
          HoodieTableConfig.TABLE_METADATA_PARTITIONS, HoodieTableMetadataUtil.PARTITION_NAME_FILES);
    }
    TypedProperties properties = new TypedProperties();
    properties.put(HoodieMetadataConfig.ENABLE.key(), String.valueOf(metadataEnabled));

    TestLocalIndex fileIndex = new TestLocalIndex(
        new HoodieLocalEngineContext(getStorageConf()),
        metaClient,
        properties,
        HoodieTableQueryType.READ_OPTIMIZED,
        Collections.emptyList(),
        Option.empty(),
        useLatestBaseFilesPathFilterForListing,
        false,
        true,
        null,
        true,
        Option.empty(),
        Option.empty()
    );

    Assertions.assertEquals(expectedEnabled, fileIndex.metadataConfig().isEnabled());
  }

  private static class TestLocalIndex extends BaseHoodieTableFileIndex {

    public TestLocalIndex(HoodieEngineContext engineContext, HoodieTableMetaClient metaClient, TypedProperties configProperties, HoodieTableQueryType queryType,
                          List<StoragePath> queryPaths, Option<String> specifiedQueryInstant, boolean useLatestBaseFilesPathFilterForListing,
                          boolean shouldIncludePendingCommits, boolean shouldValidateInstant,
                          FileStatusCache fileStatusCache, boolean shouldListLazily, Option<String> startCompletionTime, Option<String> endCompletionTime) {
      super(engineContext, metaClient, configProperties, queryType, queryPaths, specifiedQueryInstant, useLatestBaseFilesPathFilterForListing, shouldIncludePendingCommits, shouldValidateInstant,
          fileStatusCache, shouldListLazily, startCompletionTime, endCompletionTime);
    }

    HoodieMetadataConfig metadataConfig() {
      return metadataConfig;
    }

    @Override
    protected Object[] parsePartitionColumnValues(String[] partitionColumns, String partitionPath) {
      return new Object[0];
    }
  }
}