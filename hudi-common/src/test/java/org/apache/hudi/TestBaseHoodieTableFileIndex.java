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

package org.apache.hudi;

import org.apache.hudi.BaseHoodieTableFileIndex.FileStatusCache;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieTableQueryType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

@ExtendWith(MockitoExtension.class)
class TestBaseHoodieTableFileIndex extends HoodieCommonTestHarness {

  @Mock
  private FileStatusCache fileStatusCache;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void refresh(boolean useSpillableMap) throws IOException {
    initMetaClient();
    TypedProperties properties = new TypedProperties();
    if (useSpillableMap) {
      properties.put(HoodieCommonConfig.HOODIE_FILE_INDEX_USE_SPILLABLE_MAP.key(), true);
    }
    BaseHoodieTableFileIndex baseHoodieTableFileIndex = new TestLocalIndex(
        new HoodieLocalEngineContext(new Configuration()),
        metaClient,
        properties,
        HoodieTableQueryType.READ_OPTIMIZED,
        Collections.emptyList(),
        Option.empty(),
        false,
        true,
        fileStatusCache,
        true,
        Option.empty(),
        Option.empty()
    );
    Assertions.assertTrue(baseHoodieTableFileIndex.getAllInputFileSlices().isEmpty());
    baseHoodieTableFileIndex.refresh();
    Assertions.assertEquals(0, baseHoodieTableFileIndex.getTotalCachedFilesSize());
    Assertions.assertTrue(baseHoodieTableFileIndex.getAllInputFileSlices().isEmpty());
  }

  private static class TestLocalIndex extends BaseHoodieTableFileIndex {

    public TestLocalIndex(HoodieEngineContext engineContext, HoodieTableMetaClient metaClient,
                          TypedProperties configProperties, HoodieTableQueryType queryType, List<Path> queryPaths,
                          Option<String> specifiedQueryInstant, boolean shouldIncludePendingCommits, boolean shouldValidateInstant, FileStatusCache fileStatusCache,
                          boolean shouldListLazily, Option<String> beginInstantTime, Option<String> endInstantTime) {
      super(engineContext, metaClient, configProperties, queryType, queryPaths, specifiedQueryInstant, shouldIncludePendingCommits, shouldValidateInstant, fileStatusCache, shouldListLazily,
          beginInstantTime, endInstantTime);
    }

    @Override
    protected Object[] doParsePartitionColumnValues(String[] partitionColumns, String partitionPath) {
      return new Object[0];
    }
  }
}