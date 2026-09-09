/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.bootstrap;

import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link RLIBootstrapOperator}.
 */
public class TestRLIBootstrapOperator {

  @TempDir
  File tempFile;

  @Test
  void testSkipPreloadForFreshTableWithoutMetadataTable() throws Exception {
    Configuration conf = getRLIConf();
    StreamerUtil.initTableIfNotExists(conf);

    try (OneInputStreamOperatorTestHarness<HoodieFlinkInternalRow, HoodieFlinkInternalRow> harness =
             new OneInputStreamOperatorTestHarness<>(new RLIBootstrapOperator(conf), 1, 1, 0)) {
      harness.open();

      assertEquals(0, harness.getOutput().size());
    }
  }

  @Test
  void testFailFastWhenMetadataTableIsMarkedAvailableButCannotBeLoaded() throws Exception {
    Configuration conf = getRLIConf();
    HoodieTableMetaClient metaClient = StreamerUtil.initTableIfNotExists(conf);
    metaClient.getTableConfig().setMetadataPartitionState(metaClient, MetadataPartitionType.FILES.getPartitionPath(), true);

    try (OneInputStreamOperatorTestHarness<HoodieFlinkInternalRow, HoodieFlinkInternalRow> harness =
             new OneInputStreamOperatorTestHarness<>(new RLIBootstrapOperator(conf), 1, 1, 0)) {
      RuntimeException error = assertThrows(RuntimeException.class, harness::open);

      assertEquals("Can not initialize the table metadata", error.getMessage());
    }
  }

  private Configuration getRLIConf() {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.METADATA_ENABLED, true);
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX.name());
    return conf;
  }
}
