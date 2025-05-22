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

package org.apache.hudi.metadata.index.expression;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestExpressionIndexer {
  @Test
  public void testGetExpressionIndexPartitionsToInit() {
    // Mock meta client
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getIndexMetadata()).thenReturn(Option.empty());

    // Mock metadata partitions
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.getMetadataPartitions()).thenReturn(new HashSet<>(Collections.singleton("expr_index_idx_ts")));

    // Build metadata config
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true)
        .withExpressionIndexColumn("ts")
        .withExpressionIndexType("column_stats")
        .withExpressionIndexOptions(Collections.singletonMap("expr", "from_unixtime(ts, format='yyyy-MM-dd')"))
        .build();

    // Get partitions to init
    Set<String> result = ExpressionIndexer.getExpressionIndexPartitionsToInit(metadataConfig, metaClient);

    // Verify the result
    assertNotNull(result);
    assertTrue(result.isEmpty());
    verify(metaClient, atLeastOnce()).buildIndexDefinition(any());
  }
}
