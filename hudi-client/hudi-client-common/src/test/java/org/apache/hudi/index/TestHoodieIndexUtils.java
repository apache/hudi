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

package org.apache.hudi.index;

import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.MetadataPartitionType;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHoodieIndexUtils {

  @Test
  public void testGetFunctionalIndexPath() {
    MetadataPartitionType partitionType = MetadataPartitionType.FUNCTIONAL_INDEX;
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    String indexName = "testIndex";

    Map<String, HoodieIndexDefinition> indexDefinitions = new HashMap<>();
    indexDefinitions.put(
        indexName,
        new HoodieIndexDefinition("func_index_testIndex", "column_stats", "lower", Collections.singletonList("name"), null));
    HoodieIndexMetadata indexMetadata = new HoodieIndexMetadata(indexDefinitions);
    when(metaClient.getIndexMetadata()).thenReturn(Option.of(indexMetadata));

    String result = HoodieIndexUtils.getPartitionNameFromPartitionType(partitionType, metaClient, indexName);
    assertEquals("func_index_testIndex", result);
  }

  @Test
  public void testGetNonFunctionalIndexPath() {
    MetadataPartitionType partitionType = MetadataPartitionType.COLUMN_STATS;
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);

    String result = HoodieIndexUtils.getPartitionNameFromPartitionType(partitionType, metaClient, null);
    assertEquals(partitionType.getPartitionPath(), result);
  }

  @Test
  public void testExceptionForMissingFunctionalIndexMetadata() {
    MetadataPartitionType partitionType = MetadataPartitionType.FUNCTIONAL_INDEX;
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getIndexMetadata()).thenReturn(Option.empty());

    assertThrows(IllegalArgumentException.class,
        () -> HoodieIndexUtils.getPartitionNameFromPartitionType(partitionType, metaClient, "testIndex"));
  }
}
