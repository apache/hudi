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

package org.apache.hudi.table.upgrade;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.util.collection.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX_PREFIX;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestNineToEightDowngradeHandler extends BaseUpgradeDowngradeHandlerTest {
  private NineToEightDowngradeHandler downgradeHandler;

  @BeforeEach
  void setUp() {
    downgradeHandler = new NineToEightDowngradeHandler();
  }

  @Test
  void testDowngradeWithSecondaryIndexPartitions() {
    // Setup test data
    Set<String> metadataPartitions = createMetadataPartitions(true);
    setupMocks();
    when(tableConfig.getMetadataPartitions()).thenReturn(metadataPartitions);

    // Execute downgrade
    Pair<Map<ConfigProperty, String>, List<ConfigProperty>> result =
        downgradeHandler.downgrade(config, context, "001", upgradeDowngradeHelper);

    // Verify results
    verify(context).dropIndex(config, Arrays.asList(
        PARTITION_NAME_SECONDARY_INDEX_PREFIX + "idx1",
        PARTITION_NAME_SECONDARY_INDEX_PREFIX + "idx2"
    ));

    // Verify empty maps are returned
    assertTrue(result.getLeft().isEmpty());
    assertTrue(result.getRight().isEmpty());
  }

  @Test
  void testDowngradeWithoutSecondaryIndexPartitions() {
    // Setup test data
    Set<String> metadataPartitions = createMetadataPartitions(false);
    setupMocks();
    when(tableConfig.getMetadataPartitions()).thenReturn(metadataPartitions);

    // Execute downgrade
    Pair<Map<ConfigProperty, String>, List<ConfigProperty>> result =
        downgradeHandler.downgrade(config, context, "001", upgradeDowngradeHelper);

    // Verify no dropIndex was called since there are no secondary index partitions
    verify(context).dropIndex(config, Collections.emptyList());

    // Verify empty maps are returned
    assertTrue(result.getLeft().isEmpty());
    assertTrue(result.getRight().isEmpty());
  }
}
