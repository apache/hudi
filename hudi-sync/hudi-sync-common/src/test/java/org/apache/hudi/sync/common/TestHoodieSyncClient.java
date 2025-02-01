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

package org.apache.hudi.sync.common;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.sync.common.model.PartitionValueExtractor;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class TestHoodieSyncClient {
  @Test
  void partitionValueExtractorLoaded() {
    // extractor is loaded from config
    Properties properties = new Properties();
    properties.put("hoodie.datasource.hive_sync.partition_extractor_class", DummyExtractor.class.getName());
    HoodieSyncConfig config = new HoodieSyncConfig(properties, new Configuration());
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    try (DummySyncClient client = new DummySyncClient(config, metaClient)) {
      assertSame(metaClient, client.getMetaClient());
      assertTrue(client.getPartitionValueExtractor() instanceof DummyExtractor);
    }
  }

  private static class DummySyncClient extends HoodieSyncClient {

    protected DummySyncClient(HoodieSyncConfig config, HoodieTableMetaClient metaClient) {
      super(config, metaClient);
    }

    PartitionValueExtractor getPartitionValueExtractor() {
      return partitionValueExtractor;
    }

    @Override
    public void close() {
      // Do nothing
    }
  }

  public static class DummyExtractor implements PartitionValueExtractor {

    @Override
    public List<String> extractPartitionValuesInPath(String partitionPath) {
      return Collections.emptyList();
    }
  }
}
