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

package org.apache.hudi.index;

import org.apache.hudi.HoodieClientTestHarness;
import org.apache.hudi.config.HoodieHBaseIndexConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.bloom.HoodieBloomIndex;
import org.apache.hudi.index.hbase.HBaseIndex;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestHoodieIndex extends HoodieClientTestHarness {

  @Before
  public void setUp() throws Exception {
    initSparkContexts("TestHoodieIndex");
    initPath();
    initMetaClient();
  }

  @After
  public void tearDown() {
    cleanupSparkContexts();
    cleanupMetaClient();
  }

  @Test
  public void testCreateIndex() throws Exception {
    HoodieWriteConfig.Builder clientConfigBuilder = HoodieWriteConfig.newBuilder();
    HoodieIndexConfig.Builder indexConfigBuilder = HoodieIndexConfig.newBuilder();
    // Different types
    HoodieWriteConfig config = clientConfigBuilder.withPath(basePath)
        .withIndexConfig(indexConfigBuilder.withIndexType(HoodieIndex.IndexType.HBASE)
            .withHBaseIndexConfig(new HoodieHBaseIndexConfig.Builder().build()).build())
        .build();
    assertTrue(HoodieIndex.createIndex(config, jsc) instanceof HBaseIndex);
    config = clientConfigBuilder.withPath(basePath)
        .withIndexConfig(indexConfigBuilder.withIndexType(HoodieIndex.IndexType.INMEMORY).build()).build();
    assertTrue(HoodieIndex.createIndex(config, jsc) instanceof InMemoryHashIndex);
    config = clientConfigBuilder.withPath(basePath)
        .withIndexConfig(indexConfigBuilder.withIndexType(HoodieIndex.IndexType.BLOOM).build()).build();
    assertTrue(HoodieIndex.createIndex(config, jsc) instanceof HoodieBloomIndex);
  }
}
