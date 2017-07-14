/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.index;

import com.uber.hoodie.config.HoodieWriteConfig;

import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.index.bloom.HoodieBloomIndex;
import com.uber.hoodie.index.hbase.HBaseIndex;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestHoodieIndex {
    @Test
    public void testCreateIndex() throws Exception {
        HoodieWriteConfig.Builder clientConfigBuilder = HoodieWriteConfig.newBuilder();
        HoodieIndexConfig.Builder indexConfigBuilder = HoodieIndexConfig.newBuilder();
        // Different types
        HoodieWriteConfig config = clientConfigBuilder.withPath("")
            .withIndexConfig(indexConfigBuilder.withIndexType(HoodieIndex.IndexType.HBASE).build())
            .build();
        assertTrue(HoodieIndex.createIndex(config, null) instanceof HBaseIndex);
        config = clientConfigBuilder.withPath("").withIndexConfig(
            indexConfigBuilder.withIndexType(HoodieIndex.IndexType.INMEMORY).build()).build();
        assertTrue(HoodieIndex.createIndex(config, null) instanceof InMemoryHashIndex);
        config = clientConfigBuilder.withPath("")
            .withIndexConfig(indexConfigBuilder.withIndexType(HoodieIndex.IndexType.BLOOM).build())
            .build();
        assertTrue(HoodieIndex.createIndex(config, null) instanceof HoodieBloomIndex);
    }
}
