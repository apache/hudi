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

import org.apache.hudi.config.HoodieHBaseIndexConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.index.bloom.HoodieBloomIndex;
import org.apache.hudi.index.bloom.HoodieGlobalBloomIndex;
import org.apache.hudi.index.bucket.HoodieBucketIndex;
import org.apache.hudi.index.hbase.SparkHoodieHBaseIndex;
import org.apache.hudi.index.inmemory.HoodieInMemoryHashIndex;
import org.apache.hudi.index.simple.HoodieSimpleIndex;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("checkstyle:LineLength")
public class TestHoodieIndexConfigs {

  private String basePath;

  @BeforeEach
  public void setUp(@TempDir Path tempDir) {
    basePath = tempDir.toString();
  }

  @ParameterizedTest
  @EnumSource(value = IndexType.class, names = {"BLOOM", "GLOBAL_BLOOM", "SIMPLE", "GLOBAL_SIMPLE", "HBASE", "BUCKET"})
  public void testCreateIndex(IndexType indexType) {
    HoodieWriteConfig config;
    HoodieWriteConfig.Builder clientConfigBuilder = HoodieWriteConfig.newBuilder();
    HoodieIndexConfig.Builder indexConfigBuilder = HoodieIndexConfig.newBuilder();
    switch (indexType) {
      case INMEMORY:
        config = clientConfigBuilder.withPath(basePath)
            .withIndexConfig(indexConfigBuilder.withIndexType(HoodieIndex.IndexType.INMEMORY).build()).build();
        assertTrue(SparkHoodieIndexFactory.createIndex(config) instanceof HoodieInMemoryHashIndex);
        break;
      case BLOOM:
        config = clientConfigBuilder.withPath(basePath)
            .withIndexConfig(indexConfigBuilder.withIndexType(HoodieIndex.IndexType.BLOOM).build()).build();
        assertTrue(SparkHoodieIndexFactory.createIndex(config) instanceof HoodieBloomIndex);
        break;
      case GLOBAL_BLOOM:
        config = clientConfigBuilder.withPath(basePath)
            .withIndexConfig(indexConfigBuilder.withIndexType(IndexType.GLOBAL_BLOOM).build()).build();
        assertTrue(SparkHoodieIndexFactory.createIndex(config) instanceof HoodieGlobalBloomIndex);
        break;
      case SIMPLE:
        config = clientConfigBuilder.withPath(basePath)
            .withIndexConfig(indexConfigBuilder.withIndexType(IndexType.SIMPLE).build()).build();
        assertTrue(SparkHoodieIndexFactory.createIndex(config) instanceof HoodieSimpleIndex);
        break;
      case HBASE:
        config = clientConfigBuilder.withPath(basePath)
            .withIndexConfig(indexConfigBuilder.withIndexType(HoodieIndex.IndexType.HBASE)
                .withHBaseIndexConfig(new HoodieHBaseIndexConfig.Builder().build()).build())
            .build();
        assertTrue(SparkHoodieIndexFactory.createIndex(config) instanceof SparkHoodieHBaseIndex);
        break;
      case BUCKET:
        config = clientConfigBuilder.withPath(basePath)
            .withIndexConfig(indexConfigBuilder.withIndexType(IndexType.BUCKET).build()).build();
        assertTrue(SparkHoodieIndexFactory.createIndex(config) instanceof HoodieBucketIndex);
        break;
      default:
        // no -op. just for checkstyle errors
    }
  }

  @Test
  public void testCreateIndexWithException() {
    HoodieWriteConfig.Builder clientConfigBuilder = HoodieWriteConfig.newBuilder();
    HoodieIndexConfig.Builder indexConfigBuilder = HoodieIndexConfig.newBuilder();
    final HoodieWriteConfig config1 = clientConfigBuilder.withPath(basePath)
        .withIndexConfig(indexConfigBuilder.withIndexClass(IndexWithConstructor.class.getName()).build()).build();
    final Throwable thrown1 = assertThrows(HoodieException.class, () -> {
      SparkHoodieIndexFactory.createIndex(config1);
    }, "exception is expected");
    assertTrue(thrown1.getMessage().contains("is not a subclass of HoodieIndex"));

    final HoodieWriteConfig config2 = clientConfigBuilder.withPath(basePath)
        .withIndexConfig(indexConfigBuilder.withIndexClass(IndexWithoutConstructor.class.getName()).build()).build();
    final Throwable thrown2 = assertThrows(HoodieException.class, () -> {
      SparkHoodieIndexFactory.createIndex(config2);
    }, "exception is expected");
    assertTrue(thrown2.getMessage().contains("Unable to instantiate class"));
  }

  public static class IndexWithConstructor {

    public IndexWithConstructor(HoodieWriteConfig config) {
    }
  }

  public static class IndexWithoutConstructor {

  }
}
