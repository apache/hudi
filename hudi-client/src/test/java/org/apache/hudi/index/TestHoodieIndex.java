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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.HoodieClientTestHarness;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieHBaseIndexConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.index.bloom.HoodieBloomIndex;
import org.apache.hudi.index.bloom.HoodieGlobalBloomIndex;
import org.apache.hudi.index.hbase.HBaseIndex;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieIndex extends HoodieClientTestHarness {

  private HoodieWriteConfig.Builder clientConfigBuilder;
  private HoodieIndexConfig.Builder indexConfigBuilder;

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts("TestHoodieIndex");
    initPath();
    initMetaClient();
    clientConfigBuilder = HoodieWriteConfig.newBuilder();
    indexConfigBuilder = HoodieIndexConfig.newBuilder();
  }

  @AfterEach
  public void tearDown() {
    cleanupSparkContexts();
    cleanupMetaClient();
  }

  @Test
  public void testCreateIndex() {
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
    config = clientConfigBuilder.withPath(basePath)
        .withIndexConfig(indexConfigBuilder.withIndexType(IndexType.GLOBAL_BLOOM).build()).build();
    assertTrue(HoodieIndex.createIndex(config, jsc) instanceof HoodieGlobalBloomIndex);

    config = clientConfigBuilder.withPath(basePath)
        .withIndexConfig(indexConfigBuilder.withIndexClass(DummyHoodieIndex.class.getName()).build()).build();
    assertTrue(HoodieIndex.createIndex(config, jsc) instanceof DummyHoodieIndex);
  }

  @Test
  public void testCreateIndex_withException() {
    final HoodieWriteConfig config1 = clientConfigBuilder.withPath(basePath)
        .withIndexConfig(indexConfigBuilder.withIndexClass(IndexWithConstructor.class.getName()).build()).build();
    final Throwable thrown1 = assertThrows(HoodieException.class, () -> {
      HoodieIndex.createIndex(config1, jsc);
    }, "exception is expected");
    assertTrue(thrown1.getMessage().contains("is not a subclass of HoodieIndex"));

    final HoodieWriteConfig config2 = clientConfigBuilder.withPath(basePath)
        .withIndexConfig(indexConfigBuilder.withIndexClass(IndexWithoutConstructor.class.getName()).build()).build();
    final Throwable thrown2 = assertThrows(HoodieException.class, () -> {
      HoodieIndex.createIndex(config2, jsc);
    }, "exception is expected");
    assertTrue(thrown2.getMessage().contains("Unable to instantiate class"));
  }

  public static class DummyHoodieIndex<T extends HoodieRecordPayload> extends HoodieIndex<T> {

    public DummyHoodieIndex(HoodieWriteConfig config) {
      super(config);
    }

    @Override
    public JavaPairRDD<HoodieKey, Option<Pair<String, String>>> fetchRecordLocation(JavaRDD<HoodieKey> hoodieKeys, JavaSparkContext jsc, HoodieTable<T> hoodieTable) {
      return null;
    }

    @Override
    public JavaRDD<HoodieRecord<T>> tagLocation(JavaRDD<HoodieRecord<T>> recordRDD, JavaSparkContext jsc, HoodieTable<T> hoodieTable) throws HoodieIndexException {
      return null;
    }

    @Override
    public JavaRDD<WriteStatus> updateLocation(JavaRDD<WriteStatus> writeStatusRDD, JavaSparkContext jsc, HoodieTable<T> hoodieTable) throws HoodieIndexException {
      return null;
    }

    @Override
    public boolean rollbackCommit(String instantTime) {
      return false;
    }

    @Override
    public boolean isGlobal() {
      return false;
    }

    @Override
    public boolean canIndexLogFiles() {
      return false;
    }

    @Override
    public boolean isImplicitWithStorage() {
      return false;
    }
  }

  public static class IndexWithConstructor {

    public IndexWithConstructor(HoodieWriteConfig config) {
    }
  }

  public static class IndexWithoutConstructor {

  }
}
