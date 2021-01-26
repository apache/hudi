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

package org.apache.hudi.config;

import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.config.HoodieWriteConfig.Builder;

import org.apache.hudi.index.HoodieIndex;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieWriteConfig {

  @Test
  public void testPropertyLoading() throws IOException {
    Builder builder = HoodieWriteConfig.newBuilder().withPath("/tmp");
    Map<String, String> params = new HashMap<>(3);
    params.put(HoodieCompactionConfig.CLEANER_COMMITS_RETAINED_PROP, "1");
    params.put(HoodieCompactionConfig.MAX_COMMITS_TO_KEEP_PROP, "5");
    params.put(HoodieCompactionConfig.MIN_COMMITS_TO_KEEP_PROP, "2");
    ByteArrayOutputStream outStream = saveParamsIntoOutputStream(params);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(outStream.toByteArray());
    try {
      builder = builder.fromInputStream(inputStream);
    } finally {
      outStream.close();
      inputStream.close();
    }
    HoodieWriteConfig config = builder.build();
    assertEquals(5, config.getMaxCommitsToKeep());
    assertEquals(2, config.getMinCommitsToKeep());
  }

  @Test
  public void testDefaultIndexAccordingToEngineType() {
    // default bloom
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp").build();
    assertEquals(HoodieIndex.IndexType.BLOOM, writeConfig.getIndexType());

    // spark default bloom
    writeConfig = HoodieWriteConfig.newBuilder().withEngineType(EngineType.SPARK).withPath("/tmp").build();
    assertEquals(HoodieIndex.IndexType.BLOOM, writeConfig.getIndexType());

    // flink default in-memory
    writeConfig = HoodieWriteConfig.newBuilder().withEngineType(EngineType.FLINK).withPath("/tmp").build();
    assertEquals(HoodieIndex.IndexType.INMEMORY, writeConfig.getIndexType());
  }

  private ByteArrayOutputStream saveParamsIntoOutputStream(Map<String, String> params) throws IOException {
    Properties properties = new Properties();
    properties.putAll(params);
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    properties.store(outStream, "Saved on " + new Date(System.currentTimeMillis()));
    return outStream;
  }
}
