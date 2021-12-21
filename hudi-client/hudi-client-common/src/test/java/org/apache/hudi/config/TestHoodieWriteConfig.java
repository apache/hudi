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
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.config.HoodieWriteConfig.Builder;
import org.apache.hudi.index.HoodieIndex;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieWriteConfig {

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testPropertyLoading(boolean withAlternative) throws IOException {
    Builder builder = HoodieWriteConfig.newBuilder().withPath("/tmp");
    Map<String, String> params = new HashMap<>(3);
    params.put(HoodieCompactionConfig.CLEANER_COMMITS_RETAINED.key(), "1");
    params.put(HoodieCompactionConfig.MAX_COMMITS_TO_KEEP.key(), "5");
    params.put(HoodieCompactionConfig.MIN_COMMITS_TO_KEEP.key(), "2");
    if (withAlternative) {
      params.put("hoodie.avro.schema.externalTransformation", "true");
    } else {
      params.put("hoodie.avro.schema.external.transformation", "true");
    }
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
    assertTrue(config.shouldUseExternalSchemaTransformation());
  }

  @Test
  public void testDefaultIndexAccordingToEngineType() {
    testEngineSpecificConfig(HoodieWriteConfig::getIndexType,
        constructConfigMap(
            EngineType.SPARK, HoodieIndex.IndexType.BLOOM,
            EngineType.FLINK, HoodieIndex.IndexType.INMEMORY,
            EngineType.JAVA, HoodieIndex.IndexType.INMEMORY));
  }

  @Test
  public void testDefaultClusteringPlanStrategyClassAccordingToEngineType() {
    testEngineSpecificConfig(HoodieWriteConfig::getClusteringPlanStrategyClass,
        constructConfigMap(
            EngineType.SPARK, HoodieClusteringConfig.SPARK_SIZED_BASED_CLUSTERING_PLAN_STRATEGY,
            EngineType.FLINK, HoodieClusteringConfig.JAVA_SIZED_BASED_CLUSTERING_PLAN_STRATEGY,
            EngineType.JAVA, HoodieClusteringConfig.JAVA_SIZED_BASED_CLUSTERING_PLAN_STRATEGY));
  }

  @Test
  public void testDefaultClusteringExecutionStrategyClassAccordingToEngineType() {
    testEngineSpecificConfig(HoodieWriteConfig::getClusteringExecutionStrategyClass,
        constructConfigMap(
            EngineType.SPARK, HoodieClusteringConfig.SPARK_SORT_AND_SIZE_EXECUTION_STRATEGY,
            EngineType.FLINK, HoodieClusteringConfig.JAVA_SORT_AND_SIZE_EXECUTION_STRATEGY,
            EngineType.JAVA, HoodieClusteringConfig.JAVA_SORT_AND_SIZE_EXECUTION_STRATEGY));
  }

  @Test
  public void testDefaultMarkersTypeAccordingToEngineType() {
    testEngineSpecificConfig(HoodieWriteConfig::getMarkersType,
        constructConfigMap(
            EngineType.SPARK, MarkerType.TIMELINE_SERVER_BASED,
            EngineType.FLINK, MarkerType.DIRECT,
            EngineType.JAVA, MarkerType.DIRECT));
  }

  private ByteArrayOutputStream saveParamsIntoOutputStream(Map<String, String> params) throws IOException {
    Properties properties = new Properties();
    properties.putAll(params);
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    properties.store(outStream, "Saved on " + new Date(System.currentTimeMillis()));
    return outStream;
  }

  /**
   * Tests the engine-specific configuration values for one configuration key .
   *
   * @param getConfigFunc     Function to get the config value.
   * @param expectedConfigMap Expected config map, with key as the engine type
   *                          and value as the corresponding config value for the engine.
   */
  private void testEngineSpecificConfig(Function<HoodieWriteConfig, Object> getConfigFunc,
                                        Map<EngineType, Object> expectedConfigMap) {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp").build();
    assertEquals(expectedConfigMap.get(EngineType.SPARK), getConfigFunc.apply(writeConfig));

    for (EngineType engineType : expectedConfigMap.keySet()) {
      writeConfig = HoodieWriteConfig.newBuilder()
          .withEngineType(engineType).withPath("/tmp").build();
      assertEquals(expectedConfigMap.get(engineType), getConfigFunc.apply(writeConfig));
    }
  }

  /**
   * Constructs the map.
   *
   * @param k1 First engine type.
   * @param v1 Config value for the first engine type.
   * @param k2 Second engine type.
   * @param v2 Config value for the second engine type.
   * @param k3 Third engine type.
   * @param v3 Config value for the third engine type.
   * @return {@link Map<EngineType, Object>} instance, with key as the engine type
   * and value as the corresponding config value for the engine.
   */
  private Map<EngineType, Object> constructConfigMap(
      EngineType k1, Object v1, EngineType k2, Object v2, EngineType k3, Object v3) {
    Map<EngineType, Object> mapping = new HashMap<>();
    mapping.put(k1, v1);
    mapping.put(k2, v2);
    mapping.put(k3, v3);
    return mapping;
  }
}
