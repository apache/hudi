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

package org.apache.hudi.utilities.transform;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Commit callbacks preserve ordinary transformers and use each chain member's effective properties. */
public class TestTransformerCommitCallback {
  private static final String INSTANT = "20261113010101000";
  /** The prefix of a recorded pre-commit call. */
  private static final String PRE_COMMIT = "pre-commit:";

  @BeforeEach
  void clearCalls() {
    RecordingTransformer.CALLS.clear();
    RecordingTransformer.PROPERTIES.clear();
  }

  @Test
  void defaultCallbackDoesNothingWithoutReapplyingRows() {
    Transformer transformer = (jsc, spark, rows, properties) -> {
      throw new AssertionError("A commit callback must not transform the rows again");
    };
    TypedProperties properties = new TypedProperties();
    properties.setProperty("callback.common", "shared");

    assertDoesNotThrow(() -> transformer.onCommit(null, null, properties, INSTANT));

    assertEquals(Map.of("callback.common", "shared"), properties);
  }

  @Test
  void chainCallsEachTransformerInDeclaredOrder() {
    ChainedTransformer chain = new ChainedTransformer(Arrays.asList(
        new RecordingTransformer("first"), new RecordingTransformer("second")));
    TypedProperties properties = new TypedProperties();
    properties.setProperty("callback.common", "shared");

    chain.onCommit(null, null, properties, INSTANT);

    assertEquals(Arrays.asList("first@20261113010101000", "second@20261113010101000"), RecordingTransformer.CALLS);
    List<Map<Object, Object>> received = RecordingTransformer.PROPERTIES;
    assertEquals(Arrays.asList(Map.of("callback.common", "shared"), Map.of("callback.common", "shared")), received);
    assertEquals(Map.of("callback.common", "shared"), properties);
  }

  @Test
  void identifiedCallbacksReceiveIndependentEffectivePropertiesWithoutChangingTheCaller() {
    ChainedTransformer chain = new ChainedTransformer(Arrays.asList(
        "z:" + RecordingTransformer.class.getName(), "a:" + RecordingTransformer.class.getName()), Option::empty);
    TypedProperties properties = new TypedProperties();
    properties.setProperty("callback.name", "base");
    properties.setProperty("callback.name.z", "first");
    properties.setProperty("callback.name.a", "second");
    properties.setProperty("callback.only.z", "only-first");
    properties.setProperty("callback.common", "shared");
    properties.setProperty("callback.external.unknown", "retained");

    chain.onCommit(null, null, properties, INSTANT);

    assertEquals(Arrays.asList("first@20261113010101000", "second@20261113010101000"), RecordingTransformer.CALLS);
    List<Map<Object, Object>> received = RecordingTransformer.PROPERTIES;
    assertEquals(Arrays.asList(
        Map.of("callback.name", "first", "callback.only", "only-first", "callback.common", "shared",
            "callback.external.unknown", "retained"),
        Map.of("callback.name", "second", "callback.common", "shared", "callback.external.unknown", "retained")),
        received);
    assertEquals(Map.of("callback.name", "base", "callback.name.z", "first", "callback.name.a", "second",
        "callback.only.z", "only-first", "callback.common", "shared", "callback.external.unknown", "retained"), properties);
  }

  @Test
  void defaultPreCommitDoesNothingWithoutReapplyingRows() {
    Transformer transformer = (jsc, spark, rows, properties) -> {
      throw new AssertionError("A commit callback must not transform the rows again");
    };
    TypedProperties properties = new TypedProperties();
    properties.setProperty("callback.common", "shared");

    assertDoesNotThrow(() -> transformer.onPreCommit(null, null, properties, INSTANT, null));

    assertEquals(Map.of("callback.common", "shared"), properties);
  }

  @Test
  void preCommitChainCallsEachTransformerInDeclaredOrder() {
    ChainedTransformer chain = new ChainedTransformer(Arrays.asList(
        new RecordingTransformer("first"), new RecordingTransformer("second")));
    TypedProperties properties = new TypedProperties();
    properties.setProperty("callback.common", "shared");

    chain.onPreCommit(null, null, properties, INSTANT, null);

    assertEquals(Arrays.asList(PRE_COMMIT + "first@" + INSTANT, PRE_COMMIT + "second@" + INSTANT), RecordingTransformer.CALLS);
    List<Map<Object, Object>> received = RecordingTransformer.PROPERTIES;
    assertEquals(Arrays.asList(Map.of("callback.common", "shared"), Map.of("callback.common", "shared")), received);
    assertEquals(Map.of("callback.common", "shared"), properties);
  }

  @Test
  void identifiedPreCommitReceivesIndependentEffectiveProperties() {
    ChainedTransformer chain = new ChainedTransformer(Arrays.asList(
        "z:" + RecordingTransformer.class.getName(), "a:" + RecordingTransformer.class.getName()), Option::empty);
    TypedProperties properties = new TypedProperties();
    properties.setProperty("callback.name", "base");
    properties.setProperty("callback.name.z", "first");
    properties.setProperty("callback.name.a", "second");
    properties.setProperty("callback.only.z", "only-first");
    properties.setProperty("callback.common", "shared");
    properties.setProperty("callback.external.unknown", "retained");

    chain.onPreCommit(null, null, properties, INSTANT, null);

    assertEquals(Arrays.asList(PRE_COMMIT + "first@" + INSTANT, PRE_COMMIT + "second@" + INSTANT), RecordingTransformer.CALLS);
    List<Map<Object, Object>> received = RecordingTransformer.PROPERTIES;
    assertEquals(Arrays.asList(
        Map.of("callback.name", "first", "callback.only", "only-first", "callback.common", "shared",
            "callback.external.unknown", "retained"),
        Map.of("callback.name", "second", "callback.common", "shared", "callback.external.unknown", "retained")),
        received);
    assertEquals(Map.of("callback.name", "base", "callback.name.z", "first", "callback.name.a", "second",
        "callback.only.z", "only-first", "callback.common", "shared", "callback.external.unknown", "retained"), properties);
  }

  /** Real chain member: its callback records inputs, and identified instances also mutate their private property copy. */
  public static class RecordingTransformer implements Transformer {
    private static final List<String> CALLS = new ArrayList<>();
    private static final List<Map<Object, Object>> PROPERTIES = new ArrayList<>();
    private final String name;

    public RecordingTransformer() {
      this.name = null;
    }

    RecordingTransformer(String name) {
      this.name = name;
    }

    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession spark, Dataset<Row> rows, TypedProperties properties) {
      throw new AssertionError("A commit callback must not transform the rows again");
    }

    @Override
    public void onPreCommit(JavaSparkContext jsc, SparkSession spark, TypedProperties properties, String instantTime,
                            HoodieTableMetaClient metaClient) {
      record(PRE_COMMIT, properties, instantTime);
    }

    @Override
    public void onCommit(JavaSparkContext jsc, SparkSession spark, TypedProperties properties, String instantTime) {
      record("", properties, instantTime);
    }

    /** Records one callback: {@code kind} tells a pre-commit call from a post-commit one. */
    private void record(String kind, TypedProperties properties, String instantTime) {
      CALLS.add(kind + (name == null ? properties.getString("callback.name") : name) + "@" + instantTime);
      PROPERTIES.add(new HashMap<>(properties));
      if (name == null) {
        properties.setProperty("callback.common", "changed-by-callback");
      }
    }
  }
}
