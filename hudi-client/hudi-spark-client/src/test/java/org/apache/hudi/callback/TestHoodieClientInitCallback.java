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

package org.apache.hudi.callback;

import org.apache.hudi.callback.impl.HoodieWriteCommitHttpCallback;
import org.apache.hudi.client.BaseHoodieClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.execution.bulkinsert.NonSortPartitionerWithRows;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static org.apache.hudi.callback.TestHoodieClientInitCallback.AddConfigInitCallbackTestClass.CUSTOM_CONFIG_KEY1;
import static org.apache.hudi.callback.TestHoodieClientInitCallback.AddConfigInitCallbackTestClass.CUSTOM_CONFIG_VALUE1;
import static org.apache.hudi.callback.TestHoodieClientInitCallback.ChangeConfigInitCallbackTestClass.CUSTOM_CONFIG_KEY2;
import static org.apache.hudi.callback.TestHoodieClientInitCallback.ChangeConfigInitCallbackTestClass.CUSTOM_CONFIG_VALUE2;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.apache.hudi.config.HoodieWriteConfig.WRITE_SCHEMA_OVERRIDE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Tests {@link HoodieClientInitCallback}.
 */
public class TestHoodieClientInitCallback {
  @TempDir
  java.nio.file.Path tmpDir;

  @Mock
  static HoodieSparkEngineContext engineContext =
      Mockito.mock(HoodieSparkEngineContext.class);

  @BeforeAll
  public static void setup() {
    StorageConfiguration storageConfToReturn = getDefaultStorageConf();
    when(engineContext.getStorageConf()).thenReturn(storageConfToReturn);
  }

  @Test
  public void testNoClientInitCallback() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(tmpDir.toString())
        .withEmbeddedTimelineServerEnabled(false)
        .build(false);
    assertFalse(config.contains(CUSTOM_CONFIG_KEY1));

    try (SparkRDDWriteClient<Object> writeClient = new SparkRDDWriteClient<>(engineContext, config, true)) {

      assertFalse(writeClient.getConfig().contains(CUSTOM_CONFIG_KEY1));
      assertFalse(writeClient.getTableServiceClient().getConfig().contains(CUSTOM_CONFIG_KEY1));
    }
  }

  @Test
  public void testSingleClientInitCallback() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(tmpDir.toString())
        .withEmbeddedTimelineServerEnabled(false)
        .withClientInitCallbackClassNames(ChangeConfigInitCallbackTestClass.class.getName())
        .withProps(Collections.singletonMap(
            WRITE_SCHEMA_OVERRIDE.key(), TRIP_NESTED_EXAMPLE_SCHEMA))
        .build(false);
    assertFalse(config.contains(CUSTOM_CONFIG_KEY1));
    assertFalse(new Schema.Parser().parse(config.getWriteSchema())
        .getObjectProps().containsKey(CUSTOM_CONFIG_KEY2));

    try (SparkRDDWriteClient<Object> writeClient = new SparkRDDWriteClient<>(engineContext, config, true)) {

      HoodieWriteConfig updatedConfig = writeClient.getConfig();
      assertFalse(updatedConfig.contains(CUSTOM_CONFIG_KEY1));
      Schema actualSchema = new Schema.Parser().parse(updatedConfig.getWriteSchema());
      assertTrue(actualSchema.getObjectProps().containsKey(CUSTOM_CONFIG_KEY2));
      assertEquals(CUSTOM_CONFIG_VALUE2, actualSchema.getObjectProps().get(CUSTOM_CONFIG_KEY2));

      updatedConfig = writeClient.getTableServiceClient().getConfig();
      assertFalse(updatedConfig.contains(CUSTOM_CONFIG_KEY1));
      actualSchema = new Schema.Parser().parse(updatedConfig.getWriteSchema());
      assertTrue(actualSchema.getObjectProps().containsKey(CUSTOM_CONFIG_KEY2));
      assertEquals(CUSTOM_CONFIG_VALUE2, actualSchema.getObjectProps().get(CUSTOM_CONFIG_KEY2));
    }
  }

  @Test
  public void testTwoClientInitCallbacks() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(tmpDir.toString())
        .withEmbeddedTimelineServerEnabled(false)
        .withClientInitCallbackClassNames(
            ChangeConfigInitCallbackTestClass.class.getName() + ","
                + AddConfigInitCallbackTestClass.class.getName())
        .withProps(Collections.singletonMap(
            WRITE_SCHEMA_OVERRIDE.key(), TRIP_NESTED_EXAMPLE_SCHEMA))
        .build(false);
    assertFalse(config.contains(CUSTOM_CONFIG_KEY1));
    assertFalse(new Schema.Parser().parse(config.getWriteSchema())
        .getObjectProps().containsKey(CUSTOM_CONFIG_KEY2));

    try (SparkRDDWriteClient<Object> writeClient = new SparkRDDWriteClient<>(engineContext, config, true)) {

      HoodieWriteConfig updatedConfig = writeClient.getConfig();
      assertTrue(updatedConfig.contains(CUSTOM_CONFIG_KEY1));
      assertEquals(CUSTOM_CONFIG_VALUE1, updatedConfig.getString(CUSTOM_CONFIG_KEY1));
      Schema actualSchema = new Schema.Parser().parse(updatedConfig.getWriteSchema());
      assertTrue(actualSchema.getObjectProps().containsKey(CUSTOM_CONFIG_KEY2));
      assertEquals(CUSTOM_CONFIG_VALUE2, actualSchema.getObjectProps().get(CUSTOM_CONFIG_KEY2));

      updatedConfig = writeClient.getTableServiceClient().getConfig();
      assertTrue(updatedConfig.contains(CUSTOM_CONFIG_KEY1));
      assertEquals(CUSTOM_CONFIG_VALUE1, updatedConfig.getString(CUSTOM_CONFIG_KEY1));
      actualSchema = new Schema.Parser().parse(updatedConfig.getWriteSchema());
      assertTrue(actualSchema.getObjectProps().containsKey(CUSTOM_CONFIG_KEY2));
      assertEquals(CUSTOM_CONFIG_VALUE2, actualSchema.getObjectProps().get(CUSTOM_CONFIG_KEY2));
    }
  }

  @Test
  public void testClientInitCallbackThrowingException() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(tmpDir.toString())
        .withEmbeddedTimelineServerEnabled(false)
        .withClientInitCallbackClassNames(
            AddConfigInitCallbackTestClass.class.getName() + ","
                + ThrowExceptionCallbackTestClass.class.getName())
        .build(false);
    HoodieIOException exception = assertThrows(
        HoodieIOException.class,
        () -> new SparkRDDWriteClient<>(engineContext, config, true),
        "Expects the initialization to throw a HoodieIOException");
    assertEquals(
        "Throwing exception during client initialization.",
        exception.getMessage());
  }

  @ParameterizedTest
  @MethodSource("testArgsForNonCallbackClass")
  public void testNonClientInitCallbackClassInConfig(String className, String errorMsg) {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(tmpDir.toString())
        .withEmbeddedTimelineServerEnabled(false)
        .withClientInitCallbackClassNames(className)
        .build(false);
    HoodieException exception = assertThrows(
        HoodieException.class,
        () -> new SparkRDDWriteClient<>(engineContext, config, true),
        "Expects the initialization to throw a HoodieException");
    assertEquals(errorMsg, exception.getMessage());
  }

  private static Stream<Arguments> testArgsForNonCallbackClass() {
    return Arrays.stream(new String[][] {
        {HoodieWriteCommitHttpCallback.class.getName(),
            "Could not load class " + HoodieWriteCommitHttpCallback.class.getName()},
        {NonSortPartitionerWithRows.class.getName(),
            NonSortPartitionerWithRows.class.getName() + " is not a subclass of " + HoodieClientInitCallback.class.getName()}
    }).map(Arguments::of);
  }

  /**
   * A test {@link HoodieClientInitCallback} implementation to add `user.defined.key1` config.
   */
  public static class AddConfigInitCallbackTestClass implements HoodieClientInitCallback {
    public static final String CUSTOM_CONFIG_KEY1 = "user.defined.key1";
    public static final String CUSTOM_CONFIG_VALUE1 = "value1";

    @Override
    public void call(BaseHoodieClient hoodieClient) {
      HoodieWriteConfig config = hoodieClient.getConfig();
      config.setValue(CUSTOM_CONFIG_KEY1, CUSTOM_CONFIG_VALUE1);
    }
  }

  /**
   * A test {@link HoodieClientInitCallback} implementation to add the property
   * `user.defined.key2=value2` to the write schema.
   */
  public static class ChangeConfigInitCallbackTestClass implements HoodieClientInitCallback {
    public static final String CUSTOM_CONFIG_KEY2 = "user.defined.key2";
    public static final String CUSTOM_CONFIG_VALUE2 = "value2";

    @Override
    public void call(BaseHoodieClient hoodieClient) {
      HoodieWriteConfig config = hoodieClient.getConfig();
      Schema schema = new Schema.Parser().parse(config.getWriteSchema());
      if (!schema.getObjectProps().containsKey(CUSTOM_CONFIG_KEY2)) {
        schema.addProp(CUSTOM_CONFIG_KEY2, CUSTOM_CONFIG_VALUE2);
      }
      config.getProps().setProperty(WRITE_SCHEMA_OVERRIDE.key(), schema.toString());
    }
  }

  /**
   * A test {@link HoodieClientInitCallback} implementation to throw an exception.
   */
  public static class ThrowExceptionCallbackTestClass implements HoodieClientInitCallback {
    @Override
    public void call(BaseHoodieClient hoodieClient) {
      throw new HoodieIOException("Throwing exception during client initialization.");
    }
  }
}
