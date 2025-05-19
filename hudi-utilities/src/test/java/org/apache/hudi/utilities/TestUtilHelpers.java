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

package org.apache.hudi.utilities;

import org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.utilities.sources.AvroKafkaSource;
import org.apache.hudi.utilities.sources.Source;
import org.apache.hudi.utilities.sources.helpers.SchemaTestProvider;
import org.apache.hudi.utilities.streamer.DefaultStreamContext;
import org.apache.hudi.utilities.streamer.HoodieStreamerMetrics;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Test cases for {@link UtilHelpers}.
 */
public class TestUtilHelpers {
  @Test
  void testAddLockOptions() {
    TypedProperties props1 = new TypedProperties();
    UtilHelpers.addLockOptions("path1", "file", props1);
    assertEquals(FileSystemBasedLockProvider.class.getName(), props1.getString(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key()));

    TypedProperties props2 = new TypedProperties();
    props2.put(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(), "Dummy");
    UtilHelpers.addLockOptions("path2", "file", props2);
    assertEquals(1, props2.size(), "Should not add lock options if the lock provider is already there.");
  }

  @Test
  void testCreateSource() throws IOException {
    SparkSession sparkSession = mock(SparkSession.class);
    JavaSparkContext javaSparkContext = mock(JavaSparkContext.class);
    TypedProperties typedProperties = new TypedProperties();
    typedProperties.setProperty("hoodie.streamer.source.kafka.topic", "topic");
    Source source = UtilHelpers.createSource(
        "org.apache.hudi.utilities.sources.AvroKafkaSource",
        typedProperties,
        javaSparkContext,
        sparkSession,
        new HoodieStreamerMetrics(
                HoodieWriteConfig.newBuilder().withPath("mypath").build(),
                HoodieStorageUtils.getStorage(getDefaultStorageConf())),
        new DefaultStreamContext(new SchemaTestProvider(typedProperties), Option.empty()));
    assertTrue(source instanceof AvroKafkaSource);
  }

  @Test
  void testCreateSourceWithErrors() {
    SparkSession sparkSession = mock(SparkSession.class);
    JavaSparkContext javaSparkContext = mock(JavaSparkContext.class);
    TypedProperties typedProperties = new TypedProperties();
    Throwable e = assertThrows(IOException.class, () -> UtilHelpers.createSource(
        "org.apache.hudi.utilities.sources.AvroKafkaSource",
        typedProperties,
        javaSparkContext,
        sparkSession,
        new HoodieStreamerMetrics(
                HoodieWriteConfig.newBuilder().withPath("mypath").build(),
                HoodieStorageUtils.getStorage(getDefaultStorageConf())),
        new DefaultStreamContext(new SchemaTestProvider(typedProperties), Option.empty())));
    // We expect two constructors to complain about this error.
    assertEquals(2, e.getSuppressed().length);
  }
}
