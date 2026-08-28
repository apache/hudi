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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.config.GCSEventsSourceConfig;
import org.apache.hudi.utilities.deltastreamer.NoNewDataTerminationStrategy;
import org.apache.hudi.utilities.sources.helpers.gcs.GcsIngestionConfig;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the deprecated cloud-ingestion shims against drift from the {@link CloudSourceConfig} /
 * {@link GCSEventsSourceConfig} definitions they now delegate to.
 */
@SuppressWarnings("deprecation")
class TestDeprecatedCloudIngestionConfigs {

  @Test
  void cloudStoreIngestionConfigDelegatesToCloudSourceConfig() {
    // covers the implicit constructor of the deprecated constant holder
    assertNotNull(new CloudStoreIngestionConfig());

    assertEquals("hoodie.streamer.source.cloud.meta.batch.size", CloudStoreIngestionConfig.BATCH_SIZE_CONF);
    assertEquals(10, CloudStoreIngestionConfig.DEFAULT_BATCH_SIZE);
    assertEquals("hoodie.streamer.source.cloud.meta.ack", CloudStoreIngestionConfig.ACK_MESSAGES);
    assertTrue(CloudStoreIngestionConfig.ACK_MESSAGES_DEFAULT_VALUE);
    assertEquals("hoodie.streamer.source.cloud.data.check.file.exists", CloudStoreIngestionConfig.ENABLE_EXISTS_CHECK);
    assertFalse(CloudStoreIngestionConfig.DEFAULT_ENABLE_EXISTS_CHECK);
    assertEquals("hoodie.streamer.source.cloud.data.select.relpath.prefix", CloudStoreIngestionConfig.SELECT_RELATIVE_PATH_PREFIX);
    assertEquals("hoodie.streamer.source.cloud.data.ignore.relpath.prefix", CloudStoreIngestionConfig.IGNORE_RELATIVE_PATH_PREFIX);
    assertEquals("hoodie.streamer.source.cloud.data.ignore.relpath.substring", CloudStoreIngestionConfig.IGNORE_RELATIVE_PATH_SUBSTR);
    assertEquals("hoodie.streamer.source.cloud.data.datasource.options", CloudStoreIngestionConfig.SPARK_DATASOURCE_OPTIONS);
    assertEquals("hoodie.streamer.source.cloud.data.select.file.extension", CloudStoreIngestionConfig.CLOUD_DATAFILE_EXTENSION);
    assertEquals("hoodie.streamer.source.cloud.data.datafile.format", CloudStoreIngestionConfig.DATAFILE_FORMAT);
  }

  @Test
  void gcsIngestionConfigDelegatesToGcsEventsSourceConfig() {
    // covers the implicit constructor of the deprecated constant holder
    assertNotNull(new GcsIngestionConfig());

    assertEquals("hoodie.streamer.source.gcs.project.id", GcsIngestionConfig.GOOGLE_PROJECT_ID);
    assertEquals("hoodie.streamer.source.gcs.subscription.id", GcsIngestionConfig.PUBSUB_SUBSCRIPTION_ID);
  }

  @Test
  void deprecatedNoNewDataTerminationStrategyInheritsShutdownBehavior() {
    NoNewDataTerminationStrategy strategy = new NoNewDataTerminationStrategy(new TypedProperties());

    // Default is 3 consecutive empty rounds before shutdown.
    assertFalse(strategy.shouldShutdown(Option.empty()));
    assertFalse(strategy.shouldShutdown(Option.empty()));
    assertTrue(strategy.shouldShutdown(Option.empty()));
  }
}
