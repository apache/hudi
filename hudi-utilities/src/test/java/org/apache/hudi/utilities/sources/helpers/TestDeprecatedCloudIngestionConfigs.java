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
    assertNotNull(new CloudStoreIngestionConfig());

    assertEquals(CloudSourceConfig.BATCH_SIZE_CONF.key(), CloudStoreIngestionConfig.BATCH_SIZE_CONF);
    assertEquals(CloudSourceConfig.BATCH_SIZE_CONF.defaultValue().intValue(), CloudStoreIngestionConfig.DEFAULT_BATCH_SIZE);
    assertEquals(CloudSourceConfig.ACK_MESSAGES.key(), CloudStoreIngestionConfig.ACK_MESSAGES);
    assertEquals(CloudSourceConfig.ACK_MESSAGES.defaultValue(), CloudStoreIngestionConfig.ACK_MESSAGES_DEFAULT_VALUE);
    assertEquals(CloudSourceConfig.ENABLE_EXISTS_CHECK.key(), CloudStoreIngestionConfig.ENABLE_EXISTS_CHECK);
    assertEquals(CloudSourceConfig.ENABLE_EXISTS_CHECK.defaultValue(), CloudStoreIngestionConfig.DEFAULT_ENABLE_EXISTS_CHECK);
    assertEquals(CloudSourceConfig.SELECT_RELATIVE_PATH_PREFIX.key(), CloudStoreIngestionConfig.SELECT_RELATIVE_PATH_PREFIX);
    assertEquals(CloudSourceConfig.IGNORE_RELATIVE_PATH_PREFIX.key(), CloudStoreIngestionConfig.IGNORE_RELATIVE_PATH_PREFIX);
    assertEquals(CloudSourceConfig.IGNORE_RELATIVE_PATH_SUBSTR.key(), CloudStoreIngestionConfig.IGNORE_RELATIVE_PATH_SUBSTR);
    assertEquals(CloudSourceConfig.SPARK_DATASOURCE_OPTIONS.key(), CloudStoreIngestionConfig.SPARK_DATASOURCE_OPTIONS);
    assertEquals(CloudSourceConfig.CLOUD_DATAFILE_EXTENSION.key(), CloudStoreIngestionConfig.CLOUD_DATAFILE_EXTENSION);
    assertEquals(CloudSourceConfig.DATAFILE_FORMAT.key(), CloudStoreIngestionConfig.DATAFILE_FORMAT);
  }

  @Test
  void gcsIngestionConfigDelegatesToGcsEventsSourceConfig() {
    assertNotNull(new GcsIngestionConfig());

    assertEquals(GCSEventsSourceConfig.GOOGLE_PROJECT_ID.key(), GcsIngestionConfig.GOOGLE_PROJECT_ID);
    assertEquals(GCSEventsSourceConfig.PUBSUB_SUBSCRIPTION_ID.key(), GcsIngestionConfig.PUBSUB_SUBSCRIPTION_ID);
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
