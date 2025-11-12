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

package org.apache.hudi.utilities.sources.helpers.gcs;

import org.apache.hudi.utilities.config.GCSEventsSourceConfig;

/**
 * Config keys and defaults for GCS Ingestion
 */
public class GcsIngestionConfig {

  /**
   * The GCP Project Id where the Pubsub Subscription to ingest from resides. Needed to connect
   * to the Pubsub subscription
   */
  @Deprecated
  public static final String GOOGLE_PROJECT_ID = GCSEventsSourceConfig.GOOGLE_PROJECT_ID.key();

  /**
   * The GCP Pubsub subscription id for the GCS Notifications. Needed to connect to the Pubsub
   * subscription.
   */
  @Deprecated
  public static final String PUBSUB_SUBSCRIPTION_ID = GCSEventsSourceConfig.PUBSUB_SUBSCRIPTION_ID.key();

  // Size of inbound messages when pulling data, in bytes
  public static final int DEFAULT_MAX_INBOUND_MESSAGE_SIZE = 20 * 1024 * 1024; // bytes

}
