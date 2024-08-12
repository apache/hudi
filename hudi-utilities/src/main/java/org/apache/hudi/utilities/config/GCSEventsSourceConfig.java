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

package org.apache.hudi.utilities.config;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import javax.annotation.concurrent.Immutable;

import static org.apache.hudi.common.util.ConfigUtils.DELTA_STREAMER_CONFIG_PREFIX;
import static org.apache.hudi.common.util.ConfigUtils.STREAMER_CONFIG_PREFIX;

/**
 * GCS Events Source Configs
 */
@Immutable
@ConfigClassProperty(name = "GCS Events Source Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.DELTA_STREAMER_SOURCE,
    description = "Configurations controlling the behavior of GCS Events Source in Hudi Streamer.")
public class GCSEventsSourceConfig extends HoodieConfig {

  public static final ConfigProperty<String> GOOGLE_PROJECT_ID = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.gcs.project.id")
      .noDefaultValue()
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "source.gcs.project.id")
      .markAdvanced()
      .withDocumentation("The GCP Project Id where the Pubsub Subscription to ingest from resides. Needed to connect to the Pubsub subscription");

  public static final ConfigProperty<String> PUBSUB_SUBSCRIPTION_ID = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.gcs.subscription.id")
      .noDefaultValue()
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "source.gcs.subscription.id")
      .markAdvanced()
      .withDocumentation("The GCP Pubsub subscription id for the GCS Notifications. Needed to connect to the Pubsub subscription");
}
