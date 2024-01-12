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
 * Pulsar Source Configs
 */
@Immutable
@ConfigClassProperty(name = "Pulsar Source Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.DELTA_STREAMER_SOURCE,
    description = "Configurations controlling the behavior of Pulsar source in Hudi Streamer.")
public class PulsarSourceConfig extends HoodieConfig {

  public static final ConfigProperty<Long> PULSAR_SOURCE_MAX_RECORDS_PER_BATCH_THRESHOLD = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.pulsar.maxRecords")
      .defaultValue(5_000_000L)
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "source.pulsar.maxRecords")
      .markAdvanced()
      .withDocumentation("Max number of records obtained in a single each batch");

  public static final ConfigProperty<String> PULSAR_SOURCE_TOPIC_NAME = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.pulsar.topic")
      .noDefaultValue()
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "source.pulsar.topic")
      .withDocumentation("Name of the target Pulsar topic to source data from");

  public static final ConfigProperty<String> PULSAR_SOURCE_SERVICE_ENDPOINT_URL = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.pulsar.endpoint.service.url")
      .defaultValue("pulsar://localhost:6650")
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "source.pulsar.endpoint.service.url")
      .withDocumentation("URL of the target Pulsar endpoint (of the form 'pulsar://host:port'");

  public static final ConfigProperty<String> PULSAR_SOURCE_ADMIN_ENDPOINT_URL = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.pulsar.endpoint.admin.url")
      .defaultValue("http://localhost:8080")
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "source.pulsar.endpoint.admin.url")
      .withDocumentation("URL of the target Pulsar endpoint (of the form 'pulsar://host:port'");

  public static final ConfigProperty<OffsetAutoResetStrategy> PULSAR_SOURCE_OFFSET_AUTO_RESET_STRATEGY = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.pulsar.offset.autoResetStrategy")
      .defaultValue(OffsetAutoResetStrategy.LATEST)
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "source.pulsar.offset.autoResetStrategy")
      .markAdvanced()
      .withDocumentation("Policy determining how offsets shall be automatically reset in case there's "
          + "no checkpoint information present");

  public enum OffsetAutoResetStrategy {
    LATEST, EARLIEST, FAIL
  }
}
