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
import static org.apache.hudi.utilities.sources.processor.maxwell.OrderingFieldType.DATE_STRING;

/**
 * Json Kafka Post Processor Configs
 */
@Immutable
@ConfigClassProperty(name = "Json Kafka Post Processor Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.DELTA_STREAMER_SOURCE,
    description = "Configurations controlling the post processor of Json Kafka Source in Hudi Streamer.")
public class JsonKafkaPostProcessorConfig extends HoodieConfig {

  public static final ConfigProperty<String> JSON_KAFKA_PROCESSOR_CLASS = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.json.kafka.processor.class")
      .noDefaultValue()
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "source.json.kafka.processor.class")
      .markAdvanced()
      .withDocumentation("Json kafka source post processor class name, post process data after consuming from"
          + "source and before giving it to Hudi Streamer.");

  public static final ConfigProperty<String> DATABASE_NAME_REGEX = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.json.kafka.post.processor.maxwell.database.regex")
      .noDefaultValue()
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "source.json.kafka.post.processor.maxwell.database.regex")
      .markAdvanced()
      .withDocumentation("Database name regex");

  public static final ConfigProperty<String> TABLE_NAME_REGEX = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.json.kafka.post.processor.maxwell.table.regex")
      .noDefaultValue()
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "source.json.kafka.post.processor.maxwell.table.regex")
      .markAdvanced()
      .withDocumentation("Table name regex");

  public static final ConfigProperty<String> ORDERING_FIELDS_TYPE = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.json.kafka.post.processor.maxwell.ordering.fields.type")
      .defaultValue(DATE_STRING.toString())
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "source.json.kafka.post.processor.maxwell.precombine.field.type",
          STREAMER_CONFIG_PREFIX + "source.json.kafka.post.processor.maxwell.precombine.field.type")
      .markAdvanced()
      .withDocumentation("Data type of the ordering field. could be NON_TIMESTAMP, DATE_STRING,"
          + "UNIX_TIMESTAMP or EPOCHMILLISECONDS. DATE_STRING by default");

  public static final ConfigProperty<String> ORDERING_FIELDS_FORMAT = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.json.kafka.post.processor.maxwell.ordering.fields.format")
      .defaultValue("yyyy-MM-dd HH:mm:ss")
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "source.json.kafka.post.processor.maxwell.precombine.field.format",
          STREAMER_CONFIG_PREFIX + "source.json.kafka.post.processor.maxwell.precombine.field.format")
      .markAdvanced()
      .withDocumentation("When the preCombine filed is in DATE_STRING format, use should tell hoodie"
          + "what format it is. 'yyyy-MM-dd HH:mm:ss' by default");
}
