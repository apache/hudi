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

package org.apache.hudi.connect.writers;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.schema.FilebasedSchemaProvider;

import javax.annotation.concurrent.Immutable;

import java.util.Map;
import java.util.Properties;

/**
 * Class storing configs for the HoodieWriteClient.
 */
@Immutable
@ConfigClassProperty(name = "Kafka Sink Connect Configurations",
    groupName = ConfigGroups.Names.KAFKA_CONNECT,
    description = "Configurations for Kafka Connect Sink Connector for Hudi.")
public class KafkaConnectConfigs extends HoodieConfig {

  public static final int CURRENT_PROTOCOL_VERSION = 0;
  public static final String KAFKA_VALUE_CONVERTER = "value.converter";

  public static final ConfigProperty<String> KAFKA_BOOTSTRAP_SERVERS = ConfigProperty
      .key("bootstrap.servers")
      .defaultValue("localhost:9092")
      .withDocumentation("The bootstrap servers for the Kafka Cluster.");

  public static final ConfigProperty<String> CONTROL_TOPIC_NAME = ConfigProperty
      .key("hoodie.kafka.control.topic")
      .defaultValue("hudi-control-topic")
      .withDocumentation("Kafka topic name used by the Hudi Sink Connector for "
          + "sending and receiving control messages. Not used for data records.");

  public static final ConfigProperty<String> SCHEMA_PROVIDER_CLASS = ConfigProperty
      .key("hoodie.schemaprovider.class")
      .defaultValue(FilebasedSchemaProvider.class.getName())
      .withDocumentation("subclass of org.apache.hudi.schema.SchemaProvider "
          + "to attach schemas to input & target table data, built in options: "
          + "org.apache.hudi.schema.FilebasedSchemaProvider.");

  public static final ConfigProperty<String> COMMIT_INTERVAL_SECS = ConfigProperty
      .key("hoodie.kafka.commit.interval.secs")
      .defaultValue("60")
      .withDocumentation("The interval at which Hudi will commit the records written "
          + "to the files, making them consumable on the read-side.");

  public static final ConfigProperty<String> COORDINATOR_WRITE_TIMEOUT_SECS = ConfigProperty
      .key("hoodie.kafka.coordinator.write.timeout.secs")
      .defaultValue("300")
      .withDocumentation("The timeout after sending an END_COMMIT until when "
          + "the coordinator will wait for the write statuses from all the partitions"
          + "to ignore the current commit and start a new commit.");

  public static final ConfigProperty<String> ASYNC_COMPACT_ENABLE = ConfigProperty
      .key("hoodie.kafka.compaction.async.enable")
      .defaultValue("true")
      .withDocumentation("Controls whether async compaction should be turned on for MOR table writing.");

  public static final ConfigProperty<String> META_SYNC_ENABLE = ConfigProperty
      .key("hoodie.meta.sync.enable")
      .defaultValue("false")
      .withDocumentation("Enable Meta Sync such as Hive");

  public static final ConfigProperty<String> META_SYNC_CLASSES = ConfigProperty
      .key("hoodie.meta.sync.classes")
      .defaultValue(HiveSyncTool.class.getName())
      .withDocumentation("Meta sync client tool, using comma to separate multi tools");

  public static final ConfigProperty<Boolean> ALLOW_COMMIT_ON_ERRORS = ConfigProperty
      .key("hoodie.kafka.allow.commit.on.errors")
      .defaultValue(true)
      .withDocumentation("Commit even when some records failed to be written");

  protected KafkaConnectConfigs() {
    super();
  }

  protected KafkaConnectConfigs(Properties props) {
    super(props);
  }

  public static KafkaConnectConfigs.Builder newBuilder() {
    return new KafkaConnectConfigs.Builder();
  }

  public String getBootstrapServers() {
    return getString(KAFKA_BOOTSTRAP_SERVERS);
  }

  public String getControlTopicName() {
    return getString(CONTROL_TOPIC_NAME);
  }

  public String getSchemaProviderClass() {
    return getString(SCHEMA_PROVIDER_CLASS);
  }

  public Long getCommitIntervalSecs() {
    return getLong(COMMIT_INTERVAL_SECS);
  }

  public Long getCoordinatorWriteTimeoutSecs() {
    return getLong(COORDINATOR_WRITE_TIMEOUT_SECS);
  }

  public String getKafkaValueConverter() {
    return getString(KAFKA_VALUE_CONVERTER);
  }

  public Boolean isAsyncCompactEnabled() {
    return getBoolean(ASYNC_COMPACT_ENABLE);
  }

  public Boolean isMetaSyncEnabled() {
    return getBoolean(META_SYNC_ENABLE);
  }

  public String getMetaSyncClasses() {
    return getString(META_SYNC_CLASSES);
  }

  public Boolean allowCommitOnErrors() {
    return getBoolean(ALLOW_COMMIT_ON_ERRORS);
  }

  public static class Builder {

    protected final KafkaConnectConfigs connectConfigs = new KafkaConnectConfigs();

    public Builder withBootstrapServers(String bootstrapServers) {
      connectConfigs.setValue(KAFKA_BOOTSTRAP_SERVERS, bootstrapServers);
      return this;
    }

    public Builder withControlTopicName(String controlTopicName) {
      connectConfigs.setValue(CONTROL_TOPIC_NAME, controlTopicName);
      return this;
    }

    public Builder withCommitIntervalSecs(Long commitIntervalSecs) {
      connectConfigs.setValue(COMMIT_INTERVAL_SECS, String.valueOf(commitIntervalSecs));
      return this;
    }

    public Builder withCoordinatorWriteTimeoutSecs(Long coordinatorWriteTimeoutSecs) {
      connectConfigs.setValue(COORDINATOR_WRITE_TIMEOUT_SECS, String.valueOf(coordinatorWriteTimeoutSecs));
      return this;
    }

    public Builder withAllowCommitOnErrors(Boolean allowCommitOnErrors) {
      connectConfigs.setValue(ALLOW_COMMIT_ON_ERRORS, String.valueOf(allowCommitOnErrors));
      return this;
    }

    // Kafka connect task are passed with props with type Map<>
    public Builder withProperties(Map<?, ?> properties) {
      connectConfigs.getProps().putAll(properties);
      return this;
    }

    public Builder withProperties(Properties properties) {
      connectConfigs.getProps().putAll(properties);
      return this;
    }

    protected void setDefaults() {
      // Check for mandatory properties
      connectConfigs.setDefaults(KafkaConnectConfigs.class.getName());
    }

    public KafkaConnectConfigs build() {
      setDefaults();
      // Build HudiConnectConfigs at the end
      return new KafkaConnectConfigs(connectConfigs.getProps());
    }
  }
}
