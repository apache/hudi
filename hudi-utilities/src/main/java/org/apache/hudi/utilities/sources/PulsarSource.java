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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.HoodieConversionUtils;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.util.Lazy;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.pulsar.JsonUtils;

import java.util.Arrays;
import java.util.Collections;

/**
 * TODO java-doc
 */
public class PulsarSource extends RowSource {

  private static final Logger LOG = LogManager.getLogger(PulsarSource.class);

  private static final String HUDI_PULSAR_CONSUMER_ID = "hudi-pulsar-consumer";
  private static final String[] PULSAR_META_FIELDS = new String[] {
      "__key",
      "__topic",
      "__messageId",
      "__publishTime",
      "__eventTime",
      "__messageProperties"
  };

  private final String topicName;

  private final String serviceEndpointURL;
  private final String adminEndpointURL;

  // TODO dedupe
  private final Lazy<PulsarClient> pulsarClient;
  private final Lazy<Consumer<byte[]>> pulsarConsumer;

  public PulsarSource(TypedProperties props,
                      JavaSparkContext sparkContext,
                      SparkSession sparkSession,
                      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);

    DataSourceUtils.checkRequiredProperties(props,
        Arrays.asList(
            Config.PULSAR_SOURCE_TOPIC_NAME.key(),
            Config.PULSAR_SOURCE_SERVICE_ENDPOINT_URL.key()));

    // Converting to a descriptor allows us to canonicalize the topic's name properly
    this.topicName = TopicName.get(props.getString(Config.PULSAR_SOURCE_TOPIC_NAME.key())).toString();

    // TODO validate endpoints provided in the appropriate format
    this.serviceEndpointURL = props.getString(Config.PULSAR_SOURCE_SERVICE_ENDPOINT_URL.key());
    this.adminEndpointURL = props.getString(Config.PULSAR_SOURCE_ADMIN_ENDPOINT_URL.key());

    this.pulsarClient = Lazy.lazily(this::initPulsarClient);
    this.pulsarConsumer = Lazy.lazily(this::subscribeToTopic);
  }

  @Override
  protected Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCheckpointStr, long sourceLimit) {
    Pair<MessageId, MessageId> startingEndingOffsetsPair = computeOffsets(lastCheckpointStr, sourceLimit);

    MessageId startingOffset = startingEndingOffsetsPair.getLeft();
    MessageId endingOffset = startingEndingOffsetsPair.getRight();

    //
    // TODO
    //    - [P0] Add support for schema-provider
    //    - [P1] Add support for auth
    //

    String startingOffsetStr = convertToOffsetString(topicName, startingOffset);
    String endingOffsetStr = convertToOffsetString(topicName, endingOffset);

    Dataset<Row> sourceRows = sparkSession.read()
        .format("pulsar")
        .option("service.url", serviceEndpointURL)
        .option("admin.url", adminEndpointURL)
        .option("topics", topicName)
        .option("startingOffsets", startingOffsetStr)
        .option("endingOffsets", endingOffsetStr)
        .load();

    return Pair.of(Option.of(transform(sourceRows)), endingOffsetStr);
  }

  @Override
  public void onCommit(String lastCheckpointStr) {
    MessageId latestConsumedOffset = JsonUtils.topicOffsets(lastCheckpointStr).apply(topicName);
    ackOffset(latestConsumedOffset);
  }

  private Dataset<Row> transform(Dataset<Row> rows) {
    return rows.drop(PULSAR_META_FIELDS);
  }

  private Pair<MessageId, MessageId> computeOffsets(Option<String> lastCheckpointStrOpt, long sourceLimit) {
    MessageId startingOffset = decodeStartingOffset(lastCheckpointStrOpt);

    // TODO support capping the amount of records fetched
    Long maxRecordsLimit = computeTargetRecordLimit(sourceLimit, props);

    MessageId endingOffset = fetchLatestOffset();

    return Pair.of(startingOffset, endingOffset);
  }

  private void ackOffset(MessageId latestConsumedOffset) {
    try {
      pulsarConsumer.get().acknowledgeCumulative(latestConsumedOffset);
    } catch (PulsarClientException e) {
      LOG.error(String.format("Failed to ack messageId (%s) for topic '%s'", latestConsumedOffset, topicName), e);
      throw new HoodieIOException("Failed to ack message for topic", e);
    }
  }

  private MessageId fetchLatestOffset() {
    try {
      return pulsarConsumer.get().getLastMessageId();
    } catch (PulsarClientException e) {
      LOG.error(String.format("Failed to fetch latest messageId for topic '%s'", topicName), e);
      throw new HoodieIOException("Failed to fetch latest messageId for topic", e);
    }
  }

  private MessageId decodeStartingOffset(Option<String> lastCheckpointStrOpt) {
    return lastCheckpointStrOpt
        .map(lastCheckpoint -> JsonUtils.topicOffsets(lastCheckpoint).apply(topicName))
        .orElseGet(() -> {
          Config.OffsetAutoResetStrategy autoResetStrategy = Config.OffsetAutoResetStrategy.valueOf(
              props.getString(Config.PULSAR_SOURCE_OFFSET_AUTO_RESET_STRATEGY.key(),
                  Config.PULSAR_SOURCE_OFFSET_AUTO_RESET_STRATEGY.defaultValue().name()));

          switch (autoResetStrategy) {
            case LATEST:
              return fetchLatestOffset();
            case EARLIEST:
              return MessageId.earliest;
            case FAIL:
              throw new IllegalArgumentException("No checkpoint has been provided!");
            default:
              throw new UnsupportedOperationException("Unsupported offset auto-reset strategy");
          }
        });
  }

  private Consumer<byte[]> subscribeToTopic() {
    try {
      return pulsarClient.get()
          .newConsumer()
          .topic(topicName)
          .subscriptionName(HUDI_PULSAR_CONSUMER_ID)
          .subscriptionType(SubscriptionType.Exclusive)
          .subscribe();
    } catch (PulsarClientException e) {
      LOG.error(String.format("Failed to subscribe to Pulsar topic '%s'", topicName), e);
      throw new HoodieIOException("Failed to subscribe to Pulsar topic", e);
    }
  }

  private PulsarClient initPulsarClient() {
    try {
      return PulsarClient.builder()
          .serviceUrl(serviceEndpointURL)
          .build();
    } catch (PulsarClientException e) {
      LOG.error(String.format("Failed to init Pulsar client connecting to '%s'", serviceEndpointURL), e);
      throw new HoodieIOException("Failed to init Pulsar client", e);
    }
  }

  private static Long computeTargetRecordLimit(long sourceLimit, TypedProperties props) {
    if (sourceLimit < Long.MAX_VALUE) {
      return sourceLimit;
    } else {
      return props.getLong(Config.PULSAR_SOURCE_MAX_RECORDS_PER_BATCH_THRESHOLD_PROP.key(),
          Config.PULSAR_SOURCE_MAX_RECORDS_PER_BATCH_THRESHOLD_PROP.defaultValue());
    }
  }

  private static String convertToOffsetString(String topic, MessageId startingOffset) {
    return JsonUtils.topicOffsets(
        HoodieConversionUtils.mapAsScalaImmutableMap(
            Collections.singletonMap(topic, startingOffset)));
  }

  // TODO unify w/ Kafka
  public static class Config {
    private static final ConfigProperty<String> PULSAR_SOURCE_TOPIC_NAME = ConfigProperty
        .key("hoodie.deltastreamer.source.pulsar.topic")
        .noDefaultValue()
        .withDocumentation("Name of the target Pulsar topic to source data from");

    private static final ConfigProperty<String> PULSAR_SOURCE_SERVICE_ENDPOINT_URL = ConfigProperty
        .key("hoodie.deltastreamer.source.pulsar.endpoint.service.url")
        .defaultValue("pulsar://localhost:6650")
        .withDocumentation("URL of the target Pulsar endpoint (of the form 'pulsar://host:port'");

    private static final ConfigProperty<String> PULSAR_SOURCE_ADMIN_ENDPOINT_URL = ConfigProperty
        .key("hoodie.deltastreamer.source.pulsar.endpoint.admin.url")
        .defaultValue("http://localhost:8080")
        .withDocumentation("URL of the target Pulsar endpoint (of the form 'pulsar://host:port'");

    public enum OffsetAutoResetStrategy {
      LATEST, EARLIEST, FAIL
    }

    private static final ConfigProperty<OffsetAutoResetStrategy> PULSAR_SOURCE_OFFSET_AUTO_RESET_STRATEGY = ConfigProperty
        .key("hoodie.deltastreamer.source.pulsar.offset.autoResetStrategy")
        .defaultValue(OffsetAutoResetStrategy.LATEST)
        .withDocumentation("Policy determining how offsets shall be automatically reset in case there's "
            + "no checkpoint information present");

    public static final ConfigProperty<Long> PULSAR_SOURCE_MAX_RECORDS_PER_BATCH_THRESHOLD_PROP = ConfigProperty
        .key("hoodie.deltastreamer.source.pulsar.maxRecords")
        .defaultValue(10_000_000L)
        .withDocumentation("Max number of records obtained in a single each batch");
  }
}
