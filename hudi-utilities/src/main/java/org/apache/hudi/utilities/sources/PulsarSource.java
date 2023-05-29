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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.util.Lazy;
import org.apache.hudi.utilities.config.PulsarSourceConfig;
import org.apache.hudi.utilities.exception.HoodieReadFromSourceException;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.shade.io.netty.channel.EventLoopGroup;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.pulsar.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.util.ThreadUtils.collectActiveThreads;

/**
 * Source fetching data from Pulsar topics
 */
public class PulsarSource extends RowSource implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(PulsarSource.class);

  private static final Duration GRACEFUL_SHUTDOWN_TIMEOUT = Duration.ofSeconds(20);

  private static final String HUDI_PULSAR_CONSUMER_ID_FORMAT = "hudi-pulsar-consumer-%d";
  private static final String[] PULSAR_META_FIELDS = new String[]{
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

  // NOTE: We're keeping the client so that we can shut it down properly
  private final Lazy<PulsarClient> pulsarClient;
  private final Lazy<Consumer<byte[]>> pulsarConsumer;

  public PulsarSource(TypedProperties props,
                      JavaSparkContext sparkContext,
                      SparkSession sparkSession,
                      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);

    DataSourceUtils.checkRequiredProperties(props,
        Arrays.asList(
            PulsarSourceConfig.PULSAR_SOURCE_TOPIC_NAME.key(),
            PulsarSourceConfig.PULSAR_SOURCE_SERVICE_ENDPOINT_URL.key()));

    // Converting to a descriptor allows us to canonicalize the topic's name properly
    this.topicName = TopicName.get(props.getString(PulsarSourceConfig.PULSAR_SOURCE_TOPIC_NAME.key())).toString();

    // TODO validate endpoints provided in the appropriate format
    this.serviceEndpointURL = props.getString(PulsarSourceConfig.PULSAR_SOURCE_SERVICE_ENDPOINT_URL.key());
    this.adminEndpointURL = props.getString(PulsarSourceConfig.PULSAR_SOURCE_ADMIN_ENDPOINT_URL.key());

    this.pulsarClient = Lazy.lazily(this::initPulsarClient);
    this.pulsarConsumer = Lazy.lazily(this::subscribeToTopic);
  }

  @Override
  protected Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCheckpointStr, long sourceLimit) {
    Pair<MessageId, MessageId> startingEndingOffsetsPair = computeOffsets(lastCheckpointStr, sourceLimit);

    MessageId startingOffset = startingEndingOffsetsPair.getLeft();
    MessageId endingOffset = startingEndingOffsetsPair.getRight();

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
    MessageId endingOffset = fetchLatestOffset();

    if (endingOffset.compareTo(startingOffset) < 0) {
      String message = String.format("Ending offset (%s) is preceding starting offset (%s) for '%s'",
          endingOffset, startingOffset, topicName);
      throw new HoodieException(message);
    }

    // TODO support capping the amount of records fetched
    Long maxRecordsLimit = computeTargetRecordLimit(sourceLimit, props);

    return Pair.of(startingOffset, endingOffset);
  }

  private MessageId decodeStartingOffset(Option<String> lastCheckpointStrOpt) {
    return lastCheckpointStrOpt
        .map(lastCheckpoint -> JsonUtils.topicOffsets(lastCheckpoint).apply(topicName))
        .orElseGet(() -> {
          PulsarSourceConfig.OffsetAutoResetStrategy autoResetStrategy = PulsarSourceConfig.OffsetAutoResetStrategy.valueOf(
              props.getString(PulsarSourceConfig.PULSAR_SOURCE_OFFSET_AUTO_RESET_STRATEGY.key(),
                  PulsarSourceConfig.PULSAR_SOURCE_OFFSET_AUTO_RESET_STRATEGY.defaultValue().name()));

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

  private void ackOffset(MessageId latestConsumedOffset) {
    try {
      pulsarConsumer.get().acknowledgeCumulative(latestConsumedOffset);
    } catch (PulsarClientException e) {
      LOG.error(String.format("Failed to ack messageId (%s) for topic '%s'", latestConsumedOffset, topicName), e);
      throw new HoodieReadFromSourceException("Failed to ack message for topic", e);
    }
  }

  private MessageId fetchLatestOffset() {
    try {
      return pulsarConsumer.get().getLastMessageId();
    } catch (PulsarClientException e) {
      LOG.error(String.format("Failed to fetch latest messageId for topic '%s'", topicName), e);
      throw new HoodieReadFromSourceException("Failed to fetch latest messageId for topic", e);
    }
  }

  private Consumer<byte[]> subscribeToTopic() {
    try {
      // NOTE: We're generating unique subscription-id to make sure that subsequent invocation
      //       of the DS, do not interfere w/ each other
      String subscriptionId = String.format(HUDI_PULSAR_CONSUMER_ID_FORMAT, System.currentTimeMillis());
      return pulsarClient.get()
          .newConsumer()
          .topic(topicName)
          .subscriptionName(subscriptionId)
          .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
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

  @Override
  public void close() throws IOException {
    shutdownPulsarClient(pulsarClient.get());
  }

  private static Long computeTargetRecordLimit(long sourceLimit, TypedProperties props) {
    if (sourceLimit < Long.MAX_VALUE) {
      return sourceLimit;
    } else {
      return props.getLong(PulsarSourceConfig.PULSAR_SOURCE_MAX_RECORDS_PER_BATCH_THRESHOLD.key(),
          PulsarSourceConfig.PULSAR_SOURCE_MAX_RECORDS_PER_BATCH_THRESHOLD.defaultValue());
    }
  }

  private static String convertToOffsetString(String topic, MessageId startingOffset) {
    return JsonUtils.topicOffsets(
        HoodieConversionUtils.mapAsScalaImmutableMap(
            Collections.singletonMap(topic, startingOffset)));
  }

  private static void shutdownPulsarClient(PulsarClient client) throws PulsarClientException {
    client.close();
    // NOTE: Current version of Pulsar's client (in Pulsar Spark Connector 3.1.1.4) is not
    //       shutting down event-loop group properly, so we had to shut it down manually
    try {
      EventLoopGroup eventLoopGroup = ((PulsarClientImpl) client).eventLoopGroup();
      if (eventLoopGroup != null) {
        eventLoopGroup.shutdownGracefully()
            .await(GRACEFUL_SHUTDOWN_TIMEOUT.getSeconds(), TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      // No-op
    }

    // NOTE: Pulsar clients initialized by the spark-connector, might be left not shutdown
    //       properly (see above). To work this around we employ "nuclear" option of
    //       fetching all Pulsar client threads and interrupting them forcibly (to make them
    //       shutdown)
    collectActiveThreads().stream().sequential()
        .filter(t -> t.getName().startsWith("pulsar-client-io"))
        .forEach(Thread::interrupt);
  }
}
