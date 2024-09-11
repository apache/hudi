/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.callback.pulsar;

import org.apache.hudi.callback.HoodieWriteCommitCallback;
import org.apache.hudi.callback.common.HoodieWriteCommitCallbackMessage;
import org.apache.hudi.callback.util.HoodieWriteCommitCallbackUtil;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.util.DateTimeUtils.parseDuration;
import static org.apache.hudi.utilities.callback.pulsar.HoodieWriteCommitPulsarCallbackConfig.BROKER_SERVICE_URL;
import static org.apache.hudi.utilities.callback.pulsar.HoodieWriteCommitPulsarCallbackConfig.CONNECTION_TIMEOUT;
import static org.apache.hudi.utilities.callback.pulsar.HoodieWriteCommitPulsarCallbackConfig.KEEPALIVE_INTERVAL;
import static org.apache.hudi.utilities.callback.pulsar.HoodieWriteCommitPulsarCallbackConfig.OPERATION_TIMEOUT;
import static org.apache.hudi.utilities.callback.pulsar.HoodieWriteCommitPulsarCallbackConfig.PRODUCER_BLOCK_QUEUE_FULL;
import static org.apache.hudi.utilities.callback.pulsar.HoodieWriteCommitPulsarCallbackConfig.PRODUCER_PENDING_QUEUE_SIZE;
import static org.apache.hudi.utilities.callback.pulsar.HoodieWriteCommitPulsarCallbackConfig.PRODUCER_PENDING_SIZE;
import static org.apache.hudi.utilities.callback.pulsar.HoodieWriteCommitPulsarCallbackConfig.PRODUCER_ROUTE_MODE;
import static org.apache.hudi.utilities.callback.pulsar.HoodieWriteCommitPulsarCallbackConfig.PRODUCER_SEND_TIMEOUT;
import static org.apache.hudi.utilities.callback.pulsar.HoodieWriteCommitPulsarCallbackConfig.REQUEST_TIMEOUT;
import static org.apache.hudi.utilities.callback.pulsar.HoodieWriteCommitPulsarCallbackConfig.TOPIC;

/**
 * Pulsar implementation of {@link HoodieWriteCommitCallback}.
 */
public class HoodieWriteCommitPulsarCallback implements HoodieWriteCommitCallback, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieWriteCommitPulsarCallback.class);

  private final String serviceUrl;
  private final String topic;

  /**
   * The pulsar client.
   */
  private final transient PulsarClient client;

  /**
   * The pulsar producer.
   */
  private final transient Producer<String> producer;

  public HoodieWriteCommitPulsarCallback(HoodieWriteConfig config) throws PulsarClientException {
    this.serviceUrl = config.getString(BROKER_SERVICE_URL);
    this.topic = config.getString(TOPIC);
    this.client = createClient(config);
    this.producer = createProducer(config);
  }

  @Override
  public void call(HoodieWriteCommitCallbackMessage callbackMessage) {
    String callbackMsg = HoodieWriteCommitCallbackUtil.convertToJsonString(callbackMessage);
    try {
      producer.newMessage().key(callbackMessage.getTableName()).value(callbackMsg).send();
      LOG.info("Send callback message succeed");
    } catch (Exception e) {
      LOG.error("Send pulsar callback msg failed : ", e);
    }
  }

  /**
   * Method helps to create {@link Producer}.
   *
   * @param hoodieConfig Pulsar configs
   * @return A {@link Producer}
   */
  public Producer<String> createProducer(HoodieConfig hoodieConfig) throws PulsarClientException {
    MessageRoutingMode routeMode = Enum.valueOf(MessageRoutingMode.class,
        PRODUCER_ROUTE_MODE.defaultValue());
    Duration sendTimeout =
        parseDuration(hoodieConfig.getString(PRODUCER_SEND_TIMEOUT));
    int pendingQueueSize =
        hoodieConfig.getInt(PRODUCER_PENDING_QUEUE_SIZE);
    int pendingSize =
        hoodieConfig.getInt(PRODUCER_PENDING_SIZE);
    boolean blockIfQueueFull =
        hoodieConfig.getBoolean(PRODUCER_BLOCK_QUEUE_FULL);

    return client
        .newProducer(Schema.STRING)
        .topic(topic)
        .messageRoutingMode(routeMode)
        .sendTimeout((int) sendTimeout.toMillis(), TimeUnit.MILLISECONDS)
        .maxPendingMessages(pendingQueueSize)
        .maxPendingMessagesAcrossPartitions(pendingSize)
        .blockIfQueueFull(blockIfQueueFull)
        .create();
  }

  public PulsarClient createClient(HoodieConfig hoodieConfig) throws PulsarClientException {
    validatePulsarConfig();

    Duration operationTimeout =
        parseDuration(hoodieConfig.getString(OPERATION_TIMEOUT));
    Duration connectionTimeout =
        parseDuration(hoodieConfig.getString(CONNECTION_TIMEOUT));
    Duration requestTimeout =
        parseDuration(hoodieConfig.getString(REQUEST_TIMEOUT));
    Duration keepAliveInterval =
        parseDuration(hoodieConfig.getString(KEEPALIVE_INTERVAL));

    ClientConfigurationData clientConfigurationData =
        new ClientConfigurationData();
    clientConfigurationData
        .setServiceUrl(serviceUrl);
    clientConfigurationData
        .setOperationTimeoutMs(operationTimeout.toMillis());
    clientConfigurationData
        .setConnectionTimeoutMs((int) connectionTimeout.toMillis());
    clientConfigurationData
        .setRequestTimeoutMs((int) requestTimeout.toMillis());
    clientConfigurationData
        .setKeepAliveIntervalSeconds((int) keepAliveInterval.getSeconds());

    return new PulsarClientImpl(clientConfigurationData);
  }

  /**
   * Validate whether both pulsar's brokerServiceUrl and topic are configured.
   * Exception will be thrown if anyone of them is not configured.
   */
  private void validatePulsarConfig() {
    ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(serviceUrl), String.format("Config %s can not be "
        + "null or empty", BROKER_SERVICE_URL.key()));
    ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(topic), String.format("Config %s can not be null or empty",
        TOPIC.key()));
  }

  @Override
  public void close() throws IOException {
    if (producer != null) {
      try {
        producer.close();
      } catch (Throwable t) {
        LOG.warn("Could not properly close the producer.", t);
      }
    }

    if (client != null) {
      try {
        client.close();
      } catch (Throwable t) {
        LOG.warn("Could not properly close the client.", t);
      }
    }
  }
}
