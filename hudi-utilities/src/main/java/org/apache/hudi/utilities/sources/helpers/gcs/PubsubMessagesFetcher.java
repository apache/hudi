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

import org.apache.hudi.exception.HoodieException;

import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import static org.apache.hudi.utilities.sources.helpers.gcs.GcsIngestionConfig.DEFAULT_MAX_INBOUND_MESSAGE_SIZE;

/**
 * Fetch messages from a specified Google Cloud Pubsub subscription.
 */
public class PubsubMessagesFetcher {

  private static final int DEFAULT_BATCH_SIZE_ACK_API = 10;
  private static final long MAX_WAIT_TIME_TO_ACK_MESSAGES = TimeUnit.MINUTES.toMillis(1);
  private static final int ACK_PRODUCER_THREAD_POOL_SIZE = 3;

  private final ExecutorService threadPool = Executors.newFixedThreadPool(ACK_PRODUCER_THREAD_POOL_SIZE);
  private final String googleProjectId;
  private final String pubsubSubscriptionId;

  private final int batchSize;
  private final int maxMessagesPerSync;
  private final long maxFetchTimePerSync;
  private final SubscriberStubSettings subscriberStubSettings;
  private final PubsubQueueClient pubsubQueueClient;

  private static final Logger LOG = LoggerFactory.getLogger(PubsubMessagesFetcher.class);

  public PubsubMessagesFetcher(String googleProjectId, String pubsubSubscriptionId, int batchSize,
                               int maxMessagesPerSync,
                               long maxFetchTimePerSync,
                               PubsubQueueClient pubsubQueueClient) {
    this.googleProjectId = googleProjectId;
    this.pubsubSubscriptionId = pubsubSubscriptionId;
    this.batchSize = batchSize;
    this.maxMessagesPerSync = maxMessagesPerSync;
    this.maxFetchTimePerSync = maxFetchTimePerSync;

    try {
      /** For details of timeout and retry configs,
       * see {@link com.google.cloud.pubsub.v1.stub.SubscriberStubSettings#initDefaults()},
       * and the static code block in SubscriberStubSettings */
      subscriberStubSettings =
              SubscriberStubSettings.newBuilder()
                      .setTransportChannelProvider(
                              SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                                      .setMaxInboundMessageSize(DEFAULT_MAX_INBOUND_MESSAGE_SIZE)
                                      .build())
                      .build();
    } catch (IOException e) {
      throw new HoodieException("Error creating subscriber stub settings", e);
    }
    this.pubsubQueueClient = pubsubQueueClient;
  }

  public PubsubMessagesFetcher(
      String googleProjectId,
      String pubsubSubscriptionId,
      int batchSize,
      int maxMessagesPerSync,
      long maxFetchTimePerSync) {
    this(
        googleProjectId,
        pubsubSubscriptionId,
        batchSize,
        maxMessagesPerSync,
        maxFetchTimePerSync,
        new PubsubQueueClient()
    );
  }

  public List<ReceivedMessage> fetchMessages() {
    List<ReceivedMessage> messageList = new ArrayList<>();
    try (SubscriberStub subscriber = pubsubQueueClient.getSubscriber(subscriberStubSettings)) {
      String subscriptionName = ProjectSubscriptionName.format(googleProjectId, pubsubSubscriptionId);
      long startTime = System.currentTimeMillis();
      long unAckedMessages = pubsubQueueClient.getNumUnAckedMessages(this.pubsubSubscriptionId);
      LOG.info("Found unacked messages " + unAckedMessages);
      while (messageList.size() < unAckedMessages && messageList.size() < maxMessagesPerSync && (System.currentTimeMillis() - startTime < maxFetchTimePerSync)) {
        PullResponse pullResponse = pubsubQueueClient.makePullRequest(subscriber, subscriptionName, batchSize);
        messageList.addAll(pullResponse.getReceivedMessagesList());
      }
      return messageList;
    } catch (Exception e) {
      throw new HoodieException("Error when fetching metadata", e);
    }
  }

  public void sendAcks(List<String> messagesToAck) throws IOException {
    try (SubscriberStub subscriber = pubsubQueueClient.getSubscriber(subscriberStubSettings)) {
      int numberOfBatches = (int) Math.ceil((double) messagesToAck.size() / DEFAULT_BATCH_SIZE_ACK_API);
      CompletableFuture.allOf(IntStream.range(0, numberOfBatches)
              .parallel()
              .boxed()
              .map(batchIndex -> getTask(subscriber, messagesToAck, batchIndex)).toArray(CompletableFuture[]::new))
          .get(MAX_WAIT_TIME_TO_ACK_MESSAGES, TimeUnit.MILLISECONDS);
      LOG.debug("Flushed out all outstanding acknowledged messages: " + messagesToAck.size());
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      throw new IOException("Failed to ack messages from PubSub", e);
    }
  }

  private CompletableFuture<Void> getTask(SubscriberStub subscriber, List<String> messagesToAck, int batchIndex) {
    String subscriptionName = ProjectSubscriptionName.format(googleProjectId, pubsubSubscriptionId);
    List<String> messages = messagesToAck.subList(batchIndex, Math.min(batchIndex + DEFAULT_BATCH_SIZE_ACK_API, messagesToAck.size()));
    return CompletableFuture.runAsync(() -> pubsubQueueClient.makeAckRequest(subscriber, subscriptionName, messages), threadPool);
  }
}

