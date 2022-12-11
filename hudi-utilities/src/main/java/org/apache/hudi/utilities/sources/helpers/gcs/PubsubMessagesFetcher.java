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

import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import org.apache.hudi.exception.HoodieException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import static com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub.create;
import static org.apache.hudi.utilities.sources.helpers.gcs.GcsIngestionConfig.DEFAULT_MAX_INBOUND_MESSAGE_SIZE;

import java.io.IOException;
import java.util.List;

/**
 * Fetch messages from a specified Google Cloud Pubsub subscription.
 */
public class PubsubMessagesFetcher {

  private final String googleProjectId;
  private final String pubsubSubscriptionId;

  private final int batchSize;
  private final SubscriberStubSettings subscriberStubSettings;

  private static final Logger LOG = LogManager.getLogger(PubsubMessagesFetcher.class);

  public PubsubMessagesFetcher(String googleProjectId, String pubsubSubscriptionId, int batchSize) {
    this.googleProjectId = googleProjectId;
    this.pubsubSubscriptionId = pubsubSubscriptionId;
    this.batchSize = batchSize;

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
  }

  public List<ReceivedMessage> fetchMessages() {
    try {
      try (SubscriberStub subscriber = createSubscriber()) {
        String subscriptionName = getSubscriptionName();
        PullResponse pullResponse = makePullRequest(subscriber, subscriptionName);
        return pullResponse.getReceivedMessagesList();
      }
    } catch (IOException e) {
      throw new HoodieException("Error when fetching metadata", e);
    }
  }

  public void sendAcks(List<String> messagesToAck) throws IOException {
    String subscriptionName = getSubscriptionName();
    try (SubscriberStub subscriber = createSubscriber()) {

      AcknowledgeRequest acknowledgeRequest = AcknowledgeRequest.newBuilder()
              .setSubscription(subscriptionName)
              .addAllAckIds(messagesToAck)
              .build();

      subscriber.acknowledgeCallable().call(acknowledgeRequest);

      LOG.info("Acknowledged messages: " + messagesToAck);
    }
  }

  private PullResponse makePullRequest(SubscriberStub subscriber, String subscriptionName) {
    PullRequest pullRequest = PullRequest.newBuilder()
            .setMaxMessages(batchSize)
            .setSubscription(subscriptionName)
            .build();

    return subscriber.pullCallable().call(pullRequest);
  }

  private GrpcSubscriberStub createSubscriber() throws IOException {
    return create(subscriberStubSettings);
  }

  private String getSubscriptionName() {
    return ProjectSubscriptionName.format(googleProjectId, pubsubSubscriptionId);
  }
}
