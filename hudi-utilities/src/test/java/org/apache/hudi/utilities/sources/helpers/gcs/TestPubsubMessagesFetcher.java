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

import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;

import java.util.Collections;

public class TestPubsubMessagesFetcher {
  private static final String PROJECT_ID = "test-project";
  private static final String SUBSCRIPTION_ID = "test-subscription";
  private static final String SUBSCRIPTION_NAME = ProjectSubscriptionName.format(PROJECT_ID, SUBSCRIPTION_ID);
  private static final int SMALL_BATCH_SIZE = 1;
  private static final int MAX_MESSAGES_IN_REQUEST = 1000;
  private static final long MAX_WAIT_TIME_IN_REQUEST = TimeUnit.SECONDS.toMillis(1);

  private final SubscriberStub mockSubscriber = Mockito.mock(SubscriberStub.class);
  private final PubsubQueueClient mockPubsubQueueClient = Mockito.mock(PubsubQueueClient.class);

  @Test
  public void testFetchMessages() throws IOException {
    doNothing().when(mockSubscriber).close();
    when(mockPubsubQueueClient.getSubscriber(any())).thenReturn(mockSubscriber);
    when(mockPubsubQueueClient.getNumUnAckedMessages(SUBSCRIPTION_ID)).thenReturn(3L);
    doNothing().when(mockSubscriber).close();
    ReceivedMessage message1 = ReceivedMessage.newBuilder().setAckId("1").setMessage(PubsubMessage.newBuilder().setMessageId("msgId1").build()).build();
    ReceivedMessage message2 = ReceivedMessage.newBuilder().setAckId("2").setMessage(PubsubMessage.newBuilder().setMessageId("msgId2").build()).build();
    ReceivedMessage message3 = ReceivedMessage.newBuilder().setAckId("3").setMessage(PubsubMessage.newBuilder().setMessageId("msgId3").build()).build();
    when(mockPubsubQueueClient.makePullRequest(mockSubscriber, SUBSCRIPTION_NAME, SMALL_BATCH_SIZE))
        .thenReturn(PullResponse.newBuilder().addReceivedMessages(message1).build())
        .thenReturn(PullResponse.newBuilder().addReceivedMessages(message2).build())
        .thenReturn(PullResponse.newBuilder().addReceivedMessages(message3).build());

    PubsubMessagesFetcher fetcher = new PubsubMessagesFetcher(
        PROJECT_ID, SUBSCRIPTION_ID, SMALL_BATCH_SIZE,
        MAX_MESSAGES_IN_REQUEST, MAX_WAIT_TIME_IN_REQUEST, mockPubsubQueueClient
    );
    List<ReceivedMessage> messages = fetcher.fetchMessages();

    assertEquals(3, messages.size());
    assertEquals("1", messages.get(0).getAckId());
    assertEquals("2", messages.get(1).getAckId());
    assertEquals("3", messages.get(2).getAckId());
    verify(mockPubsubQueueClient, times(3)).makePullRequest(mockSubscriber, SUBSCRIPTION_NAME, SMALL_BATCH_SIZE);
  }

  @Test
  public void testFetchMessagesZeroTimeout() throws IOException {
    doNothing().when(mockSubscriber).close();
    when(mockPubsubQueueClient.getSubscriber(any())).thenReturn(mockSubscriber);
    when(mockPubsubQueueClient.getNumUnAckedMessages(SUBSCRIPTION_ID)).thenReturn(100L);
    PubsubMessagesFetcher fetcher = new PubsubMessagesFetcher(
        PROJECT_ID, SUBSCRIPTION_ID, SMALL_BATCH_SIZE,
        MAX_MESSAGES_IN_REQUEST, 0, mockPubsubQueueClient
    );

    List<ReceivedMessage> messages = fetcher.fetchMessages();
    assertEquals(0, messages.size());
  }

  @Test
  public void testSendAcks() throws IOException {
    doNothing().when(mockSubscriber).close();
    when(mockPubsubQueueClient.getSubscriber(any())).thenReturn(mockSubscriber);
    List<String> messageAcks = IntStream.range(0, 25).mapToObj(i -> "msg_" + i).collect(Collectors.toList());
    doNothing().when(mockPubsubQueueClient).makeAckRequest(eq(mockSubscriber), eq(SUBSCRIPTION_NAME), any());
    PubsubMessagesFetcher fetcher = new PubsubMessagesFetcher(
        PROJECT_ID, SUBSCRIPTION_ID, SMALL_BATCH_SIZE,
        MAX_MESSAGES_IN_REQUEST, MAX_WAIT_TIME_IN_REQUEST, mockPubsubQueueClient
    );

    fetcher.sendAcks(messageAcks);
    verify(mockPubsubQueueClient, times(3)).makeAckRequest(eq(mockSubscriber), eq(SUBSCRIPTION_NAME), any());

    // Verify first batch (messages 0-9) was sent
    List<String> expectedBatch1 = IntStream.range(0, 10).mapToObj(i -> "msg_" + i).collect(Collectors.toList());
    verify(mockPubsubQueueClient).makeAckRequest(mockSubscriber, SUBSCRIPTION_NAME, expectedBatch1);

    // Verify second batch (messages 10-19) was sent
    List<String> expectedBatch2 = IntStream.range(10, 20).mapToObj(i -> "msg_" + i).collect(Collectors.toList());
    verify(mockPubsubQueueClient).makeAckRequest(mockSubscriber, SUBSCRIPTION_NAME, expectedBatch2);

    // Verify third batch (messages 20-24) was sent
    List<String> expectedBatch3 = IntStream.range(20, 25).mapToObj(i -> "msg_" + i).collect(Collectors.toList());
    verify(mockPubsubQueueClient).makeAckRequest(mockSubscriber, SUBSCRIPTION_NAME, expectedBatch3);
  }

  @Test
  void testSendAcks_EmptyMessageList() throws IOException {
    doNothing().when(mockSubscriber).close();
    when(mockPubsubQueueClient.getSubscriber(any())).thenReturn(mockSubscriber);
    PubsubMessagesFetcher fetcher = new PubsubMessagesFetcher(
        PROJECT_ID, SUBSCRIPTION_ID, SMALL_BATCH_SIZE,
        MAX_MESSAGES_IN_REQUEST, MAX_WAIT_TIME_IN_REQUEST, mockPubsubQueueClient
    );

    fetcher.sendAcks(Collections.emptyList());
    verify(mockPubsubQueueClient, never()).makeAckRequest(any(), any(), any());
  }
}