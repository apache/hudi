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

import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.ServiceOptions;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.monitoring.v3.ListTimeSeriesRequest;
import com.google.monitoring.v3.Point;
import com.google.monitoring.v3.TimeSeries;
import com.google.monitoring.v3.TypedValue;
import com.google.protobuf.Empty;
import com.google.protobuf.util.Timestamps;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestPubsubQueueClient {
  private static Stream<Arguments> getNumUnAckedMessages() {
    TimeSeries timeSeries = TimeSeries.newBuilder()
        .addPoints(Point.newBuilder().setValue(TypedValue.newBuilder().setInt64Value(100L).build()).build())
        .build();
    return Stream.of(
        Arguments.of(Collections.singletonList(timeSeries), 100L),
        Arguments.of(Collections.emptyList(), 0L));
  }

  @ParameterizedTest
  @MethodSource
  void getNumUnAckedMessages(List<TimeSeries> responseValues, long expected) throws Exception {
    String subscriptionId = "subscriptionId";
    PubsubQueueClient pubsubQueueClient = new PubsubQueueClient();
    try (MockedStatic<MetricServiceClient> mockedStaticServiceClient = Mockito.mockStatic(MetricServiceClient.class);
         MockedStatic<ServiceOptions> mockedStaticServiceOptions = Mockito.mockStatic(ServiceOptions.class)) {
      MetricServiceClient metricServiceClient = Mockito.mock(MetricServiceClient.class);
      mockedStaticServiceClient.when(MetricServiceClient::create).thenReturn(metricServiceClient);
      mockedStaticServiceOptions.when(ServiceOptions::getDefaultProjectId).thenReturn("projectId");

      MetricServiceClient.ListTimeSeriesPagedResponse response = mock(MetricServiceClient.ListTimeSeriesPagedResponse.class, RETURNS_DEEP_STUBS);
      ArgumentCaptor<ListTimeSeriesRequest> requestCaptor = ArgumentCaptor.forClass(ListTimeSeriesRequest.class);
      when(metricServiceClient.listTimeSeries(requestCaptor.capture())).thenReturn(response);

      when(response.getPage().getValues()).thenReturn(responseValues);
      long actualNumUnAckedMessages = pubsubQueueClient.getNumUnAckedMessages(subscriptionId);
      assertEquals(expected, actualNumUnAckedMessages);

      ListTimeSeriesRequest request = requestCaptor.getValue();
      long durationMs = Timestamps.toMillis(request.getInterval().getEndTime()) - Timestamps.toMillis(request.getInterval().getStartTime());
      assertEquals(120_000, durationMs);
      assertEquals("metric.type=\"pubsub.googleapis.com/subscription/num_undelivered_messages\" AND resource.label.subscription_id=\"subscriptionId\"", request.getFilter());
    }
  }

  @Test
  void makePullRequest() {
    SubscriberStub subscriber = mock(SubscriberStub.class);
    UnaryCallable<PullRequest, PullResponse> pullCallable = mock(UnaryCallable.class);
    PullResponse expectedResponse = PullResponse.newBuilder().build();
    ArgumentCaptor<PullRequest> requestCaptor = ArgumentCaptor.forClass(PullRequest.class);
    when(subscriber.pullCallable()).thenReturn(pullCallable);
    when(pullCallable.call(requestCaptor.capture())).thenReturn(expectedResponse);

    PullResponse actualResponse = new PubsubQueueClient().makePullRequest(subscriber, "projects/project/subscriptions/subscription", 10);

    assertSame(expectedResponse, actualResponse);
    assertEquals("projects/project/subscriptions/subscription", requestCaptor.getValue().getSubscription());
    assertEquals(10, requestCaptor.getValue().getMaxMessages());
  }

  @Test
  void makeAckRequest() {
    SubscriberStub subscriber = mock(SubscriberStub.class);
    UnaryCallable<AcknowledgeRequest, Empty> acknowledgeCallable = mock(UnaryCallable.class);
    ArgumentCaptor<AcknowledgeRequest> requestCaptor = ArgumentCaptor.forClass(AcknowledgeRequest.class);
    when(subscriber.acknowledgeCallable()).thenReturn(acknowledgeCallable);

    new PubsubQueueClient().makeAckRequest(subscriber, "projects/project/subscriptions/subscription", Arrays.asList("ack1", "ack2"));

    verify(acknowledgeCallable).call(requestCaptor.capture());
    assertEquals("projects/project/subscriptions/subscription", requestCaptor.getValue().getSubscription());
    assertEquals(Arrays.asList("ack1", "ack2"), requestCaptor.getValue().getAckIdsList());
  }

  @Test
  void getSubscriber() throws Exception {
    SubscriberStubSettings subscriberStubSettings = mock(SubscriberStubSettings.class);
    GrpcSubscriberStub expectedSubscriber = mock(GrpcSubscriberStub.class);
    try (MockedStatic<GrpcSubscriberStub> mockedStaticSubscriberStub = Mockito.mockStatic(GrpcSubscriberStub.class)) {
      mockedStaticSubscriberStub.when(() -> GrpcSubscriberStub.create(subscriberStubSettings)).thenReturn(expectedSubscriber);

      assertSame(expectedSubscriber, new PubsubQueueClient().getSubscriber(subscriberStubSettings));
    }
  }
}
