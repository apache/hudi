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

import com.google.cloud.ServiceOptions;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.monitoring.v3.ListTimeSeriesRequest;
import com.google.monitoring.v3.Point;
import com.google.monitoring.v3.ProjectName;
import com.google.monitoring.v3.TimeInterval;
import com.google.monitoring.v3.TimeSeries;
import com.google.protobuf.util.Timestamps;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;

import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class PubsubQueueClient {
  private static final String METRIC_FILTER_PATTERN = "metric.type=\"pubsub.googleapis.com/subscription/%s\" AND resource.label.subscription_id=\"%s\"";
  private static final String NUM_UNDELIVERED_MESSAGES = "num_undelivered_messages";

  public SubscriberStub getSubscriber(SubscriberStubSettings subscriberStubSettings) throws IOException {
    return GrpcSubscriberStub.create(subscriberStubSettings);
  }

  public PullResponse makePullRequest(SubscriberStub subscriber, String subscriptionName, int batchSize) {
    PullRequest pullRequest = PullRequest.newBuilder()
        .setMaxMessages(batchSize)
        .setSubscription(subscriptionName)
        .build();
    return subscriber.pullCallable().call(pullRequest);
  }

  public void makeAckRequest(SubscriberStub subscriber, String subscriptionName, List<String> messages) {
    AcknowledgeRequest acknowledgeRequest = AcknowledgeRequest.newBuilder()
        .setSubscription(subscriptionName)
        .addAllAckIds(messages)
        .build();
    subscriber.acknowledgeCallable().call(acknowledgeRequest);
  }

  public long getNumUnAckedMessages(String subscriptionId) throws IOException {
    try (MetricServiceClient metricServiceClient = MetricServiceClient.create()) {
      MetricServiceClient.ListTimeSeriesPagedResponse response = metricServiceClient.listTimeSeries(
          ListTimeSeriesRequest.newBuilder()
              .setName(ProjectName.of(ServiceOptions.getDefaultProjectId()).toString())
              .setFilter(String.format(METRIC_FILTER_PATTERN, NUM_UNDELIVERED_MESSAGES, subscriptionId))
              .setInterval(TimeInterval.newBuilder()
                  .setStartTime(Timestamps.fromSeconds(Instant.now().getEpochSecond() - TimeUnit.MINUTES.toSeconds(2)))
                  .setEndTime(Timestamps.fromSeconds(Instant.now().getEpochSecond()))
                  .build())
              .build());
      // use the latest value from the window
      Iterator<TimeSeries> values = response.getPage().getValues().iterator();
      if (!values.hasNext()) {
        return 0;
      }
      List<Point> pointList = values.next().getPointsList();
      return pointList.stream().findFirst().map(point -> point.getValue().getInt64Value()).orElse(Long.MAX_VALUE);
    }
  }
}