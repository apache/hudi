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

package org.apache.hudi.utilities.testutils;

import com.amazonaws.kinesis.agg.AggRecord;
import com.amazonaws.kinesis.agg.RecordAggregator;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.ScalingType;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;
import software.amazon.awssdk.services.kinesis.model.UpdateShardCountRequest;

import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.ByteBuffer;
import java.net.URI;
import java.util.List;

/**
 * Test utilities for Kinesis Data Streams using LocalStack.
 */
public class KinesisTestUtils {
  private static final DockerImageName LOCALSTACK_IMAGE =
      DockerImageName.parse("localstack/localstack:4.1.0");

  private LocalStackContainer localStack;
  private KinesisClient kinesisClient;

  public KinesisTestUtils setup() {
    localStack = new LocalStackContainer(LOCALSTACK_IMAGE)
        .withServices(LocalStackContainer.Service.KINESIS);
    localStack.start();
    kinesisClient = KinesisClient.builder()
        .endpointOverride(localStack.getEndpointOverride(LocalStackContainer.Service.KINESIS))
        .credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(localStack.getAccessKey(), localStack.getSecretKey())))
        .region(Region.of(localStack.getRegion()))
        .build();
    return this;
  }

  public String getEndpointUrl() {
    if (localStack == null || !localStack.isRunning()) {
      throw new IllegalStateException("LocalStack container is not running. Please start the container first.");
    }
    URI endpoint = localStack.getEndpointOverride(LocalStackContainer.Service.KINESIS);
    return endpoint.toString();
  }

  public String getRegion() {
    if (localStack == null || !localStack.isRunning()) {
      throw new IllegalStateException("LocalStack container is not running. Please start the container first.");
    }
    return localStack.getRegion();
  }

  public void createStream(String streamName) {
    createStream(streamName, 1);
  }

  public void createStream(String streamName, int shardCount) {
    kinesisClient.createStream(
        CreateStreamRequest.builder()
            .streamName(streamName)
            .shardCount(shardCount)
            .build());
    waitForStreamActive(streamName);
  }

  private void waitForStreamActive(String streamName) {
    waitForStreamActive(streamName, 30);
  }

  private void waitForStreamActive(String streamName, int maxAttempts) {
    try {
      for (int i = 0; i < maxAttempts; i++) {
        String status = kinesisClient.describeStream(
            DescribeStreamRequest.builder().streamName(streamName).build())
            .streamDescription().streamStatus().toString();
        if (StreamStatus.ACTIVE.toString().equals(status)) {
          return;
        }
        Thread.sleep(500);
      }
      throw new RuntimeException("Stream " + streamName + " did not become ACTIVE in time");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while waiting for stream", e);
    }
  }

  public void sendRecords(String streamName, List<String> records) {
    for (String record : records) {
      kinesisClient.putRecord(
          PutRecordRequest.builder()
              .streamName(streamName)
              .partitionKey(String.valueOf(System.nanoTime()))
              .data(SdkBytes.fromUtf8String(record))
              .build());
    }
  }

  public void sendRecords(String streamName, String[] records) {
    for (String record : records) {
      kinesisClient.putRecord(
          PutRecordRequest.builder()
              .streamName(streamName)
              .partitionKey(String.valueOf(System.nanoTime()))
              .data(SdkBytes.fromUtf8String(record))
              .build());
    }
  }

  /**
   * Send KPL-aggregated records. Multiple user records are combined into one Kinesis record.
   */
  public void sendAggregatedRecords(String streamName, String[] userRecords) throws Exception {
    RecordAggregator aggregator = new RecordAggregator();
    for (String data : userRecords) {
      AggRecord aggRecord = aggregator.addUserRecord("pk-" + System.nanoTime(), null, data.getBytes("UTF-8"));
      if (aggRecord != null) {
        putAggregatedRecord(streamName, aggRecord);
      }
    }
    AggRecord remaining = aggregator.clearAndGet();
    if (remaining != null && remaining.getNumUserRecords() > 0) {
      putAggregatedRecord(streamName, remaining);
    }
  }

  private void putAggregatedRecord(String streamName, AggRecord aggRecord) {
    com.amazonaws.services.kinesis.model.PutRecordRequest v1Request = aggRecord.toPutRecordRequest(streamName);
    ByteBuffer dataBuffer = v1Request.getData();
    byte[] dataBytes = new byte[dataBuffer.remaining()];
    dataBuffer.get(dataBytes);
    kinesisClient.putRecord(
        PutRecordRequest.builder()
            .streamName(streamName)
            .partitionKey(v1Request.getPartitionKey())
            .data(SdkBytes.fromByteArray(dataBytes))
            .build());
  }

  /**
   * Update shard count (triggers split or merge). Waits for stream to become ACTIVE again.
   */
  public void updateShardCount(String streamName, int targetShardCount) throws InterruptedException {
    kinesisClient.updateShardCount(
        UpdateShardCountRequest.builder()
            .streamName(streamName)
            .targetShardCount(targetShardCount)
            .scalingType(ScalingType.UNIFORM_SCALING)
            .build());
    waitForStreamActive(streamName, 60);
  }

  public void teardown() {
    try {
      if (kinesisClient != null) {
        kinesisClient.close();
      }
    } finally {
      kinesisClient = null;
      if (localStack != null && localStack.isRunning()) {
        localStack.stop();
      }
    }
  }
}
