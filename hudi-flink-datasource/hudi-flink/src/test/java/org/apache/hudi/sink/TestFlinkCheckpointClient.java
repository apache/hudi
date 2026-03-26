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

package org.apache.hudi.sink;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.sink.checkpoint.AthenaCheckpointService;
import org.apache.hudi.sink.checkpoint.CheckpointService;
import org.apache.hudi.sink.checkpoint.CheckpointService.CheckpointInfo;
import org.apache.hudi.sink.checkpoint.CheckpointService.CheckpointRequest;
import org.apache.hudi.sink.checkpoint.NoOpCheckpointService;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;

public class TestFlinkCheckpointClient {

  private static final Logger LOG = LoggerFactory.getLogger(TestFlinkCheckpointClient.class);

  @Test
  public void testCheckpointRequestBuilder() {
    // Test the builder pattern
    CheckpointRequest request = CheckpointRequest.builder()
        .dc("test-dc")
        .env("test-env")
        .checkpointId(12345L)
        .jobName("test-job")
        .hadoopUser("test-user")
        .sourceCluster("source-cluster")
        .targetCluster("target-cluster")
        .checkpointLookback(5)
        .topicOperatorIds(Collections.singletonMap("topic1", "operator1"))
        .serviceTier("DEFAULT")
        .serviceName("ingestion-rt")
        .build();

    assertEquals("test-dc", request.getDc());
    assertEquals("test-env", request.getEnv());
    assertEquals(12345L, request.getCheckpointId());
    assertEquals("test-job", request.getJobName());
    assertEquals("test-user", request.getHadoopUser());
    assertEquals("source-cluster", request.getSourceCluster());
    assertEquals("target-cluster", request.getTargetCluster());
    assertEquals(5, request.getCheckpointLookback());
    assertEquals(1, request.getTopicOperatorIds().size());
    assertEquals("operator1", request.getTopicOperatorIds().get("topic1"));
  }

  @Test
  public void testCheckpointRequestSettersAndGetters() {
    // Test setters and getters
    CheckpointRequest request = new CheckpointRequest();

    request.setDc("dc-setter");
    request.setEnv("env-setter");
    request.setCheckpointId(999L);
    request.setJobName("job-setter");
    request.setHadoopUser("user-setter");
    request.setSourceCluster("source-setter");
    request.setTargetCluster("target-setter");
    request.setCheckpointLookback(10);
    request.setTopicOperatorIds(Collections.singletonMap("topic-setter", "topic-operator"));
    request.setServiceTier("DEFAULT");
    request.setServiceName("ingestion-rt");

    assertEquals("dc-setter", request.getDc());
    assertEquals("env-setter", request.getEnv());
    assertEquals(999L, request.getCheckpointId());
    assertEquals("job-setter", request.getJobName());
    assertEquals("user-setter", request.getHadoopUser());
    assertEquals("source-setter", request.getSourceCluster());
    assertEquals("target-setter", request.getTargetCluster());
    assertEquals(10, request.getCheckpointLookback());
    assertEquals(1, request.getTopicOperatorIds().size());
    assertEquals("topic-operator", request.getTopicOperatorIds().get("topic-setter"));
    assertEquals("DEFAULT", request.getServiceTier());
    assertEquals("ingestion-rt", request.getServiceName());
  }

  @Test
  public void testCheckpointRequestAllArgsConstructor() {
    // Test all-args constructor
    CheckpointRequest request = new CheckpointRequest(
        "dc1", "env1", 111L, "job1", "user1", "source1", "target1",
        1, Collections.singletonMap("t1", "operator1"), "DEFAULT", "ingestion-rt"
    );

    assertEquals("dc1", request.getDc());
    assertEquals("env1", request.getEnv());
    assertEquals(111L, request.getCheckpointId());
    assertEquals("job1", request.getJobName());
    assertEquals("user1", request.getHadoopUser());
    assertEquals("source1", request.getSourceCluster());
    assertEquals("target1", request.getTargetCluster());
    assertEquals(1, request.getCheckpointLookback());
    assertEquals(1, request.getTopicOperatorIds().size());
    assertEquals("operator1", request.getTopicOperatorIds().get("t1"));
  }

  @Test
  public void testCheckpointRequestToString() {
    // Test toString method
    CheckpointRequest request = CheckpointRequest.builder()
        .dc("dc-test")
        .env("env-test")
        .checkpointId(777L)
        .jobName("job-test")
        .hadoopUser("user-test")
        .sourceCluster("source-test")
        .targetCluster("target-test")
        .checkpointLookback(7)
        .topicOperatorIds(Collections.singletonMap("topic-test", "topic-operator"))
        .serviceTier("DEFAULT")
        .serviceName("ingestion-rt")
        .build();

    String str = request.toString();
    assertTrue(str.contains("dc='dc-test'"));
    assertTrue(str.contains("env='env-test'"));
    assertTrue(str.contains("checkpointId=777"));
    assertTrue(str.contains("jobName='job-test'"));
    assertTrue(str.contains("hadoopUser='user-test'"));
    assertTrue(str.contains("sourceCluster='source-test'"));
    assertTrue(str.contains("targetCluster='target-test'"));
    assertTrue(str.contains("checkpointLookback=7"));
    assertTrue(str.contains("topicOperatorIds={topic-test=topic-operator}"));
    assertTrue(str.contains("serviceTier='DEFAULT'"));
    assertTrue(str.contains("serviceName='ingestion-rt'"));
  }

  @Test
  public void testCheckpointRequestBuilderWithEmptyTopicIds() {
    // Test builder with empty topic IDs
    CheckpointRequest request = CheckpointRequest.builder()
        .dc("dc")
        .env("env")
        .checkpointId(1L)
        .jobName("job")
        .hadoopUser("user")
        .sourceCluster("source")
        .targetCluster("target")
        .checkpointLookback(0)
        .topicOperatorIds(Collections.emptyMap())
        .serviceTier("DEFAULT")
        .serviceName("ingestion-rt")
        .build();

    assertNotNull(request.getTopicOperatorIds());
    assertEquals(0, request.getTopicOperatorIds().size());
  }

  @Test
  public void testCheckpointRequestBuilderPartialBuild() {
    // Test builder with some null values
    CheckpointRequest.Builder builder = CheckpointRequest.builder();
    builder.dc("only-dc");
    builder.checkpointId(123L);
    CheckpointRequest request = builder.build();

    assertEquals("only-dc", request.getDc());
    assertEquals(123L, request.getCheckpointId());
    assertNull(request.getEnv());
    assertNull(request.getJobName());
    assertNull(request.getHadoopUser());
    assertNull(request.getSourceCluster());
    assertNull(request.getTargetCluster());
    assertEquals(0, request.getCheckpointLookback());
    assertNull(request.getTopicOperatorIds());
  }

  @Test
  public void testFlinkCheckpointClientDefaultConstructor() {
    // Test default constructor creates client with NoOp service
    FlinkCheckpointClient client = new FlinkCheckpointClient();
    assertNotNull(client);
  }

  @Test
  public void testFlinkCheckpointClientWithCustomService() {
    // Test that we can create a client with a custom service
    CheckpointService customService = new NoOpCheckpointService();
    FlinkCheckpointClient client = new FlinkCheckpointClient(customService);
    assertNotNull(client);
  }

  @Test
  public void testCheckpointRequestDefaultConstructor() {
    // Test default constructor initializes properly
    CheckpointRequest request = new CheckpointRequest();

    assertNull(request.getDc());
    assertNull(request.getEnv());
    assertEquals(0L, request.getCheckpointId());
    assertNull(request.getJobName());
    assertNull(request.getHadoopUser());
    assertNull(request.getSourceCluster());
    assertNull(request.getTargetCluster());
    assertEquals(0, request.getCheckpointLookback());
    assertNull(request.getTopicOperatorIds());
  }

  @Test
  public void testGetCheckpointInfoWithNoOpService() throws IOException {
    // Test with NoOp service - should return empty
    FlinkCheckpointClient client = new FlinkCheckpointClient();

    CheckpointRequest request = CheckpointRequest.builder()
        .dc("dca")
        .env("production")
        .checkpointId(12345L)
        .jobName("test-job")
        .hadoopUser("test-user")
        .sourceCluster("kafka-source")
        .targetCluster("kafka-target")
        .checkpointLookback(0)
        .topicOperatorIds(Collections.singletonMap("test-topic", "test-operator"))
        .serviceTier("DEFAULT")
        .serviceName("ingestion-rt")
        .build();

    Option<CheckpointInfo> result = client.getCheckpointInfo(request);

    // NoOp service should return empty
    assertFalse(result.isPresent());
  }

  @Test
  public void testGetCheckpointInfoWithMockSuccess() throws IOException {
    // Create mock CheckpointService
    CheckpointService mockService = Mockito.mock(CheckpointService.class);

    // Create mock response
    Map<String, String> kafkaOffsets = new HashMap<>();
    kafkaOffsets.put("test-topic:test-cluster:0", "1000");
    kafkaOffsets.put("test-topic:test-cluster:1", "2000");
    kafkaOffsets.put("test-topic:test-cluster:2", "3000");

    CheckpointInfo mockInfo = new CheckpointInfo(kafkaOffsets, 12345L);

    // Configure mock
    Mockito.when(mockService.getCheckpointInfo(any(CheckpointRequest.class)))
        .thenReturn(Option.of(mockInfo));

    // Create client with mock service
    FlinkCheckpointClient client = new FlinkCheckpointClient(mockService);

    // Create request
    CheckpointRequest request = CheckpointRequest.builder()
        .dc("dca")
        .env("production")
        .checkpointId(12345L)
        .jobName("test-job")
        .hadoopUser("test-user")
        .sourceCluster("kafka-source")
        .targetCluster("kafka-target")
        .checkpointLookback(0)
        .topicOperatorIds(Collections.singletonMap("test-topic", "test-operator"))
        .serviceTier("DEFAULT")
        .serviceName("ingestion-rt")
        .build();

    // Test
    Option<CheckpointInfo> result = client.getCheckpointInfo(request);

    // Verify
    assertTrue(result.isPresent());
    assertEquals(12345L, result.get().getCheckpointId());
    assertEquals(3, result.get().getKafkaOffsets().size());
    assertEquals("1000", result.get().getKafkaOffsets().get("test-topic:test-cluster:0"));
    assertEquals("2000", result.get().getKafkaOffsets().get("test-topic:test-cluster:1"));
    assertEquals("3000", result.get().getKafkaOffsets().get("test-topic:test-cluster:2"));
  }

  @Test
  public void testGetCheckpointInfoWithMockEmpty() throws IOException {
    // Create mock CheckpointService that returns empty
    CheckpointService mockService = Mockito.mock(CheckpointService.class);

    Mockito.when(mockService.getCheckpointInfo(any(CheckpointRequest.class)))
        .thenReturn(Option.empty());

    // Create client with mock service
    FlinkCheckpointClient client = new FlinkCheckpointClient(mockService);

    // Create request
    CheckpointRequest request = CheckpointRequest.builder()
        .dc("dca")
        .env("production")
        .checkpointId(12345L)
        .jobName("test-job")
        .hadoopUser("test-user")
        .sourceCluster("kafka-source")
        .targetCluster("kafka-target")
        .checkpointLookback(0)
        .topicOperatorIds(Collections.singletonMap("test-topic", "test-operator"))
        .serviceTier("DEFAULT")
        .serviceName("ingestion-rt")
        .build();

    // Test
    Option<CheckpointInfo> result = client.getCheckpointInfo(request);

    // Verify
    assertFalse(result.isPresent());
  }

  @Test
  public void testGetCheckpointInfoWithMockIOException() throws IOException {
    // Create mock CheckpointService that throws IOException
    CheckpointService mockService = Mockito.mock(CheckpointService.class);

    Mockito.when(mockService.getCheckpointInfo(any(CheckpointRequest.class)))
        .thenThrow(new IOException("Network error"));

    // Create client with mock service
    FlinkCheckpointClient client = new FlinkCheckpointClient(mockService);

    // Create request
    CheckpointRequest request = CheckpointRequest.builder()
        .dc("dca")
        .env("production")
        .checkpointId(12345L)
        .jobName("test-job")
        .hadoopUser("test-user")
        .sourceCluster("kafka-source")
        .targetCluster("kafka-target")
        .checkpointLookback(0)
        .topicOperatorIds(Collections.singletonMap("test-topic", "test-operator"))
        .serviceTier("DEFAULT")
        .serviceName("ingestion-rt")
        .build();

    // Test - should throw IOException
    try {
      client.getCheckpointInfo(request);
      fail("Expected IOException");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Network error"));
    }
  }

  @Test
  public void testGetCheckpointInfoWithMockRuntimeException() throws IOException {
    // Create mock CheckpointService that throws RuntimeException
    CheckpointService mockService = Mockito.mock(CheckpointService.class);

    Mockito.when(mockService.getCheckpointInfo(any(CheckpointRequest.class)))
        .thenThrow(new RuntimeException("Unexpected error"));

    // Create client with mock service
    FlinkCheckpointClient client = new FlinkCheckpointClient(mockService);

    // Create request
    CheckpointRequest request = CheckpointRequest.builder()
        .dc("dca")
        .env("production")
        .checkpointId(12345L)
        .jobName("test-job")
        .hadoopUser("test-user")
        .sourceCluster("kafka-source")
        .targetCluster("kafka-target")
        .checkpointLookback(0)
        .topicOperatorIds(Collections.singletonMap("test-topic", "test-operator"))
        .serviceTier("DEFAULT")
        .serviceName("ingestion-rt")
        .build();

    // Test - RuntimeException should be wrapped in IOException
    try {
      client.getCheckpointInfo(request);
      fail("Expected IOException");
    } catch (IOException e) {
      assertEquals("Unexpected error getting checkpoint info", e.getMessage());
      assertTrue(e.getCause() instanceof RuntimeException);
      assertEquals("Unexpected error", e.getCause().getMessage());
    }
  }

  @Test
  public void testCheckpointInfo() {
    // Test CheckpointInfo class
    Map<String, String> offsets = new HashMap<>();
    offsets.put("topic1:cluster1:0", "1000");
    offsets.put("topic1:cluster1:1", "2000");

    CheckpointInfo info = new CheckpointInfo(offsets, 54321L);

    assertEquals(2, info.getKafkaOffsets().size());
    assertEquals("1000", info.getKafkaOffsets().get("topic1:cluster1:0"));
    assertEquals("2000", info.getKafkaOffsets().get("topic1:cluster1:1"));
    assertEquals(54321L, info.getCheckpointId());

    String str = info.toString();
    assertTrue(str.contains("kafkaOffsets"));
    assertTrue(str.contains("checkpointId=54321"));
  }
}
