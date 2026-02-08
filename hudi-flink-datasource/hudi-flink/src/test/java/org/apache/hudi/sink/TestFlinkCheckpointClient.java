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
import org.apache.hudi.sink.FlinkCheckpointClient.CheckpointRequest;
import org.apache.hudi.sink.muttley.AthenaIngestionGateway;
import org.apache.hudi.sink.muttley.AthenaIngestionGateway.CheckpointKafkaOffsetInfo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestFlinkCheckpointClient {

  private static final Logger LOG = LoggerFactory.getLogger(TestFlinkCheckpointClient.class);

  @Test
  @Disabled("Requires real connection to Athena service")
  public void testGetKafkaCheckpointsInfo() throws IOException {
    LOG.info("Test starts");
    // Create client with MuttleyClient implementation
    // TODO: Update with actual Athena service endpoint when available
    FlinkCheckpointClient client = new FlinkCheckpointClient(
        "test-service",  // Caller service name
        "athena-job-manager"  // Athena service name
    );

    // Build request with your provided values
    CheckpointRequest request = CheckpointRequest.builder()
        .dc("dca")
        .env("production")
        .checkpointId(14458L)
        .jobName("production_hp-bliss-policy-engine-execution-service-execute-policy_streaming_shadow_1")
        .hadoopUser("hoover")
        .sourceCluster("kafka-ingestion-dca")
        .targetCluster("kafka-ingestion-dca")
        .checkpointLookback(0)
        .topicOperatorIds(Collections.singletonMap(
            "hp-bliss-policy-engine-execution-service-execute-policy_streaming_shadow",
            "production_hp-bliss-policy-engine-execution-service-execute-policy_streaming_shadow_1_production_dca1_hadoop_platform_self_serve_5_kafka_hoodie_streaming_"
            + "hp-bliss-policy-engine-execution-service-execute-policy_streaming_shadow"))
        .serviceTier("DEFAULT")
        .serviceName("ingestion-rt")
        .build();

    try {
      Option<CheckpointKafkaOffsetInfo> result = client.getKafkaCheckpointsInfo(request);
      
      // Assert the result
      assertNotNull(result);
      LOG.info("Result is not null");
      if (result.isPresent()) {
        CheckpointKafkaOffsetInfo info = result.get();
        LOG.info("Checkpoint info retrieved successfully:");
        LOG.info("  Checkpoint ID: {}", info.getCheckpointId());
        LOG.info("  Checkpoint Timestamp: {}", info.getCheckpointTimestamp());
        LOG.info("  Hudi Commit Time: {}", info.getHudiCommitInstantTime());
        LOG.info("  Kafka Offsets Info: {}", info.getKafkaOffsetsInfo());
      } else {
        LOG.warn("No checkpoint info found");
      }
    } catch (IOException e) {
      LOG.error("Failed to connect to Athena service", e);
      // This is expected when not connected to a real Athena service
      LOG.info("Test skipped - requires actual Athena service connection");
    }
  }
  
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
  public void testFlinkCheckpointClientConstructor() {
    // Test that we can create a client with both caller and callee service names
    FlinkCheckpointClient client = new FlinkCheckpointClient("my-service", "athena-job-manager");
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
  public void testGetKafkaCheckpointsInfoWithMockSuccess() throws IOException {
    // Create mock AthenaIngestionGateway
    AthenaIngestionGateway mockGateway = Mockito.mock(AthenaIngestionGateway.class);
    
    // Create mock response
    Map<Integer, Long> offsetMap = new HashMap<>();
    offsetMap.put(0, 1000L);
    offsetMap.put(1, 2000L);
    offsetMap.put(2, 3000L);
    
    AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo.Offsets offsets = 
        new AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo.Offsets(offsetMap);
    
    AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo kafkaOffsetsInfo = 
        new AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo("test-topic", "test-cluster", offsets);
    
    AthenaIngestionGateway.CheckpointKafkaOffsetInfo offsetInfo = 
        new AthenaIngestionGateway.CheckpointKafkaOffsetInfo(
            "1234567890",
            Collections.singletonList(kafkaOffsetsInfo),
            "20231201120000",
            "12345"
        );
    
    // Configure mock
    Mockito.when(mockGateway.getKafkaCheckpointsInfo(
        anyString(), anyString(), anyLong(), anyString(), anyString(), 
        anyString(), anyString(), anyInt(), any(Map.class), anyString(), anyString()))
        .thenReturn(Option.of(offsetInfo));
    
    // Create client with mock gateway
    FlinkCheckpointClient client = new FlinkCheckpointClient(mockGateway);
    
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
    Option<CheckpointKafkaOffsetInfo> result = client.getKafkaCheckpointsInfo(request);
    
    // Verify
    assertTrue(result.isPresent());
    assertEquals("12345", result.get().getCheckpointId());
    assertEquals("20231201120000", result.get().getHudiCommitInstantTime());
    assertEquals("1234567890", result.get().getCheckpointTimestamp());
    assertEquals(1, result.get().getKafkaOffsetsInfo().size());
    assertEquals("test-topic", result.get().getKafkaOffsetsInfo().get(0).getTopicName());
    assertEquals(3, result.get().getKafkaOffsetsInfo().get(0).getOffsets().getOffsets().size());
  }
  
  @Test
  public void testGetKafkaCheckpointsInfoWithMockEmpty() throws IOException {
    // Create mock AthenaIngestionGateway that returns empty
    AthenaIngestionGateway mockGateway = Mockito.mock(AthenaIngestionGateway.class);
    
    Mockito.when(mockGateway.getKafkaCheckpointsInfo(
        anyString(), anyString(), anyLong(), anyString(), anyString(), 
        anyString(), anyString(), anyInt(), any(Map.class), anyString(), anyString()))
        .thenReturn(Option.empty());
    
    // Create client with mock gateway
    FlinkCheckpointClient client = new FlinkCheckpointClient(mockGateway);
    
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
    Option<CheckpointKafkaOffsetInfo> result = client.getKafkaCheckpointsInfo(request);
    
    // Verify
    assertFalse(result.isPresent());
  }
  
  @Test
  public void testGetKafkaCheckpointsInfoWithMockIOException() throws IOException {
    // Create mock AthenaIngestionGateway that throws IOException
    AthenaIngestionGateway mockGateway = Mockito.mock(AthenaIngestionGateway.class);
    
    Mockito.when(mockGateway.getKafkaCheckpointsInfo(
        anyString(), anyString(), anyLong(), anyString(), anyString(), 
        anyString(), anyString(), anyInt(), any(Map.class), anyString(), anyString()))
        .thenThrow(new IOException("Network error"));
    
    // Create client with mock gateway
    FlinkCheckpointClient client = new FlinkCheckpointClient(mockGateway);
    
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
      client.getKafkaCheckpointsInfo(request);
      fail("Expected IOException");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Network error"));
    }
  }
  
  @Test
  public void testGetKafkaCheckpointsInfoWithMockRuntimeException() throws IOException {
    // Create mock AthenaIngestionGateway that throws RuntimeException
    AthenaIngestionGateway mockGateway = Mockito.mock(AthenaIngestionGateway.class);
    
    Mockito.when(mockGateway.getKafkaCheckpointsInfo(
        anyString(), anyString(), anyLong(), anyString(), anyString(), 
        anyString(), anyString(), anyInt(), any(Map.class), anyString(), anyString()))
        .thenThrow(new RuntimeException("Unexpected error"));
    
    // Create client with mock gateway
    FlinkCheckpointClient client = new FlinkCheckpointClient(mockGateway);
    
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
      client.getKafkaCheckpointsInfo(request);
      fail("Expected IOException");
    } catch (IOException e) {
      assertEquals("Unexpected error getting checkpoint info", e.getMessage());
      assertTrue(e.getCause() instanceof RuntimeException);
      assertEquals("Unexpected error", e.getCause().getMessage());
    }
  }
}