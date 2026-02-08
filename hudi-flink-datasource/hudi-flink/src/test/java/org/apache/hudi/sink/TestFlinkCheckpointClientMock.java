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
import org.apache.hudi.sink.muttley.AthenaIngestionGateway;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * Test cases for FlinkCheckpointClient with mocked dependencies.
 */
public class TestFlinkCheckpointClientMock {

  @Test
  public void testGetKafkaCheckpointsInfoSuccess() throws IOException {
    // Create mock AthenaIngestionGateway
    AthenaIngestionGateway mockGateway = Mockito.mock(AthenaIngestionGateway.class);
    
    // Create mock response
    Map<Integer, Long> offsetMap = new HashMap<>();
    offsetMap.put(0, 1000L);
    offsetMap.put(1, 2000L);
    
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
    
    Mockito.when(mockGateway.getKafkaCheckpointsInfo(
        anyString(), anyString(), anyLong(), anyString(), anyString(), 
        anyString(), anyString(), anyInt(), any(Map.class), anyString(), anyString()))
        .thenReturn(Option.of(offsetInfo));
    
    // Create client with mock gateway
    FlinkCheckpointClient client = new FlinkCheckpointClient(mockGateway);
    
    // Create request
    FlinkCheckpointClient.CheckpointRequest request = FlinkCheckpointClient.CheckpointRequest.builder()
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
    Option<AthenaIngestionGateway.CheckpointKafkaOffsetInfo> result = client.getKafkaCheckpointsInfo(request);
    
    // Verify
    assertTrue(result.isPresent());
    assertEquals("12345", result.get().getCheckpointId());
    assertEquals("20231201120000", result.get().getHudiCommitInstantTime());
    assertEquals(1, result.get().getKafkaOffsetsInfo().size());
    assertEquals("test-topic", result.get().getKafkaOffsetsInfo().get(0).getTopicName());
  }

  @Test
  public void testGetKafkaCheckpointsInfoEmpty() throws IOException {
    // Create mock AthenaIngestionGateway that returns empty
    AthenaIngestionGateway mockGateway = Mockito.mock(AthenaIngestionGateway.class);
    
    Mockito.when(mockGateway.getKafkaCheckpointsInfo(
        anyString(), anyString(), anyLong(), anyString(), anyString(), 
        anyString(), anyString(), anyInt(), any(Map.class), anyString(), anyString()))
        .thenReturn(Option.empty());
    
    // Create client with mock gateway
    FlinkCheckpointClient client = new FlinkCheckpointClient(mockGateway);
    
    // Create request
    FlinkCheckpointClient.CheckpointRequest request = FlinkCheckpointClient.CheckpointRequest.builder()
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
    Option<AthenaIngestionGateway.CheckpointKafkaOffsetInfo> result = client.getKafkaCheckpointsInfo(request);
    
    // Verify
    assertFalse(result.isPresent());
  }

  @Test
  public void testGetKafkaCheckpointsInfoIOException() throws IOException {
    // Create mock AthenaIngestionGateway that throws IOException
    AthenaIngestionGateway mockGateway = Mockito.mock(AthenaIngestionGateway.class);
    
    Mockito.when(mockGateway.getKafkaCheckpointsInfo(
        anyString(), anyString(), anyLong(), anyString(), anyString(), 
        anyString(), anyString(), anyInt(), any(Map.class), anyString(), anyString()))
        .thenThrow(new IOException("Network error"));
    
    // Create client with mock gateway
    FlinkCheckpointClient client = new FlinkCheckpointClient(mockGateway);
    
    // Create request
    FlinkCheckpointClient.CheckpointRequest request = FlinkCheckpointClient.CheckpointRequest.builder()
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
    assertThrows(IOException.class, () -> client.getKafkaCheckpointsInfo(request));
  }

  @Test
  public void testGetKafkaCheckpointsInfoRuntimeException() throws IOException {
    // Create mock AthenaIngestionGateway that throws RuntimeException
    AthenaIngestionGateway mockGateway = Mockito.mock(AthenaIngestionGateway.class);
    
    Mockito.when(mockGateway.getKafkaCheckpointsInfo(
        anyString(), anyString(), anyLong(), anyString(), anyString(), 
        anyString(), anyString(), anyInt(), any(Map.class), anyString(), anyString()))
        .thenThrow(new RuntimeException("Unexpected error"));
    
    // Create client with mock gateway
    FlinkCheckpointClient client = new FlinkCheckpointClient(mockGateway);
    
    // Create request
    FlinkCheckpointClient.CheckpointRequest request = FlinkCheckpointClient.CheckpointRequest.builder()
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
    IOException thrown = assertThrows(IOException.class, () -> client.getKafkaCheckpointsInfo(request));
    assertEquals("Unexpected error getting checkpoint info", thrown.getMessage());
    assertTrue(thrown.getCause() instanceof RuntimeException);
  }

  @Test
  public void testCheckpointRequestBuilder() {
    // Test all builder methods
    FlinkCheckpointClient.CheckpointRequest request = FlinkCheckpointClient.CheckpointRequest.builder()
        .dc("test-dc")
        .env("test-env")
        .checkpointId(999L)
        .jobName("test-job-name")
        .hadoopUser("test-hadoop-user")
        .sourceCluster("test-source-cluster")
        .targetCluster("test-target-cluster")
        .checkpointLookback(5)
        .topicOperatorIds(Collections.singletonMap("topic1", "operator1"))
        .serviceTier("DEFAULT")
        .serviceName("ingestion-rt")
        .build();

    assertEquals("test-dc", request.getDc());
    assertEquals("test-env", request.getEnv());
    assertEquals(999L, request.getCheckpointId());
    assertEquals("test-job-name", request.getJobName());
    assertEquals("test-hadoop-user", request.getHadoopUser());
    assertEquals("test-source-cluster", request.getSourceCluster());
    assertEquals("test-target-cluster", request.getTargetCluster());
    assertEquals(5, request.getCheckpointLookback());
    assertEquals("operator1", request.getTopicOperatorIds().get("topic1"));
  }

  @Test
  public void testCheckpointRequestSetters() {
    // Test all setter methods
    FlinkCheckpointClient.CheckpointRequest request = new FlinkCheckpointClient.CheckpointRequest();
    
    request.setDc("set-dc");
    request.setEnv("set-env");
    request.setCheckpointId(888L);
    request.setJobName("set-job");
    request.setHadoopUser("set-user");
    request.setSourceCluster("set-source");
    request.setTargetCluster("set-target");
    request.setCheckpointLookback(10);
    request.setTopicOperatorIds(Collections.singletonMap("set-topic", "set-operator"));
    request.setServiceTier("DEFAULT");
    request.setServiceName("ingestion-rt");

    assertEquals("set-dc", request.getDc());
    assertEquals("set-env", request.getEnv());
    assertEquals(888L, request.getCheckpointId());
    assertEquals("set-job", request.getJobName());
    assertEquals("set-user", request.getHadoopUser());
    assertEquals("set-source", request.getSourceCluster());
    assertEquals("set-target", request.getTargetCluster());
    assertEquals(10, request.getCheckpointLookback());
    assertEquals("set-operator", request.getTopicOperatorIds().get("set-topic"));
    assertEquals("DEFAULT", request.getServiceTier());
    assertEquals("ingestion-rt", request.getServiceName());
  }

  @Test
  public void testCheckpointRequestToString() {
    FlinkCheckpointClient.CheckpointRequest request = FlinkCheckpointClient.CheckpointRequest.builder()
        .dc("dca")
        .env("prod")
        .checkpointId(123L)
        .jobName("job1")
        .hadoopUser("user1")
        .sourceCluster("src")
        .targetCluster("tgt")
        .checkpointLookback(0)
        .topicOperatorIds(Collections.singletonMap("topic1", "operator1"))
        .serviceTier("DEFAULT")
        .serviceName("ingestion-rt")
        .build();

    String str = request.toString();
    assertTrue(str.contains("dc='dca'"));
    assertTrue(str.contains("env='prod'"));
    assertTrue(str.contains("checkpointId=123"));
    assertTrue(str.contains("jobName='job1'"));
    assertTrue(str.contains("hadoopUser='user1'"));
    assertTrue(str.contains("sourceCluster='src'"));
    assertTrue(str.contains("targetCluster='tgt'"));
    assertTrue(str.contains("checkpointLookback=0"));
    assertTrue(str.contains("topicOperatorIds={topic1=operator1}"));
    assertTrue(str.contains("serviceTier='DEFAULT'"));
    assertTrue(str.contains("serviceName='ingestion-rt'"));
  }

  @Test
  public void testCheckpointRequestAllArgsConstructor() {
    FlinkCheckpointClient.CheckpointRequest request = new FlinkCheckpointClient.CheckpointRequest(
        "dc1", "env1", 456L, "job2", "user2", "src2", "tgt2", 3,
        Collections.singletonMap("t1", "operator1"), "DEFAULT", "ingestion-rt"
    );

    assertEquals("dc1", request.getDc());
    assertEquals("env1", request.getEnv());
    assertEquals(456L, request.getCheckpointId());
    assertEquals("job2", request.getJobName());
    assertEquals("user2", request.getHadoopUser());
    assertEquals("src2", request.getSourceCluster());
    assertEquals("tgt2", request.getTargetCluster());
    assertEquals(3, request.getCheckpointLookback());
    assertEquals("operator1", request.getTopicOperatorIds().get("t1"));
  }

  @Test
  public void testFlinkCheckpointClientWithCallerServiceName() {
    // Test constructor with caller service name
    FlinkCheckpointClient client = new FlinkCheckpointClient("test-caller-service", "athena-job-manager");
    assertNotNull(client);
  }
}