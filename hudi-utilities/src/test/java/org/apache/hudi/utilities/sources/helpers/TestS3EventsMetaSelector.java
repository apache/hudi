/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;
import org.apache.hudi.utilities.testutils.CloudObjectTestUtils;

import org.apache.hadoop.fs.Path;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.utilities.config.S3SourceConfig.S3_SOURCE_QUEUE_REGION;
import static org.apache.hudi.utilities.config.S3SourceConfig.S3_SOURCE_QUEUE_URL;
import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector.SQS_ATTR_APPROX_MESSAGES;
import static org.apache.hudi.utilities.sources.helpers.TestCloudObjectsSelector.REGION_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class TestS3EventsMetaSelector extends HoodieSparkClientTestHarness {

  TypedProperties props;
  String sqsUrl;

  @Mock
  SqsClient sqs;

  @Mock
  private S3EventsMetaSelector s3EventsMetaSelector;

  @BeforeEach
  void setUp() {
    initSparkContexts();
    initPath();
    initHoodieStorage();
    MockitoAnnotations.initMocks(this);

    props = new TypedProperties();
    sqsUrl = "test-queue";
    props.setProperty(S3_SOURCE_QUEUE_URL.key(), sqsUrl);
    props.setProperty(S3_SOURCE_QUEUE_REGION.key(), REGION_NAME);
  }

  @AfterEach
  public void teardown() throws Exception {
    Mockito.reset(s3EventsMetaSelector);
    cleanupResources();
  }

  @ParameterizedTest
  @ValueSource(classes = {S3EventsMetaSelector.class})
  public void testNextEventsFromQueueShouldReturnsEventsFromQueue(Class<?> clazz) {
    S3EventsMetaSelector selector = (S3EventsMetaSelector) ReflectionUtils.loadClass(clazz.getName(), props);
    // setup s3 record
    String bucket = "test-bucket";
    String key = "part%3Dpart%2Bpart%24part%C2%A3part%23part%26part%3Fpart%7Epart%25.snappy.parquet";
    String keyRes = "part=part+part$part£part#part&part?part~part%.snappy.parquet";
    Path path = new Path(bucket, key);
    CloudObjectTestUtils.setMessagesInQueue(sqs, path);

    List<CloudObjectsSelector.MessageTracker> processed = new ArrayList<>();

    // test the return values
    Pair<List<String>, Checkpoint> eventFromQueue =
        selector.getNextEventsFromQueue(sqs, Option.empty(), processed);

    assertEquals(1, eventFromQueue.getLeft().size());
    assertEquals(1, processed.size());
    assertEquals(
        keyRes,
        new JSONObject(eventFromQueue.getLeft().get(0))
            .getJSONObject("s3")
            .getJSONObject("object")
            .getString("key"));
    assertEquals("1627376736755", eventFromQueue.getRight().getCheckpointKey());
  }

  @Test
  public void testEventsFromQueueNoMessages() {
    S3EventsMetaSelector selector = new S3EventsMetaSelector(props);
    Map<String, String> attribute = new HashMap<>();
    attribute.put(SQS_ATTR_APPROX_MESSAGES, "0");
    when(sqs.getQueueAttributes(any(GetQueueAttributesRequest.class)))
        .thenReturn(
            GetQueueAttributesResponse.builder()
                .attributesWithStrings(attribute)
                .build());

    List<CloudObjectsSelector.MessageTracker> processed = new ArrayList<>();
    Pair<List<String>, Checkpoint> eventFromQueue =
        selector.getNextEventsFromQueue(sqs, Option.empty(), processed);

    assertEquals(0, eventFromQueue.getLeft().size());
    assertEquals(0, processed.size());
    assertNull(eventFromQueue.getRight());
  }
}
