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
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;
import org.apache.hudi.utilities.testutils.CloudObjectTestUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.hudi.utilities.config.S3SourceConfig.S3_SOURCE_QUEUE_PROCESSING_PARALLELISM;
import static org.apache.hudi.utilities.config.S3SourceConfig.S3_SOURCE_QUEUE_REGION;
import static org.apache.hudi.utilities.config.S3SourceConfig.S3_SOURCE_QUEUE_URL;
import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector.S3_FILE_PATH;
import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector.S3_FILE_SIZE;
import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector.S3_MODEL_EVENT_TIME;
import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector.S3_PREFIX;
import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector.SQS_ATTR_APPROX_MESSAGES;
import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector.SQS_ATTR_MESSAGE_RETENTION_PERIOD;
import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector.SQS_MODEL_EVENT_RECORDS;
import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector.SQS_MODEL_MESSAGE;
import static org.apache.hudi.utilities.testutils.CloudObjectTestUtils.deleteMessagesInQueue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestCloudObjectsSelector extends HoodieSparkClientTestHarness {

  static final String REGION_NAME = "us-east-1";

  TypedProperties props;
  String sqsUrl;

  @Mock
  SqsClient sqs;

  @Mock
  private CloudObjectsSelector cloudObjectsSelector;

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
    Mockito.reset(cloudObjectsSelector);
    cleanupResources();
  }

  @ParameterizedTest
  @ValueSource(classes = {CloudObjectsSelector.class})
  public void testSqsQueueAttributesShouldReturnsRequiredAttribute(Class<?> clazz) {
    CloudObjectsSelector selector =
        (CloudObjectsSelector) ReflectionUtils.loadClass(clazz.getName(), props);

    // setup the queue attributes
    CloudObjectTestUtils.setMessagesInQueue(sqs, null);

    // test the return values
    Map<String, String> queueAttributes = selector.getSqsQueueAttributes(sqs, sqsUrl);
    assertEquals(1, queueAttributes.size());
    // ApproximateNumberOfMessages is a required queue attribute for Cloud object selector
    assertEquals("0", queueAttributes.get(SQS_ATTR_APPROX_MESSAGES));
  }

  @ParameterizedTest
  @ValueSource(classes = {CloudObjectsSelector.class})
  public void testFileAttributesFromRecordShouldReturnsExpectOutput(Class<?> clazz)
      throws IOException {

    CloudObjectsSelector selector =
        (CloudObjectsSelector) ReflectionUtils.loadClass(clazz.getName(), props);

    // setup s3 record
    String bucket = "test-bucket";
    String key = "test/year=test1/month=test2/day=test3/part-foo-bar.snappy.parquet";

    String s3Records =
        "{\n  \"Type\" : \"Notification\",\n  \"MessageId\" : \"1\",\n  \"TopicArn\" : \"arn:aws:sns:foo:123:"
            + "foo-bar\",\n  \"Subject\" : \"Amazon S3 Notification\",\n  \"Message\" : \"{\\\"Records\\\":"
            + "[{\\\"eventVersion\\\":\\\"2.1\\\",\\\"eventSource\\\":\\\"aws:s3\\\",\\\"awsRegion\\\":\\\"us"
            + "-west-2\\\",\\\"eventTime\\\":\\\"2021-07-27T09:05:36.755Z\\\",\\\"eventName\\\":\\\"ObjectCreated"
            + ":Copy\\\",\\\"userIdentity\\\":{\\\"principalId\\\":\\\"AWS:test\\\"},\\\"requestParameters\\\":"
            + "{\\\"sourceIPAddress\\\":\\\"0.0.0.0\\\"},\\\"responseElements\\\":{\\\"x-amz-request-id\\\":\\\""
            + "test\\\",\\\"x-amz-id-2\\\":\\\"foobar\\\"},\\\"s3\\\":{\\\"s3SchemaVersion\\\":\\\"1.0\\\",\\\""
            + "configurationId\\\":\\\"foobar\\\",\\\"bucket\\\":{\\\"name\\\":\\\""
            + bucket
            + "\\\",\\\"ownerIdentity\\\":{\\\"principalId\\\":\\\"foo\\\"},\\\"arn\\\":\\\"arn:aws:s3:::foo\\\"}"
            + ",\\\"object\\\":{\\\"key\\\":\\\""
            + key
            + "\\\",\\\"size\\\":123,\\\"eTag\\\":\\\"test\\\",\\\"sequencer\\\":\\\"1\\\"}}}]}\"}";
    JSONObject messageBody = new JSONObject(s3Records);
    Map<String, Object> messageMap = new HashMap<>();
    if (messageBody.has(SQS_MODEL_MESSAGE)) {
      ObjectMapper mapper = new ObjectMapper();
      messageMap =
          (Map<String, Object>) mapper.readValue(messageBody.getString(SQS_MODEL_MESSAGE), Map.class);
    }
    List<Map<String, Object>> records = (List<Map<String, Object>>) messageMap.get(SQS_MODEL_EVENT_RECORDS);

    // test the return values
    Map<String, Object> fileAttributes =
        selector.getFileAttributesFromRecord(new JSONObject(records.get(0)));

    assertEquals(3, fileAttributes.size());
    assertEquals(123L, (long) fileAttributes.get(S3_FILE_SIZE));
    assertEquals(S3_PREFIX + bucket + "/" + key, fileAttributes.get(S3_FILE_PATH));
    assertEquals(1627376736755L, (long) fileAttributes.get(S3_MODEL_EVENT_TIME));
  }

  @ParameterizedTest
  @ValueSource(classes = {CloudObjectsSelector.class})
  public void testCreateListPartitionsReturnsExpectedSetOfBatch(Class<?> clazz) {

    CloudObjectsSelector selector =
        (CloudObjectsSelector) ReflectionUtils.loadClass(clazz.getName(), props);

    // setup lists
    List<CloudObjectsSelector.MessageTracker> testSingleList = new ArrayList<>();
    testSingleList.add(new CloudObjectsSelector.MessageTracker(Message.builder().attributesWithStrings(createAttributeMap("id", "1")).build()));
    testSingleList.add(new CloudObjectsSelector.MessageTracker(Message.builder().attributesWithStrings(createAttributeMap("id", "2")).build()));
    testSingleList.add(new CloudObjectsSelector.MessageTracker(Message.builder().attributesWithStrings(createAttributeMap("id", "3")).build()));
    testSingleList.add(new CloudObjectsSelector.MessageTracker(Message.builder().attributesWithStrings(createAttributeMap("id", "4")).build()));
    testSingleList.add(new CloudObjectsSelector.MessageTracker(Message.builder().attributesWithStrings(createAttributeMap("id", "5")).build()));

    List<CloudObjectsSelector.MessageTracker> expectedFirstList = new ArrayList<>();
    expectedFirstList.add(testSingleList.get(0));
    expectedFirstList.add(testSingleList.get(1));

    List<CloudObjectsSelector.MessageTracker> expectedSecondList = new ArrayList<>();
    expectedSecondList.add(testSingleList.get(2));
    expectedSecondList.add(testSingleList.get(3));

    List<CloudObjectsSelector.MessageTracker> expectedFinalList = new ArrayList<>();
    expectedFinalList.add(testSingleList.get(4));

    //  test the return values
    List<List<CloudObjectsSelector.MessageTracker>> partitionedList = selector.createListPartitions(testSingleList, 2);

    assertEquals(3, partitionedList.size());
    assertEquals(expectedFirstList, partitionedList.get(0));
    assertEquals(expectedSecondList, partitionedList.get(1));
    assertEquals(expectedFinalList, partitionedList.get(2));
  }

  @ParameterizedTest
  @ValueSource(classes = {CloudObjectsSelector.class})
  public void testCreateListPartitionsReturnsEmptyIfBatchSizeIsZero(Class<?> clazz) {

    CloudObjectsSelector selector =
        (CloudObjectsSelector) ReflectionUtils.loadClass(clazz.getName(), props);

    // setup lists
    List<CloudObjectsSelector.MessageTracker> testSingleList = new ArrayList<>();
    testSingleList.add(new CloudObjectsSelector.MessageTracker(Message.builder().attributesWithStrings(createAttributeMap("id", "1")).build()));
    testSingleList.add(new CloudObjectsSelector.MessageTracker(Message.builder().attributesWithStrings(createAttributeMap("id", "2")).build()));

    //  test the return values
    List<List<CloudObjectsSelector.MessageTracker>> partitionedList = selector.createListPartitions(testSingleList, 0);

    assertEquals(0, partitionedList.size());
  }

  @ParameterizedTest
  @ValueSource(classes = {CloudObjectsSelector.class})
  public void testOnCommitDeleteProcessedMessages(Class<?> clazz) {

    CloudObjectsSelector selector =
        (CloudObjectsSelector) ReflectionUtils.loadClass(clazz.getName(), props);

    // setup lists
    List<CloudObjectsSelector.MessageTracker> testSingleList = new ArrayList<>();
    testSingleList.add(
        new CloudObjectsSelector.MessageTracker(Message.builder()
                    .attributesWithStrings(createAttributeMap("MessageId", "1"))
                    .attributesWithStrings(createAttributeMap("ReceiptHandle", "1"))
                    .build()));
    testSingleList.add(
        new CloudObjectsSelector.MessageTracker(Message.builder()
                    .attributesWithStrings(createAttributeMap("MessageId", "2"))
                    .attributesWithStrings(createAttributeMap("ReceiptHandle", "1"))
                    .build()));

    deleteMessagesInQueue(sqs);

    //  test the return values
    selector.deleteProcessedMessages(sqs, sqsUrl, testSingleList);
  }

  @Test
  public void testEmptyReceivesToConfirmDrainScalesWithPollCost() {
    // Under long polling AWS sends an empty response "only if the polling wait time expires", so an
    // empty costs a full longPollWait. The number of empties required is therefore whatever fits in one
    // drain-confirmation window, which holds the cost of proving "drained" to ~one poll window at every
    // setting instead of multiplying it by a fixed count.
    assertEquals(1, CloudObjectsSelector.emptyReceivesToConfirmDrain(20));
    assertEquals(2, CloudObjectsSelector.emptyReceivesToConfirmDrain(10));
    assertEquals(4, CloudObjectsSelector.emptyReceivesToConfirmDrain(5));
    assertEquals(20, CloudObjectsSelector.emptyReceivesToConfirmDrain(1));
    // A wait longer than the window still needs one empty, never zero.
    assertEquals(1, CloudObjectsSelector.emptyReceivesToConfirmDrain(60));
    // Short polling samples only a subset of servers and returns immediately: weak evidence, but nearly
    // free, so it keeps the higher fixed count.
    assertEquals(CloudObjectsSelector.MAX_EMPTY_RECEIVES_SHORT_POLL,
        CloudObjectsSelector.emptyReceivesToConfirmDrain(0));
    assertEquals(CloudObjectsSelector.MAX_EMPTY_RECEIVES_SHORT_POLL,
        CloudObjectsSelector.emptyReceivesToConfirmDrain(-1));
  }

  @Test
  public void testReceiveStopsOnFirstEmptyLongPoll() {
    // Backlog reports 100 messages available but ReceiveMessage returns empty (e.g. the rest are already
    // in-flight). At the maximum long-poll wait a single empty response means SQS queried every server
    // and waited the full 20s, so it ends the batch immediately. The previous behaviour spent five
    // sequential 20-second long polls (~100s) to reach the same conclusion.
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "1");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    Map<String, String> attributes = new HashMap<>();
    attributes.put(SQS_ATTR_APPROX_MESSAGES, "100");
    when(sqs.getQueueAttributes(any(GetQueueAttributesRequest.class)))
        .thenReturn(GetQueueAttributesResponse.builder().attributesWithStrings(attributes).build());
    when(sqs.receiveMessage(any(ReceiveMessageRequest.class)))
        .thenReturn(ReceiveMessageResponse.builder().build());

    List<Message> messages = selector.getMessagesToProcess(sqs, sqsUrl, 20, 30, 100, 10);

    assertEquals(0, messages.size());
    verify(sqs, times(1)).receiveMessage(any(ReceiveMessageRequest.class));
    // One attributes call at loop start + one in-flight probe, since the drain fell materially short of
    // the 100 messages the queue claimed were available.
    verify(sqs, times(2)).getQueueAttributes(any(GetQueueAttributesRequest.class));
  }

  @Test
  public void testShortPollStillRequiresMultipleEmpties() {
    // longPollWait=0 is short polling: SQS "samples a subset of its servers" and answers immediately, so
    // a single empty response is genuinely weak evidence - and cheap to repeat. The higher fixed count
    // must still apply there.
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "1");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    Map<String, String> attributes = new HashMap<>();
    attributes.put(SQS_ATTR_APPROX_MESSAGES, "100");
    when(sqs.getQueueAttributes(any(GetQueueAttributesRequest.class)))
        .thenReturn(GetQueueAttributesResponse.builder().attributesWithStrings(attributes).build());
    when(sqs.receiveMessage(any(ReceiveMessageRequest.class)))
        .thenReturn(ReceiveMessageResponse.builder().build());

    List<Message> messages = selector.getMessagesToProcess(sqs, sqsUrl, 0, 30, 100, 10);

    assertEquals(0, messages.size());
    verify(sqs, times(CloudObjectsSelector.MAX_EMPTY_RECEIVES_SHORT_POLL))
        .receiveMessage(any(ReceiveMessageRequest.class));
  }

  @Test
  public void testReceiveDoesNotTruncateOnSpuriousEmptyWhenPollWaitIsLow() {
    // AWS scopes the false-empty risk to low wait times, and the drain count grows to match: at
    // longPollWait=5 four empties are required, so one spurious empty must not truncate the batch and the
    // messages arriving after it must survive.
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "1");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    Map<String, String> attributes = new HashMap<>();
    attributes.put(SQS_ATTR_APPROX_MESSAGES, "100");
    when(sqs.getQueueAttributes(any(GetQueueAttributesRequest.class)))
        .thenReturn(GetQueueAttributesResponse.builder().attributesWithStrings(attributes).build());

    ReceiveMessageResponse empty = ReceiveMessageResponse.builder().build();
    ReceiveMessageResponse twoMessages = ReceiveMessageResponse.builder()
        .messages(sqsMessage("m1", "r1"), sqsMessage("m2", "r2")).build();
    // empty, 2 msgs, then empty forever (Mockito repeats the last stub).
    when(sqs.receiveMessage(any(ReceiveMessageRequest.class)))
        .thenReturn(empty, twoMessages, empty);

    List<Message> messages = selector.getMessagesToProcess(sqs, sqsUrl, 5, 30, 100, 10);

    // Both real messages survive the spurious leading empty; validate the actual ids, not just the count.
    assertEquals(2, messages.size());
    List<String> ids = messages.stream().map(Message::messageId).sorted().collect(Collectors.toList());
    assertEquals("m1", ids.get(0));
    assertEquals("m2", ids.get(1));
    // 4 empties are required in total, and one of the 5 calls returned messages.
    verify(sqs, times(5)).receiveMessage(any(ReceiveMessageRequest.class));
  }

  @Test
  public void testTricklingQueueTerminatesOnAccumulatedEmpties() {
    // A queue with slow ingress: 4 empty polls then 1 message, repeating. Counting empties consecutively
    // let every 5th call reset the counter, so the phase ran the entire call budget - 80 calls, 64 of them
    // full-length empty long polls (~21 minutes of wall time at longPollWait=20). Counting them as a batch
    // total makes the run terminate on the accumulated empties instead.
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "1");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    Map<String, String> attributes = new HashMap<>();
    attributes.put(SQS_ATTR_APPROX_MESSAGES, "5000");
    when(sqs.getQueueAttributes(any(GetQueueAttributesRequest.class)))
        .thenReturn(GetQueueAttributesResponse.builder().attributesWithStrings(attributes).build());

    AtomicInteger calls = new AtomicInteger();
    when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer(invocation -> {
      int call = calls.incrementAndGet();
      if (call % 5 == 0) {
        return ReceiveMessageResponse.builder().messages(sqsMessage("m" + call, "r" + call)).build();
      }
      return ReceiveMessageResponse.builder().build();
    });

    // maxMessagePerBatch=200, maxMessagesPerRequest=10 -> plannedReceiveCalls=20, budget=20*4=80.
    List<Message> messages = selector.getMessagesToProcess(sqs, sqsUrl, 5, 30, 200, 10);

    // longPollWait=5 -> 4 empties confirm the drain, which the first four calls supply.
    assertEquals(4, calls.get(), "must terminate on accumulated empties, not the call budget");
    assertEquals(0, messages.size());
    assertTrue(calls.get() < 20 * CloudObjectsSelector.RECEIVE_CALL_BUDGET_FACTOR,
        "must stop far short of the call budget, was " + calls.get());
  }

  @Test
  public void testParallelReceiveFetchesEveryMessageExactlyOnce() {
    // Real concurrency: 8 workers drain a shared backlog of 250 messages (10 per receive call). Every
    // message must be fetched exactly once - no loss, no duplication - and merged into the result.
    int totalMessages = 250;
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "8");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    Map<String, String> attributes = new HashMap<>();
    attributes.put(SQS_ATTR_APPROX_MESSAGES, String.valueOf(totalMessages));
    when(sqs.getQueueAttributes(any(GetQueueAttributesRequest.class)))
        .thenReturn(GetQueueAttributesResponse.builder().attributesWithStrings(attributes).build());

    ConcurrentLinkedQueue<Message> backlog = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < totalMessages; i++) {
      backlog.add(sqsMessage("m" + i, "r" + i));
    }
    // Thread-safe stub: each receive pops up to 10 messages; empty response once drained.
    when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer(invocation -> {
      List<Message> batch = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        Message polled = backlog.poll();
        if (polled == null) {
          break;
        }
        batch.add(polled);
      }
      return ReceiveMessageResponse.builder().messages(batch).build();
    });

    List<Message> messages = selector.getMessagesToProcess(sqs, sqsUrl, 20, 30, 1000, 10);

    assertEquals(totalMessages, messages.size());
    List<String> distinctIds =
        messages.stream().map(Message::messageId).distinct().collect(Collectors.toList());
    assertEquals(totalMessages, distinctIds.size());
    assertTrue(backlog.isEmpty());
  }

  @Test
  public void testSlowCallDoesNotStallSiblingWorkers() {
    // The point of the redesign: a worker that finishes proceeds to its next call immediately instead of
    // waiting for the slowest call in a wave. One straggler is held until its siblings have completed
    // eight further calls between them. Under the previous wave-based invokeAll the siblings could issue
    // at most (workers - 1) more calls before the wave barrier blocked them behind the straggler, so this
    // test would deadlock until the latch timed out.
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "4");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    Map<String, String> attributes = new HashMap<>();
    attributes.put(SQS_ATTR_APPROX_MESSAGES, "1000");
    when(sqs.getQueueAttributes(any(GetQueueAttributesRequest.class)))
        .thenReturn(GetQueueAttributesResponse.builder().attributesWithStrings(attributes).build());

    CountDownLatch stragglerMayReturn = new CountDownLatch(1);
    AtomicInteger calls = new AtomicInteger();
    AtomicInteger siblingCalls = new AtomicInteger();
    AtomicBoolean siblingsProgressed = new AtomicBoolean();
    AtomicInteger messageId = new AtomicInteger();
    when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer(invocation -> {
      if (calls.incrementAndGet() == 1) {
        siblingsProgressed.set(stragglerMayReturn.await(30, TimeUnit.SECONDS));
        return ReceiveMessageResponse.builder().build();
      }
      if (siblingCalls.incrementAndGet() >= 8) {
        stragglerMayReturn.countDown();
      }
      List<Message> batch = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        int id = messageId.incrementAndGet();
        batch.add(sqsMessage("m" + id, "r" + id));
      }
      return ReceiveMessageResponse.builder().messages(batch).build();
    });

    List<Message> messages = selector.getMessagesToProcess(sqs, sqsUrl, 20, 30, 150, 10);

    assertTrue(siblingsProgressed.get(),
        "siblings were blocked behind the straggler - the receive phase is not dispatching continuously");
    assertTrue(siblingCalls.get() >= 8, "expected at least 8 sibling calls, was " + siblingCalls.get());
    // Every message the siblings returned is kept, including the ones that landed while the straggler ran.
    assertEquals(10 * siblingCalls.get(), messages.size());
    assertEquals(messages.size(), messages.stream().map(Message::messageId).distinct().count());
  }

  @Test
  public void testReceiveOvershootIsBoundedByInFlightAccounting() {
    // 16 workers are configured but only 11 calls are needed for the 105-message target. Admission credits
    // every in-flight call with a full maxMessagesPerRequest, so no redundant call is ever dispatched and
    // the overshoot stays under maxMessagesPerRequest rather than a whole wave (16*10-1 = 159 messages
    // beyond the operator's configured cap).
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "16");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    Map<String, String> attributes = new HashMap<>();
    attributes.put(SQS_ATTR_APPROX_MESSAGES, "1000");
    when(sqs.getQueueAttributes(any(GetQueueAttributesRequest.class)))
        .thenReturn(GetQueueAttributesResponse.builder().attributesWithStrings(attributes).build());

    ConcurrentLinkedQueue<Message> backlog = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < 1000; i++) {
      backlog.add(sqsMessage("m" + i, "r" + i));
    }
    when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer(invocation -> {
      List<Message> batch = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        Message polled = backlog.poll();
        if (polled == null) {
          break;
        }
        batch.add(polled);
      }
      return ReceiveMessageResponse.builder().messages(batch).build();
    });

    int maxMessagePerBatch = 105;
    List<Message> messages = selector.getMessagesToProcess(sqs, sqsUrl, 20, 30, maxMessagePerBatch, 10);

    // ceil(105/10) = 11 calls, each returning 10 -> exactly 110.
    assertEquals(110, messages.size());
    assertTrue(messages.size() <= maxMessagePerBatch + 9,
        "overshoot must be bounded by maxMessagesPerRequest-1, was " + (messages.size() - maxMessagePerBatch));
    verify(sqs, times(11)).receiveMessage(any(ReceiveMessageRequest.class));
  }

  @Test
  public void testReceiveToleratesIndividualTransientFailure() {
    // One ReceiveMessage call fails with a throttle. The messages fetched by the sibling calls are already
    // in-flight, so the batch must keep them and carry on rather than abort - aborting would strand them
    // until the visibility timeout expires.
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "4");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    Map<String, String> attributes = new HashMap<>();
    attributes.put(SQS_ATTR_APPROX_MESSAGES, "40");
    when(sqs.getQueueAttributes(any(GetQueueAttributesRequest.class)))
        .thenReturn(GetQueueAttributesResponse.builder().attributesWithStrings(attributes).build());

    ConcurrentLinkedQueue<Message> backlog = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < 40; i++) {
      backlog.add(sqsMessage("m" + i, "r" + i));
    }
    AtomicInteger calls = new AtomicInteger();
    when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer(invocation -> {
      if (calls.incrementAndGet() == 2) {
        throw sqsError("RequestThrottled", 400, "Rate exceeded");
      }
      List<Message> batch = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        Message polled = backlog.poll();
        if (polled == null) {
          break;
        }
        batch.add(polled);
      }
      return ReceiveMessageResponse.builder().messages(batch).build();
    });

    List<Message> messages = selector.getMessagesToProcess(sqs, sqsUrl, 20, 30, 40, 10);

    assertEquals(40, messages.size());
    assertEquals(40, messages.stream().map(Message::messageId).distinct().count());
    assertTrue(backlog.isEmpty());
  }

  @Test
  public void testReceiveStopsAfterConsecutiveTransientFailures() {
    // A systemic failure with nothing fetched must surface as an exception - returning an empty batch
    // would be indistinguishable from a drained queue - and must stop well short of the call budget.
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "2");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    Map<String, String> attributes = new HashMap<>();
    attributes.put(SQS_ATTR_APPROX_MESSAGES, "100");
    when(sqs.getQueueAttributes(any(GetQueueAttributesRequest.class)))
        .thenReturn(GetQueueAttributesResponse.builder().attributesWithStrings(attributes).build());

    AtomicInteger calls = new AtomicInteger();
    when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer(invocation -> {
      calls.incrementAndGet();
      throw sqsError("InternalFailure", 500, "We encountered an internal error");
    });

    HoodieException thrown = assertThrows(HoodieException.class,
        () -> selector.getMessagesToProcess(sqs, sqsUrl, 20, 30, 100, 10));
    assertTrue(thrown.getCause() instanceof SqsException, "the SQS cause must be preserved");
    // 2 primed, then one replacement per completion until the 5th consecutive failure stops dispatch,
    // leaving the last outstanding call to drain: 2 + 4 = 6 calls, against a budget of 4 x 10 = 40.
    assertEquals(2 + CloudObjectsSelector.MAX_CONSECUTIVE_TRANSIENT_FAILURES - 1, calls.get());
    assertTrue(calls.get() < 40, "must stop well short of the call budget, was " + calls.get());
  }

  @Test
  public void testReceiveStopsImmediatelyOnNonRetryableFailure() {
    // A deleted queue cannot be fixed by retrying. Previously it burned the full transient tolerance
    // (5 round-trips); it must now stop at the first one, with only the already-dispatched siblings
    // draining behind it.
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "2");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    Map<String, String> attributes = new HashMap<>();
    attributes.put(SQS_ATTR_APPROX_MESSAGES, "100");
    when(sqs.getQueueAttributes(any(GetQueueAttributesRequest.class)))
        .thenReturn(GetQueueAttributesResponse.builder().attributesWithStrings(attributes).build());

    AtomicInteger calls = new AtomicInteger();
    when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer(invocation -> {
      calls.incrementAndGet();
      throw sqsError("QueueDoesNotExist", 400, "The specified queue does not exist");
    });

    HoodieException thrown = assertThrows(HoodieException.class,
        () -> selector.getMessagesToProcess(sqs, sqsUrl, 20, 30, 100, 10));
    assertTrue(thrown.getCause() instanceof SqsException, "the SQS cause must be preserved");
    // Only the two primed calls run: the first fatal completion stops dispatch outright.
    assertEquals(2, calls.get(), "a non-retryable failure must not consume the transient tolerance");
  }

  @Test
  public void testNonRetryableFailureAfterMessagesFetchedReturnsThem() {
    // Credentials revoked mid-batch. The messages already fetched are in-flight and would be stranded for
    // the whole visibility timeout if discarded, and the condition resurfaces on the next batch's first
    // call anyway - so the phase returns them rather than throwing.
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "1");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    Map<String, String> attributes = new HashMap<>();
    attributes.put(SQS_ATTR_APPROX_MESSAGES, "100");
    when(sqs.getQueueAttributes(any(GetQueueAttributesRequest.class)))
        .thenReturn(GetQueueAttributesResponse.builder().attributesWithStrings(attributes).build());

    AtomicInteger calls = new AtomicInteger();
    when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer(invocation -> {
      if (calls.incrementAndGet() == 1) {
        List<Message> batch = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
          batch.add(sqsMessage("m" + i, "r" + i));
        }
        return ReceiveMessageResponse.builder().messages(batch).build();
      }
      throw sqsError("AccessDeniedException", 400, "Access to the resource is denied");
    });

    List<Message> messages = selector.getMessagesToProcess(sqs, sqsUrl, 20, 30, 100, 10);

    assertEquals(10, messages.size(), "already-fetched messages must not be discarded");
    assertEquals(2, calls.get());
  }

  @Test
  public void testOverLimitEndsBatchCleanlyAndKeepsMessages() {
    // OverLimit is the in-flight ceiling (~120,000 standard / 20,000 FIFO), i.e. backpressure rather than
    // an error: the received messages stay in-flight until onCommit deletes them, which is exactly what
    // frees the quota. Stop receiving, hand back what was fetched, and never fail the job over it.
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "1");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    Map<String, String> attributes = new HashMap<>();
    attributes.put(SQS_ATTR_APPROX_MESSAGES, "1000");
    when(sqs.getQueueAttributes(any(GetQueueAttributesRequest.class)))
        .thenReturn(GetQueueAttributesResponse.builder().attributesWithStrings(attributes).build());

    AtomicInteger calls = new AtomicInteger();
    when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer(invocation -> {
      if (calls.incrementAndGet() == 1) {
        List<Message> batch = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
          batch.add(sqsMessage("m" + i, "r" + i));
        }
        return ReceiveMessageResponse.builder().messages(batch).build();
      }
      throw sqsError(CloudObjectsSelector.SQS_OVER_LIMIT_ERROR_CODE, 400,
          "Number of in flight messages exceeded");
    });

    List<Message> messages = selector.getMessagesToProcess(sqs, sqsUrl, 20, 30, 1000, 10);

    assertEquals(10, messages.size());
    assertEquals(2, calls.get(), "OverLimit must stop dispatch at once, not consume the retry tolerance");
  }

  @Test
  public void testOverLimitWithNothingFetchedDoesNotThrow() {
    // The in-flight quota can already be exhausted before this batch fetches anything - a previous batch's
    // messages are still awaiting their commit. That is a real, self-clearing queue state, so it must
    // return empty rather than throw: failing the job would trade normal saturation for an outage.
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "1");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    Map<String, String> attributes = new HashMap<>();
    attributes.put(SQS_ATTR_APPROX_MESSAGES, "1000");
    when(sqs.getQueueAttributes(any(GetQueueAttributesRequest.class)))
        .thenReturn(GetQueueAttributesResponse.builder().attributesWithStrings(attributes).build());

    AtomicInteger calls = new AtomicInteger();
    when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer(invocation -> {
      calls.incrementAndGet();
      throw sqsError(CloudObjectsSelector.SQS_OVER_LIMIT_ERROR_CODE, 400,
          "Number of in flight messages exceeded");
    });

    List<Message> messages = selector.getMessagesToProcess(sqs, sqsUrl, 20, 30, 1000, 10);

    assertTrue(messages.isEmpty());
    assertEquals(1, calls.get());
  }

  @Test
  public void testInFlightCallsAreDrainedAndTheirMessagesKept() {
    // When a stop condition fires there are still calls in flight. They must be allowed to return and
    // their messages kept: a ReceiveMessage that already succeeded server-side has made those messages
    // invisible, so discarding the response would strand them for the whole visibility timeout.
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "4");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    Map<String, String> attributes = new HashMap<>();
    attributes.put(SQS_ATTR_APPROX_MESSAGES, "1000");
    when(sqs.getQueueAttributes(any(GetQueueAttributesRequest.class)))
        .thenReturn(GetQueueAttributesResponse.builder().attributesWithStrings(attributes).build());

    AtomicInteger calls = new AtomicInteger();
    AtomicInteger messageId = new AtomicInteger();
    when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer(invocation -> {
      // The first call returns empty at once and, at longPollWait=20, that alone confirms the drain and
      // stops dispatch. The three siblings primed alongside it are slower and land afterwards.
      if (calls.incrementAndGet() == 1) {
        return ReceiveMessageResponse.builder().build();
      }
      Thread.sleep(250);
      List<Message> batch = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        int id = messageId.incrementAndGet();
        batch.add(sqsMessage("m" + id, "r" + id));
      }
      return ReceiveMessageResponse.builder().messages(batch).build();
    });

    List<Message> messages = selector.getMessagesToProcess(sqs, sqsUrl, 20, 30, 1000, 10);

    assertEquals(4, calls.get(), "the primed calls must all be drained, never cancelled");
    assertEquals(30, messages.size(), "messages from in-flight calls must be kept after the stop decision");
    assertEquals(30, messages.stream().map(Message::messageId).distinct().count());
  }

  @Test
  public void testEmptyResultThrowsOnlyWhenNothingProvedTheQueueReachable() {
    // The boundary for turning an empty result into a job failure. Failures interleaved with empty
    // responses (call 1 throttled, call 2 empty) must NOT throw: the successful empty response is a real
    // answer from SQS, so the endpoint, the credentials and the queue are all demonstrably fine and the
    // empty result means drained, not broken. Only a batch where nothing answered at all is a stalled
    // pipeline masquerading as a drained queue - covered by
    // testReceiveStopsAfterConsecutiveTransientFailures and testReceiveStopsImmediatelyOnNonRetryableFailure.
    // Sequential so the alternation is deterministic; concurrently the call count would depend on
    // completion order.
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "1");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    Map<String, String> attributes = new HashMap<>();
    attributes.put(SQS_ATTR_APPROX_MESSAGES, "100");
    when(sqs.getQueueAttributes(any(GetQueueAttributesRequest.class)))
        .thenReturn(GetQueueAttributesResponse.builder().attributesWithStrings(attributes).build());

    AtomicInteger calls = new AtomicInteger();
    when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer(invocation -> {
      if (calls.incrementAndGet() % 2 == 1) {
        throw sqsError("RequestThrottled", 400, "Rate exceeded");
      }
      return ReceiveMessageResponse.builder().build();
    });

    List<Message> messages = selector.getMessagesToProcess(sqs, sqsUrl, 20, 30, 100, 10);

    assertTrue(messages.isEmpty());
    // The throttle, then the empty that confirms the drain at longPollWait=20.
    assertEquals(2, calls.get());
  }

  @Test
  public void testDrainedQueueWithOneThrottledCallDoesNotThrow() {
    // A drained queue whose concurrent polls also got one throttle must NOT fail the job. 15 empty
    // responses are positive proof the endpoint, credentials and queue are healthy - the same evidence the
    // loop uses to reset the transient-failure run - so an empty result here is a drained queue, not a
    // broken one. Throwing would turn the steady state of a caught-up pipeline into a HoodieStreamer
    // shutdown every time SQS throttled one of 16 concurrent ReceiveMessage calls.
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "16");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    Map<String, String> attributes = new HashMap<>();
    attributes.put(SQS_ATTR_APPROX_MESSAGES, "1000");
    when(sqs.getQueueAttributes(any(GetQueueAttributesRequest.class)))
        .thenReturn(GetQueueAttributesResponse.builder().attributesWithStrings(attributes).build());

    AtomicInteger calls = new AtomicInteger();
    when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer(invocation -> {
      if (calls.incrementAndGet() == 1) {
        throw sqsError("RequestThrottled", 400, "Rate exceeded");
      }
      return ReceiveMessageResponse.builder().build();
    });

    List<Message> messages = selector.getMessagesToProcess(sqs, sqsUrl, 20, 30, 1000, 10);

    assertTrue(messages.isEmpty());
    assertTrue(calls.get() >= 2, "the drain must still have been observed, was " + calls.get());
  }

  @Test
  public void testInFlightLimitWithOneThrottledCallDoesNotThrow() {
    // The in-flight ceiling and throttling co-occur, since both are symptoms of a saturated queue. A batch
    // where most concurrent calls returned OverLimit and one was throttled fetches nothing - but OverLimit
    // is an answer from SQS, so the queue is demonstrably reachable and this is backpressure, not a
    // failure. Failing the job here is exactly what the backpressure classification exists to prevent.
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "8");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    Map<String, String> attributes = new HashMap<>();
    attributes.put(SQS_ATTR_APPROX_MESSAGES, "1000");
    when(sqs.getQueueAttributes(any(GetQueueAttributesRequest.class)))
        .thenReturn(GetQueueAttributesResponse.builder().attributesWithStrings(attributes).build());

    AtomicInteger calls = new AtomicInteger();
    when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer(invocation -> {
      if (calls.incrementAndGet() == 1) {
        throw sqsError("RequestThrottled", 400, "Rate exceeded");
      }
      throw sqsError(CloudObjectsSelector.SQS_OVER_LIMIT_ERROR_CODE, 400,
          "Number of in flight messages exceeded");
    });

    List<Message> messages = selector.getMessagesToProcess(sqs, sqsUrl, 20, 30, 1000, 10);

    assertTrue(messages.isEmpty());
  }

  @Test
  public void testShortPollEmptiesAccumulateAndCanEndABatchBeforeItsTarget() {
    // Empties are a batch total in both polling modes - counting them consecutively is what let a
    // slow-ingress queue reset the counter indefinitely and hold the phase for its whole call budget.
    // The trade lands here. Short polling "samples a subset of its servers", so an empty response on a
    // queue that still holds messages is expected rather than exceptional, and scattered empties therefore
    // accumulate until the batch ends short of its target. That is throughput on a non-default setting,
    // not correctness: the remainder is simply left for the next ingestion cycle, never dropped. This test
    // pins that deliberate behaviour so a future change to the counting cannot alter it silently.
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "1");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    Map<String, String> attributes = new HashMap<>();
    attributes.put(SQS_ATTR_APPROX_MESSAGES, "100");
    when(sqs.getQueueAttributes(any(GetQueueAttributesRequest.class)))
        .thenReturn(GetQueueAttributesResponse.builder().attributesWithStrings(attributes).build());

    AtomicInteger calls = new AtomicInteger();
    AtomicInteger messageId = new AtomicInteger();
    when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer(invocation -> {
      if (calls.incrementAndGet() % 2 == 1) {
        return ReceiveMessageResponse.builder().build();
      }
      List<Message> batch = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        int id = messageId.incrementAndGet();
        batch.add(sqsMessage("m" + id, "r" + id));
      }
      return ReceiveMessageResponse.builder().messages(batch).build();
    });

    List<Message> messages = selector.getMessagesToProcess(sqs, sqsUrl, 0, 30, 100, 10);

    // Every other call is empty, so the 5th empty lands on call 9 having yielded 4 batches of 10. The
    // batch ends there rather than running on to its 100-message target.
    int expectedCalls = 2 * CloudObjectsSelector.MAX_EMPTY_RECEIVES_SHORT_POLL - 1;
    assertEquals(expectedCalls, calls.get());
    assertEquals(10 * (CloudObjectsSelector.MAX_EMPTY_RECEIVES_SHORT_POLL - 1), messages.size());
    assertEquals(messages.size(), messages.stream().map(Message::messageId).distinct().count(),
        "whatever was fetched before the drain must still be returned intact");
    assertTrue(messages.size() < 100, "the accumulated empties end the batch before its target");
  }

  @Test
  public void testProcessingParallelismIsClampedToAtLeastOne() {
    // A misconfigured non-positive parallelism must degrade to sequential rather than blowing up in
    // Executors.newFixedThreadPool, which rejects a size of 0.
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "0");
    assertEquals(1, new CloudObjectsSelector(props).processingParallelism);

    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "-4");
    assertEquals(1, new CloudObjectsSelector(props).processingParallelism);
  }

  @Test
  public void testErrorFromReceiveWorkerIsNotToleratedAsACallFailure() {
    // An Error on a worker (OutOfMemoryError here) is not an SQS failure: absorbing it into failedCalls
    // would keep dispatching and could even return a "successful" partial batch after a fatal condition.
    // It must propagate unchanged, and dispatch must stop at the calls already in flight.
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "2");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    Map<String, String> attributes = new HashMap<>();
    attributes.put(SQS_ATTR_APPROX_MESSAGES, "1000");
    when(sqs.getQueueAttributes(any(GetQueueAttributesRequest.class)))
        .thenReturn(GetQueueAttributesResponse.builder().attributesWithStrings(attributes).build());

    AtomicInteger calls = new AtomicInteger();
    when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer(invocation -> {
      calls.incrementAndGet();
      throw new OutOfMemoryError("Java heap space");
    });

    OutOfMemoryError thrown = assertThrows(OutOfMemoryError.class,
        () -> selector.getMessagesToProcess(sqs, sqsUrl, 20, 30, 1000, 10));
    assertEquals("Java heap space", thrown.getMessage(), "the Error must propagate unwrapped");
    // Both primed calls ran; the first Error folded in aborts the loop before any replacement is issued.
    assertEquals(2, calls.get());
  }

  @Test
  public void testConnectionPoolIsOnlyResizedAboveTheSdkDefault() {
    // Naming ApacheHttpClient to resize the pool makes software.amazon.awssdk:apache-client a hard
    // runtime requirement of this source, so it must only happen when the parallelism actually outgrows
    // the SDK's default pool of 50. The default parallelism (16) must leave the client untouched.
    assertFalse(CloudObjectsSelector.requiresLargerConnectionPool(
        S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.defaultValue()));
    assertFalse(CloudObjectsSelector.requiresLargerConnectionPool(1));
    assertFalse(CloudObjectsSelector.requiresLargerConnectionPool(50));
    assertTrue(CloudObjectsSelector.requiresLargerConnectionPool(51));
  }

  @Test
  public void testParallelDeleteRemovesEveryMessageExactlyOnce() throws Exception {
    // Real concurrency: 8 workers delete 250 messages in batches of SQS_BATCH_MAX_ENTRIES. Every receipt
    // handle must be deleted exactly once - no loss, no duplication - and the calls must genuinely
    // overlap, otherwise the fan-out has silently regressed to serial. Overlap is proven rather than
    // inferred: each call blocks until WORKERS of them are in flight simultaneously.
    int totalMessages = 250;
    int workers = 8;
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), String.valueOf(workers));
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    ConcurrentLinkedQueue<String> deletedHandles = new ConcurrentLinkedQueue<>();
    Set<String> threadNames = ConcurrentHashMap.newKeySet();
    AtomicInteger deleteCalls = new AtomicInteger();
    AtomicBoolean neverOverlapped = new AtomicBoolean();
    CountDownLatch allWorkersInFlight = new CountDownLatch(workers);
    when(sqs.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenAnswer(invocation -> {
      threadNames.add(Thread.currentThread().getName());
      deleteCalls.incrementAndGet();
      allWorkersInFlight.countDown();
      // Blocks until `workers` calls are in flight at once, so overlap is proven rather than inferred.
      // A serial regression cannot satisfy the latch: record that and release everyone, so the test
      // fails on the assertion below in seconds instead of timing out once per remaining batch.
      if (!allWorkersInFlight.await(10, TimeUnit.SECONDS)) {
        neverOverlapped.set(true);
        while (allWorkersInFlight.getCount() > 0) {
          allWorkersInFlight.countDown();
        }
      }
      DeleteMessageBatchRequest request = invocation.getArgument(0);
      request.entries().forEach(entry -> deletedHandles.add(entry.receiptHandle()));
      return DeleteMessageBatchResponse.builder().build();
    });

    selector.deleteProcessedMessages(sqs, sqsUrl, trackers(totalMessages));

    assertFalse(neverOverlapped.get(),
        "delete calls never ran " + workers + "-way concurrently; the fan-out has regressed to serial");
    // Validate the actual handles deleted, not just the count: exactly r0..r249, each once.
    assertEquals(totalMessages, deletedHandles.size());
    assertEquals(expectedHandles(totalMessages), new HashSet<>(deletedHandles));
    assertEquals(totalMessages / CloudObjectsSelector.SQS_BATCH_MAX_ENTRIES, deleteCalls.get());
    assertEquals(workers, threadNames.size(), "expected one call per worker thread: " + threadNames);
    assertTrue(threadNames.stream().allMatch(name -> name.startsWith("sqs-delete-")),
        "delete calls must run on the named delete pool, got: " + threadNames);
  }

  @Test
  public void testDeleteAggregatesFailuresAcrossConcurrentBatches() {
    // Partial failures from several concurrent batches must be merged into a single retry pass rather
    // than retried per batch: 5 batches each fail one entry, and the 5 survivors re-partition into one
    // batch of 5.
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "5");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    Set<String> attempted = ConcurrentHashMap.newKeySet();
    ConcurrentLinkedQueue<String> deletedHandles = new ConcurrentLinkedQueue<>();
    AtomicInteger deleteCalls = new AtomicInteger();
    when(sqs.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenAnswer(invocation -> {
      deleteCalls.incrementAndGet();
      DeleteMessageBatchRequest request = invocation.getArgument(0);
      List<BatchResultErrorEntry> failures = new ArrayList<>();
      for (DeleteMessageBatchRequestEntry entry : request.entries()) {
        // Fail the first entry of every batch, but only on its first attempt, so the retry succeeds.
        if (attempted.add(entry.receiptHandle()) && "0".equals(entry.id())) {
          failures.add(errorEntry(entry.id(), "InternalError", false));
        } else {
          deletedHandles.add(entry.receiptHandle());
        }
      }
      return DeleteMessageBatchResponse.builder().failed(failures).build();
    });

    String infoLogs = captureLogs("info", () -> selector.deleteProcessedMessages(sqs, sqsUrl, trackers(50)));

    // 5 first-pass batches + 1 retry batch holding all 5 failed entries.
    assertEquals(6, deleteCalls.get());
    ArgumentCaptor<DeleteMessageBatchRequest> captor = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
    verify(sqs, times(6)).deleteMessageBatch(captor.capture());
    DeleteMessageBatchRequest retry = captor.getAllValues().get(5);
    assertEquals(new HashSet<>(Arrays.asList("r0", "r10", "r20", "r30", "r40")),
        retry.entries().stream().map(DeleteMessageBatchRequestEntry::receiptHandle).collect(Collectors.toSet()));
    assertEquals(expectedHandles(50), new HashSet<>(deletedHandles));
    assertTrue(infoLogs.contains("Deleted 50 of 50"), "delete summary should report a full delete: " + infoLogs);
  }

  @Test
  public void testDeleteToleratesThrownCallAndRetriesItsBatch() {
    // A DeleteMessageBatch that throws leaves its whole batch undeleted, exactly like an SQS-reported
    // entry failure, so it must feed the same retry path. Aborting instead would discard the other
    // batches' results and propagate out of onCommit - which StreamSync calls after the Hudi commit has
    // already landed, so HoodieStreamer would shut the job down over a transient throttle.
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "3");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    Set<String> alreadyThrown = ConcurrentHashMap.newKeySet();
    ConcurrentLinkedQueue<String> deletedHandles = new ConcurrentLinkedQueue<>();
    AtomicInteger deleteCalls = new AtomicInteger();
    when(sqs.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenAnswer(invocation -> {
      deleteCalls.incrementAndGet();
      DeleteMessageBatchRequest request = invocation.getArgument(0);
      boolean isFirstBatch = request.entries().stream()
          .anyMatch(entry -> "r0".equals(entry.receiptHandle()));
      if (isFirstBatch && alreadyThrown.add("first-attempt")) {
        throw SqsException.builder().message("Rate exceeded").build();
      }
      request.entries().forEach(entry -> deletedHandles.add(entry.receiptHandle()));
      return DeleteMessageBatchResponse.builder().build();
    });

    String warnings = captureLogs("warn", () -> selector.deleteProcessedMessages(sqs, sqsUrl, trackers(25)));

    // 3 first-pass batches (one threw) + 1 retry batch for the 10 messages it stranded.
    assertEquals(4, deleteCalls.get());
    assertEquals(expectedHandles(25), new HashSet<>(deletedHandles));
    assertTrue(warnings.contains("DeleteMessageBatch calls failed"),
        "a thrown call must be reported with its cause: " + warnings);
  }

  @Test
  public void testDeleteThrowsOnlyWhenNoMessageCouldBeDeleted() {
    // Systemic failure - every call throws on every pass, nothing is deleted. Unlike a partial failure
    // this must surface, because the caller is about to clear its tracked messages as if they had been
    // handled.
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "2");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    AtomicInteger deleteCalls = new AtomicInteger();
    when(sqs.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenAnswer(invocation -> {
      deleteCalls.incrementAndGet();
      throw SqsException.builder().message("queue does not exist").build();
    });

    HoodieException thrown = assertThrows(HoodieException.class,
        () -> selector.deleteProcessedMessages(sqs, sqsUrl, trackers(15)));

    assertTrue(thrown.getMessage().contains("Failed to delete any of the 15"), thrown.getMessage());
    assertTrue(thrown.getCause() instanceof SqsException, "the SQS cause must be preserved");
    // 2 batches x (1 initial pass + DELETE_MAX_RETRIES retry passes).
    assertEquals(2 * (1 + CloudObjectsSelector.DELETE_MAX_RETRIES), deleteCalls.get());
  }

  @Test
  public void testDeleteDoesNotRetrySenderFaultFailuresAndDoesNotThrow() {
    // senderFault=true means SQS blames the caller - a receipt handle that expired with the visibility
    // timeout, say - so no retry can succeed and the retry budget must not be spent on it. It is also
    // not fatal even when nothing at all was deleted: the messages are redelivered regardless, so
    // failing the job would change nothing.
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    AtomicInteger deleteCalls = new AtomicInteger();
    when(sqs.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenAnswer(invocation -> {
      deleteCalls.incrementAndGet();
      return DeleteMessageBatchResponse.builder()
          .failed(errorEntry("0", "ReceiptHandleIsInvalid", true))
          .build();
    });

    String warnings = captureLogs("warn",
        () -> selector.deleteProcessedMessages(sqs, sqsUrl, Collections.singletonList(tracker("m0", "r0"))));

    assertEquals(1, deleteCalls.get(), "a senderFault failure must not be retried");
    // The residual WARN is what on-call reads: it must name the reason and identify the message.
    assertTrue(warnings.contains("ReceiptHandleIsInvalid (senderFault=true)"), warnings);
    assertTrue(warnings.contains("m0"), "the failed messageId must be sampled into the WARN: " + warnings);
    assertTrue(warnings.contains("after 0 retries"), warnings);
  }

  @Test
  public void testDeleteRetriesOnlyTheTransientFailureInAMixedBatch() {
    // One batch, one permanent (senderFault=true) and one transient failure. Only the transient entry
    // may be retried; the permanent one is set aside and reported.
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    AtomicInteger deleteCalls = new AtomicInteger();
    when(sqs.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenAnswer(invocation -> {
      if (deleteCalls.getAndIncrement() == 0) {
        return DeleteMessageBatchResponse.builder()
            .failed(errorEntry("0", "ReceiptHandleIsInvalid", true), errorEntry("1", "InternalError", false))
            .build();
      }
      return DeleteMessageBatchResponse.builder().build();
    });

    List<CloudObjectsSelector.MessageTracker> processed =
        Arrays.asList(tracker("m0", "r0"), tracker("m1", "r1"));
    String logs = captureLogs("info", () -> selector.deleteProcessedMessages(sqs, sqsUrl, processed));

    assertEquals(2, deleteCalls.get());
    ArgumentCaptor<DeleteMessageBatchRequest> captor = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
    verify(sqs, times(2)).deleteMessageBatch(captor.capture());
    // Negative verification: the permanent failure (r0) must NOT appear in the retry.
    DeleteMessageBatchRequest retry = captor.getAllValues().get(1);
    assertEquals(1, retry.entries().size());
    assertEquals("r1", retry.entries().get(0).receiptHandle());
    assertTrue(logs.contains("Deleted 1 of 2"), logs);
  }

  @Test
  public void testDeleteReportsEntryIdsItCannotMapBackToAMessage() {
    // Defensive: SQS should only echo ids we sent. If it echoes an unknown one we cannot tell which
    // message failed, so it must be logged loudly rather than dropped silently. Covers both unmappable
    // shapes - out of range and non-numeric.
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    AtomicInteger deleteCalls = new AtomicInteger();
    when(sqs.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenAnswer(invocation -> {
      deleteCalls.incrementAndGet();
      return DeleteMessageBatchResponse.builder()
          .failed(errorEntry("99", "InternalError", false), errorEntry("-1", "InternalError", false),
              errorEntry("not-a-number", "InternalError", false))
          .build();
    });

    String warnings = captureLogs("warn",
        () -> selector.deleteProcessedMessages(sqs, sqsUrl, Collections.singletonList(tracker("m0", "r0"))));

    // Nothing mappable failed, so there is nothing to retry - but every id must be reported.
    assertEquals(1, deleteCalls.get());
    assertTrue(warnings.contains("unknown entry id \"99\""), warnings);
    assertTrue(warnings.contains("unknown entry id \"-1\""), warnings);
    assertTrue(warnings.contains("unknown entry id \"not-a-number\""), warnings);
  }

  @Test
  public void testDeleteNormalizesMissingSqsErrorFields() {
    // code, message and senderFault are all optional on BatchResultErrorEntry. A missing senderFault must
    // read as "not the caller's fault" (so the entry is still retried), and the residual WARN must never
    // render a bare "null" at the reason an operator is trying to read.
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    AtomicInteger deleteCalls = new AtomicInteger();
    when(sqs.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenAnswer(invocation -> {
      deleteCalls.incrementAndGet();
      return DeleteMessageBatchResponse.builder()
          .failed(BatchResultErrorEntry.builder().id("0").build())
          .build();
    });

    String warnings = captureLogs("warn",
        () -> selector.deleteProcessedMessages(sqs, sqsUrl, Collections.singletonList(tracker("m0", "r0"))));

    // senderFault absent => treated as transient => retried the full budget.
    assertEquals(1 + CloudObjectsSelector.DELETE_MAX_RETRIES, deleteCalls.get());
    assertTrue(warnings.contains("UNKNOWN (senderFault=false)"), warnings);
    assertTrue(warnings.contains("<none provided by SQS>"), warnings);
  }

  @Test
  public void testDeleteAssignsDistinctEntryIdsForDuplicateMessageIds() {
    // Under at-least-once delivery the same messageId can appear twice in one batch. Entry ids must stay
    // distinct or SQS rejects the whole request with BatchEntryIdsNotDistinct.
    CloudObjectsSelector selector = new CloudObjectsSelector(props);
    when(sqs.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
        .thenReturn(DeleteMessageBatchResponse.builder().build());

    selector.deleteProcessedMessages(sqs, sqsUrl,
        Arrays.asList(tracker("duplicate-id", "r0"), tracker("duplicate-id", "r1")));

    ArgumentCaptor<DeleteMessageBatchRequest> captor = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
    verify(sqs, times(1)).deleteMessageBatch(captor.capture());
    List<DeleteMessageBatchRequestEntry> entries = captor.getValue().entries();
    assertEquals(2, entries.size());
    assertEquals(2, entries.stream().map(DeleteMessageBatchRequestEntry::id).distinct().count(),
        "duplicate messageIds must not produce duplicate entry ids");
    assertEquals(new HashSet<>(Arrays.asList("r0", "r1")),
        entries.stream().map(DeleteMessageBatchRequestEntry::receiptHandle).collect(Collectors.toSet()));
  }

  @Test
  public void testDeleteBatchRejectsMoreEntriesThanTheSqsCap() {
    // deleteBatchOfMessages is protected and documents a 10-entry cap; SQS rejects a larger batch
    // wholesale with TooManyEntriesInBatchRequest, so fail fast at the boundary instead.
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    assertThrows(IllegalArgumentException.class, () -> selector.deleteBatchOfMessages(sqs, sqsUrl,
        trackers(CloudObjectsSelector.SQS_BATCH_MAX_ENTRIES + 1)));
    assertTrue(selector.deleteBatchOfMessages(sqs, sqsUrl, Collections.emptyList()).isEmpty());
    verify(sqs, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
  }

  @Test
  public void testClassifySqsFailureUsesErrorCodeNotStatus() {
    // SQS status codes do not track severity: RequestThrottled and OverLimit are HTTP 400,
    // ThrottlingException is HTTP 403, MalformedQueryString is HTTP 404. A status-only rule would call a
    // throttle fatal and the in-flight ceiling fatal, so the error code has to decide.
    assertEquals(CloudObjectsSelector.SqsFailureKind.BACKPRESSURE,
        CloudObjectsSelector.classifySqsFailure(sqsError("OverLimit", 400, "in flight limit")));
    assertEquals(CloudObjectsSelector.SqsFailureKind.TRANSIENT,
        CloudObjectsSelector.classifySqsFailure(sqsError("RequestThrottled", 400, "throttled")));
    assertEquals(CloudObjectsSelector.SqsFailureKind.TRANSIENT,
        CloudObjectsSelector.classifySqsFailure(sqsError("ThrottlingException", 403, "throttled")));
    // KmsThrottled is an HTTP 400 that the SDK's own throttling code set does not recognise, so it would
    // fall through to the status rule and be misjudged fatal without the explicit transient set.
    assertEquals(CloudObjectsSelector.SqsFailureKind.TRANSIENT,
        CloudObjectsSelector.classifySqsFailure(sqsError("KmsThrottled", 400, "kms throttled")));
    assertEquals(CloudObjectsSelector.SqsFailureKind.TRANSIENT,
        CloudObjectsSelector.classifySqsFailure(sqsError("InternalFailure", 500, "internal error")));
    assertEquals(CloudObjectsSelector.SqsFailureKind.FATAL,
        CloudObjectsSelector.classifySqsFailure(sqsError("QueueDoesNotExist", 400, "no such queue")));
    assertEquals(CloudObjectsSelector.SqsFailureKind.FATAL,
        CloudObjectsSelector.classifySqsFailure(sqsError("MalformedQueryString", 404, "malformed")));
    assertEquals(CloudObjectsSelector.SqsFailureKind.FATAL,
        CloudObjectsSelector.classifySqsFailure(sqsError("InvalidClientTokenId", 403, "bad key id")));
    // Transport-level failures are transient by nature.
    assertEquals(CloudObjectsSelector.SqsFailureKind.TRANSIENT,
        CloudObjectsSelector.classifySqsFailure(SdkClientException.create("Connection reset")));
    // No usable status at all: tolerate rather than fail outright.
    assertEquals(CloudObjectsSelector.SqsFailureKind.TRANSIENT,
        CloudObjectsSelector.classifySqsFailure(SqsException.builder().message("no details").build()));
    assertEquals(CloudObjectsSelector.SqsFailureKind.FATAL,
        CloudObjectsSelector.classifySqsFailure(new IllegalStateException("boom")));
  }

  @Test
  public void testDeleteDoesNotRetryANonRetryableThrownCall() {
    // A thrown call was previously recorded as senderFault=false, i.e. always retryable - so a deleted
    // queue or revoked credentials was retried across every batch for all DELETE_MAX_RETRIES passes.
    // Nothing about a fatal condition becomes retryable just because it surfaced as a thrown call rather
    // than a per-entry error, and this runs from onCommit AFTER the Hudi commit has already landed, so
    // the wasted calls are pure post-commit driver time.
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "4");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    int totalMessages = 200;
    AtomicInteger calls = new AtomicInteger();
    when(sqs.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenAnswer(invocation -> {
      calls.incrementAndGet();
      throw sqsError("QueueDoesNotExist", 400, "The specified queue does not exist");
    });

    // Nothing could be deleted and no call ever answered, so this is systemic and still surfaces.
    assertThrows(HoodieException.class,
        () -> selector.deleteProcessedMessages(sqs, sqsUrl, trackers(totalMessages)));

    // 20 batches x (1 + 3 retry passes) = 80 calls before. Dispatch now stops as soon as the first fatal
    // completion is folded in, so only the calls already in flight run and no retry pass is attempted.
    assertTrue(calls.get() <= 4,
        "a non-retryable failure must stop dispatch instead of retrying every batch, was " + calls.get());
  }

  @Test
  public void testDeleteDoesNotThrowWhenStaleHandlesCoincideWithOneThrottle() {
    // A commit that outruns the visibility timeout leaves every receipt handle stale, so every entry fails
    // with senderFault=true - a real queue state the code deliberately treats as non-fatal. If one
    // unrelated call is also throttled (fanning out makes that MORE likely), deleted==0 and firstThrown is
    // set, and the systemic guard used to fire: one transient throttle turned a documented-benign
    // condition into a HoodieStreamer shutdown. A call that returned HTTP 200 proves the queue is
    // reachable, so it must not throw.
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "2");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    AtomicInteger calls = new AtomicInteger();
    when(sqs.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenAnswer(invocation -> {
      DeleteMessageBatchRequest request = invocation.getArgument(0);
      if (calls.incrementAndGet() == 1) {
        throw sqsError("RequestThrottled", 400, "Rate exceeded");
      }
      // Every other call answers with HTTP 200 but reports each entry as a permanent sender fault.
      List<BatchResultErrorEntry> failed = request.entries().stream()
          .map(entry -> errorEntry(entry.id(), "ReceiptHandleIsInvalid", true))
          .collect(Collectors.toList());
      return DeleteMessageBatchResponse.builder().failed(failed).build();
    });

    // Must not throw: nothing here is fixable by failing the job, and the messages are redelivered anyway.
    selector.deleteProcessedMessages(sqs, sqsUrl, trackers(30));
  }

  @Test
  public void testDeleteStillThrowsWhenNoCallEverAnswered() {
    // The complement of the guard above: when every call threw, nothing proved the queue reachable, the
    // caller is about to clear its tracked messages as if they had been handled, and that must surface.
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "2");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    when(sqs.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
        .thenThrow(sqsError("InternalFailure", 500, "We encountered an internal error"));

    HoodieException thrown = assertThrows(HoodieException.class,
        () -> selector.deleteProcessedMessages(sqs, sqsUrl, trackers(20)));
    assertTrue(thrown.getMessage().contains("Failed to delete any of the"));
  }

  @Test
  public void testPartialNonRetryableDeleteFailureIsReportedAtError() {
    // The silent variant: a session token expiring mid-phase leaves deleted > 0, so the systemic throw
    // stays quiet by design. Without an explicit signal it would look like an ordinary residual-failure
    // WARN, yet no retry can clear it and it recurs on every commit until an operator acts.
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "1");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    AtomicInteger calls = new AtomicInteger();
    when(sqs.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenAnswer(invocation -> {
      if (calls.incrementAndGet() <= 2) {
        return DeleteMessageBatchResponse.builder().build();
      }
      throw sqsError("ExpiredToken", 403, "The security token included in the request is expired");
    });

    String errors = captureLogs("error", () -> selector.deleteProcessedMessages(sqs, sqsUrl, trackers(100)));

    assertTrue(errors.contains("Non-retryable SQS DeleteMessageBatch failure"),
        "a partial fatal must be named explicitly, logs were: " + errors);
    assertTrue(errors.contains("did not retry"), "the ERROR must state that retrying was skipped");
  }

  @Test
  public void testDeleteOfNoProcessedMessagesMakesNoSqsCall() {
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    selector.deleteProcessedMessages(sqs, sqsUrl, Collections.emptyList());

    verify(sqs, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
  }

  @Test
  public void testDeleteSurfacesInterruptionWhileWaitingOnBatches() throws Exception {
    // Interrupting the driver thread mid-delete must restore the interrupt flag and surface a
    // HoodieException rather than silently reporting a successful delete.
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    CountDownLatch callInFlight = new CountDownLatch(1);
    when(sqs.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenAnswer(invocation -> {
      callInFlight.countDown();
      // Held until the delete pool is shut down, guaranteeing the master thread is still in invokeAll.
      Thread.sleep(30_000);
      return DeleteMessageBatchResponse.builder().build();
    });

    Thread driver = Thread.currentThread();
    Thread interrupter = new Thread(() -> {
      try {
        assertTrue(callInFlight.await(30, TimeUnit.SECONDS));
        driver.interrupt();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    interrupter.start();
    try {
      HoodieException thrown = assertThrows(HoodieException.class, () -> selector.deleteProcessedMessages(
          sqs, sqsUrl, Collections.singletonList(tracker("m0", "r0"))));
      assertTrue(thrown.getMessage().contains("Interrupted while deleting messages"), thrown.getMessage());
      assertTrue(Thread.currentThread().isInterrupted(), "the interrupt flag must be restored");
    } finally {
      // Clear the flag so it cannot leak into the next test.
      Thread.interrupted();
      interrupter.join();
    }
  }

  @Test
  public void testErrorFromDeleteWorkerIsNotFunnelledIntoRetries() {
    // An Error on a delete worker is not a delete failure: funnelling it into the retry path would replay
    // the batch DELETE_MAX_RETRIES times and then reduce a fatal condition to a residual-failure log line.
    // It must propagate unwrapped, on the first pass.
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "2");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    AtomicInteger deleteCalls = new AtomicInteger();
    when(sqs.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenAnswer(invocation -> {
      deleteCalls.incrementAndGet();
      throw new OutOfMemoryError("Java heap space");
    });

    OutOfMemoryError thrown = assertThrows(OutOfMemoryError.class,
        () -> selector.deleteProcessedMessages(sqs, sqsUrl, trackers(2)));
    assertEquals("Java heap space", thrown.getMessage(), "the Error must propagate unwrapped");
    // Both messages fit one batch, so exactly one call ran and no retry pass followed it.
    assertEquals(1, deleteCalls.get());
  }

  @Test
  public void testDeleteRetriesPartialFailureThenSucceeds() {
    // DeleteMessageBatch can return a partial failure on an HTTP 200. The failed entry must be
    // collected and retried; after the retry succeeds there must be zero residual failures.
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "4");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    List<CloudObjectsSelector.MessageTracker> processed = new ArrayList<>();
    processed.add(tracker("m0", "r0"));
    processed.add(tracker("m1", "r1"));

    AtomicInteger deleteCalls = new AtomicInteger();
    when(sqs.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenAnswer(invocation -> {
      DeleteMessageBatchRequest request = invocation.getArgument(0);
      // First call: fail whichever entry carries receiptHandle r0. Later calls: succeed.
      if (deleteCalls.getAndIncrement() == 0) {
        String failedEntryId = request.entries().stream()
            .filter(entry -> entry.receiptHandle().equals("r0"))
            .map(entry -> entry.id())
            .findFirst().orElseThrow(() -> new AssertionError("r0 not present in first batch"));
        return DeleteMessageBatchResponse.builder()
            .failed(BatchResultErrorEntry.builder().id(failedEntryId).senderFault(false).code("500").build())
            .build();
      }
      return DeleteMessageBatchResponse.builder().build();
    });

    selector.deleteProcessedMessages(sqs, sqsUrl, processed);

    // 1 initial batch call + 1 retry call for the single failed entry.
    assertEquals(2, deleteCalls.get());
    ArgumentCaptor<DeleteMessageBatchRequest> captor = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
    verify(sqs, times(2)).deleteMessageBatch(captor.capture());
    // The retry must target exactly the originally-failed message (receiptHandle r0), nothing else.
    DeleteMessageBatchRequest retry = captor.getAllValues().get(1);
    assertEquals(1, retry.entries().size());
    assertEquals("r0", retry.entries().get(0).receiptHandle());
  }

  @Test
  public void testDeleteExhaustsRetriesAndReportsResidualFailure() {
    // When an entry keeps failing, deletion must stop after DELETE_MAX_RETRIES passes without throwing,
    // leaving the residual failure to be logged (and eventually redriven by SQS).
    props.setProperty(S3_SOURCE_QUEUE_PROCESSING_PARALLELISM.key(), "4");
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    List<CloudObjectsSelector.MessageTracker> processed = new ArrayList<>();
    processed.add(tracker("m0", "r0"));

    AtomicInteger deleteCalls = new AtomicInteger();
    when(sqs.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenAnswer(invocation -> {
      deleteCalls.incrementAndGet();
      DeleteMessageBatchRequest request = invocation.getArgument(0);
      return DeleteMessageBatchResponse.builder()
          .failed(BatchResultErrorEntry.builder()
              .id(request.entries().get(0).id()).senderFault(false).code("500").build())
          .build();
    });

    selector.deleteProcessedMessages(sqs, sqsUrl, processed);

    // 1 initial pass + DELETE_MAX_RETRIES retry passes; no exception despite the persistent failure.
    assertEquals(1 + CloudObjectsSelector.DELETE_MAX_RETRIES, deleteCalls.get());
  }

  @ParameterizedTest
  @ValueSource(classes = {CloudObjectsSelector.class})
  public void testSqsQueueAttributesRequestsAllOnly(Class<?> clazz) {
    CloudObjectsSelector selector =
        (CloudObjectsSelector) ReflectionUtils.loadClass(clazz.getName(), props);
    CloudObjectTestUtils.setMessagesInQueue(sqs, null);

    selector.getSqsQueueAttributes(sqs, sqsUrl);

    ArgumentCaptor<GetQueueAttributesRequest> captor =
        ArgumentCaptor.forClass(GetQueueAttributesRequest.class);
    verify(sqs).getQueueAttributes(captor.capture());
    // Must request ALL only. Naming FIFO-only attributes (e.g. FifoQueue) or dead-letter-queue-only
    // attributes (e.g. RedrivePolicy) makes SQS reject the entire GetQueueAttributes call with
    // InvalidAttributeNameException, which takes ingestion down since the receive loop depends on
    // this call. Assert the serialized wire value too, not just the enum constant.
    assertEquals(Collections.singletonList(QueueAttributeName.ALL), captor.getValue().attributeNames());
    assertEquals(Collections.singletonList("All"), captor.getValue().attributeNamesAsStrings());
  }

  @Test
  public void testProbeFailureAtBreakDoesNotFailBatch() {
    CloudObjectsSelector selector = new CloudObjectsSelector(props);

    // The in-flight probe at the receive-loop break is purely diagnostic. Even if SQS rejects it,
    // the batch must still complete - diagnostics must never sit on the load-bearing path.
    Map<String, String> attributes = new HashMap<>();
    attributes.put(SQS_ATTR_APPROX_MESSAGES, "100");
    when(sqs.getQueueAttributes(any(GetQueueAttributesRequest.class)))
        .thenReturn(GetQueueAttributesResponse.builder().attributesWithStrings(attributes).build())
        .thenThrow(SqsException.builder().message("InvalidAttributeName").build());
    when(sqs.receiveMessage(any(ReceiveMessageRequest.class)))
        .thenReturn(ReceiveMessageResponse.builder().build());

    List<Message> messages = selector.getMessagesToProcess(sqs, sqsUrl, 20, 30, 100, 10);

    assertEquals(0, messages.size());
    // One attributes call at loop start + the failed probe at the break, both consumed.
    verify(sqs, times(2)).getQueueAttributes(any(GetQueueAttributesRequest.class));
  }

  @Test
  public void testQueueConfigLoggingFailureDoesNotFailBatch() {
    // A .fifo url exercises the suffix-based FIFO derivation, and the attribute map blows up
    // mid-log to prove the once-per-selector config logging can never fail the receive path.
    String fifoQueueUrl = "test-queue.fifo";
    Map<String, String> explodingAttributes = new HashMap<String, String>() {
      @Override
      public String get(Object key) {
        if (SQS_ATTR_MESSAGE_RETENTION_PERIOD.equals(key)) {
          throw new IllegalStateException("attribute lookup failed");
        }
        return super.get(key);
      }
    };
    explodingAttributes.put(SQS_ATTR_APPROX_MESSAGES, "0");
    CloudObjectsSelector selector = new CloudObjectsSelector(props) {
      @Override
      protected Map<String, String> getSqsQueueAttributes(SqsClient sqsClient, String queueUrl) {
        return explodingAttributes;
      }
    };

    List<Message> messages = selector.getMessagesToProcess(sqs, fifoQueueUrl, 20, 30, 100, 10);

    assertEquals(0, messages.size());
    // Backlog is empty, so the receive loop never runs; the failed log must not have thrown.
    verify(sqs, never()).receiveMessage(any(ReceiveMessageRequest.class));
  }

  public Map<String, String> createAttributeMap(String key, String value) {
    Map<String, String> attribute = new HashMap<>();
    attribute.put(key, value);
    return attribute;
  }

  /**
   * Builds an SQS service exception carrying both an error code and an HTTP status, which is what the
   * failure classification reads. The two are supplied independently because SQS pairs them in ways a
   * status-only rule gets wrong (RequestThrottled and OverLimit are both HTTP 400).
   */
  private static SqsException sqsError(String errorCode, int statusCode, String message) {
    return (SqsException) SqsException.builder()
        .awsErrorDetails(AwsErrorDetails.builder().errorCode(errorCode).errorMessage(message).build())
        .statusCode(statusCode)
        .message(message)
        .build();
  }

  private static Message sqsMessage(String messageId, String receiptHandle) {
    return Message.builder().messageId(messageId).receiptHandle(receiptHandle).body("{}").build();
  }

  private static CloudObjectsSelector.MessageTracker tracker(String messageId, String receiptHandle) {
    return new CloudObjectsSelector.MessageTracker(sqsMessage(messageId, receiptHandle));
  }

  /** {@code count} trackers with ids m0..m(count-1) and receipt handles r0..r(count-1). */
  private static List<CloudObjectsSelector.MessageTracker> trackers(int count) {
    List<CloudObjectsSelector.MessageTracker> trackers = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      trackers.add(tracker("m" + i, "r" + i));
    }
    return trackers;
  }

  /** The receipt handles {@link #trackers(int)} produces, for asserting on the full deleted set. */
  private static Set<String> expectedHandles(int count) {
    Set<String> handles = new HashSet<>();
    for (int i = 0; i < count; i++) {
      handles.add("r" + i);
    }
    return handles;
  }

  private static BatchResultErrorEntry errorEntry(String id, String code, boolean senderFault) {
    return BatchResultErrorEntry.builder().id(id).code(code).senderFault(senderFault)
        .message(code + " for entry " + id).build();
  }

  /**
   * Runs {@code body} with {@link CloudObjectsSelector#log} swapped for a mock and returns everything it
   * logged at {@code level} - format string and arguments flattened - so a test can assert on what
   * on-call would actually see. The real logger is always restored.
   */
  private static String captureLogs(String level, Runnable body) {
    Logger capturingLogger = Mockito.mock(Logger.class);
    Logger original = CloudObjectsSelector.log;
    CloudObjectsSelector.log = capturingLogger;
    try {
      body.run();
    } finally {
      CloudObjectsSelector.log = original;
    }
    return Mockito.mockingDetails(capturingLogger).getInvocations().stream()
        .filter(invocation -> level.equals(invocation.getMethod().getName()))
        .map(invocation -> renderLogEvent(invocation.getArguments()))
        .collect(Collectors.joining("\n"));
  }

  /**
   * Substitutes a captured slf4j event's arguments into its {@code {}} placeholders, so assertions can
   * be written against the line an operator reads rather than against the unformatted template.
   */
  private static String renderLogEvent(Object[] arguments) {
    if (arguments.length == 0) {
      return "";
    }
    StringBuilder rendered = new StringBuilder(String.valueOf(arguments[0]));
    int searchFrom = 0;
    for (int i = 1; i < arguments.length; i++) {
      String value = String.valueOf(arguments[i]);
      int placeholder = rendered.indexOf("{}", searchFrom);
      if (placeholder < 0) {
        // A trailing Throwable (or a surplus argument) is appended by slf4j, not substituted.
        rendered.append(" | ").append(value);
      } else {
        rendered.replace(placeholder, placeholder + 2, value);
        searchFrom = placeholder + value.length();
      }
    }
    return rendered.toString();
  }

}
