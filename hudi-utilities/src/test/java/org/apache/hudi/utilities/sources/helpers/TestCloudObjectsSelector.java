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
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.hudi.utilities.testutils.CloudObjectTestUtils;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector.Config.S3_SOURCE_QUEUE_REGION;
import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector.Config.S3_SOURCE_QUEUE_URL;
import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector.S3_FILE_PATH;
import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector.S3_FILE_SIZE;
import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector.S3_MODEL_EVENT_TIME;
import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector.S3_PREFIX;
import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector.SQS_ATTR_APPROX_MESSAGES;
import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector.SQS_MODEL_EVENT_RECORDS;
import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector.SQS_MODEL_MESSAGE;
import static org.apache.hudi.utilities.testutils.CloudObjectTestUtils.deleteMessagesInQueue;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestCloudObjectsSelector extends HoodieClientTestHarness {

  static final String REGION_NAME = "us-east-1";

  TypedProperties props;
  String sqsUrl;

  @Mock
  AmazonSQS sqs;

  @Mock
  private CloudObjectsSelector cloudObjectsSelector;

  @BeforeEach
  void setUp() {
    initSparkContexts();
    initPath();
    initFileSystem();
    MockitoAnnotations.initMocks(this);

    props = new TypedProperties();
    sqsUrl = "test-queue";
    props.setProperty(S3_SOURCE_QUEUE_URL, sqsUrl);
    props.setProperty(S3_SOURCE_QUEUE_REGION, REGION_NAME);
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
    List<Message> testSingleList = new ArrayList<>();
    testSingleList.add(new Message().addAttributesEntry("id", "1"));
    testSingleList.add(new Message().addAttributesEntry("id", "2"));
    testSingleList.add(new Message().addAttributesEntry("id", "3"));
    testSingleList.add(new Message().addAttributesEntry("id", "4"));
    testSingleList.add(new Message().addAttributesEntry("id", "5"));

    List<Message> expectedFirstList = new ArrayList<>();
    expectedFirstList.add(new Message().addAttributesEntry("id", "1"));
    expectedFirstList.add(new Message().addAttributesEntry("id", "2"));

    List<Message> expectedSecondList = new ArrayList<>();
    expectedSecondList.add(new Message().addAttributesEntry("id", "3"));
    expectedSecondList.add(new Message().addAttributesEntry("id", "4"));

    List<Message> expectedFinalList = new ArrayList<>();
    expectedFinalList.add(new Message().addAttributesEntry("id", "5"));

    //  test the return values
    List<List<Message>> partitionedList = selector.createListPartitions(testSingleList, 2);

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
    List<Message> testSingleList = new ArrayList<>();
    testSingleList.add(new Message().addAttributesEntry("id", "1"));
    testSingleList.add(new Message().addAttributesEntry("id", "2"));

    //  test the return values
    List<List<Message>> partitionedList = selector.createListPartitions(testSingleList, 0);

    assertEquals(0, partitionedList.size());
  }

  @ParameterizedTest
  @ValueSource(classes = {CloudObjectsSelector.class})
  public void testOnCommitDeleteProcessedMessages(Class<?> clazz) {

    CloudObjectsSelector selector =
        (CloudObjectsSelector) ReflectionUtils.loadClass(clazz.getName(), props);

    // setup lists
    List<Message> testSingleList = new ArrayList<>();
    testSingleList.add(
        new Message()
            .addAttributesEntry("MessageId", "1")
            .addAttributesEntry("ReceiptHandle", "1"));
    testSingleList.add(
        new Message()
            .addAttributesEntry("MessageId", "2")
            .addAttributesEntry("ReceiptHandle", "1"));

    deleteMessagesInQueue(sqs);

    //  test the return values
    selector.deleteProcessedMessages(sqs, sqsUrl, testSingleList);
  }
}
