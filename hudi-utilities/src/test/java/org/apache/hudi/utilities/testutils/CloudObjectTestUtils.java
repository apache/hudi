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

package org.apache.hudi.utilities.testutils;

import org.apache.hadoop.fs.Path;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector.SQS_ATTR_APPROX_MESSAGES;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Utils Class for unit testing on CloudObject related sources.
 */
public class CloudObjectTestUtils {

  /**
   * Set a return value for mocked sqs instance. It will add a new messages (s3 Event) and set
   * ApproximateNumberOfMessages attribute of the queue.
   *
   * @param sqs  Mocked instance of AmazonSQS
   * @param path Absolute Path of file in FileSystem
   */
  public static void setMessagesInQueue(SqsClient sqs, Path path) {

    ReceiveMessageResponse receiveMessageResult = ReceiveMessageResponse.builder().build();
    String approximateNumberOfMessages = "0";

    if (path != null) {
      String body =
          "{\n  \"Type\" : \"Notification\",\n  \"MessageId\" : \"1\",\n  \"TopicArn\" : \"arn:aws:sns:foo:123:"
              + "foo-bar\",\n  \"Subject\" : \"Amazon S3 Notification\",\n  \"Message\" : \"{\\\"Records\\\":"
              + "[{\\\"eventVersion\\\":\\\"2.1\\\",\\\"eventSource\\\":\\\"aws:s3\\\",\\\"awsRegion\\\":\\\"us"
              + "-west-2\\\",\\\"eventTime\\\":\\\"2021-07-27T09:05:36.755Z\\\",\\\"eventName\\\":\\\"ObjectCreated"
              + ":Copy\\\",\\\"userIdentity\\\":{\\\"principalId\\\":\\\"AWS:test\\\"},\\\"requestParameters\\\":"
              + "{\\\"sourceIPAddress\\\":\\\"0.0.0.0\\\"},\\\"responseElements\\\":{\\\"x-amz-request-id\\\":\\\""
              + "test\\\",\\\"x-amz-id-2\\\":\\\"foobar\\\"},\\\"s3\\\":{\\\"s3SchemaVersion\\\":\\\"1.0\\\",\\\""
              + "configurationId\\\":\\\"foobar\\\",\\\"bucket\\\":{\\\"name\\\":\\\""
              + path.getParent().toString().replace("hdfs://", "")
              + "\\\",\\\"ownerIdentity\\\":{\\\"principalId\\\":\\\"foo\\\"},\\\"arn\\\":\\\"arn:aws:s3:::foo\\\"}"
              + ",\\\"object\\\":{\\\"key\\\":\\\""
              + path.getName()
              + "\\\",\\\"size\\\":123,\\\"eTag\\\":\\\"test\\\",\\\"sequencer\\\":\\\"1\\\"}}}]}\"}";

      Message message = Message.builder()
              .receiptHandle("1")
              .messageId("1")
              .body(body)
              .build();

      List<Message> messages = new ArrayList<>();
      messages.add(message);
      receiveMessageResult = ReceiveMessageResponse.builder().messages(messages).build();
      approximateNumberOfMessages = "1";
    }
    Map<String, String> attributes = new HashMap();
    attributes.put(SQS_ATTR_APPROX_MESSAGES, approximateNumberOfMessages);
    when(sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(receiveMessageResult);
    when(sqs.getQueueAttributes(any(GetQueueAttributesRequest.class)))
        .thenReturn(
            GetQueueAttributesResponse.builder()
                    .attributesWithStrings(attributes)
                    .build());
  }

  /**
   * Mock the sqs.deleteMessageBatch() method from queue.
   *
   * @param sqs Mocked instance of AmazonSQS
   */
  public static void deleteMessagesInQueue(SqsClient sqs) {
    when(sqs.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
        .thenReturn(DeleteMessageBatchResponse.builder().build());
  }
}
