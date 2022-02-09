/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.config.TypedProperties;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.BatchResultErrorEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class has methods for processing cloud objects.
 * It currently supports only AWS S3 objects and AWS SQS queue.
 */
public class CloudObjectsSelector {
  public static final List<String> ALLOWED_S3_EVENT_PREFIX =
      Collections.singletonList("ObjectCreated");
  public static final String S3_PREFIX = "s3://";
  public static volatile Logger log = LogManager.getLogger(CloudObjectsSelector.class);
  public static final String SQS_ATTR_APPROX_MESSAGES = "ApproximateNumberOfMessages";
  static final String SQS_MODEL_MESSAGE = "Message";
  static final String SQS_MODEL_EVENT_RECORDS = "Records";
  static final String SQS_MODEL_EVENT_NAME = "eventName";
  static final String S3_MODEL_EVENT_TIME = "eventTime";
  static final String S3_FILE_SIZE = "fileSize";
  static final String S3_FILE_PATH = "filePath";
  public final String queueUrl;
  public final int longPollWait;
  public final int maxMessagePerBatch;
  public final int maxMessagesPerRequest;
  public final int visibilityTimeout;
  public final TypedProperties props;
  public final String fsName;
  private final String regionName;

  /**
   * Cloud Objects Selector Class. {@link CloudObjectsSelector}
   */
  public CloudObjectsSelector(TypedProperties props) {
    DataSourceUtils.checkRequiredProperties(props, Arrays.asList(Config.S3_SOURCE_QUEUE_URL, Config.S3_SOURCE_QUEUE_REGION));
    this.props = props;
    this.queueUrl = props.getString(Config.S3_SOURCE_QUEUE_URL);
    this.regionName = props.getString(Config.S3_SOURCE_QUEUE_REGION);
    this.fsName = props.getString(Config.S3_SOURCE_QUEUE_FS, "s3").toLowerCase();
    this.longPollWait = props.getInteger(Config.S3_QUEUE_LONG_POLL_WAIT, 20);
    this.maxMessagePerBatch = props.getInteger(Config.S3_SOURCE_QUEUE_MAX_MESSAGES_PER_BATCH, 5);
    this.maxMessagesPerRequest = props.getInteger(Config.S3_SOURCE_QUEUE_MAX_MESSAGES_PER_REQUEST, 10);
    this.visibilityTimeout = props.getInteger(Config.S3_SOURCE_QUEUE_VISIBILITY_TIMEOUT, 30);
  }

  /**
   * Get SQS queue attributes.
   *
   * @param sqsClient AWSClient for sqsClient
   * @param queueUrl  queue full url
   * @return map of attributes needed
   */
  protected Map<String, String> getSqsQueueAttributes(AmazonSQS sqsClient, String queueUrl) {
    GetQueueAttributesResult queueAttributesResult = sqsClient.getQueueAttributes(
        new GetQueueAttributesRequest(queueUrl).withAttributeNames(SQS_ATTR_APPROX_MESSAGES)
    );
    return queueAttributesResult.getAttributes();
  }

  /**
   * Get the file attributes filePath, eventTime and size from JSONObject record.
   *
   * @param record of object event
   * @return map of file attribute
   */
  protected Map<String, Object> getFileAttributesFromRecord(JSONObject record) throws UnsupportedEncodingException {
    Map<String, Object> fileRecord = new HashMap<>();
    String eventTimeStr = record.getString(S3_MODEL_EVENT_TIME);
    long eventTime =
        Date.from(Instant.from(DateTimeFormatter.ISO_INSTANT.parse(eventTimeStr))).getTime();
    JSONObject s3Object = record.getJSONObject("s3").getJSONObject("object");
    String bucket = URLDecoder.decode(record.getJSONObject("s3").getJSONObject("bucket").getString("name"), "UTF-8");
    String key = URLDecoder.decode(s3Object.getString("key"), "UTF-8");
    String filePath = this.fsName + "://" + bucket + "/" + key;
    fileRecord.put(S3_MODEL_EVENT_TIME, eventTime);
    fileRecord.put(S3_FILE_SIZE, s3Object.getLong("size"));
    fileRecord.put(S3_FILE_PATH, filePath);
    return fileRecord;
  }

  /**
   * Amazon SQS Client Builder.
   */
  public AmazonSQS createAmazonSqsClient() {
    return AmazonSQSClientBuilder.standard().withRegion(Regions.fromName(regionName)).build();
  }

  /**
   * List messages from queue.
   */
  protected List<Message> getMessagesToProcess(
      AmazonSQS sqsClient,
      String queueUrl,
      int longPollWait,
      int visibilityTimeout,
      int maxMessagePerBatch,
      int maxMessagesPerRequest) {
    List<Message> messagesToProcess = new ArrayList<>();
    ReceiveMessageRequest receiveMessageRequest =
        new ReceiveMessageRequest()
            .withQueueUrl(queueUrl)
            .withWaitTimeSeconds(longPollWait)
            .withVisibilityTimeout(visibilityTimeout);
    receiveMessageRequest.setMaxNumberOfMessages(maxMessagesPerRequest);
    // Get count for available messages
    Map<String, String> queueAttributesResult = getSqsQueueAttributes(sqsClient, queueUrl);
    long approxMessagesAvailable = Long.parseLong(queueAttributesResult.get(SQS_ATTR_APPROX_MESSAGES));
    log.info("Approximately " + approxMessagesAvailable + " messages available in queue.");
    long numMessagesToProcess = Math.min(approxMessagesAvailable, maxMessagePerBatch);
    for (int i = 0;
         i < (int) Math.ceil((double) numMessagesToProcess / maxMessagesPerRequest);
         ++i) {
      List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).getMessages();
      log.debug("Number of messages: " + messages.size());
      messagesToProcess.addAll(messages);
      if (messages.isEmpty()) {
        // ApproximateNumberOfMessages value is eventually consistent.
        // So, we still need to check and break if there are no messages.
        break;
      }
    }
    return messagesToProcess;
  }

  /**
   * Create partitions of list using specific batch size. we can't use third party API for this
   * functionality, due to https://github.com/apache/hudi/blob/master/style/checkstyle.xml#L270
   */
  protected List<List<Message>> createListPartitions(List<Message> singleList, int eachBatchSize) {
    List<List<Message>> listPartitions = new ArrayList<>();
    if (singleList.size() == 0 || eachBatchSize < 1) {
      return listPartitions;
    }
    for (int start = 0; start < singleList.size(); start += eachBatchSize) {
      int end = Math.min(start + eachBatchSize, singleList.size());
      if (start > end) {
        throw new IndexOutOfBoundsException(
            "Index " + start + " is out of the list range <0," + (singleList.size() - 1) + ">");
      }
      listPartitions.add(new ArrayList<>(singleList.subList(start, end)));
    }
    return listPartitions;
  }

  /**
   * Delete batch of messages from queue.
   */
  protected void deleteBatchOfMessages(AmazonSQS sqs, String queueUrl, List<Message> messagesToBeDeleted) {
    DeleteMessageBatchRequest deleteBatchReq =
        new DeleteMessageBatchRequest().withQueueUrl(queueUrl);
    List<DeleteMessageBatchRequestEntry> deleteEntries = deleteBatchReq.getEntries();
    for (Message message : messagesToBeDeleted) {
      deleteEntries.add(
          new DeleteMessageBatchRequestEntry()
              .withId(message.getMessageId())
              .withReceiptHandle(message.getReceiptHandle()));
    }
    DeleteMessageBatchResult deleteResult = sqs.deleteMessageBatch(deleteBatchReq);
    List<String> deleteFailures =
        deleteResult.getFailed().stream()
            .map(BatchResultErrorEntry::getId)
            .collect(Collectors.toList());
    if (!deleteFailures.isEmpty()) {
      log.warn(
          "Failed to delete "
              + deleteFailures.size()
              + " messages out of "
              + deleteEntries.size()
              + " from queue.");
    } else {
      log.info("Successfully deleted " + deleteEntries.size() + " messages from queue.");
    }
  }

  /**
   * Delete Queue Messages after hudi commit. This method will be invoked by source.onCommit.
   */
  public void deleteProcessedMessages(AmazonSQS sqs, String queueUrl, List<Message> processedMessages) {
    if (!processedMessages.isEmpty()) {
      // create batch for deletion, SES DeleteMessageBatchRequest only accept max 10 entries
      List<List<Message>> deleteBatches = createListPartitions(processedMessages, 10);
      for (List<Message> deleteBatch : deleteBatches) {
        deleteBatchOfMessages(sqs, queueUrl, deleteBatch);
      }
    }
  }

  /**
   * Configs supported.
   */
  public static class Config {
    private static final String HOODIE_DELTASTREAMER_S3_SOURCE = "hoodie.deltastreamer.s3.source";
    /**
     * {@value #S3_SOURCE_QUEUE_URL} is the queue url for cloud object events.
     */
    public static final String S3_SOURCE_QUEUE_URL = HOODIE_DELTASTREAMER_S3_SOURCE + ".queue.url";

    /**
     * {@value #S3_SOURCE_QUEUE_REGION} is the case-sensitive region name of the cloud provider for the queue. For example, "us-east-1".
     */
    public static final String S3_SOURCE_QUEUE_REGION = HOODIE_DELTASTREAMER_S3_SOURCE + ".queue.region";

    /**
     * {@value #S3_SOURCE_QUEUE_FS} is file system corresponding to queue. For example, for AWS SQS it is s3/s3a.
     */
    public static final String S3_SOURCE_QUEUE_FS = HOODIE_DELTASTREAMER_S3_SOURCE + ".queue.fs";

    /**
     * {@value #S3_QUEUE_LONG_POLL_WAIT} is the long poll wait time in seconds If set as 0 then
     * client will fetch on short poll basis.
     */
    public static final String S3_QUEUE_LONG_POLL_WAIT =
        HOODIE_DELTASTREAMER_S3_SOURCE + ".queue.long.poll.wait";

    /**
     * {@value #S3_SOURCE_QUEUE_MAX_MESSAGES_PER_BATCH} is max messages for each batch of delta streamer
     * run. Source will process these maximum number of message at a time.
     */
    public static final String S3_SOURCE_QUEUE_MAX_MESSAGES_PER_BATCH =
        HOODIE_DELTASTREAMER_S3_SOURCE + ".queue.max.messages.per.batch";

    /**
     * {@value #S3_SOURCE_QUEUE_MAX_MESSAGES_PER_REQUEST} is max messages for each request.
     */
    public static final String S3_SOURCE_QUEUE_MAX_MESSAGES_PER_REQUEST =
        HOODIE_DELTASTREAMER_S3_SOURCE + ".queue.max.messages.per.request";

    /**
     * {@value #S3_SOURCE_QUEUE_VISIBILITY_TIMEOUT} is visibility timeout for messages in queue. After we
     * consume the message, queue will move the consumed messages to in-flight state, these messages
     * can't be consumed again by source for this timeout period.
     */
    public static final String S3_SOURCE_QUEUE_VISIBILITY_TIMEOUT =
        HOODIE_DELTASTREAMER_S3_SOURCE + ".queue.visibility.timeout";

    /**
     * {@value #SOURCE_INPUT_SELECTOR} source input selector.
     */
    public static final String SOURCE_INPUT_SELECTOR = "hoodie.deltastreamer.source.input.selector";
  }
}
