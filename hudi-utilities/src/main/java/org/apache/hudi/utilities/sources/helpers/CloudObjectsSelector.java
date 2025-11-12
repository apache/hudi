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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.config.DFSPathSelectorConfig;
import org.apache.hudi.utilities.config.S3SourceConfig;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
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

import static org.apache.hudi.common.util.ConfigUtils.checkRequiredConfigProperties;
import static org.apache.hudi.common.util.ConfigUtils.getIntWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;

/**
 * This class has methods for processing cloud objects.
 * It currently supports only AWS S3 objects and AWS SQS queue.
 */
public class CloudObjectsSelector {
  public static final List<String> ALLOWED_S3_EVENT_PREFIX =
      Collections.singletonList("ObjectCreated");
  public static final String S3_PREFIX = "s3://";
  public static volatile Logger log = LoggerFactory.getLogger(CloudObjectsSelector.class);
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
    checkRequiredConfigProperties(props, Arrays.asList(
        S3SourceConfig.S3_SOURCE_QUEUE_URL, S3SourceConfig.S3_SOURCE_QUEUE_REGION));
    this.props = props;
    this.queueUrl = getStringWithAltKeys(props, S3SourceConfig.S3_SOURCE_QUEUE_URL);
    this.regionName = getStringWithAltKeys(props, S3SourceConfig.S3_SOURCE_QUEUE_REGION);
    this.fsName = getStringWithAltKeys(props, S3SourceConfig.S3_SOURCE_QUEUE_FS, true);
    this.longPollWait = getIntWithAltKeys(props, S3SourceConfig.S3_QUEUE_LONG_POLL_WAIT);
    this.maxMessagePerBatch = getIntWithAltKeys(props, S3SourceConfig.S3_SOURCE_QUEUE_MAX_MESSAGES_PER_BATCH);
    this.maxMessagesPerRequest = getIntWithAltKeys(props, S3SourceConfig.S3_SOURCE_QUEUE_MAX_MESSAGES_PER_REQUEST);
    this.visibilityTimeout = getIntWithAltKeys(props, S3SourceConfig.S3_SOURCE_QUEUE_VISIBILITY_TIMEOUT);
  }

  /**
   * Get SQS queue attributes.
   *
   * @param sqsClient AWSClient for sqsClient
   * @param queueUrl  queue full url
   * @return map of attributes needed
   */
  protected Map<String, String> getSqsQueueAttributes(SqsClient sqsClient, String queueUrl) {
    GetQueueAttributesResponse queueAttributesResult = sqsClient.getQueueAttributes(
            GetQueueAttributesRequest.builder()
                    .queueUrl(queueUrl)
                    .attributeNames(QueueAttributeName.fromValue(SQS_ATTR_APPROX_MESSAGES))
                    .build()
    );
    return queueAttributesResult.attributesAsStrings();
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
  public SqsClient createAmazonSqsClient() {
    return SqsClient.builder().region(Region.of(regionName)).build();
  }

  /**
   * List messages from queue.
   */
  protected List<Message> getMessagesToProcess(
      SqsClient sqsClient,
      String queueUrl,
      int longPollWait,
      int visibilityTimeout,
      int maxMessagePerBatch,
      int maxMessagesPerRequest) {
    List<Message> messagesToProcess = new ArrayList<>();
    ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
            .queueUrl(queueUrl)
            .waitTimeSeconds(longPollWait)
            .visibilityTimeout(visibilityTimeout)
            .maxNumberOfMessages(maxMessagesPerRequest)
            .build();
    // Get count for available messages
    Map<String, String> queueAttributesResult = getSqsQueueAttributes(sqsClient, queueUrl);
    long approxMessagesAvailable = Long.parseLong(queueAttributesResult.get(SQS_ATTR_APPROX_MESSAGES));
    log.info("Approximately {} messages available in queue.", approxMessagesAvailable);
    long numMessagesToProcess = Math.min(approxMessagesAvailable, maxMessagePerBatch);
    for (int i = 0;
         i < (int) Math.ceil((double) numMessagesToProcess / maxMessagesPerRequest);
         ++i) {
      List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
      log.debug("Number of messages: {}", messages.size());
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
  protected void deleteBatchOfMessages(SqsClient sqs, String queueUrl, List<Message> messagesToBeDeleted) {
    if (messagesToBeDeleted.isEmpty()) {
      return;
    }
    DeleteMessageBatchRequest.Builder builder = DeleteMessageBatchRequest.builder().queueUrl(queueUrl);
    List<DeleteMessageBatchRequestEntry> deleteEntries = new ArrayList<>();

    for (Message message : messagesToBeDeleted) {
      deleteEntries.add(
          DeleteMessageBatchRequestEntry.builder()
                  .id(message.messageId())
                  .receiptHandle(message.receiptHandle())
                  .build());
    }
    builder.entries(deleteEntries);
    DeleteMessageBatchResponse deleteResponse = sqs.deleteMessageBatch(builder.build());
    List<String> deleteFailures =
        deleteResponse.failed().stream()
            .map(BatchResultErrorEntry::id)
            .collect(Collectors.toList());
    if (!deleteFailures.isEmpty()) {
      log.warn(
          "Failed to delete {} messages out of {} from queue.", deleteFailures.size(), deleteEntries.size());
    } else {
      log.info("Successfully deleted {} messages from queue.", deleteEntries.size());
    }
  }

  /**
   * Delete Queue Messages after hudi commit. This method will be invoked by source.onCommit.
   */
  public void deleteProcessedMessages(SqsClient sqs, String queueUrl, List<Message> processedMessages) {
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
    /**
     * {@link  #S3_SOURCE_QUEUE_URL} is the queue url for cloud object events.
     */
    @Deprecated
    public static final String S3_SOURCE_QUEUE_URL = S3SourceConfig.S3_SOURCE_QUEUE_URL.key();

    /**
     * {@link  #S3_SOURCE_QUEUE_REGION} is the case-sensitive region name of the cloud provider for the queue. For example, "us-east-1".
     */
    @Deprecated
    public static final String S3_SOURCE_QUEUE_REGION = S3SourceConfig.S3_SOURCE_QUEUE_REGION.key();

    /**
     * {@link  #S3_SOURCE_QUEUE_FS} is file system corresponding to queue. For example, for AWS SQS it is s3/s3a.
     */
    @Deprecated
    public static final String S3_SOURCE_QUEUE_FS = S3SourceConfig.S3_SOURCE_QUEUE_FS.key();

    /**
     * {@link  #S3_QUEUE_LONG_POLL_WAIT} is the long poll wait time in seconds If set as 0 then
     * client will fetch on short poll basis.
     */
    @Deprecated
    public static final String S3_QUEUE_LONG_POLL_WAIT = S3SourceConfig.S3_QUEUE_LONG_POLL_WAIT.key();

    /**
     * {@link  #S3_SOURCE_QUEUE_MAX_MESSAGES_PER_BATCH} is max messages for each batch of Hudi Streamer
     * run. Source will process these maximum number of message at a time.
     */
    @Deprecated
    public static final String S3_SOURCE_QUEUE_MAX_MESSAGES_PER_BATCH = S3SourceConfig.S3_SOURCE_QUEUE_MAX_MESSAGES_PER_BATCH.key();

    /**
     * {@link  #S3_SOURCE_QUEUE_MAX_MESSAGES_PER_REQUEST} is max messages for each request.
     */
    @Deprecated
    public static final String S3_SOURCE_QUEUE_MAX_MESSAGES_PER_REQUEST = S3SourceConfig.S3_SOURCE_QUEUE_MAX_MESSAGES_PER_REQUEST.key();

    /**
     * {@link  #S3_SOURCE_QUEUE_VISIBILITY_TIMEOUT} is visibility timeout for messages in queue. After we
     * consume the message, queue will move the consumed messages to in-flight state, these messages
     * can't be consumed again by source for this timeout period.
     */
    @Deprecated
    public static final String S3_SOURCE_QUEUE_VISIBILITY_TIMEOUT = S3SourceConfig.S3_SOURCE_QUEUE_VISIBILITY_TIMEOUT.key();

    /**
     * {@link  #SOURCE_INPUT_SELECTOR} source input selector.
     */
    @Deprecated
    public static final String SOURCE_INPUT_SELECTOR = DFSPathSelectorConfig.SOURCE_INPUT_SELECTOR.key();
  }
}
