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
 * Cloud Objects Selector Class. This class has methods for processing cloud objects. It currently
 * supports only AWS S3 objects and AWS SQS queue.
 */
public class CloudObjectsSelector {
  public static final List<String> ALLOWED_S3_EVENT_PREFIX =
      Collections.singletonList("ObjectCreated");
  public static volatile Logger log = LogManager.getLogger(CloudObjectsSelector.class);
  public final String queueUrl;
  public final int longPollWait;
  public final int maxMessagesEachRequest;
  public final int maxMessageEachBatch;
  public final int visibilityTimeout;
  public final TypedProperties props;
  public final String fsName;
  private final String regionName;

  /**
   * Cloud Objects Selector Class. {@link CloudObjectsSelector}
   */
  public CloudObjectsSelector(TypedProperties props) {
    DataSourceUtils.checkRequiredProperties(props, Arrays.asList(Config.QUEUE_URL_PROP, Config.QUEUE_REGION));
    this.props = props;
    this.queueUrl = props.getString(Config.QUEUE_URL_PROP);
    this.regionName = props.getString(Config.QUEUE_REGION);
    this.fsName = props.getString(Config.SOURCE_QUEUE_FS_PROP, "s3").toLowerCase();
    this.longPollWait = props.getInteger(Config.QUEUE_LONGPOLLWAIT_PROP, 20);
    this.maxMessageEachBatch = props.getInteger(Config.QUEUE_MAXMESSAGESEACHBATCH_PROP, 5);
    this.visibilityTimeout = props.getInteger(Config.QUEUE_VISIBILITYTIMEOUT_PROP, 30);
    this.maxMessagesEachRequest = 10;
  }

  /**
   * Get SQS queue attributes.
   *
   * @param sqsClient AWSClient for sqsClient
   * @param queueUrl  queue full url
   * @return map of attributes needed
   */
  protected Map<String, String> getSqsQueueAttributes(AmazonSQS sqsClient, String queueUrl) {
    GetQueueAttributesResult queueAttributesResult =
        sqsClient.getQueueAttributes(
            new GetQueueAttributesRequest(queueUrl)
                .withAttributeNames("ApproximateNumberOfMessages"));
    return queueAttributesResult.getAttributes();
  }

  /**
   * Get the file attributes filePath, eventTime and size from JSONObject record.
   *
   * @param record of object event
   * @return map of file attribute
   */
  protected Map<String, Object> getFileAttributesFromRecord(JSONObject record)
      throws UnsupportedEncodingException {

    Map<String, Object> fileRecord = new HashMap<>();
    String eventTimeStr = record.getString("eventTime");
    long eventTime =
        Date.from(Instant.from(DateTimeFormatter.ISO_INSTANT.parse(eventTimeStr))).getTime();

    JSONObject s3Object = record.getJSONObject("s3").getJSONObject("object");
    String bucket =
        URLDecoder.decode(
            record.getJSONObject("s3").getJSONObject("bucket").getString("name"), "UTF-8");
    String key = URLDecoder.decode(s3Object.getString("key"), "UTF-8");
    String filePath = this.fsName + "://" + bucket + "/" + key;

    fileRecord.put("eventTime", eventTime);
    fileRecord.put("fileSize", s3Object.getLong("size"));
    fileRecord.put("filePath", filePath);
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
      ReceiveMessageRequest receiveMessageRequest,
      int maxMessageEachBatch,
      int maxMessagesEachRequest) {
    List<Message> messagesToProcess = new ArrayList<>();

    // Get count for available messages
    Map<String, String> queueAttributesResult = getSqsQueueAttributes(sqsClient, queueUrl);
    long approxMessagesAvailable =
        Long.parseLong(queueAttributesResult.get("ApproximateNumberOfMessages"));
    log.info("Approx. " + approxMessagesAvailable + " messages available in queue.");

    for (int i = 0;
         i < (int) Math.ceil((double) approxMessagesAvailable / maxMessagesEachRequest);
         ++i) {
      List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).getMessages();
      log.debug("Messages size: " + messages.size());

      for (Message message : messages) {
        log.debug("message id: " + message.getMessageId());
        messagesToProcess.add(message);
      }
      log.debug("total fetched messages size: " + messagesToProcess.size());
      if (messages.isEmpty() || (messagesToProcess.size() >= maxMessageEachBatch)) {
        break;
      }
    }
    return messagesToProcess;
  }

  /**
   * create partitions of list using specific batch size. we can't use third party API for this
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
   * delete batch of messages from queue.
   */
  protected void deleteBatchOfMessages(
      AmazonSQS sqs, String queueUrl, List<Message> messagesToBeDeleted) {
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
    System.out.println("Delete is" + deleteFailures.isEmpty() + "or ignoring it.");
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
  public void onCommitDeleteProcessedMessages(
      AmazonSQS sqs, String queueUrl, List<Message> processedMessages) {

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
     * {@value #QUEUE_URL_PROP} is the queue url for cloud object events.
     */
    public static final String QUEUE_URL_PROP = "hoodie.deltastreamer.source.queue.url";

    /**
     * {@value #QUEUE_REGION} is the case-sensitive region name of the cloud provider for the queue. For example, "us-east-1".
     */
    public static final String QUEUE_REGION = "hoodie.deltastreamer.source.queue.region";

    /**
     * {@value #SOURCE_QUEUE_FS_PROP} is file system corresponding to queue. For example, for AWS SQS it is s3/s3a.
     */
    public static final String SOURCE_QUEUE_FS_PROP = "hoodie.deltastreamer.source.queue.fs";

    /**
     * {@value #QUEUE_LONGPOLLWAIT_PROP} is the long poll wait time in seconds If set as 0 then
     * client will fetch on short poll basis.
     */
    public static final String QUEUE_LONGPOLLWAIT_PROP =
        "hoodie.deltastreamer.source.queue.longpoll.wait";

    /**
     * {@value #QUEUE_MAXMESSAGESEACHBATCH_PROP} is max messages for each batch of delta streamer
     * run. Source will process these maximum number of message at a time.
     */
    public static final String QUEUE_MAXMESSAGESEACHBATCH_PROP =
        "hoodie.deltastreamer.source.queue.max.messages.eachbatch";

    /**
     * {@value #QUEUE_VISIBILITYTIMEOUT_PROP} is visibility timeout for messages in queue. After we
     * consume the message, queue will move the consumed messages to in-flight state, these messages
     * can't be consumed again by source for this timeout period.
     */
    public static final String QUEUE_VISIBILITYTIMEOUT_PROP =
        "hoodie.deltastreamer.source.queue.visibility.timeout";

    /**
     * {@value #SOURCE_INPUT_SELECTOR} source input selector.
     */
    public static final String SOURCE_INPUT_SELECTOR = "hoodie.deltastreamer.source.input.selector";
  }
}
