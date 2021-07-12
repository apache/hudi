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

package com.infinilake.sources.helper;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class CloudObjectsDfsSelector implements Serializable {

    protected static volatile Logger log = LogManager.getLogger(CloudObjectsDfsSelector.class);
    public static HashMap<String, Boolean> map = new HashMap<>();

    /**
     * Configs supported.
     */
    public static class Config {

        public static final String SQS_QUEUE_URL_PROP = "hoodie.deltastreamer.source.sqs.queueurl";
        public static final String SQS_QUEUE_LONGPOLLWAIT_PROP = "hoodie.deltastreamer.source.sqs.long_poll_wait";
        public static final String SQS_QUEUE_MAXMessages_PROP = "hoodie.deltastreamer.source.sqs.max_message_each_request";
        public static final String SOURCE_INPUT_SELECTOR = "hoodie.deltastreamer.source.input.selector";
    }

    protected static final List<String> IGNORE_FILEPREFIX_LIST = Arrays.asList(".", "_");
    protected static final List<String> ALLOWED_S3_EVENT_PREFIX = Arrays.asList("ObjectCreated");

    //    protected final transient FileSystem fs;
    protected final TypedProperties props;
    protected final String queueUrl;
    protected final AmazonSQS sqs;
    protected final int longPollWait;
    protected final int MaxNumberOfMessages;

    public CloudObjectsDfsSelector(TypedProperties props, Configuration hadoopConf) {
        DataSourceUtils.checkRequiredProperties(props, Collections.singletonList(Config.SQS_QUEUE_URL_PROP));
        this.props = props;
        this.queueUrl = props.getString(Config.SQS_QUEUE_URL_PROP);
        this.longPollWait = props.getInteger(Config.SQS_QUEUE_LONGPOLLWAIT_PROP);
        this.MaxNumberOfMessages = props.getInteger(Config.SQS_QUEUE_MAXMessages_PROP);

        // ToDO - Update it for handing AWS Client creation but not using default only.
        this.sqs = AmazonSQSClientBuilder.defaultClient();
    }

    /**
     * Factory method for creating custom CloudObjectsDfsSelector. Default selector
     * to use is {@link CloudObjectsDfsSelector}
     */
    public static CloudObjectsDfsSelector createSourceSelector(TypedProperties props,
                                                               Configuration conf) {
        String sourceSelectorClass = props.getString(CloudObjectsDfsSelector.Config.SOURCE_INPUT_SELECTOR,
                CloudObjectsDfsSelector.class.getName());
        try {
            CloudObjectsDfsSelector selector = (CloudObjectsDfsSelector) ReflectionUtils.loadClass(sourceSelectorClass,
                    new Class<?>[]{TypedProperties.class, Configuration.class},
                    props, conf);

            log.info("Using path selector " + selector.getClass().getName());
            return selector;
        } catch (Exception e) {
            throw new HoodieException("Could not load source selector class " + sourceSelectorClass, e);
        }
    }

    /**
     * Get the messages from queue from last checkpoint.
     *
     * @param sourceLimit max bytes to read each time
     * @return the list of files concatenated and their latest modified time
     */
    public Pair<Option<String>, String> getNextFilePathsFromQueue(Option<String> lastCheckpointStr, long sourceLimit) {
        System.out.println("Reading messages....");

//        String queueUrl = sqs.getQueueUrl(this.queueUrl).getQueueUrl();

        try {
            System.out.println("lastCheckpointStr" + lastCheckpointStr);

            long lastCheckpointTime = lastCheckpointStr.map(Long::parseLong).orElse(Long.MIN_VALUE);

            System.out.println(lastCheckpointTime);

            List<JSONObject> eligibleFiles = listFilesAfterCheckpoint(this.sqs, this.queueUrl, lastCheckpointTime);
            System.out.println("eligibleFiles size: " + eligibleFiles.size());

            // sort all files by event time.
            eligibleFiles.sort(Comparator.comparingLong(record -> record.getLong("eventTimeLong")));

            List<String> filteredFiles = new ArrayList<>();
            long currentBytes = 0;
            long newCheckpointTime = lastCheckpointTime;

            for (JSONObject record : eligibleFiles) {

                long eventTime = record.getLong("eventTimeLong");
                JSONObject s3Object = record.getJSONObject("s3").getJSONObject("object");
                String bucket = URLDecoder.decode(record.getJSONObject("s3").getJSONObject("bucket").getString("name"), "UTF-8");
                String key = URLDecoder.decode(s3Object.getString("key"), "UTF-8");
                String filePath = "s3://" + bucket + "/" + key;

                String fileName = StringUtils.substringAfterLast(key, "/");
//                System.out.println(eventTime);

                // toDO - move this check to listFilesAfterCheckpoint function
                if (IGNORE_FILEPREFIX_LIST.stream().noneMatch(fileName::startsWith)) {
                    long fileSize = s3Object.getLong("size");
//                    System.out.println(filePath);
                    // we will fetch all files from queue until
                    // we reach the max byte limit for batch
                    if ((currentBytes + fileSize) >= sourceLimit && eventTime > newCheckpointTime) {
                        break;
                    }
//                    map.put(filePath, Boolean.TRUE);
//                        System.out.println(map);
                    newCheckpointTime = eventTime;
                    currentBytes += fileSize;
                    filteredFiles.add(filePath);
                } else {
                    System.out.println("Ignoring the fle: " + filePath);
                }
            }
            if (filteredFiles.isEmpty()) {
                return new ImmutablePair<>(Option.empty(), String.valueOf(newCheckpointTime));
            }
            String pathStr = filteredFiles.stream().collect(Collectors.joining(","));
            return new ImmutablePair<>(Option.ofNullable(pathStr), String.valueOf(newCheckpointTime));
        } catch (JSONException | UnsupportedEncodingException e) {
            e.printStackTrace();
            throw new HoodieException("Unable to read from SQS: ", e);
        }

    }

    /**
     * List messages from queue, filter out illegible files/directories while doing so.
     */
    protected List<JSONObject> listFilesAfterCheckpoint(AmazonSQS sqs, String queueUrl, long lastCheckpointTime) {

        List<JSONObject> result = new ArrayList<>();

        // toDO - set larger visibility timeout
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                            .withQueueUrl(queueUrl)
                            .withWaitTimeSeconds(this.longPollWait);
        receiveMessageRequest.setMaxNumberOfMessages(this.MaxNumberOfMessages);

        while (true) {
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            System.out.println(messages.isEmpty());

            // we need to all fetch message to sort and
            // then compare with checkpoint.
            // queue provides very unordered messages
            // toDo - add message fetch limit
            if (messages.isEmpty()) {
                break;
            }

            DeleteMessageBatchRequest deleteBatchReq = new DeleteMessageBatchRequest().withQueueUrl(queueUrl);
            List<DeleteMessageBatchRequestEntry> deleteEntries = deleteBatchReq.getEntries();

            for (Message message : messages) {
                System.out.println(message.getMessageId());
                boolean isMessageDelete = Boolean.TRUE;

                JSONObject messageBody = new JSONObject(message.getBody());
                if (messageBody.has("Message")) {
                    messageBody = new JSONObject(StringEscapeUtils.unescapeJava(
                            messageBody.getString("Message")));
                }
                if (messageBody.has("Records")) {
                    JSONArray records = messageBody.getJSONArray("Records");
                    for (int j = 0; j < records.length(); ++j) {
                        JSONObject record = records.getJSONObject(j);
                        String eventTimeStr = record.getString("eventTime");
                        System.out.println(eventTimeStr);
                        String eventName = record.getString("eventName");
                        long eventTime = Date.from(Instant.from(DateTimeFormatter.ISO_INSTANT.parse(eventTimeStr))).getTime();

//                        System.out.println(map);
                        if (ALLOWED_S3_EVENT_PREFIX.stream()
                                .anyMatch(eventName::startsWith)) {
                            if (eventTime > lastCheckpointTime) {
                                record.put("eventTimeLong", eventTime);

                                // toDo - use FileStatus Class for storing file information

                                result.add(record);
                                isMessageDelete = Boolean.FALSE;
                            }
                        } else {
                            System.out.println("This S3 event " + eventName + " is not allowed, so ignoring it.");
                        }
                    }
                } else {
                    System.out.println("Message is not expected format or it's s3:TestEvent");
                }
                if (isMessageDelete) {
                    deleteEntries.add(new DeleteMessageBatchRequestEntry()
                            .withId(message.getMessageId())
                            .withReceiptHandle(message.getReceiptHandle()));
//                    sqs.deleteMessage(queueUrl, message.getReceiptHandle());
                }
            }
            if (!deleteEntries.isEmpty()) {
                DeleteMessageBatchResult deleteResult = sqs.deleteMessageBatch(deleteBatchReq);
                List<String> deleteFailures = deleteResult.getFailed()
                        .stream()
                        .map(BatchResultErrorEntry::getId)
                        .collect(Collectors.toList());
                if (!deleteFailures.isEmpty()) {
                    System.out.println("Failed to delete " + deleteFailures.size()
                            + " messages out of " + deleteEntries.size()
                            + " from queue.");
                } else {
                    System.out.println("Successfully deleted "+ deleteEntries.size() + " messages from queue.");
                }
            }
        }
        return result;
    }

}