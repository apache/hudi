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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.config.DFSPathSelectorConfig;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.utils.http.SdkHttpUtils;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;

/**
 * S3 events metadata selector class. This class provides methods to process the
 * messages from SQS for {@link org.apache.hudi.utilities.sources.S3EventsSource}.
 */
public class S3EventsMetaSelector extends CloudObjectsSelector {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String S3_EVENT_RESPONSE_ELEMENTS = "responseElements";

  /**
   * Cloud Objects Meta Selector Class. {@link CloudObjectsSelector}
   */
  public S3EventsMetaSelector(TypedProperties props) {
    super(props);
  }

  /**
   * Factory method for creating custom CloudObjectsMetaSelector. Default selector to use is {@link
   * S3EventsMetaSelector}
   */
  public static S3EventsMetaSelector createSourceSelector(TypedProperties props) {
    String sourceSelectorClass = getStringWithAltKeys(
        props, DFSPathSelectorConfig.SOURCE_INPUT_SELECTOR, S3EventsMetaSelector.class.getName());
    try {
      S3EventsMetaSelector selector =
          (S3EventsMetaSelector)
              ReflectionUtils.loadClass(
                  sourceSelectorClass, new Class<?>[] {TypedProperties.class}, props);

      log.info("Using path selector " + selector.getClass().getName());
      return selector;
    } catch (Exception e) {
      throw new HoodieException("Could not load source selector class " + sourceSelectorClass, e);
    }
  }

  /**
   * List messages from queue, filter out illegible events while doing so. It will also delete the
   * ineligible messages from queue.
   *
   * @param processedMessages array of processed messages to add more messages
   * @return the filtered list of valid S3 events in SQS.
   */
  protected List<Map<String, Object>> getValidEvents(SqsClient sqs, List<MessageTracker> processedMessages) throws IOException {
    List<Message> messages =
        getMessagesToProcess(
            sqs,
            this.queueUrl,
            this.longPollWait,
            this.visibilityTimeout,
            this.maxMessagePerBatch,
            this.maxMessagesPerRequest);
    return processAndDeleteInvalidMessages(processedMessages, messages);
  }

  private List<Map<String, Object>> processAndDeleteInvalidMessages(List<MessageTracker> processedMessages,
                                                                    List<Message> messages) throws IOException {
    List<Map<String, Object>> validEvents = new ArrayList<>();
    int skippedMsgCount = 0;
    for (Message message : messages) {
      Map<String, Object> messageMap = MAPPER.readValue(message.body(), Map.class);
      if (messageMap.containsKey(SQS_MODEL_MESSAGE)) {
        // If this messages is from S3Event -> SNS -> SQS
        messageMap = (Map<String, Object>) MAPPER.readValue(messageMap.get(SQS_MODEL_MESSAGE).toString(), Map.class);
      }
      if (messageMap.containsKey(SQS_MODEL_EVENT_RECORDS)) {
        List<Map<String, Object>> events = (List<Map<String, Object>>) messageMap.get(SQS_MODEL_EVENT_RECORDS);
        for (Map<String, Object> event : events) {
          event.remove(S3_EVENT_RESPONSE_ELEMENTS);
          String eventName = (String) event.get(SQS_MODEL_EVENT_NAME);
          // filter only allowed s3 event types
          if (ALLOWED_S3_EVENT_PREFIX.stream().anyMatch(eventName::startsWith)) {
            validEvents.add(event);
          } else {
            log.debug("This S3 event {} is not allowed, so ignoring it.", eventName);
            skippedMsgCount++;
          }
        }
      } else {
        log.debug("Message is not expected format or it's s3:TestEvent. Message: {}", message);
        skippedMsgCount++;
      }
      processedMessages.add(new MessageTracker(message));
    }
    log.info("Messages received: {}, toBeProcessed: {}, skipped: {}", messages.size(), validEvents.size(), skippedMsgCount);
    return validEvents;
  }

  /**
   * Get the list of events from queue.
   *
   * @param lastCheckpoint The last checkpoint instant string, empty if first run.
   * @return A pair of dataset of event records and the next checkpoint instant string.
   */
  public Pair<List<String>, Checkpoint> getNextEventsFromQueue(SqsClient sqs,
                                                               Option<Checkpoint> lastCheckpoint,
                                                               List<MessageTracker> processedMessages) {
    processedMessages.clear();
    log.info("Reading messages....");
    try {
      log.info("Start Checkpoint : {}", lastCheckpoint);
      List<Map<String, Object>> eventRecords = getValidEvents(sqs, processedMessages);
      log.info("Number of valid events: {}", eventRecords.size());
      List<String> filteredEventRecords = new ArrayList<>();
      long newCheckpointTime = eventRecords.stream()
          .mapToLong(eventRecord -> Date.from(Instant.from(
                  DateTimeFormatter.ISO_INSTANT.parse((String) eventRecord.get(S3_MODEL_EVENT_TIME))))
              .getTime()).max().orElse(lastCheckpoint.map(e -> Long.parseLong(e.getCheckpointKey())).orElse(0L));

      for (Map<String, Object> eventRecord : eventRecords) {
        filteredEventRecords.add(SdkHttpUtils.urlDecode(MAPPER.writeValueAsString(eventRecord)));
      }
      // Return the old checkpoint if no messages to consume from queue.
      Checkpoint newCheckpoint = newCheckpointTime == 0 ? lastCheckpoint.orElse(null) : new StreamerCheckpointV2(String.valueOf(newCheckpointTime));
      return new ImmutablePair<>(filteredEventRecords, newCheckpoint);
    } catch (JSONException | IOException e) {
      throw new HoodieException("Unable to read from SQS: ", e);
    }
  }
}
