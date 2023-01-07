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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * S3 events metadata selector class. This class provides methods to process the
 * messages from SQS for {@link org.apache.hudi.utilities.sources.S3EventsSource}.
 */
public class S3EventsMetaSelector extends CloudObjectsSelector {

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
    String sourceSelectorClass =
        props.getString(
            S3EventsMetaSelector.Config.SOURCE_INPUT_SELECTOR,
            S3EventsMetaSelector.class.getName());
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
  protected List<Map<String, Object>> getValidEvents(AmazonSQS sqs, List<Message> processedMessages) throws IOException {
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

  private List<Map<String, Object>> processAndDeleteInvalidMessages(List<Message> processedMessages,
                                                                    List<Message> messages) throws IOException {
    List<Map<String, Object>> validEvents = new ArrayList<>();
    for (Message message : messages) {
      JSONObject messageBody = new JSONObject(message.getBody());
      Map<String, Object> messageMap;
      ObjectMapper mapper = new ObjectMapper();
      if (messageBody.has(SQS_MODEL_MESSAGE)) {
        // If this messages is from S3Event -> SNS -> SQS
        messageMap = (Map<String, Object>) mapper.readValue(messageBody.getString(SQS_MODEL_MESSAGE), Map.class);
      } else {
        // If this messages is from S3Event -> SQS
        messageMap = (Map<String, Object>) mapper.readValue(messageBody.toString(), Map.class);
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
            log.debug(String.format("This S3 event %s is not allowed, so ignoring it.", eventName));
          }
        }
      } else {
        log.debug(String.format("Message is not expected format or it's s3:TestEvent. Message: %s", message));
      }
      processedMessages.add(message);
    }
    return validEvents;
  }

  /**
   * Get the list of events from queue.
   *
   * @param lastCheckpointStr The last checkpoint instant string, empty if first run.
   * @return A pair of dataset of event records and the next checkpoint instant string.
   */
  public Pair<List<String>, String> getNextEventsFromQueue(AmazonSQS sqs,
                                                           Option<String> lastCheckpointStr,
                                                           List<Message> processedMessages) {
    processedMessages.clear();
    log.info("Reading messages....");
    try {
      log.info("Start Checkpoint : " + lastCheckpointStr);
      List<Map<String, Object>> eventRecords = getValidEvents(sqs, processedMessages);
      log.info("Number of valid events: " + eventRecords.size());
      List<String> filteredEventRecords = new ArrayList<>();
      long newCheckpointTime = eventRecords.stream()
          .mapToLong(eventRecord -> Date.from(Instant.from(
                  DateTimeFormatter.ISO_INSTANT.parse((String) eventRecord.get(S3_MODEL_EVENT_TIME))))
              .getTime()).max().orElse(lastCheckpointStr.map(Long::parseLong).orElse(0L));

      for (Map<String, Object> eventRecord : eventRecords) {
        filteredEventRecords.add(new ObjectMapper().writeValueAsString(eventRecord).replace("%3D", "=")
            .replace("%24", "$").replace("%A3", "£").replace("%23", "#").replace("%26", "&").replace("%3F", "?")
            .replace("%7E", "~").replace("%25", "%").replace("%2B", "+"));
      }
      // Return the old checkpoint if no messages to consume from queue.
      String newCheckpoint = newCheckpointTime == 0 ? lastCheckpointStr.orElse(null) : String.valueOf(newCheckpointTime);
      return new ImmutablePair<>(filteredEventRecords, newCheckpoint);
    } catch (JSONException | IOException e) {
      throw new HoodieException("Unable to read from SQS: ", e);
    }
  }
}
