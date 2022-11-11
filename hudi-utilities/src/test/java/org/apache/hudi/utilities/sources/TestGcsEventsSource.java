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

package org.apache.hudi.utilities.sources;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ReceivedMessage;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.sources.helpers.gcs.PubsubMessagesFetcher;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.apache.hudi.utilities.sources.helpers.gcs.GcsIngestionConfig.GOOGLE_PROJECT_ID;
import static org.apache.hudi.utilities.sources.helpers.gcs.GcsIngestionConfig.PUBSUB_SUBSCRIPTION_ID;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestGcsEventsSource extends UtilitiesTestBase {

  @Mock
  PubsubMessagesFetcher pubsubMessagesFetcher;

  protected FilebasedSchemaProvider schemaProvider;
  private TypedProperties props;

  private static final String CHECKPOINT_VALUE_ZERO = "0";

  @BeforeAll
  public static void beforeAll() throws Exception {
    UtilitiesTestBase.initTestServices();
  }

  @BeforeEach
  public void beforeEach() throws Exception {
    schemaProvider = new FilebasedSchemaProvider(Helpers.setupSchemaOnDFS(), jsc);
    MockitoAnnotations.initMocks(this);

    props = new TypedProperties();
    props.put(GOOGLE_PROJECT_ID, "dummy-project");
    props.put(PUBSUB_SUBSCRIPTION_ID, "dummy-subscription");
  }

  @Test
  public void shouldReturnEmptyOnNoMessages() {
    when(pubsubMessagesFetcher.fetchMessages()).thenReturn(Collections.emptyList());

    GcsEventsSource source = new GcsEventsSource(props, jsc, sparkSession, null,
            pubsubMessagesFetcher);

    Pair<Option<Dataset<Row>>, String> expected = Pair.of(Option.empty(), "0");
    Pair<Option<Dataset<Row>>, String> dataAndCheckpoint = source.fetchNextBatch(Option.of("0"), 100);

    assertEquals(expected, dataAndCheckpoint);
  }

  @Test
  public void shouldReturnDataOnValidMessages() {
    ReceivedMessage msg1 = fileCreateMessage("objectId-1", "{'data':{'bucket':'bucket-1'}}");
    ReceivedMessage msg2 = fileCreateMessage("objectId-2", "{'data':{'bucket':'bucket-2'}}");

    when(pubsubMessagesFetcher.fetchMessages()).thenReturn(Arrays.asList(msg1, msg2));

    GcsEventsSource source = new GcsEventsSource(props, jsc, sparkSession, null,
            pubsubMessagesFetcher);
    Pair<Option<Dataset<Row>>, String> dataAndCheckpoint = source.fetchNextBatch(Option.of("0"), 100);
    source.onCommit(dataAndCheckpoint.getRight());

    assertEquals(CHECKPOINT_VALUE_ZERO, dataAndCheckpoint.getRight());

    Dataset<Row> resultDs = dataAndCheckpoint.getLeft().get();
    List<Row> result = resultDs.collectAsList();

    assertBucket(result.get(0), "bucket-1");
    assertBucket(result.get(1), "bucket-2");

    verify(pubsubMessagesFetcher).fetchMessages();
  }

  @Test
  public void shouldFetchMessagesInBatches() {
    ReceivedMessage msg1 = fileCreateMessage("objectId-1", "{'data':{'bucket':'bucket-1'}}");
    ReceivedMessage msg2 = fileCreateMessage("objectId-2", "{'data':{'bucket':'bucket-2'}}");
    ReceivedMessage msg3 = fileCreateMessage("objectId-3", "{'data':{'bucket':'bucket-3'}}");
    ReceivedMessage msg4 = fileCreateMessage("objectId-4", "{'data':{'bucket':'bucket-4'}}");

    // dataFetcher should return only two messages each time it's called
    when(pubsubMessagesFetcher.fetchMessages())
            .thenReturn(Arrays.asList(msg1, msg2))
            .thenReturn(Arrays.asList(msg3, msg4));

    GcsEventsSource source = new GcsEventsSource(props, jsc, sparkSession, null,
            pubsubMessagesFetcher);
    Pair<Option<Dataset<Row>>, String> dataAndCheckpoint1 = source.fetchNextBatch(Option.of("0"), 100);
    source.onCommit(dataAndCheckpoint1.getRight());

    assertEquals(CHECKPOINT_VALUE_ZERO, dataAndCheckpoint1.getRight());
    List<Row> result1 = dataAndCheckpoint1.getLeft().get().collectAsList();
    assertBucket(result1.get(0), "bucket-1");
    assertBucket(result1.get(1), "bucket-2");

    Pair<Option<Dataset<Row>>, String> dataAndCheckpoint2 = source.fetchNextBatch(Option.of("0"), 100);
    source.onCommit(dataAndCheckpoint2.getRight());

    List<Row> result2 = dataAndCheckpoint2.getLeft().get().collectAsList();
    assertBucket(result2.get(0), "bucket-3");
    assertBucket(result2.get(1), "bucket-4");

    verify(pubsubMessagesFetcher, times(2)).fetchMessages();
  }

  @Test
  public void shouldSkipInvalidMessages1() {
    ReceivedMessage invalid1 = fileDeleteMessage("objectId-1", "{'data':{'bucket':'bucket-1'}}");
    ReceivedMessage invalid2 = fileCreateMessageWithOverwroteGen("objectId-2", "{'data':{'bucket':'bucket-2'}}");
    ReceivedMessage valid1 = fileCreateMessage("objectId-3", "{'data':{'bucket':'bucket-3'}}");

    when(pubsubMessagesFetcher.fetchMessages()).thenReturn(Arrays.asList(invalid1, valid1, invalid2));

    GcsEventsSource source = new GcsEventsSource(props, jsc, sparkSession, null,
            pubsubMessagesFetcher);
    Pair<Option<Dataset<Row>>, String> dataAndCheckpoint = source.fetchNextBatch(Option.of("0"), 100);
    source.onCommit(dataAndCheckpoint.getRight());
    assertEquals(CHECKPOINT_VALUE_ZERO, dataAndCheckpoint.getRight());

    Dataset<Row> resultDs = dataAndCheckpoint.getLeft().get();
    List<Row> result = resultDs.collectAsList();

    assertEquals(1, result.size());
    assertBucket(result.get(0), "bucket-3");

    verify(pubsubMessagesFetcher).fetchMessages();
  }

  @Test
  public void shouldGcsEventsSourceDoesNotDedupeInterally() {
    ReceivedMessage dupe1 = fileCreateMessage("objectId-1", "{'data':{'bucket':'bucket-1'}}");
    ReceivedMessage dupe2 = fileCreateMessage("objectId-1", "{'data':{'bucket':'bucket-1'}}");

    when(pubsubMessagesFetcher.fetchMessages()).thenReturn(Arrays.asList(dupe1, dupe2));

    GcsEventsSource source = new GcsEventsSource(props, jsc, sparkSession, null,
            pubsubMessagesFetcher);
    Pair<Option<Dataset<Row>>, String> dataAndCheckpoint = source.fetchNextBatch(Option.of("0"), 100);
    source.onCommit(dataAndCheckpoint.getRight());

    assertEquals(CHECKPOINT_VALUE_ZERO, dataAndCheckpoint.getRight());

    Dataset<Row> resultDs = dataAndCheckpoint.getLeft().get();
    List<Row> result = resultDs.collectAsList();
    assertEquals(2, result.size());
    assertBucket(result.get(0), "bucket-1");
    assertBucket(result.get(1), "bucket-1");

    verify(pubsubMessagesFetcher).fetchMessages();
  }

  private ReceivedMessage fileCreateMessageWithOverwroteGen(String objectId, String payload) {
    Map<String, String> attrs = new HashMap<>();
    attrs.put("overwroteGeneration", "objectId-N");

    return ReceivedMessage.newBuilder().setMessage(
            objectWithEventTypeAndAttrs(objectId, "OBJECT_FINALIZE", attrs, payload)
    ).setAckId(objectId).build();
  }

  private ReceivedMessage fileCreateMessage(String objectId, String payload) {
    return ReceivedMessage.newBuilder().setMessage(
            objectFinalizeMessage(objectId, payload)
    ).setAckId(objectId).build();
  }

  private ReceivedMessage fileDeleteMessage(String objectId, String payload) {
    return ReceivedMessage.newBuilder().setMessage(
            objectDeleteMessage(objectId, payload)
    ).setAckId(objectId).build();
  }

  private PubsubMessage.Builder objectFinalizeMessage(String objectId, String dataMessage) {
    return objectWithEventType(objectId, "OBJECT_FINALIZE", dataMessage);
  }

  private PubsubMessage.Builder objectDeleteMessage(String objectId, String dataMessage) {
    return objectWithEventType(objectId, "OBJECT_DELETE", dataMessage);
  }

  private PubsubMessage.Builder objectWithEventType(String objectId, String eventType, String dataMessage) {
    return messageWithAttrs(createBasicAttrs(objectId, eventType), dataMessage);
  }

  private PubsubMessage.Builder objectWithEventTypeAndAttrs(String objectId, String eventType,
                                                            Map<String, String> attrs, String dataMessage) {
    Map<String, String> allAttrs = createBasicAttrs(objectId, eventType);
    allAttrs.putAll(attrs);

    return messageWithAttrs(allAttrs, dataMessage);
  }

  private Map<String, String> createBasicAttrs(String objectId, String eventType) {
    Map<String, String> map = new HashMap<>();
    map.put("objectId", objectId);
    map.put("eventType", eventType);

    return map;
  }

  private PubsubMessage.Builder messageWithAttrs(Map<String, String> attrs, String dataMessage) {
    return PubsubMessage.newBuilder()
            .putAllAttributes(new HashMap<>(attrs))
            .setData(ByteString.copyFrom(dataMessage.getBytes()));
  }

  private void assertBucket(Row row, String expectedBucketName) {
    Row record = row.getAs("data");
    String bucket = record.getAs("bucket");
    assertEquals(expectedBucketName, bucket);
  }
}
