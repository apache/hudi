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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.hudi.utilities.testutils.CloudObjectTestUtils;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import org.apache.hadoop.fs.Path;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector.Config.QUEUE_REGION;
import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector.Config.QUEUE_URL_PROP;
import static org.apache.hudi.utilities.sources.helpers.TestCloudObjectsSelector.REGION_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestCloudObjectsMetaSelector extends HoodieClientTestHarness {

  TypedProperties props;
  String sqsUrl;

  @Mock
  AmazonSQS sqs;

  @Mock
  private CloudObjectsMetaSelector cloudObjectsMetaSelector;

  @BeforeEach
  void setUp() {
    initSparkContexts();
    initPath();
    initFileSystem();
    MockitoAnnotations.initMocks(this);

    props = new TypedProperties();
    sqsUrl = "test-queue";
    props.setProperty(QUEUE_URL_PROP, sqsUrl);
    props.setProperty(QUEUE_REGION, REGION_NAME);
  }

  @AfterEach
  public void teardown() throws Exception {
    Mockito.reset(cloudObjectsMetaSelector);
    cleanupResources();
  }

  @ParameterizedTest
  @ValueSource(classes = {CloudObjectsMetaSelector.class})
  public void testNextEventsFromQueueShouldReturnsEventsFromQueue(Class<?> clazz) {

    CloudObjectsMetaSelector selector =
        (CloudObjectsMetaSelector) ReflectionUtils.loadClass(clazz.getName(), props);

    // setup s3 record
    String bucket = "test-bucket";
    String key = "part-foo-bar.snappy.parquet";
    Path path = new Path(bucket, key);
    CloudObjectTestUtils.setMessagesInQueue(sqs, path);

    List<Message> processed = new ArrayList<>();

    // test the return values
    Pair<List<String>, String> eventFromQueue =
        selector.getNextEventsFromQueue(sqs, jsc, Option.empty(), processed);

    assertEquals(1, eventFromQueue.getLeft().size());
    assertEquals(1, processed.size());
    assertEquals(
        key,
        new JSONObject(eventFromQueue.getLeft().get(0))
            .getJSONObject("s3")
            .getJSONObject("object")
            .getString("key"));
    assertEquals("1627376736755", eventFromQueue.getRight());
  }
}
