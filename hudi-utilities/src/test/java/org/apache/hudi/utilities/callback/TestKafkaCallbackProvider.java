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

package org.apache.hudi.utilities.callback;

import org.apache.hudi.callback.HoodieWriteCommitCallback;
import org.apache.hudi.callback.common.HoodieWriteCommitCallbackMessage;
import org.apache.hudi.callback.util.HoodieCommitCallbackFactory;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.config.HoodieWriteCommitCallbackConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.callback.kafka.HoodieWriteCommitKafkaCallback;
import org.apache.hudi.utilities.callback.kafka.HoodieWriteCommitKafkaCallbackConfig;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.spark.streaming.kafka010.KafkaTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.apache.hudi.common.testutils.HoodieTestTable.makeNewCommitTime;
import static org.apache.hudi.common.testutils.HoodieTestUtils.generateFakeHoodieWriteStat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class TestKafkaCallbackProvider extends UtilitiesTestBase {
  private final String testTopicName = "hoodie_test_" + UUID.randomUUID();

  private KafkaTestUtils testUtils;

  @BeforeAll
  public static void initClass() throws Exception {
    UtilitiesTestBase.initTestServices();
  }

  @BeforeEach
  public void setup() {
    testUtils = new KafkaTestUtils();
    testUtils.setup();
  }

  @AfterEach
  public void tearDown() {
    testUtils.teardown();
  }

  @AfterAll
  public static void cleanupClass() throws IOException {
    UtilitiesTestBase.cleanUpUtilitiesTestServices();
  }

  @Test
  public void testCallbackMessage() {
    testUtils.createTopic(testTopicName, 2);

    HoodieWriteConfig hoodieConfig = createConfigForKafkaCallback();
    HoodieWriteCommitCallback commitCallback = HoodieCommitCallbackFactory.create(hoodieConfig);

    List<HoodieWriteStat> stats = generateFakeHoodieWriteStat(1);

    assertDoesNotThrow(() -> commitCallback.call(new HoodieWriteCommitCallbackMessage(makeNewCommitTime(), hoodieConfig.getTableName(), hoodieConfig.getBasePath(), stats)));
  }

  private HoodieWriteConfig createConfigForKafkaCallback() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieWriteCommitKafkaCallbackConfig.TOPIC.key(), testTopicName);
    props.setProperty(HoodieWriteCommitKafkaCallbackConfig.BOOTSTRAP_SERVERS.key(), testUtils.brokerAddress());

    HoodieWriteConfig hoodieWriteConfig = HoodieWriteConfig.newBuilder()
            .withCallbackConfig(
                    HoodieWriteCommitCallbackConfig.newBuilder()
                            .writeCommitCallbackOn("true")
                            .withCallbackClass(HoodieWriteCommitKafkaCallback.class.getName())
                            .fromProperties(props)
                            .build())
            .withPath("/tmp")
            .forTable("test-trip-table")
            .build(false);
    return hoodieWriteConfig;
  }
}
