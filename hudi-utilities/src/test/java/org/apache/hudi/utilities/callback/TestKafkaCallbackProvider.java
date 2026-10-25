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

import org.apache.hudi.callback.HoodieCommitCallbackFactory;
import org.apache.hudi.callback.HoodieWriteCommitCallback;
import org.apache.hudi.callback.common.HoodieWriteCommitCallbackMessage;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.config.HoodieWriteCommitCallbackConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.callback.kafka.HoodieWriteCommitKafkaCallback;
import org.apache.hudi.utilities.callback.kafka.HoodieWriteCommitKafkaCallbackConfig;
import org.apache.hudi.utilities.testutils.KafkaTestUtils;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.testutils.HoodieTestTable.makeNewCommitTime;
import static org.apache.hudi.common.testutils.HoodieTestUtils.generateFakeHoodieWriteStat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestKafkaCallbackProvider extends UtilitiesTestBase {
  private static final long POLL_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(60);

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
    int numPartitions = 2;
    testUtils.createTopic(testTopicName, numPartitions);

    List<HoodieWriteStat> stats = generateFakeHoodieWriteStat(1);

    // without a partition config the message is routed by hashing the table name key
    HoodieWriteConfig defaultRoutedConfig = createConfigForKafkaCallback(null);
    HoodieWriteCommitCallback defaultRoutedCallback = HoodieCommitCallbackFactory.create(defaultRoutedConfig);
    String defaultRoutedCommitTime = makeNewCommitTime();
    defaultRoutedCallback.call(new HoodieWriteCommitCallbackMessage(
        defaultRoutedCommitTime, defaultRoutedConfig.getTableName(), defaultRoutedConfig.getBasePath(), stats));

    // an explicit partition config overrides the key hashing
    HoodieWriteConfig pinnedConfig = createConfigForKafkaCallback("1");
    HoodieWriteCommitCallback pinnedCallback = HoodieCommitCallbackFactory.create(pinnedConfig);
    String pinnedCommitTime = makeNewCommitTime(Instant.now().plusSeconds(1));
    pinnedCallback.call(new HoodieWriteCommitCallbackMessage(
        pinnedCommitTime, pinnedConfig.getTableName(), pinnedConfig.getBasePath(), stats));

    // call() swallows send failures, so consuming the topic is the only proof the sends went through;
    // hashing the table name key routes to partition 0, so partition 1 can only come from the config
    List<ConsumerRecord<String, String>> consumed = consumeCallbackMessages(numPartitions, 2);
    assertEquals(Arrays.asList(0, 1),
        consumed.stream().map(ConsumerRecord::partition).sorted().collect(Collectors.toList()));
    Map<Integer, String> expectedCommitTimeByPartition = new HashMap<>();
    expectedCommitTimeByPartition.put(0, defaultRoutedCommitTime);
    expectedCommitTimeByPartition.put(1, pinnedCommitTime);
    for (ConsumerRecord<String, String> record : consumed) {
      assertTrue(record.value().contains(expectedCommitTimeByPartition.get(record.partition())),
          () -> "unexpected callback message on partition " + record.partition() + ": " + record.value());
    }
  }

  private List<ConsumerRecord<String, String>> consumeCallbackMessages(int numPartitions, int expectedCount) {
    Properties consumerProps = new Properties();
    consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, testUtils.brokerAddress());
    consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-kafka-callback-" + UUID.randomUUID());
    consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    List<ConsumerRecord<String, String>> records = new ArrayList<>();
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
      List<TopicPartition> partitions = IntStream.range(0, numPartitions)
          .mapToObj(partition -> new TopicPartition(testTopicName, partition))
          .collect(Collectors.toList());
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);
      long deadline = System.currentTimeMillis() + POLL_TIMEOUT_MS;
      while (records.size() < expectedCount && System.currentTimeMillis() < deadline) {
        consumer.poll(Duration.ofSeconds(1)).forEach(records::add);
      }
    }
    return records;
  }

  private HoodieWriteConfig createConfigForKafkaCallback(String partition) {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieWriteCommitKafkaCallbackConfig.TOPIC.key(), testTopicName);
    props.setProperty(HoodieWriteCommitKafkaCallbackConfig.BOOTSTRAP_SERVERS.key(), testUtils.brokerAddress());
    if (partition != null) {
      props.setProperty(HoodieWriteCommitKafkaCallbackConfig.PARTITION.key(), partition);
    }

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
