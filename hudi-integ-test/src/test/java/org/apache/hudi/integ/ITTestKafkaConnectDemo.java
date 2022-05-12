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

package org.apache.hudi.integ;

import org.apache.hudi.common.util.collection.Pair;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled("This de-stablizes docker env in CI. Disable this due to the coverage gain is low.")
public class ITTestKafkaConnectDemo extends ITTestBase {

  private static final String SPARKDATASOURCE_BATCH1_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/sparkdatasource-batch1.commands";

  @BeforeEach
  public void setupAdhoc() throws Exception {
    executeCommandStringInDocker(ADHOC_1_CONTAINER, "sh -c '" + HOODIE_WS_ROOT + "/docker/demo/setup_demo_container.sh'", true);
    executeCommandStringInDocker(ADHOC_2_CONTAINER, "sh -c '" + HOODIE_WS_ROOT + "/docker/demo/setup_demo_container.sh'", true);
  }

  @Test
  public void testKafkaConnectDemo() throws Exception {
    ingestFirstBatch();
    testSparkDataSourceAfterFirstBatch();
  }

  private void ingestFirstBatch() throws Exception {
    executeCommandStringInDocker(KAFKA_BROKER_CONTAINER, "sh -c '" + HOODIE_WS_ROOT + "/docker/demo/kafkaproduce-batch1.sh'", true);
    executeCommandStringInDocker(KAFKA_CONNECT_CONTAINER, "sh -c '" + HOODIE_WS_ROOT + "/docker/demo/kafkaconnect-init.sh'", true);
  }

  private void testSparkDataSourceAfterFirstBatch() throws Exception {
    Pair<String, String> stdOutErrPair = executeSparkDataSourceCommand(SPARKDATASOURCE_BATCH1_COMMANDS, true);
    assertStdOutContains(stdOutErrPair, "|GOOG  |2018-08-31 09:59:00|6330  |1230.5   |1230.02 |");
  }
}
