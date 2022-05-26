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

package org.apache.hudi.client.heartbeat;

import org.apache.hudi.common.testutils.HoodieCommonTestHarness;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link ReaderHeartbeat}.
 */
public class TestReaderHeartbeat extends HoodieCommonTestHarness {

  @BeforeEach
  public void init() throws IOException {
    initMetaClient();
  }

  @Test
  public void testIsHeartbeatExpired() {
    ReaderHeartbeat readerHeartbeat = ReaderHeartbeat.create(metaClient.getFs(), metaClient.getBasePath(), 1000L, 1);
    readerHeartbeat.start("100");
    readerHeartbeat.stop("100");

    readerHeartbeat.start("earliest");
    readerHeartbeat.start("101");
    readerHeartbeat.start("102");
    readerHeartbeat.start("103");

    ReaderHeartbeat.Heartbeats heartbeats = readerHeartbeat.getValidReaderHeartbeats();
    assertTrue(heartbeats.isConsumingFromEarliest());
    assertThat(heartbeats.getEarliestConsumingInstant().get(), is("101"));
  }
}
