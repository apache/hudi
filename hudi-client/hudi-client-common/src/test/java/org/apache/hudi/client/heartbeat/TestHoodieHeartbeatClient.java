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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieHeartbeatClient extends HoodieCommonTestHarness {

  private static String instantTime1 = "100";
  private static String instantTime2 = "101";
  private static Long heartBeatInterval = 1000L;
  private static int numTolerableMisses = 1;

  @BeforeEach
  public void init() throws IOException {
    initMetaClient();
  }

  @Test
  public void testStartHeartbeat() throws IOException {
    HoodieHeartbeatClient hoodieHeartbeatClient =
        new HoodieHeartbeatClient(metaClient.getFs(), metaClient.getBasePath(), heartBeatInterval, numTolerableMisses);
    hoodieHeartbeatClient.start(instantTime1);
    FileStatus [] fs = metaClient.getFs().listStatus(new Path(hoodieHeartbeatClient.getHeartbeatFolderPath()));
    assertTrue(fs.length == 1);
    assertTrue(fs[0].getPath().toString().contains(instantTime1));
  }

  @Test
  public void testStopHeartbeat() {
    HoodieHeartbeatClient hoodieHeartbeatClient =
        new HoodieHeartbeatClient(metaClient.getFs(), metaClient.getBasePath(), heartBeatInterval, numTolerableMisses);
    hoodieHeartbeatClient.start(instantTime1);
    hoodieHeartbeatClient.stop(instantTime1);
    await().atMost(5, SECONDS).until(() -> hoodieHeartbeatClient.getHeartbeat(instantTime1).getNumHeartbeats() > 0);
    Integer numHeartBeats = hoodieHeartbeatClient.getHeartbeat(instantTime1).getNumHeartbeats();
    assertTrue(numHeartBeats == 1);
  }

  @Test
  public void testIsHeartbeatExpired() throws IOException {
    HoodieHeartbeatClient hoodieHeartbeatClient =
        new HoodieHeartbeatClient(metaClient.getFs(), metaClient.getBasePath(), heartBeatInterval, numTolerableMisses);
    hoodieHeartbeatClient.start(instantTime1);
    hoodieHeartbeatClient.stop(instantTime1);
    assertFalse(hoodieHeartbeatClient.isHeartbeatExpired(instantTime1));
  }

  @Test
  public void testNumHeartbeatsGenerated() {
    Long heartBeatInterval = 5000L;
    HoodieHeartbeatClient hoodieHeartbeatClient =
        new HoodieHeartbeatClient(metaClient.getFs(), metaClient.getBasePath(), heartBeatInterval, numTolerableMisses);
    hoodieHeartbeatClient.start("100");
    await().atMost(5, SECONDS).until(() -> hoodieHeartbeatClient.getHeartbeat(instantTime1).getNumHeartbeats() >= 1);
  }

  @Test
  public void testDeleteWrongHeartbeat() throws IOException {
    HoodieHeartbeatClient hoodieHeartbeatClient =
        new HoodieHeartbeatClient(metaClient.getFs(), metaClient.getBasePath(), heartBeatInterval, numTolerableMisses);
    hoodieHeartbeatClient.start(instantTime1);
    hoodieHeartbeatClient.stop(instantTime1);
    assertFalse(HeartbeatUtils.deleteHeartbeatFile(metaClient.getFs(), basePath, instantTime2));
  }
}
