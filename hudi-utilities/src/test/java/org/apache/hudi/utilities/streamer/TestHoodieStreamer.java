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

package org.apache.hudi.utilities.streamer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.config.TypedProperties;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

public class TestHoodieStreamer {
  @Test
  void testInitialiseCheckpoint() throws IOException {
    TypedProperties properties = mock(TypedProperties.class);
    Configuration mockConf = mock(Configuration.class);
    HoodieStreamer.Config cfg = new HoodieStreamer.Config();

    // Test case 1: Ignore checkpoint is set
    cfg.ignoreCheckpoint = Instant.now().toString();
    HoodieStreamer hoodieStreamer1 = new HoodieStreamer(cfg,properties,mockConf);
    assertNull(hoodieStreamer1.cfg.checkpoint);

    // Test case 2: Checkpoint is not null
    String existingCheckpoint = "checkpoint";
    cfg.ignoreCheckpoint = null;
    cfg.checkpoint = existingCheckpoint;
    HoodieStreamer hoodieStreamer2 = new HoodieStreamer(cfg,properties,mockConf);
    assertEquals(existingCheckpoint,hoodieStreamer2.cfg.checkpoint);

    // Test case 3: Checkpoint and provider both are null
    cfg.checkpoint = null;
    cfg.initialCheckpointProvider = null;
    HoodieStreamer hoodieStreamer3 = new HoodieStreamer(cfg,properties,mockConf);
    assertNull(hoodieStreamer3.cfg.checkpoint);
  }
}
