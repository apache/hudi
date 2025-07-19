/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.configuration;

import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.util.ClientIds;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test cases for {@link OptionsInference}.
 */
public class TestOptionsInference {
  @TempDir
  File tempFile;

  @Test
  void testSetupClientId() throws Exception {
    Configuration conf = getConf();
    conf.set(FlinkOptions.WRITE_CLIENT_ID, "2");
    OptionsInference.setupClientId(conf);
    assertThat("Explicit client id has higher priority",
        conf.get(FlinkOptions.WRITE_CLIENT_ID), is("2"));

    for (int i = 0; i < 3; i++) {
      conf = getConf();
      try (ClientIds clientIds = ClientIds.builder().conf(conf).build()) {
        OptionsInference.setupClientId(conf);
        String expectedId = i == 0 ? ClientIds.INIT_CLIENT_ID : i + "";
        assertThat("The client id should auto inc to " + expectedId,
            conf.get(FlinkOptions.WRITE_CLIENT_ID), is(expectedId));
      }
    }

    // sleep 1 second to simulate a zombie heartbeat
    Thread.sleep(1000);
    conf = getConf();
    try (ClientIds clientIds = ClientIds.builder()
        .conf(conf)
        .heartbeatIntervalInMs(10) // max 10 milliseconds tolerable heartbeat timeout
        .numTolerableHeartbeatMisses(1). build()) {
      String nextId = clientIds.nextId(conf);
      assertThat("The inactive client id should be reused",
          nextId, is(""));
    }
  }

  private Configuration getConf() {
    Configuration conf = new Configuration();
    conf.setString(HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(), WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.name());
    conf.set(FlinkOptions.PATH, tempFile.getAbsolutePath());
    return conf;
  }
}
