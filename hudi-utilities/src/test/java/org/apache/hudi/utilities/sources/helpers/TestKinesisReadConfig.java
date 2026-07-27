/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.hudi.utilities.config.KinesisSourceConfig;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Config-plumbing tests for the cross-account Kinesis role ARN: the config-key contract and the ARN
 * surviving transport to Spark executors via the serializable {@link KinesisReadConfig}.
 */
class TestKinesisReadConfig {

  private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/kinesis-cross-account-read";

  @Test
  void roleArnKeyMatchesExpectedContract() {
    assertEquals("hoodie.streamer.source.kinesis.role.arn",
        KinesisSourceConfig.KINESIS_ROLE_ARN.key());
    // No default: an absent key must read as null so the client falls back to the default chain.
    assertFalse(KinesisSourceConfig.KINESIS_ROLE_ARN.hasDefaultValue());
  }

  @Test
  void readConfigRoundTripsRoleArn() {
    assertEquals(ROLE_ARN, newReadConfig(ROLE_ARN).getRoleArn());
    // Legacy same-account path: null ARN is preserved (not coerced to empty).
    assertNull(newReadConfig(null).getRoleArn());
  }

  /**
   * The client is rebuilt on executors from a deserialized KinesisReadConfig, so the ARN must
   * survive Java serialization for cross-account reads to work outside the driver.
   */
  @Test
  void roleArnSurvivesSerialization() throws Exception {
    KinesisReadConfig deserialized = serializeRoundTrip(newReadConfig(ROLE_ARN));
    assertEquals(ROLE_ARN, deserialized.getRoleArn());
  }

  private static KinesisReadConfig newReadConfig(String roleArn) {
    return new KinesisReadConfig("test-stream", "us-west-2", null, null, null, roleArn,
        KinesisSourceConfig.KinesisStartingPositionStrategy.LATEST, false, true,
        10000, 200L, 5000L, 1000L, 10000L, 600000L);
  }

  private static KinesisReadConfig serializeRoundTrip(KinesisReadConfig config) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(config);
    }
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (KinesisReadConfig) in.readObject();
    }
  }
}
