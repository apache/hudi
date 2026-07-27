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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.config.KinesisSourceConfig;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.kinesis.KinesisClient;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Covers the credential-provider branches of {@link KinesisOffsetGen#createKinesisClient}. AWS SDK v2
 * clients resolve credentials lazily (only on the first request), so a client can be built for each
 * branch without any AWS environment or network access.
 */
class TestKinesisOffsetGenClient {

  private static final String REGION = "us-west-2";
  private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/kinesis-cross-account-read";

  @Test
  void buildsClientWithAssumeRoleProviderWhenArnPresent() {
    try (KinesisClient client = KinesisOffsetGen.createKinesisClient(REGION, null, null, null, ROLE_ARN)) {
      assertNotNull(client);
    }
  }

  @Test
  void buildsClientWithDefaultChainWhenNoArnOrKeys() {
    try (KinesisClient client = KinesisOffsetGen.createKinesisClient(REGION, null, null, null, null)) {
      assertNotNull(client);
    }
  }

  @Test
  void staticKeysTakePrecedenceOverAssumeRole() {
    // Both static keys and an ARN set: the static-credentials branch wins (no STS assume-role).
    try (KinesisClient client =
        KinesisOffsetGen.createKinesisClient(REGION, null, "access", "secret", ROLE_ARN)) {
      assertNotNull(client);
    }
  }

  @Test
  void instanceClientReadsRoleArnFromProps() {
    TypedProperties props = new TypedProperties();
    props.setProperty(KinesisSourceConfig.KINESIS_STREAM_NAME.key(), "test-stream");
    props.setProperty(KinesisSourceConfig.KINESIS_REGION.key(), REGION);
    props.setProperty(KinesisSourceConfig.KINESIS_ROLE_ARN.key(), ROLE_ARN);

    try (KinesisClient client = new KinesisOffsetGen(props).createKinesisClient()) {
      assertNotNull(client);
    }
  }
}
