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
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Covers the credential-provider branches of {@link KinesisOffsetGen#createKinesisClient}, the per-JVM
 * assume-role provider cache and the STS request it builds. AWS SDK v2 clients resolve credentials
 * lazily (only on the first request), so a client can be built for each branch without any AWS
 * environment or network access, and the chosen provider is observable through
 * {@code serviceClientConfiguration().credentialsProvider()}.
 */
class TestKinesisOffsetGenClient {

  private static final String REGION = "us-west-2";
  private static final String ROLE_ARN_PREFIX = "arn:aws:iam::123456789012:role/kinesis-cross-account-";
  private static final String SESSION_NAME = "hudi-kinesis-source";

  private static IdentityProvider<?> providerOf(KinesisClient client) {
    return client.serviceClientConfiguration().credentialsProvider();
  }

  @Test
  void testAssumeRoleProviderUsedWhenRoleArnPresent() {
    try (KinesisClient client = KinesisOffsetGen.createKinesisClient(
        REGION, null, null, null, ROLE_ARN_PREFIX + "arn-only", null, SESSION_NAME)) {
      assertInstanceOf(StsAssumeRoleCredentialsProvider.class, providerOf(client));
    }
  }

  @Test
  void testDefaultChainUsedWhenNoRoleArnOrKeys() {
    try (KinesisClient client = KinesisOffsetGen.createKinesisClient(
        REGION, null, null, null, null, null, SESSION_NAME)) {
      assertInstanceOf(DefaultCredentialsProvider.class, providerOf(client));
    }
  }

  @Test
  void testStaticKeysTakePrecedenceOverAssumeRole() {
    // Both static keys and a role ARN set: the static-credentials branch wins (no STS assume-role).
    try (KinesisClient client = KinesisOffsetGen.createKinesisClient(
        REGION, null, "access", "secret", ROLE_ARN_PREFIX + "with-keys", null, SESSION_NAME)) {
      assertInstanceOf(StaticCredentialsProvider.class, providerOf(client));
    }
  }

  @Test
  void testAssumeRoleProviderCachedPerRoleExternalIdAndSession() {
    String roleArn = ROLE_ARN_PREFIX + "cached";
    IdentityProvider<?> firstProvider;
    try (KinesisClient first = KinesisOffsetGen.createKinesisClient(
        REGION, null, null, null, roleArn, "ext-1", SESSION_NAME)) {
      firstProvider = providerOf(first);
    }
    // The first client is closed above; the cached provider must still be handed to later clients with
    // the same role tuple, and any change to external id, session name or role must yield a new one.
    try (KinesisClient second = KinesisOffsetGen.createKinesisClient(
             REGION, null, null, null, roleArn, "ext-1", SESSION_NAME);
         KinesisClient otherExternalId = KinesisOffsetGen.createKinesisClient(
             REGION, null, null, null, roleArn, "ext-2", SESSION_NAME);
         KinesisClient otherSession = KinesisOffsetGen.createKinesisClient(
             REGION, null, null, null, roleArn, "ext-1", "other-session");
         KinesisClient otherRole = KinesisOffsetGen.createKinesisClient(
             REGION, null, null, null, ROLE_ARN_PREFIX + "cached-other", "ext-1", SESSION_NAME)) {
      assertSame(firstProvider, providerOf(second));
      assertNotSame(firstProvider, providerOf(otherExternalId));
      assertNotSame(firstProvider, providerOf(otherSession));
      assertNotSame(firstProvider, providerOf(otherRole));
    }
  }

  @Test
  void testInstanceClientForwardsRoleTupleFromProps() {
    String roleArn = ROLE_ARN_PREFIX + "from-props";
    TypedProperties props = new TypedProperties();
    props.setProperty(KinesisSourceConfig.KINESIS_STREAM_NAME.key(), "test-stream");
    props.setProperty(KinesisSourceConfig.KINESIS_REGION.key(), REGION);
    props.setProperty(KinesisSourceConfig.KINESIS_ROLE_ARN.key(), roleArn);
    props.setProperty(KinesisSourceConfig.KINESIS_ROLE_EXTERNAL_ID.key(), "ext-props");

    try (KinesisClient fromProps = new KinesisOffsetGen(props).createKinesisClient();
         KinesisClient expected = KinesisOffsetGen.createKinesisClient(REGION, null, null, null,
             roleArn, "ext-props", KinesisSourceConfig.KINESIS_ROLE_SESSION_NAME.defaultValue())) {
      // The cache is keyed by the full role tuple, so provider identity proves that role ARN, external
      // id and the defaulted session name all reached the client in the right slots.
      assertSame(providerOf(expected), providerOf(fromProps));
    }
  }

  @Test
  void testAssumeRoleRequestCarriesRoleExternalIdAndSession() {
    String roleArn = ROLE_ARN_PREFIX + "request";
    AssumeRoleRequest request = KinesisOffsetGen.buildAssumeRoleRequest(roleArn, "ext-req", "sess-req");
    assertEquals(roleArn, request.roleArn());
    assertEquals("ext-req", request.externalId());
    assertEquals("sess-req", request.roleSessionName());
    // No external id: omitted from the request rather than sent as an empty string.
    assertNull(KinesisOffsetGen.buildAssumeRoleRequest(roleArn, null, "sess-req").externalId());
  }

  @Test
  void testBlankExternalIdAndSessionNameTreatedAsUnset() {
    String roleArn = ROLE_ARN_PREFIX + "blank";
    try (KinesisClient blank = KinesisOffsetGen.createKinesisClient(
             REGION, null, null, null, roleArn, "", "");
         KinesisClient unset = KinesisOffsetGen.createKinesisClient(
             REGION, null, null, null, roleArn, null, KinesisSourceConfig.KINESIS_ROLE_SESSION_NAME.defaultValue())) {
      // Blank external id / session name normalize to unset / default, so both hit the same cache entry.
      assertSame(providerOf(unset), providerOf(blank));
    }
  }
}
