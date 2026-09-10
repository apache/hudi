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

package org.apache.hudi.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.apache.hudi.config.DynamoDbBasedLockConfig.DYNAMODB_LOCK_BILLING_MODE;
import static org.apache.hudi.config.DynamoDbBasedLockConfig.DYNAMODB_LOCK_PARTITION_KEY;
import static org.apache.hudi.config.DynamoDbBasedLockConfig.DYNAMODB_LOCK_READ_CAPACITY;
import static org.apache.hudi.config.DynamoDbBasedLockConfig.DYNAMODB_LOCK_REGION;
import static org.apache.hudi.config.DynamoDbBasedLockConfig.DYNAMODB_LOCK_TABLE_NAME;
import static org.apache.hudi.config.DynamoDbBasedLockConfig.DYNAMODB_LOCK_WRITE_CAPACITY;
import static org.apache.hudi.config.GlueCatalogSyncClientConfig.GLUE_SKIP_TABLE_ARCHIVE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class TestHoodieAWSConfig {

  @Test
  void testBuilderWritesEveryPropertyItIsGiven() {
    HoodieAWSConfig config = HoodieAWSConfig.newBuilder()
        .withAccessKey("access-key")
        .withSecretKey("secret-key")
        .withSessionToken("session-token")
        .withAssumeRoleARN("arn:aws:iam::123456789012:role/hudi")
        .withAssumeRoleExternalID("external-id")
        .withAssumeRoleSessionName("session-name")
        .withDynamoDBTable("lock_table")
        .withDynamoDBPartitionKey("partition-key")
        .withDynamoDBRegion("eu-west-1")
        .withDynamoDBBillingMode("PAY_PER_REQUEST")
        .withDynamoDBReadCapacity("12")
        .withDynamoDBWriteCapacity("13")
        .withGlueSkipTableArchive("false")
        .build();

    assertEquals("access-key", config.getAWSAccessKey());
    assertEquals("secret-key", config.getAWSSecretKey());
    assertEquals("session-token", config.getAWSSessionToken());
    assertEquals("arn:aws:iam::123456789012:role/hudi", config.getAWSAssumeRoleARN());
    assertEquals("external-id", config.getAWSAssumeRoleExternalID());
    assertEquals("session-name", config.getAWSAssumeRoleSessionName());

    Properties props = config.getProps();
    assertEquals("access-key", props.getProperty(HoodieAWSConfig.AWS_ACCESS_KEY.key()));
    assertEquals("secret-key", props.getProperty(HoodieAWSConfig.AWS_SECRET_KEY.key()));
    assertEquals("session-token", props.getProperty(HoodieAWSConfig.AWS_SESSION_TOKEN.key()));
    assertEquals("arn:aws:iam::123456789012:role/hudi", props.getProperty(HoodieAWSConfig.AWS_ASSUME_ROLE_ARN.key()));
    assertEquals("external-id", props.getProperty(HoodieAWSConfig.AWS_ASSUME_ROLE_EXTERNAL_ID.key()));
    assertEquals("session-name", props.getProperty(HoodieAWSConfig.AWS_ASSUME_ROLE_SESSION_NAME.key()));
    assertEquals("lock_table", props.getProperty(DYNAMODB_LOCK_TABLE_NAME.key()));
    assertEquals("partition-key", props.getProperty(DYNAMODB_LOCK_PARTITION_KEY.key()));
    assertEquals("eu-west-1", props.getProperty(DYNAMODB_LOCK_REGION.key()));
    assertEquals("PAY_PER_REQUEST", props.getProperty(DYNAMODB_LOCK_BILLING_MODE.key()));
    assertEquals("12", props.getProperty(DYNAMODB_LOCK_READ_CAPACITY.key()));
    assertEquals("13", props.getProperty(DYNAMODB_LOCK_WRITE_CAPACITY.key()));
    assertEquals("false", props.getProperty(GLUE_SKIP_TABLE_ARCHIVE.key()));
  }

  @Test
  void testBuildFillsInTheSessionNameDefaultOnly() {
    HoodieAWSConfig config = HoodieAWSConfig.newBuilder().build();

    assertEquals(HoodieAWSConfig.AWS_ASSUME_ROLE_SESSION_NAME.defaultValue(), config.getAWSAssumeRoleSessionName());
    assertFalse(config.getProps().containsKey(HoodieAWSConfig.AWS_ACCESS_KEY.key()),
        "properties without a default stay unset");
  }

  @Test
  void testFromPropertiesCopiesTheGivenProperties() {
    Properties given = new Properties();
    given.setProperty(HoodieAWSConfig.AWS_ACCESS_KEY.key(), "from-properties");

    HoodieAWSConfig config = HoodieAWSConfig.newBuilder().fromProperties(given).build();

    assertEquals("from-properties", config.getAWSAccessKey());
  }

  @Test
  void testFromFileReadsTheGivenPropertiesFile(@TempDir Path tempDir) throws IOException {
    File propertiesFile = tempDir.resolve("aws.properties").toFile();
    Files.write(propertiesFile.toPath(),
        (HoodieAWSConfig.AWS_ACCESS_KEY.key() + "=from-file\n"
            + HoodieAWSConfig.AWS_SECRET_KEY.key() + "=secret-from-file\n").getBytes("UTF-8"));

    HoodieAWSConfig config = HoodieAWSConfig.newBuilder().fromFile(propertiesFile).build();

    assertEquals("from-file", config.getAWSAccessKey());
    assertEquals("secret-from-file", config.getAWSSecretKey());
  }
}
