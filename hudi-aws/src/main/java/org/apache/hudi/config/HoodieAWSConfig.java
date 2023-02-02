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

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import javax.annotation.concurrent.Immutable;

import static org.apache.hudi.config.DynamoDbBasedLockConfig.DYNAMODB_LOCK_BILLING_MODE;
import static org.apache.hudi.config.DynamoDbBasedLockConfig.DYNAMODB_LOCK_PARTITION_KEY;
import static org.apache.hudi.config.DynamoDbBasedLockConfig.DYNAMODB_LOCK_READ_CAPACITY;
import static org.apache.hudi.config.DynamoDbBasedLockConfig.DYNAMODB_LOCK_REGION;
import static org.apache.hudi.config.DynamoDbBasedLockConfig.DYNAMODB_LOCK_TABLE_NAME;
import static org.apache.hudi.config.DynamoDbBasedLockConfig.DYNAMODB_LOCK_WRITE_CAPACITY;

/**
 * Configurations used by the AWS credentials and AWS DynamoDB based lock.
 */
@Immutable
@ConfigClassProperty(name = "Amazon Web Services Configs",
        groupName = ConfigGroups.Names.AWS,
        description = "Amazon Web Services configurations to access resources like Amazon DynamoDB (for locks),"
            + " Amazon CloudWatch (metrics).")
public class HoodieAWSConfig extends HoodieConfig {
  public static final ConfigProperty<String> AWS_ACCESS_KEY = ConfigProperty
        .key("hoodie.aws.access.key")
        .noDefaultValue()
        .sinceVersion("0.10.0")
        .withDocumentation("AWS access key id");

  public static final ConfigProperty<String> AWS_SECRET_KEY = ConfigProperty
        .key("hoodie.aws.secret.key")
        .noDefaultValue()
        .sinceVersion("0.10.0")
        .withDocumentation("AWS secret key");

  public static final ConfigProperty<String> AWS_SESSION_TOKEN = ConfigProperty
        .key("hoodie.aws.session.token")
        .noDefaultValue()
        .sinceVersion("0.10.0")
        .withDocumentation("AWS session token");

  private HoodieAWSConfig() {
    super();
  }

  public static HoodieAWSConfig.Builder newBuilder() {
    return new HoodieAWSConfig.Builder();
  }

  public String getAWSAccessKey() {
    return getString(AWS_ACCESS_KEY);
  }

  public String getAWSSecretKey() {
    return getString(AWS_SECRET_KEY);
  }

  public String getAWSSessionToken() {
    return getString(AWS_SESSION_TOKEN);
  }

  public static class Builder {

    private final HoodieAWSConfig awsConfig = new HoodieAWSConfig();

    public HoodieAWSConfig.Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.awsConfig.getProps().load(reader);
        return this;
      }
    }

    public HoodieAWSConfig.Builder fromProperties(Properties props) {
      this.awsConfig.getProps().putAll(props);
      return this;
    }

    public HoodieAWSConfig.Builder withAccessKey(String accessKey) {
      awsConfig.setValue(AWS_ACCESS_KEY, accessKey);
      return this;
    }

    public HoodieAWSConfig.Builder withSecretKey(String secretKey) {
      awsConfig.setValue(AWS_SECRET_KEY, secretKey);
      return this;
    }

    public HoodieAWSConfig.Builder withSessionToken(String sessionToken) {
      awsConfig.setValue(AWS_SESSION_TOKEN, sessionToken);
      return this;
    }

    public Builder withDynamoDBTable(String dynamoDbTableName) {
      awsConfig.setValue(DYNAMODB_LOCK_TABLE_NAME, dynamoDbTableName);
      return this;
    }

    public Builder withDynamoDBPartitionKey(String partitionKey) {
      awsConfig.setValue(DYNAMODB_LOCK_PARTITION_KEY, partitionKey);
      return this;
    }

    public Builder withDynamoDBRegion(String region) {
      awsConfig.setValue(DYNAMODB_LOCK_REGION, region);
      return this;
    }

    public Builder withDynamoDBBillingMode(String mode) {
      awsConfig.setValue(DYNAMODB_LOCK_BILLING_MODE, mode);
      return this;
    }

    public Builder withDynamoDBReadCapacity(String capacity) {
      awsConfig.setValue(DYNAMODB_LOCK_READ_CAPACITY, capacity);
      return this;
    }

    public Builder withDynamoDBWriteCapacity(String capacity) {
      awsConfig.setValue(DYNAMODB_LOCK_WRITE_CAPACITY, capacity);
      return this;
    }

    public HoodieAWSConfig build() {
      awsConfig.setDefaults(HoodieAWSConfig.class.getName());
      return awsConfig;
    }
  }
}
