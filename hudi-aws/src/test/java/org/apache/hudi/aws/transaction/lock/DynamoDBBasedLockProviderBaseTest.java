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

package org.apache.hudi.aws.transaction.lock;

import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.config.DynamoDbBasedLockConfig;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;

import java.util.Properties;

import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY;

class DynamoDBBasedLockProviderBaseTest {
  private static final LockConfiguration LOCK_CONFIGURATION;
  @Mock
  private static DynamoDbClient mockClient = new DynamoDbClient() {
    @Override
    public String serviceName() {
      return "";
    }

    @Override
    public void close() {

    }
  };

  static {
    Properties dynamoDblpProps = new TypedProperties();
    dynamoDblpProps.setProperty(DynamoDbBasedLockConfig.DYNAMODB_LOCK_BILLING_MODE.key(), BillingMode.PAY_PER_REQUEST.name());
    dynamoDblpProps.setProperty(DynamoDbBasedLockConfig.DYNAMODB_LOCK_TABLE_CREATION_TIMEOUT.key(), Integer.toString(20 * 1000 * 5));
    dynamoDblpProps.setProperty(DynamoDbBasedLockConfig.DYNAMODB_LOCK_REGION.key(), "us-east-2");
    dynamoDblpProps.setProperty(DynamoDbBasedLockConfig.DYNAMODB_LOCK_TABLE_NAME.key(), "my-table");
    dynamoDblpProps.setProperty(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "1000");
    dynamoDblpProps.setProperty(DynamoDbBasedLockConfig.DYNAMODB_LOCK_READ_CAPACITY.key(), "0");
    dynamoDblpProps.setProperty(DynamoDbBasedLockConfig.DYNAMODB_LOCK_WRITE_CAPACITY.key(), "0");
    dynamoDblpProps.setProperty(DynamoDbBasedLockConfig.DYNAMODB_LOCK_PARTITION_KEY.key(), "testKey");
    LOCK_CONFIGURATION = new LockConfiguration(dynamoDblpProps);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testLockProviderBaseInitialization(boolean isNull) {
    Exception e = null;
    try {
      new DynamoDBBasedLockProvider(LOCK_CONFIGURATION, new HadoopStorageConfiguration(true), isNull ? null : mockClient);
    } catch (Exception ex) {
      e = ex;
    }
    Assertions.assertNotNull(e);
    if (isNull) {
      // Initialization should fail on AWS API call due to invalid setup.
      Assertions.assertEquals(software.amazon.awssdk.core.exception.SdkClientException.class, e.getClass());
    } else {
      // Otherwise it should be anything but NPE.
      Assertions.assertNotEquals(java.lang.NullPointerException.class, e.getClass());
    }
  }
}