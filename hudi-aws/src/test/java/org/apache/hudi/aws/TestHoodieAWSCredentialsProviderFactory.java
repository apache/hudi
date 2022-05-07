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

package org.apache.hudi.aws;

import com.amazonaws.auth.BasicSessionCredentials;
import org.apache.hudi.config.HoodieAWSConfig;
import org.apache.hudi.common.config.HoodieConfig;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieAWSCredentialsProviderFactory {

  @Test
  public void testGetAWSCredentials() {
    HoodieConfig cfg = new HoodieConfig();
    cfg.setValue(HoodieAWSConfig.AWS_ACCESS_KEY, "random-access-key");
    cfg.setValue(HoodieAWSConfig.AWS_SECRET_KEY, "random-secret-key");
    cfg.setValue(HoodieAWSConfig.AWS_SESSION_TOKEN, "random-session-token");
    BasicSessionCredentials credentials = (BasicSessionCredentials) org.apache.hudi.aws.credentials.HoodieAWSCredentialsProviderFactory.getAwsCredentialsProvider(cfg.getProps()).getCredentials();
    assertEquals("random-access-key", credentials.getAWSAccessKeyId());
    assertEquals("random-secret-key", credentials.getAWSSecretKey());
    assertEquals("random-session-token", credentials.getSessionToken());
  }
}
