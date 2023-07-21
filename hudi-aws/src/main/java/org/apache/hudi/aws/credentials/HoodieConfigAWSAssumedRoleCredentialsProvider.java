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

package org.apache.hudi.aws.credentials;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieAWSConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Credentials provider which fetches AWS access key from Hoodie config.
 */
public class HoodieConfigAWSAssumedRoleCredentialsProvider implements AWSCredentialsProvider {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieConfigAWSAssumedRoleCredentialsProvider.class);

  private final STSAssumeRoleSessionCredentialsProvider credentialsProvider;

  public HoodieConfigAWSAssumedRoleCredentialsProvider(Properties props) {
    if (!validConf(props)) {
      LOG.debug("AWS role ARN not found in the Hudi configuration.");
      throw new IllegalArgumentException("AWS role ARN not found in the Hudi configuration.");
    } else {
      String roleArn = props.getProperty(HoodieAWSConfig.AWS_ASSUME_ROLE_ARN.key());
      this.credentialsProvider = new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn, "glue-access").build();
    }
  }

  public static boolean validConf(Properties props) {
    String roleArn = props.getProperty(HoodieAWSConfig.AWS_ASSUME_ROLE_ARN.key());
    return !StringUtils.isNullOrEmpty(roleArn);
  }

  @Override
  public AWSCredentials getCredentials() {
    return this.credentialsProvider.getCredentials();
  }

  @Override
  public void refresh() {
    this.credentialsProvider.refresh();
  }
}
