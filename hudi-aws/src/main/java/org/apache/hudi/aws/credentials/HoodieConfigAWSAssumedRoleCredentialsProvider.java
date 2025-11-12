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

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieAWSConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.util.Properties;

/**
 * Credentials provider which assumes AWS role from Hoodie config and fetches its credentials.
 */
public class HoodieConfigAWSAssumedRoleCredentialsProvider implements AwsCredentialsProvider {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieConfigAWSAssumedRoleCredentialsProvider.class);

  private final StsAssumeRoleCredentialsProvider credentialsProvider;

  public HoodieConfigAWSAssumedRoleCredentialsProvider(Properties props) {
    String roleArn = props.getProperty(HoodieAWSConfig.AWS_ASSUME_ROLE_ARN.key());
    String externalId = props.getProperty(HoodieAWSConfig.AWS_ASSUME_ROLE_EXTERNAL_ID.key());
    String sessionName = props.getProperty(HoodieAWSConfig.AWS_ASSUME_ROLE_SESSION_NAME.key());
    AssumeRoleRequest req = AssumeRoleRequest.builder()
          .roleArn(roleArn)
          .roleSessionName(sessionName)
          .externalId(externalId)
          .build();
    StsClient stsClient = StsClient.builder().build();

    this.credentialsProvider = StsAssumeRoleCredentialsProvider.builder()
          .stsClient(stsClient)
          .refreshRequest(req)
          .build();
  }

  public static boolean validConf(Properties props) {
    String roleArn = props.getProperty(HoodieAWSConfig.AWS_ASSUME_ROLE_ARN.key());
    return !StringUtils.isNullOrEmpty(roleArn);
  }

  @Override
  public AwsCredentials resolveCredentials() {
    return credentialsProvider.resolveCredentials();
  }
}
