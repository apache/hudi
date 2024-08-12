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
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import java.util.Properties;

/**
 * Credentials provider which fetches AWS access key from Hoodie config.
 */
public class HoodieConfigAWSCredentialsProvider implements AwsCredentialsProvider {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieConfigAWSCredentialsProvider.class);

  private AwsCredentials awsCredentials;

  public HoodieConfigAWSCredentialsProvider(Properties props) {
    String accessKey = props.getProperty(HoodieAWSConfig.AWS_ACCESS_KEY.key());
    String secretKey = props.getProperty(HoodieAWSConfig.AWS_SECRET_KEY.key());
    String sessionToken = props.getProperty(HoodieAWSConfig.AWS_SESSION_TOKEN.key());

    if (StringUtils.isNullOrEmpty(accessKey) || StringUtils.isNullOrEmpty(secretKey)) {
      LOG.debug("AWS access key or secret key not found in the Hudi configuration. "
              + "Use default AWS credentials");
    } else {
      this.awsCredentials = createCredentials(accessKey, secretKey, sessionToken);
    }
  }

  private static AwsCredentials createCredentials(String accessKey, String secretKey,
                                                    String sessionToken) {
    return (sessionToken == null)
            ? AwsBasicCredentials.create(accessKey, secretKey)
            : AwsSessionCredentials.create(accessKey, secretKey, sessionToken);
  }

  @Override
  public AwsCredentials resolveCredentials() {
    return this.awsCredentials;
  }
}
