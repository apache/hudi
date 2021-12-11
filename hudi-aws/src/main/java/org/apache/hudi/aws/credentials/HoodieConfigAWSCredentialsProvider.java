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
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import org.apache.hudi.config.HoodieAWSConfig;
import org.apache.hudi.common.util.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * Credentials provider which fetches AWS access key from Hoodie config.
 */
public class HoodieConfigAWSCredentialsProvider implements AWSCredentialsProvider {

  private static final Logger LOG = LogManager.getLogger(HoodieConfigAWSCredentialsProvider.class);

  private AWSCredentials awsCredentials;

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

  private static AWSCredentials createCredentials(String accessKey, String secretKey,
                                                    String sessionToken) {
    return (sessionToken == null)
            ? new BasicAWSCredentials(accessKey, secretKey)
            : new BasicSessionCredentials(accessKey, secretKey, sessionToken);
  }

  @Override
  public AWSCredentials getCredentials() {
    return this.awsCredentials;
  }

  @Override
  public void refresh() {

  }
}
