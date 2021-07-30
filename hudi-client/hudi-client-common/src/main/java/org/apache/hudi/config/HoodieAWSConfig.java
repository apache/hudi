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

/**
 * Configurations used by the AWS credentials.
 */
@Immutable
@ConfigClassProperty(name = "AWS credential Configs",
        groupName = ConfigGroups.Names.WRITE_CLIENT,
        description = "Configurations used for AWS credentials to get AWS resources.")
public class HoodieAWSConfig extends HoodieConfig {
  public static final ConfigProperty<String> AWS_ACCESS_KEY = ConfigProperty
        .key("hoodie.aws.access-key")
        .noDefaultValue()
        .withDocumentation("AWS access key id");

  public static final ConfigProperty<String> AWS_SECRET_KEY = ConfigProperty
        .key("hoodie.aws.secret-key")
        .noDefaultValue()
        .withDocumentation("AWS secret key");

  public static final ConfigProperty<String> AWS_SESSION_TOKEN = ConfigProperty
        .key("hoodie.aws.session.token")
        .noDefaultValue()
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

    public HoodieAWSConfig build() {
      awsConfig.setDefaults(HoodieAWSConfig.class.getName());
      return awsConfig;
    }
  }
}
