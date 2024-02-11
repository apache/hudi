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

package org.apache.hudi.aws.sync;

import org.apache.hudi.aws.credentials.HoodieAWSCredentialsProviderFactory;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.config.HoodieAWSConfig;
import org.apache.hudi.hive.HiveSyncConfig;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueAsyncClient;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

public class ITTestGlueUtil extends ITTestSyncUtil {

  protected static final String MOTO_ENDPOINT = "http://localhost:5000";
  public static final String AWS_REGION = "eu-west-1";


  private AwsGlueCatalogSyncTool awsGlueCatalogSyncTool;
  protected GlueAsyncClient glueClient;

  @BeforeEach
  @Override
  public void setup() {
    getAwsProperties().forEach((k, v) -> hiveProps.setProperty(k.toString(), v.toString()));
    super.setup();
    try {
      initGlueClient();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @AfterEach
  @Override
  public void cleanUp() {
    super.cleanUp();
    // drop database and table
    glueClient.deleteDatabase(r -> r.name(DB_NAME));
    glueClient.deleteTable(r -> r.name(TABLE_NAME).databaseName(DB_NAME));
    awsGlueCatalogSyncTool.close();
  }

  protected Properties getAwsProperties() {
    Properties hiveProps = new TypedProperties();
    hiveProps.setProperty(HoodieAWSConfig.AWS_ACCESS_KEY.key(), "dummy");
    hiveProps.setProperty(HoodieAWSConfig.AWS_SECRET_KEY.key(), "dummy");
    hiveProps.setProperty(HoodieAWSConfig.AWS_SESSION_TOKEN.key(), "dummy");
    hiveProps.setProperty(HoodieAWSConfig.AWS_GLUE_ENDPOINT.key(), MOTO_ENDPOINT);
    hiveProps.setProperty(HoodieAWSConfig.AWS_GLUE_REGION.key(), AWS_REGION);
    return hiveProps;
  }

  protected void initGlueClient() throws URISyntaxException {
    glueClient = GlueAsyncClient.builder()
        .credentialsProvider(HoodieAWSCredentialsProviderFactory.getAwsCredentialsProvider(getAwsProperties()))
        .endpointOverride(new URI(MOTO_ENDPOINT))
        .region(Region.of(AWS_REGION))
        .build();
  }

  public AwsGlueCatalogSyncTool getAwsGlueCatalogSyncTool() {
    awsGlueCatalogSyncTool = new AwsGlueCatalogSyncTool(hiveProps, hadoopConf);
    awsGlueCatalogSyncTool.initSyncClient(new HiveSyncConfig(hiveProps));
    return awsGlueCatalogSyncTool;
  }
}
