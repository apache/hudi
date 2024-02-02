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
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.config.HoodieAWSConfig;
import org.apache.hudi.hive.HiveSyncConfig;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueAsyncClient;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * docker run --rm -p 5000:5000 --name moto motoserver/moto:latest
 * see http://localhost:5000/moto-api/# to check aws resources
 */
public class ITTestDummy {
  @Test
  public void testAwsCreateDatabase() throws URISyntaxException, ExecutionException, InterruptedException {
    HoodieConfig cfg = new HoodieConfig();
    cfg.setValue(HoodieAWSConfig.AWS_ACCESS_KEY, "random-access-key");
    cfg.setValue(HoodieAWSConfig.AWS_SECRET_KEY, "random-secret-key");
    cfg.setValue(HoodieAWSConfig.AWS_SESSION_TOKEN, "random-session-token");

    GlueAsyncClient awsGlue = GlueAsyncClient.builder()
            .credentialsProvider(HoodieAWSCredentialsProviderFactory.getAwsCredentialsProvider(cfg.getProps()))
            .endpointOverride(new URI("http://localhost:5000"))
            .build();

    String dummyDb = "dummy_db";
    awsGlue.deleteDatabase(DeleteDatabaseRequest.builder().name(dummyDb).build()).get();
    CreateDatabaseResponse resp = awsGlue.createDatabase(CreateDatabaseRequest.builder()
            .databaseInput(DatabaseInput.builder().name(dummyDb).build()).build()).get();

    assertTrue(resp.sdkHttpResponse().isSuccessful());
  }

  @Test
  public void testClientCreateDatabase() {
    // see more details here org/apache/hudi/hive/TestHiveSyncTool.java
    TypedProperties hiveSyncProps = new TypedProperties();
    hiveSyncProps.setProperty(HoodieAWSConfig.AWS_ACCESS_KEY.key(), "dummy");
    hiveSyncProps.setProperty(HoodieAWSConfig.AWS_SECRET_KEY.key(), "dummy");
    hiveSyncProps.setProperty(HoodieAWSConfig.AWS_SESSION_TOKEN.key(), "dummy");
    hiveSyncProps.setProperty(HoodieAWSConfig.AWS_ENDPOINT.key(), "http://localhost:5000");
    hiveSyncProps.setProperty(META_SYNC_BASE_PATH.key(), "/tmp/bar");
    AWSGlueCatalogSyncClient glueSync = new AWSGlueCatalogSyncClient(new HiveSyncConfig(hiveSyncProps));
    glueSync.createDatabase("foo_db");
  }
}
