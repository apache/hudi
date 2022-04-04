/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.sync.datahub.config;

import org.apache.hudi.common.config.TypedProperties;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DatasetUrn;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;

import static org.apache.hudi.sync.datahub.config.DataHubSyncConfig.META_SYNC_DATAHUB_DATASET_IDENTIFIER_CLASS;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TestDataHubSyncConfig {

  @Test
  void testInstantiationWithProps() {
    TypedProperties props = new TypedProperties();
    props.setProperty(META_SYNC_DATAHUB_DATASET_IDENTIFIER_CLASS.key(), DummyIdentifier.class.getName());
    DataHubSyncConfig syncConfig = new DataHubSyncConfig(props);
    DatasetUrn datasetUrn = syncConfig.datasetIdentifier.getDatasetUrn();
    assertEquals("foo", datasetUrn.getPlatformEntity().getPlatformNameEntity());
    assertEquals("project.database.table", datasetUrn.getDatasetNameEntity());
    assertEquals(FabricType.PROD, datasetUrn.getOriginEntity());
  }

  public static class DummyIdentifier extends HoodieDataHubDatasetIdentifier {

    public DummyIdentifier(TypedProperties props) {
      super(props);
    }

    @Override
    public DatasetUrn getDatasetUrn() {
      try {
        return DatasetUrn.createFromString("urn:li:dataset:(urn:li:dataPlatform:foo,project.database.table,PROD)");
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
