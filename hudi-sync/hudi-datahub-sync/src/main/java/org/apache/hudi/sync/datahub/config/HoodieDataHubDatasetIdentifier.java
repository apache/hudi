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

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;

import java.util.Properties;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;
import static org.apache.hudi.sync.datahub.config.DataHubSyncConfig.META_SYNC_DATAHUB_DATAPLATFORM_NAME;
import static org.apache.hudi.sync.datahub.config.DataHubSyncConfig.META_SYNC_DATAHUB_DATASET_ENV;

/**
 * Construct and provide the default {@link DatasetUrn} to identify the Dataset on DataHub.
 * <p>
 * Extend this to customize the way of constructing {@link DatasetUrn}.
 */
public class HoodieDataHubDatasetIdentifier {

  public static final String DEFAULT_HOODIE_DATAHUB_PLATFORM_NAME = "hudi";
  public static final FabricType DEFAULT_DATAHUB_ENV = FabricType.DEV;

  protected final Properties props;

  public HoodieDataHubDatasetIdentifier(Properties props) {
    this.props = props;
  }

  public DatasetUrn getDatasetUrn() {
    DataHubSyncConfig config = new DataHubSyncConfig(props);

    return new DatasetUrn(
            createDataPlatformUrn(config.getStringOrDefault(META_SYNC_DATAHUB_DATAPLATFORM_NAME)),
            createDatasetName(config.getString(META_SYNC_DATABASE_NAME), config.getString(META_SYNC_TABLE_NAME)),
            FabricType.valueOf(config.getStringOrDefault(META_SYNC_DATAHUB_DATASET_ENV))
    );
  }

  private static DataPlatformUrn createDataPlatformUrn(String platformUrn) {
    return new DataPlatformUrn(platformUrn);
  }

  private static String createDatasetName(String databaseName, String tableName) {
    return String.format("%s.%s", databaseName, tableName);
  }
}
