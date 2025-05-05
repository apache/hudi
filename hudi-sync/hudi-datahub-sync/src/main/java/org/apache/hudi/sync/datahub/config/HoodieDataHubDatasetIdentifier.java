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

import org.apache.hudi.common.util.Option;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import io.datahubproject.models.util.DatabaseKey;

import java.util.Properties;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;
import static org.apache.hudi.sync.datahub.config.DataHubSyncConfig.META_SYNC_DATAHUB_DATAPLATFORM_INSTANCE_NAME;
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
  private final String dataPlatform;
  private final DataPlatformUrn dataPlatformUrn;
  private final Option<String> dataPlatformInstance;
  private final Option<Urn> dataPlatformInstanceUrn;
  private final DatasetUrn datasetUrn;
  private final Urn databaseUrn;
  private final String tableName;
  private final String databaseName;

  public HoodieDataHubDatasetIdentifier(Properties props) {
    this.props = props;
    if (props == null || props.isEmpty()) {
      throw new IllegalArgumentException("Properties cannot be null or empty");
    }
    DataHubSyncConfig config = new DataHubSyncConfig(props);

    this.dataPlatform = config.getStringOrDefault(META_SYNC_DATAHUB_DATAPLATFORM_NAME);
    this.dataPlatformUrn = createDataPlatformUrn(this.dataPlatform);
    this.dataPlatformInstance = Option.ofNullable(config.getString(META_SYNC_DATAHUB_DATAPLATFORM_INSTANCE_NAME));
    this.dataPlatformInstanceUrn = createDataPlatformInstanceUrn(
        this.dataPlatformUrn,
        Option.ofNullable(config.getString(META_SYNC_DATAHUB_DATAPLATFORM_INSTANCE_NAME))
    );
    this.datasetUrn = new DatasetUrn(
            this.dataPlatformUrn,
            createDatasetName(this.dataPlatformInstance, config.getString(META_SYNC_DATABASE_NAME), config.getString(META_SYNC_TABLE_NAME)),
            FabricType.valueOf(config.getStringOrDefault(META_SYNC_DATAHUB_DATASET_ENV))
    );

    this.tableName = config.getString(META_SYNC_TABLE_NAME);
    this.databaseName = config.getString(META_SYNC_DATABASE_NAME);

    // https://github.com/datahub-project/datahub/blob/0b105395e913cc47a59bdeed0c56d7c0d4b71b63/metadata-ingestion/src/datahub/emitter/mcp_builder.py#L69-L72
    DatabaseKey databaseKey = DatabaseKey.builder()
            .platform(config.getStringOrDefault(META_SYNC_DATAHUB_DATAPLATFORM_NAME))
            .instance(this.dataPlatformInstance.orElse(config.getStringOrDefault(META_SYNC_DATAHUB_DATASET_ENV)))
            .database(this.databaseName)
            .build();

    this.databaseUrn = databaseKey.asUrn();
  }

  public DatasetUrn getDatasetUrn() {
    return this.datasetUrn;
  }

  public String getDataPlatform() {
    return this.dataPlatform;
  }

  public DataPlatformUrn getDataPlatformUrn() {
    return this.dataPlatformUrn;
  }

  public Option<String> getDataPlatformInstance() {
    return this.dataPlatformInstance;
  }

  public Option<Urn> getDataPlatformInstanceUrn() {
    return this.dataPlatformInstanceUrn;
  }

  public Urn getDatabaseUrn() {
    return this.databaseUrn;
  }

  public String getTableName() {
    return this.tableName;
  }

  public String getDatabaseName() {
    return this.databaseName;
  }

  private static DataPlatformUrn createDataPlatformUrn(String platformUrn) {
    return new DataPlatformUrn(platformUrn);
  }

  private static Option<Urn> createDataPlatformInstanceUrn(DataPlatformUrn dataPlatformUrn, Option<String> dataPlatformInstance) {
    if (dataPlatformInstance.isEmpty()) {
      return Option.empty();
    }
    String dataPlatformInstanceStr = String.format("urn:li:dataPlatformInstance:(%s,%s)", dataPlatformUrn.toString(), dataPlatformInstance.get());
    try {
      return Option.of(Urn.createFromString(dataPlatformInstanceStr));
    } catch (Exception e) {
      throw new IllegalArgumentException(String.format("Failed to create DataPlatformInstance URN from string: %s", dataPlatformInstanceStr), e);
    }
  }

  private static String createDatasetName(Option<String> dataPlatformInstance, String databaseName, String tableName) {
    if (dataPlatformInstance.isPresent()) {
      return String.format("%s.%s.%s", dataPlatformInstance.get(), databaseName, tableName);
    }
    return String.format("%s.%s", databaseName, tableName);
  }
}