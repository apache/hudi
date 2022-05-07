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
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;

/**
 * Construct and provide the default {@link DatasetUrn} to identify the Dataset on DataHub.
 * <p>
 * Extend this to customize the way of constructing {@link DatasetUrn}.
 */
public class HoodieDataHubDatasetIdentifier {

  public static final String DEFAULT_HOODIE_DATAHUB_PLATFORM_NAME = "hudi";

  protected final TypedProperties props;

  public HoodieDataHubDatasetIdentifier(TypedProperties props) {
    this.props = props;
  }

  public DatasetUrn getDatasetUrn() {
    DataPlatformUrn dataPlatformUrn = new DataPlatformUrn(DEFAULT_HOODIE_DATAHUB_PLATFORM_NAME);
    DataHubSyncConfig config = new DataHubSyncConfig(props);
    return new DatasetUrn(dataPlatformUrn, String.format("%s.%s", config.databaseName, config.tableName), FabricType.DEV);
  }
}
