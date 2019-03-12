/*
 *  Copyright (c) 2019 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.bench.configuration;

import com.uber.hoodie.bench.DeltaInputFormat;
import com.uber.hoodie.bench.DeltaSinkType;
import com.uber.hoodie.common.SerializableConfiguration;

/**
 * Configuration to hold details about a DFS based sink, implements {@link DeltaConfig}
 */
public class DFSDeltaConfig extends DeltaConfig {

  // The base path where the generated data should be written to. This data will in turn be used to write into a hudi
  // dataset
  private final String deltaBasePath;
  private final String datasetOutputPath;
  private final String schemaStr;
  // Maximum file size for the files generated
  private final Long maxFileSize;
  // The current batch id for the sink
  private Integer batchId;

  public DFSDeltaConfig(DeltaSinkType deltaSinkType, DeltaInputFormat deltaInputFormat,
      SerializableConfiguration configuration,
      String deltaBasePath, String targetBasePath, String schemaStr, Long maxFileSize) {
    super(deltaSinkType, deltaInputFormat, configuration);
    this.deltaBasePath = deltaBasePath;
    this.schemaStr = schemaStr;
    this.maxFileSize = maxFileSize;
    this.datasetOutputPath = targetBasePath;
  }

  public String getDeltaBasePath() {
    return deltaBasePath;
  }

  public String getDatasetOutputPath() {
    return datasetOutputPath;
  }

  public String getSchemaStr() {
    return schemaStr;
  }

  public Long getMaxFileSize() {
    return maxFileSize;
  }

  public Integer getBatchId() {
    return batchId;
  }

  public void setBatchId(Integer batchId) {
    this.batchId = batchId;
  }
}
