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

package org.apache.hudi.integ.testsuite.configuration;

import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.integ.testsuite.reader.DeltaInputType;
import org.apache.hudi.integ.testsuite.writer.DeltaOutputMode;

/**
 * Configuration to hold details about a DFS based output type, implements {@link DeltaConfig}.
 */
public class DFSDeltaConfig extends DeltaConfig {

  // The base path where the generated data should be written to. This data will in turn be used to write into a hudi
  // dataset
  private final String deltaBasePath;
  private final String datasetOutputPath;
  private final String schemaStr;
  // Maximum file size for the files generated
  private final Long maxFileSize;
  // The current batch id
  private Integer batchId;

  public DFSDeltaConfig(DeltaOutputMode deltaOutputMode, DeltaInputType deltaInputType,
                        SerializableConfiguration configuration,
                        String deltaBasePath, String targetBasePath, String schemaStr, Long maxFileSize) {
    super(deltaOutputMode, deltaInputType, configuration);
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
