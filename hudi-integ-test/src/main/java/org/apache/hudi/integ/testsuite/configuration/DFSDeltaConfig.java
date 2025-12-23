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

import org.apache.hudi.integ.testsuite.reader.DeltaInputType;
import org.apache.hudi.integ.testsuite.writer.DeltaOutputMode;
import org.apache.hudi.storage.StorageConfiguration;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.conf.Configuration;

/**
 * Configuration to hold details about a DFS based output type, implements {@link DeltaConfig}.
 */
@Getter
public class DFSDeltaConfig extends DeltaConfig {

  // The base path where the generated data should be written to. This data will in turn be used to write into a hudi
  // dataset
  private final String deltaBasePath;
  private final String datasetOutputPath;
  private final String schemaStr;
  // Maximum file size for the files generated
  private final Long maxFileSize;
  // The current batch id
  @Setter
  private Integer batchId;
  // Parallelism to use when generating input data
  private int inputParallelism;
  // Whether to delete older input data once it has been ingested
  private boolean oldInputDataDeleted;
  private boolean hudiUpdatesEnabled;

  public DFSDeltaConfig(DeltaOutputMode deltaOutputMode, DeltaInputType deltaInputType,
                        StorageConfiguration<Configuration> storageConf,
                        String deltaBasePath, String targetBasePath, String schemaStr, Long maxFileSize,
                        int inputParallelism, boolean oldInputDataDeleted, boolean hudiUpdatesEnabled) {
    super(deltaOutputMode, deltaInputType, storageConf);
    this.deltaBasePath = deltaBasePath;
    this.schemaStr = schemaStr;
    this.maxFileSize = maxFileSize;
    this.datasetOutputPath = targetBasePath;
    this.inputParallelism = inputParallelism;
    this.oldInputDataDeleted = oldInputDataDeleted;
    this.hudiUpdatesEnabled = hudiUpdatesEnabled;
  }
}
