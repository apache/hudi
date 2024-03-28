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

package org.apache.hudi.client;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.clustering.run.strategy.ParquetToolsExecutionStrategy;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;

import java.io.IOException;

/**
 * Test execution strategy for testing the skeleton of the ParquetToolsExecutionStrategy.
 * It creates a copy of the original file with a different commit timestamp.
 */
public class ParquetToolsTestExecutionStrategy<T extends HoodieRecordPayload<T>>
    extends ParquetToolsExecutionStrategy<T> {

  public ParquetToolsTestExecutionStrategy(
      HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  protected void executeTools(Path oldFilePath, Path newFilePath) {
    FileSystem fs = getHoodieTable().getMetaClient().getFs();
    try {
      FileUtil.copy(fs, oldFilePath, fs, newFilePath, false, false, fs.getConf());
    } catch (IOException e) {
      throw new HoodieIOException("Exception in copying files.", e);
    }
  }
}
