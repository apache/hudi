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

package org.apache.hudi.tableformat;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.TableMetadataFactory;
import org.apache.hudi.storage.HoodieStorage;

/**
 * The test implementation of {@link TableMetadataFactory} used by test-format.
 */
public class TestTableMetadataFactory extends TableMetadataFactory {
  private static final TestTableMetadataFactory INSTANCE = new TestTableMetadataFactory();

  public static TestTableMetadataFactory getInstance() {
    return INSTANCE;
  }

  @Override
  public HoodieTableMetadata create(
      HoodieEngineContext engineContext,
      HoodieStorage storage,
      HoodieMetadataConfig metadataConfig,
      String datasetBasePath,
      boolean reuse) {
    return new TestFormatBackedTableMetadata(engineContext, storage, datasetBasePath);
  }
}
