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

package org.apache.hudi.table.action.index.functional;

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.storage.HoodieLocation;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class BaseHoodieFunctionalIndexClient {

  private static final Logger LOG = LoggerFactory.getLogger(BaseHoodieFunctionalIndexClient.class);

  public BaseHoodieFunctionalIndexClient() {
  }

  /**
   * Register a functional index.
   * Index definitions are stored in user-specified path or, by default, in .hoodie/.index_defs/index.json.
   * For the first time, the index definition file will be created if not exists.
   * For the second time, the index definition file will be updated if exists.
   * Table Config is updated if necessary.
   */
  public void register(HoodieTableMetaClient metaClient, String indexName, String indexType, Map<String, Map<String, String>> columns, Map<String, String> options) {
    LOG.info("Registering index {} of using {}", indexName, indexType);
    String indexMetaPath = metaClient.getTableConfig().getIndexDefinitionPath()
        .orElseGet(() -> metaClient.getMetaPath()
            + HoodieLocation.SEPARATOR + HoodieTableMetaClient.INDEX_DEFINITION_FOLDER_NAME
            + HoodieLocation.SEPARATOR + HoodieTableMetaClient.INDEX_DEFINITION_FILE_NAME);
    // build HoodieFunctionalIndexMetadata and then add to index definition file
    metaClient.buildFunctionalIndexDefinition(indexMetaPath, indexName, indexType, columns, options);
    // update table config if necessary
    if (!metaClient.getTableConfig().getProps().containsKey(HoodieTableConfig.INDEX_DEFINITION_PATH) || !metaClient.getTableConfig().getIndexDefinitionPath().isPresent()) {
      metaClient.getTableConfig().setValue(HoodieTableConfig.INDEX_DEFINITION_PATH, indexMetaPath);
      HoodieTableConfig.update(metaClient.getFs(), new Path(metaClient.getMetaPath()), metaClient.getTableConfig().getProps());
    }
  }

  /**
   * Create a functional index.
   */
  public abstract void create(HoodieTableMetaClient metaClient, String indexName, String indexType, Map<String, Map<String, String>> columns, Map<String, String> options);
}
