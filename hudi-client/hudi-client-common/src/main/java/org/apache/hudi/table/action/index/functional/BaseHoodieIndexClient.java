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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.storage.StoragePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public abstract class BaseHoodieIndexClient {

  private static final Logger LOG = LoggerFactory.getLogger(BaseHoodieIndexClient.class);

  public BaseHoodieIndexClient() {
  }

  /**
   * Register a expression index.
   * Index definitions are stored in user-specified path or, by default, in .hoodie/.index_defs/index.json.
   * For the first time, the index definition file will be created if not exists.
   * For the second time, the index definition file will be updated if exists.
   * Table Config is updated if necessary.
   */
  public void register(HoodieTableMetaClient metaClient, HoodieIndexDefinition indexDefinition) {
    LOG.info("Registering index {} of using {}", indexDefinition.getIndexName(), indexDefinition.getIndexType());
    // build HoodieIndexMetadata and then add to index definition file
    boolean indexDefnUpdated = metaClient.buildIndexDefinition(indexDefinition);
    if (indexDefnUpdated) {
      String indexMetaPath = metaClient.getIndexDefinitionPath();
      // update table config if necessary
      if (!metaClient.getTableConfig().getProps().containsKey(HoodieTableConfig.RELATIVE_INDEX_DEFINITION_PATH.key())
          || !metaClient.getTableConfig().getRelativeIndexDefinitionPath().isPresent()) {
        metaClient.getTableConfig().setValue(HoodieTableConfig.RELATIVE_INDEX_DEFINITION_PATH, FSUtils.getRelativePartitionPath(metaClient.getBasePath(), new StoragePath(indexMetaPath)));
        HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(), metaClient.getTableConfig().getProps());
      }
    }
  }

  /**
   * Create a expression index.
   */
  public abstract void create(HoodieTableMetaClient metaClient, String indexName, String indexType, Map<String, Map<String, String>> columns, Map<String, String> options,
                              Map<String, String> tableProperties) throws Exception;

  /**
   * Creates or updated the col stats index definition.
   * @param metaClient data table's {@link HoodieTableMetaClient} instance.
   * @param columnsToIndex list of columns to index.
   */
  public abstract void createOrUpdateColumnStatsIndexDefinition(HoodieTableMetaClient metaClient, List<String> columnsToIndex);

  /**
   * Drop an index. By default, ignore drop if index does not exist.
   *
   * @param metaClient        {@link HoodieTableMetaClient} instance
   * @param indexName         index name for the index to be dropped
   * @param ignoreIfNotExists ignore drop if index does not exist
   */
  public abstract void drop(HoodieTableMetaClient metaClient, String indexName, boolean ignoreIfNotExists);
}
