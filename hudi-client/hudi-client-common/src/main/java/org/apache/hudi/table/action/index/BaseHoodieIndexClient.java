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

package org.apache.hudi.table.action.index;

import org.apache.hudi.common.table.HoodieTableMetaClient;

import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@NoArgsConstructor
public abstract class BaseHoodieIndexClient {

  /**
   * Create a metadata index.
   */
  public abstract void create(HoodieTableMetaClient metaClient, String indexName, String indexType, Map<String, Map<String, String>> columns, Map<String, String> options,
                              Map<String, String> tableProperties) throws Exception;

  /**
   * Creates or updated the col stats index definition.
   *
   * @param metaClient     data table's {@link HoodieTableMetaClient} instance.
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
