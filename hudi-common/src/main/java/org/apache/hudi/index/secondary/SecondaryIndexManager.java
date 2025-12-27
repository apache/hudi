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

package org.apache.hudi.index.secondary;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieSecondaryIndexException;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Manages secondary index.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class SecondaryIndexManager {

  private static volatile SecondaryIndexManager _instance;

  public static SecondaryIndexManager getInstance() {
    if (_instance == null) {
      synchronized (SecondaryIndexManager.class) {
        if (_instance == null) {
          _instance = new SecondaryIndexManager();
        }
      }
    }

    return _instance;
  }

  /**
   * Create a secondary index for hoodie table, two steps will be performed:
   * 1. Add secondary index metadata to hoodie.properties
   * 2. Trigger build secondary index
   *
   * @param metaClient     Hoodie table meta client
   * @param indexName      The unique secondary index name
   * @param indexType      Index type
   * @param ignoreIfExists Whether ignore the creation if the specific secondary index exists
   * @param columns        The columns referenced by this secondary index, each column
   *                       has its own options
   * @param options        Options for this secondary index
   */
  public void create(
      HoodieTableMetaClient metaClient,
      String indexName,
      String indexType,
      boolean ignoreIfExists,
      LinkedHashMap<String, Map<String, String>> columns,
      Map<String, String> options) {
    Option<List<HoodieSecondaryIndex>> secondaryIndexes = SecondaryIndexUtils.getSecondaryIndexes(metaClient);
    Set<String> colNames = columns.keySet();
    HoodieSchema schema;
    try {
      schema = new TableSchemaResolver(metaClient).getTableSchema(false);
    } catch (Exception e) {
      throw new HoodieSecondaryIndexException(
          "Failed to get table avro schema: " + metaClient.getTableConfig().getTableName());
    }

    for (String col : colNames) {
      if (schema.getField(col) == null) {
        throw new HoodieSecondaryIndexException("Field not exists: " + col);
      }
    }

    if (indexExists(secondaryIndexes, indexName, Option.of(indexType), Option.of(colNames))) {
      if (ignoreIfExists) {
        return;
      } else {
        throw new HoodieSecondaryIndexException("Secondary index already exists: " + indexName);
      }
    }

    HoodieSecondaryIndex secondaryIndexToAdd = HoodieSecondaryIndex.builder()
        .setIndexName(indexName)
        .setIndexType(indexType)
        .setColumns(columns)
        .setOptions(options)
        .build();

    List<HoodieSecondaryIndex> newSecondaryIndexes = secondaryIndexes.map(h -> {
      h.add(secondaryIndexToAdd);
      return h;
    }).orElseGet(() -> Collections.singletonList(secondaryIndexToAdd));
    newSecondaryIndexes.sort(new HoodieSecondaryIndex.HoodieIndexCompactor());

    // Persistence secondary indexes' metadata to hoodie.properties file
    Properties updatedProps = new Properties();
    updatedProps.put(HoodieTableConfig.SECONDARY_INDEXES_METADATA.key(),
        SecondaryIndexUtils.toJsonString(newSecondaryIndexes));
    HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(), updatedProps);

    log.info("Success to add secondary index metadata: {}", secondaryIndexToAdd);

    // TODO: build index
  }

  /**
   * Drop a secondary index by index name
   *
   * @param metaClient        Hoodie table meta client
   * @param indexName         The unique secondary index name
   * @param ignoreIfNotExists Whether ignore drop if the specific secondary index no exists
   */
  public void drop(HoodieTableMetaClient metaClient, String indexName, boolean ignoreIfNotExists) {
    Option<List<HoodieSecondaryIndex>> secondaryIndexes = SecondaryIndexUtils.getSecondaryIndexes(metaClient);
    if (!indexExists(secondaryIndexes, indexName, Option.empty(), Option.empty())) {
      if (ignoreIfNotExists) {
        return;
      } else {
        throw new HoodieSecondaryIndexException("Secondary index not exists: " + indexName);
      }
    }

    List<HoodieSecondaryIndex> secondaryIndexesToKeep = secondaryIndexes.get().stream()
        .filter(i -> !i.getIndexName().equals(indexName))
        .sorted(new HoodieSecondaryIndex.HoodieIndexCompactor())
        .collect(Collectors.toList());
    if (CollectionUtils.nonEmpty(secondaryIndexesToKeep)) {
      Properties updatedProps = new Properties();
      updatedProps.put(HoodieTableConfig.SECONDARY_INDEXES_METADATA.key(),
          SecondaryIndexUtils.toJsonString(secondaryIndexesToKeep));
      HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(), updatedProps);
    } else {
      HoodieTableConfig.delete(metaClient.getStorage(), metaClient.getMetaPath(),
          CollectionUtils.createSet(HoodieTableConfig.SECONDARY_INDEXES_METADATA.key()));
    }

    log.info("Success to delete secondary index metadata: {}", indexName);

    // TODO: drop index data
  }

  /**
   * Show secondary indexes from hoodie table
   *
   * @param metaClient Hoodie table meta client
   * @return Indexes in this table
   */
  public Option<List<HoodieSecondaryIndex>> show(HoodieTableMetaClient metaClient) {
    return SecondaryIndexUtils.getSecondaryIndexes(metaClient);
  }

  /**
   * Refresh the specific secondary index
   *
   * @param metaClient Hoodie table meta client
   * @param indexName  The target secondary index name
   */
  public void refresh(HoodieTableMetaClient metaClient, String indexName) {
    // TODO
  }

  /**
   * Check if the specific secondary index exists. When drop a secondary index,
   * only check index name, but for adding a secondary index, we should also
   * check the index type and columns when index name is different.
   *
   * @param secondaryIndexes Current secondary indexes in this table
   * @param indexName        The index name of target secondary index
   * @param indexType        The index type of target secondary index
   * @param colNames         The column names of target secondary index
   * @return true if secondary index exists
   */
  private boolean indexExists(
      Option<List<HoodieSecondaryIndex>> secondaryIndexes,
      String indexName,
      Option<String> indexType,
      Option<Set<String>> colNames) {
    return secondaryIndexes.map(indexes ->
        indexes.stream().anyMatch(index -> {
          if (index.getIndexName().equals(indexName)) {
            return true;
          } else if (indexType.isPresent() && colNames.isPresent()) {
            // When secondary index names are different, we should check index type
            // and index columns to avoid repeatedly creating the same index.
            // For example:
            //   create index idx_name on test using lucene (name);
            //   create index idx_name_1 on test using lucene (name);
            return index.getIndexType().name().equalsIgnoreCase(indexType.get())
                && CollectionUtils.diff(index.getColumns().keySet(), colNames.get()).isEmpty();
          }

          return false;
        })).orElse(false);
  }
}
