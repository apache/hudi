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

package org.apache.hudi;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.config.HoodieIndexingConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieMetadataIndexException;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.action.index.functional.BaseHoodieIndexClient;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.collection.JavaConverters;

import static org.apache.hudi.HoodieConversionUtils.mapAsScalaImmutableMap;
import static org.apache.hudi.HoodieConversionUtils.toScalaOption;
import static org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE_METADATA_INDEX_BLOOM_FILTER;
import static org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS;
import static org.apache.hudi.common.config.HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_FUNCTIONAL_INDEX_PREFIX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX_PREFIX;

public class HoodieSparkIndexClient extends BaseHoodieIndexClient {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieSparkIndexClient.class);

  private static volatile HoodieSparkIndexClient _instance;

  private final SparkSession sparkSession;

  private HoodieSparkIndexClient(SparkSession sparkSession) {
    super();
    this.sparkSession = sparkSession;
  }

  public static HoodieSparkIndexClient getInstance(SparkSession sparkSession) {
    if (_instance == null) {
      synchronized (HoodieSparkIndexClient.class) {
        if (_instance == null) {
          _instance = new HoodieSparkIndexClient(sparkSession);
        }
      }
    }
    return _instance;
  }

  @Override
  public void create(HoodieTableMetaClient metaClient, String userIndexName, String indexType, Map<String, Map<String, String>> columns, Map<String, String> options) {
    String fullIndexName = indexType.equals(PARTITION_NAME_SECONDARY_INDEX)
        ? PARTITION_NAME_SECONDARY_INDEX_PREFIX + userIndexName
        : PARTITION_NAME_FUNCTIONAL_INDEX_PREFIX + userIndexName;
    if (indexExists(metaClient, fullIndexName)) {
      throw new HoodieMetadataIndexException("Index already exists: " + userIndexName);
    }
    checkArgument(columns.size() == 1, "Only one column can be indexed for functional or secondary index.");

    if (!isEligibleForIndexing(metaClient, indexType, options)) {
      throw new HoodieMetadataIndexException("Not eligible for indexing: " + indexType + ", indexName: " + userIndexName);
    }

    if (!metaClient.getTableConfig().getRelativeIndexDefinitionPath().isPresent()
        || !metaClient.getIndexMetadata().isPresent()
        || !metaClient.getIndexMetadata().get().getIndexDefinitions().containsKey(fullIndexName)) {
      LOG.info("Index definition is not present. Registering the index first");
      register(metaClient, fullIndexName, indexType, columns, options);
    }

    ValidationUtils.checkState(metaClient.getIndexMetadata().isPresent(), "Index definition is not present");

    LOG.info("Creating index {} of using {}", fullIndexName, indexType);
    Option<HoodieIndexDefinition> functionalIndexDefinitionOpt = Option.ofNullable(metaClient.getIndexMetadata().get().getIndexDefinitions().get(fullIndexName));
    try (SparkRDDWriteClient writeClient = HoodieCLIUtils.createHoodieWriteClient(
        sparkSession, metaClient.getBasePath().toString(), mapAsScalaImmutableMap(buildWriteConfig(metaClient, functionalIndexDefinitionOpt)), toScalaOption(Option.empty()))) {
      // generate index plan
      Option<String> indexInstantTime = doSchedule(writeClient, metaClient, fullIndexName);
      if (indexInstantTime.isPresent()) {
        // build index
        writeClient.index(indexInstantTime.get());
      } else {
        throw new HoodieMetadataIndexException("Scheduling of index action did not return any instant.");
      }
    }
  }

  @Override
  public void drop(HoodieTableMetaClient metaClient, String indexName, boolean ignoreIfNotExists) {
    LOG.info("Dropping index {}", indexName);
    Option<HoodieIndexDefinition> indexDefinitionOpt = Option.ofNullable(metaClient.getIndexMetadata().get().getIndexDefinitions().get(indexName));
    try (SparkRDDWriteClient writeClient = HoodieCLIUtils.createHoodieWriteClient(
        sparkSession, metaClient.getBasePath().toString(), mapAsScalaImmutableMap(buildWriteConfig(metaClient, indexDefinitionOpt)), toScalaOption(Option.empty()))) {
      writeClient.dropIndex(Collections.singletonList(indexName));
    }
  }

  private static Option<String> doSchedule(SparkRDDWriteClient<HoodieRecordPayload> client, HoodieTableMetaClient metaClient, String indexName) {
    List<MetadataPartitionType> partitionTypes = Collections.singletonList(MetadataPartitionType.FUNCTIONAL_INDEX);
    checkArgument(partitionTypes.size() == 1, "Currently, only one index type can be scheduled at a time.");
    if (metaClient.getTableConfig().getMetadataPartitions().isEmpty()) {
      throw new HoodieException("Metadata table is not yet initialized. Initialize FILES partition before any other partition " + Arrays.toString(partitionTypes.toArray()));
    }
    return client.scheduleIndexing(partitionTypes, Collections.singletonList(indexName));
  }

  private static boolean indexExists(HoodieTableMetaClient metaClient, String indexName) {
    return metaClient.getTableConfig().getMetadataPartitions().stream().anyMatch(partition -> partition.equals(indexName));
  }

  private static Map<String, String> buildWriteConfig(HoodieTableMetaClient metaClient, Option<HoodieIndexDefinition> indexDefinitionOpt) {
    Map<String, String> writeConfig = new HashMap<>();
    if (metaClient.getTableConfig().isMetadataTableAvailable()) {
      writeConfig.put(HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(), WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.name());
      writeConfig.putAll(JavaConverters.mapAsJavaMapConverter(HoodieCLIUtils.getLockOptions(metaClient.getBasePath().toString(),
              metaClient.getBasePath().toUri().getScheme(), new TypedProperties())).asJava());

      // [HUDI-7472] Ensure write-config contains the existing MDT partition to prevent those from getting deleted
      metaClient.getTableConfig().getMetadataPartitions().forEach(partitionPath -> {
        if (partitionPath.equals(MetadataPartitionType.RECORD_INDEX.getPartitionPath())) {
          writeConfig.put(RECORD_INDEX_ENABLE_PROP.key(), "true");
        }

        if (partitionPath.equals(MetadataPartitionType.BLOOM_FILTERS.getPartitionPath())) {
          writeConfig.put(ENABLE_METADATA_INDEX_BLOOM_FILTER.key(), "true");
        }

        if (partitionPath.equals(MetadataPartitionType.COLUMN_STATS.getPartitionPath())) {
          writeConfig.put(ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "true");
        }
      });
    }

    indexDefinitionOpt.ifPresent(indexDefinition ->
        HoodieIndexingConfig.fromIndexDefinition(indexDefinition).getProps().forEach((key, value) -> writeConfig.put(key.toString(), value.toString())));
    return writeConfig;
  }

  private static boolean isEligibleForIndexing(HoodieTableMetaClient metaClient, String indexType, Map<String, String> options) {
    // for secondary index, record index is a must
    if (indexType.equals(PARTITION_NAME_SECONDARY_INDEX)) {
      // either record index is enabled or record index partition is already present
      return metaClient.getTableConfig().getMetadataPartitions().stream().anyMatch(partition -> partition.equals(MetadataPartitionType.RECORD_INDEX.getPartitionPath()))
          || Boolean.parseBoolean(options.getOrDefault(RECORD_INDEX_ENABLE_PROP.key(), RECORD_INDEX_ENABLE_PROP.defaultValue().toString()));
    }
    return true;
  }
}
