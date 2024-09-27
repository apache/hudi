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
import org.apache.hudi.exception.HoodieFunctionalIndexException;
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
  public void create(HoodieTableMetaClient metaClient, String indexName, String indexType, Map<String, Map<String, String>> columns, Map<String, String> options) {
    indexName = indexType.equals(PARTITION_NAME_SECONDARY_INDEX) ? PARTITION_NAME_SECONDARY_INDEX_PREFIX + indexName : PARTITION_NAME_FUNCTIONAL_INDEX_PREFIX + indexName;
    if (indexExists(metaClient, indexName)) {
      throw new HoodieFunctionalIndexException("Index already exists: " + indexName);
    }
    checkArgument(columns.size() == 1, "Only one column can be indexed for functional or secondary index.");

    if (!metaClient.getTableConfig().getIndexDefinitionPath().isPresent()
        || !metaClient.getIndexMetadata().isPresent()
        || !metaClient.getIndexMetadata().get().getIndexDefinitions().containsKey(indexName)) {
      LOG.info("Index definition is not present. Registering the index first");
      register(metaClient, indexName, indexType, columns, options);
    }

    ValidationUtils.checkState(metaClient.getIndexMetadata().isPresent(), "Index definition is not present");

    LOG.info("Creating index {} of using {}", indexName, indexType);
    HoodieIndexDefinition functionalIndexDefinition = metaClient.getIndexMetadata().get().getIndexDefinitions().get(indexName);
    try (SparkRDDWriteClient writeClient = HoodieCLIUtils.createHoodieWriteClient(
        sparkSession, metaClient.getBasePath().toString(), mapAsScalaImmutableMap(buildWriteConfig(metaClient, functionalIndexDefinition)), toScalaOption(Option.empty()))) {
      // generate index plan
      Option<String> indexInstantTime = doSchedule(writeClient, metaClient, indexName);
      if (indexInstantTime.isPresent()) {
        // build index
        writeClient.index(indexInstantTime.get());
      } else {
        throw new HoodieFunctionalIndexException("Scheduling of index action did not return any instant.");
      }
    }
  }

  @Override
  public void drop(HoodieTableMetaClient metaClient, String indexName, boolean ignoreIfNotExists) {
    if (!indexExists(metaClient, indexName)) {
      if (ignoreIfNotExists) {
        return;
      } else {
        throw new HoodieFunctionalIndexException("Index does not exist: " + indexName);
      }
    }

    LOG.info("Dropping index {}", indexName);
    HoodieIndexDefinition indexDefinition = metaClient.getIndexMetadata().get().getIndexDefinitions().get(indexName);
    try (SparkRDDWriteClient writeClient = HoodieCLIUtils.createHoodieWriteClient(
        sparkSession, metaClient.getBasePath().toString(), mapAsScalaImmutableMap(buildWriteConfig(metaClient, indexDefinition)), toScalaOption(Option.empty()))) {
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

  private static Map<String, String> buildWriteConfig(HoodieTableMetaClient metaClient, HoodieIndexDefinition indexDefinition) {
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

    HoodieIndexingConfig.fromIndexDefinition(indexDefinition).getProps().forEach((key, value) -> writeConfig.put(key.toString(), value.toString()));
    return writeConfig;
  }
}
