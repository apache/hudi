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
import org.apache.hudi.common.config.HoodieFunctionalIndexConfig;
import org.apache.hudi.common.model.HoodieFunctionalIndexDefinition;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieFunctionalIndexException;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.action.index.functional.BaseHoodieFunctionalIndexClient;

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
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

public class HoodieSparkFunctionalIndexClient extends BaseHoodieFunctionalIndexClient {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieSparkFunctionalIndexClient.class);

  private static volatile HoodieSparkFunctionalIndexClient _instance;

  private final SparkSession sparkSession;

  private HoodieSparkFunctionalIndexClient(SparkSession sparkSession) {
    super();
    this.sparkSession = sparkSession;
  }

  public static HoodieSparkFunctionalIndexClient getInstance(SparkSession sparkSession) {
    if (_instance == null) {
      synchronized (HoodieSparkFunctionalIndexClient.class) {
        if (_instance == null) {
          _instance = new HoodieSparkFunctionalIndexClient(sparkSession);
        }
      }
    }
    return _instance;
  }

  @Override
  public void create(HoodieTableMetaClient metaClient, String indexName, String indexType, Map<String, Map<String, String>> columns, Map<String, String> options) {
    indexName = HoodieTableMetadataUtil.PARTITION_NAME_FUNCTIONAL_INDEX_PREFIX + indexName;
    if (indexExists(metaClient, indexName)) {
      throw new HoodieFunctionalIndexException("Index already exists: " + indexName);
    }

    if (!metaClient.getTableConfig().getIndexDefinitionPath().isPresent() || !metaClient.getFunctionalIndexMetadata().isPresent()) {
      LOG.info("Index definition is not present. Registering the index first");
      register(metaClient, indexName, indexType, columns, options);
    }

    ValidationUtils.checkState(metaClient.getFunctionalIndexMetadata().isPresent(), "Index definition is not present");

    LOG.info("Creating index {} of using {}", indexName, indexType);
    HoodieFunctionalIndexDefinition functionalIndexDefinition = metaClient.getFunctionalIndexMetadata().get().getIndexDefinitions().get(indexName);
    try (SparkRDDWriteClient writeClient = HoodieCLIUtils.createHoodieWriteClient(
        sparkSession, metaClient.getBasePathV2().toString(), mapAsScalaImmutableMap(buildWriteConfig(metaClient, functionalIndexDefinition)), toScalaOption(Option.empty()))) {
      // generate index plan
      Option<String> indexInstantTime = doSchedule(writeClient, metaClient);
      if (indexInstantTime.isPresent()) {
        // build index
        writeClient.index(indexInstantTime.get());
      } else {
        throw new HoodieFunctionalIndexException("Scheduling of index action did not return any instant.");
      }
    }
  }

  private static Option<String> doSchedule(SparkRDDWriteClient<HoodieRecordPayload> client, HoodieTableMetaClient metaClient) {
    List<MetadataPartitionType> partitionTypes = Collections.singletonList(MetadataPartitionType.FUNCTIONAL_INDEX);
    checkArgument(partitionTypes.size() == 1, "Currently, only one index type can be scheduled at a time.");
    if (metaClient.getTableConfig().getMetadataPartitions().isEmpty()) {
      throw new HoodieException("Metadata table is not yet initialized. Initialize FILES partition before any other partition " + Arrays.toString(partitionTypes.toArray()));
    }
    return client.scheduleIndexing(partitionTypes);
  }

  private static boolean indexExists(HoodieTableMetaClient metaClient, String indexName) {
    return metaClient.getTableConfig().getMetadataPartitions().stream().anyMatch(partition -> partition.equals(indexName));
  }

  private static Map<String, String> buildWriteConfig(HoodieTableMetaClient metaClient, HoodieFunctionalIndexDefinition indexDefinition) {
    Map<String, String> writeConfig = new HashMap<>();
    if (metaClient.getTableConfig().isMetadataTableAvailable()) {
      if (!writeConfig.containsKey(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key())) {
        writeConfig.put(HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(), WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.name());
        writeConfig.putAll(JavaConverters.mapAsJavaMapConverter(HoodieCLIUtils.getLockOptions(metaClient.getBasePathV2().toString())).asJava());
      }
    }
    HoodieFunctionalIndexConfig.fromIndexDefinition(indexDefinition).getProps().forEach((key, value) -> writeConfig.put(key.toString(), value.toString()));
    return writeConfig;
  }
}
