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

package org.apache.hudi.index;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieIndexingConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.exception.HoodieMetadataIndexException;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.storage.StorageSchemes;
import org.apache.hudi.table.action.index.BaseHoodieIndexClient;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.collection.JavaConverters;

import static org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE_METADATA_INDEX_BLOOM_FILTER;
import static org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS;
import static org.apache.hudi.common.config.HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP;
import static org.apache.hudi.index.HoodieIndexUtils.indexExists;
import static org.apache.hudi.index.HoodieIndexUtils.register;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.existingIndexVersionOrDefault;

public class HoodieSparkIndexClient extends BaseHoodieIndexClient {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieSparkIndexClient.class);

  private Option<SparkSession> sparkSessionOpt = Option.empty();
  private Option<HoodieWriteConfig> writeConfigOpt = Option.empty();
  private Option<HoodieEngineContext> engineContextOpt = Option.empty();

  public HoodieSparkIndexClient(SparkSession sparkSession) {
    this(Option.of(sparkSession), Option.empty(), Option.empty());
  }

  public HoodieSparkIndexClient(HoodieWriteConfig writeConfig, HoodieEngineContext engineContext) {
    this(Option.empty(), Option.of(writeConfig), Option.of(engineContext));
  }

  public HoodieSparkIndexClient(Option<SparkSession> sparkSessionOpt, Option<HoodieWriteConfig> writeConfig, Option<HoodieEngineContext> engineContext) {
    super();
    this.sparkSessionOpt = sparkSessionOpt;
    this.writeConfigOpt = writeConfig;
    this.engineContextOpt = engineContext;
  }

  @Override
  public void create(HoodieTableMetaClient metaClient, String userIndexName, String indexType, Map<String, Map<String, String>> columns, Map<String, String> options,
                     Map<String, String> tableProperties) throws Exception {
    if (indexType.equals(PARTITION_NAME_SECONDARY_INDEX) || indexType.equals(PARTITION_NAME_BLOOM_FILTERS)
        || indexType.equals(PARTITION_NAME_COLUMN_STATS)) {
      createExpressionOrSecondaryIndex(metaClient, userIndexName, indexType, columns, options, tableProperties);
    } else {
      createRecordIndex(metaClient, userIndexName, indexType);
    }
  }

  private void createRecordIndex(HoodieTableMetaClient metaClient, String userIndexName, String indexType) {
    if (!userIndexName.equals(PARTITION_NAME_RECORD_INDEX)) {
      throw new HoodieIndexException("Record index should be named as record_index");
    }

    String fullIndexName = PARTITION_NAME_RECORD_INDEX;
    if (indexExists(metaClient, fullIndexName)) {
      throw new HoodieMetadataIndexException("Index already exists: " + userIndexName);
    }

    HoodieIndexVersion version = HoodieIndexVersion.getCurrentVersion(metaClient.getTableConfig().getTableVersion(), MetadataPartitionType.RECORD_INDEX);
    LOG.info("Creating index {} using version {}", fullIndexName, version);
    try (SparkRDDWriteClient writeClient = getWriteClient(metaClient, Option.empty(), Option.of(indexType))) {
      // generate index plan
      HoodieIndexVersion currentVersion = HoodieIndexVersion.getCurrentVersion(
          metaClient.getTableConfig().getTableVersion(), MetadataPartitionType.RECORD_INDEX);
      Option<String> indexInstantTime = doSchedule(
          writeClient, metaClient, fullIndexName, MetadataPartitionType.RECORD_INDEX, currentVersion);
      if (indexInstantTime.isPresent()) {
        // build index
        writeClient.index(indexInstantTime.get());
      } else {
        throw new HoodieMetadataIndexException("Scheduling of index action did not return any instant.");
      }
    } catch (Throwable t) {
      drop(metaClient, fullIndexName, Option.empty());
      throw t;
    }
  }

  @Override
  public void createOrUpdateColumnStatsIndexDefinition(HoodieTableMetaClient metaClient, List<String> columnsToIndex) {
    HoodieIndexDefinition indexDefinition = HoodieIndexDefinition.newBuilder()
        .withIndexName(PARTITION_NAME_COLUMN_STATS)
        .withIndexType(PARTITION_NAME_COLUMN_STATS)
        .withIndexFunction(PARTITION_NAME_COLUMN_STATS)
        .withSourceFields(columnsToIndex)
        // Use the existing version if exists, otherwise fall back to the default version.
        .withVersion(existingIndexVersionOrDefault(PARTITION_NAME_COLUMN_STATS, metaClient))
        .withIndexOptions(Collections.EMPTY_MAP)
        .build();
    LOG.info("Registering Or Updating the index " + PARTITION_NAME_COLUMN_STATS);
    register(metaClient, indexDefinition);
  }

  private void createExpressionOrSecondaryIndex(HoodieTableMetaClient metaClient, String userIndexName, String indexType,
                                                Map<String, Map<String, String>> columns, Map<String, String> options, Map<String, String> tableProperties) throws Exception {
    HoodieIndexDefinition indexDefinition = HoodieIndexUtils.getSecondaryOrExpressionIndexDefinition(metaClient, userIndexName, indexType, columns, options, tableProperties);
    if (!metaClient.getTableConfig().getRelativeIndexDefinitionPath().isPresent()
        || !metaClient.getIndexForMetadataPartition(indexDefinition.getIndexName()).isPresent()) {
      LOG.info("Index definition is not present. Registering the index first");
      register(metaClient, indexDefinition);
    }

    ValidationUtils.checkState(metaClient.getIndexMetadata().isPresent(), "Index definition is not present");

    LOG.info("Creating index {}", indexDefinition);
    Option<HoodieIndexDefinition> expressionIndexDefinitionOpt = Option.ofNullable(indexDefinition);
    try (SparkRDDWriteClient writeClient = getWriteClient(metaClient, expressionIndexDefinitionOpt, Option.of(indexType))) {
      MetadataPartitionType partitionType = indexType.equals(PARTITION_NAME_SECONDARY_INDEX) ? MetadataPartitionType.SECONDARY_INDEX : MetadataPartitionType.EXPRESSION_INDEX;
      // generate index plan
      HoodieIndexVersion currentVersion = HoodieIndexVersion.getCurrentVersion(metaClient.getTableConfig().getTableVersion(), MetadataPartitionType.RECORD_INDEX);

      Option<String> indexInstantTime = doSchedule(
          writeClient, metaClient, indexDefinition.getIndexName(), partitionType, currentVersion);
      if (indexInstantTime.isPresent()) {
        // build index
        writeClient.index(indexInstantTime.get());
      } else {
        throw new HoodieMetadataIndexException("Scheduling of index action did not return any instant.");
      }
    } catch (Throwable t) {
      LOG.warn("Error while creating index: {}. So drop it.", indexDefinition.getIndexName(), t);
      drop(metaClient, indexDefinition.getIndexName(), Option.ofNullable(indexDefinition));
      throw t;
    }
  }

  private void drop(HoodieTableMetaClient metaClient, String indexName, Option<HoodieIndexDefinition> indexDefinitionOpt) {
    LOG.info("Dropping index {}", indexName);
    try (SparkRDDWriteClient writeClient = getWriteClient(metaClient, indexDefinitionOpt, Option.empty())) {
      writeClient.dropIndex(Collections.singletonList(indexName));
    }
  }

  @Override
  public void drop(HoodieTableMetaClient metaClient, String indexName, boolean ignoreIfNotExists) {
    LOG.info("Dropping index {}", indexName);
    Option<HoodieIndexDefinition> indexDefinitionOpt = metaClient.getIndexMetadata()
        .map(HoodieIndexMetadata::getIndexDefinitions)
        .map(definition -> definition.get(indexName));
    try (SparkRDDWriteClient writeClient = getWriteClient(metaClient, indexDefinitionOpt, Option.empty())) {
      writeClient.dropIndex(Collections.singletonList(indexName));
    }
  }

  private SparkRDDWriteClient getWriteClient(HoodieTableMetaClient metaClient, Option<HoodieIndexDefinition> indexDefinitionOpt,
                                             Option<String> indexTypeOpt) {
    try {
      TableSchemaResolver schemaUtil = new TableSchemaResolver(metaClient);
      String schemaStr = schemaUtil.getTableAvroSchema(false).toString();
      TypedProperties props = getProps(metaClient, indexDefinitionOpt, indexTypeOpt, schemaStr);
      if (!engineContextOpt.isPresent()) {
        engineContextOpt = Option.of(new HoodieSparkEngineContext(new JavaSparkContext(sparkSessionOpt.get().sparkContext())));
      }
      HoodieWriteConfig localWriteConfig = HoodieWriteConfig.newBuilder()
          .withPath(metaClient.getBasePath())
          .withProperties(props)
          .withEmbeddedTimelineServerEnabled(false)
          .withSchema(schemaStr)
          .withEngineType(EngineType.SPARK).build();
      return new SparkRDDWriteClient(engineContextOpt.get(), localWriteConfig, Option.empty());
    } catch (Exception e) {
      throw new HoodieException("Failed to create write client while performing index operation ", e);
    }
  }

  private TypedProperties getProps(HoodieTableMetaClient metaClient, Option<HoodieIndexDefinition> indexDefinitionOpt,
                                   Option<String> indexTypeOpt, String schemaStr) {
    if (writeConfigOpt.isPresent()) {
      return writeConfigOpt.get().getProps();
    } else {
      TypedProperties typedProperties = metaClient.getTableConfig().getProps();
      JavaConverters.mapAsJavaMapConverter(sparkSessionOpt.get().sqlContext().getAllConfs()).asJava().forEach((k, v) -> {
        if (k.startsWith("hoodie.")) {
          typedProperties.put(k, v);
        }
      });
      typedProperties.putAll(buildWriteConfig(metaClient, indexDefinitionOpt, indexTypeOpt));
      typedProperties.put(HoodieWriteConfig.AVRO_SCHEMA_STRING.key(), schemaStr);
      return typedProperties;
    }
  }

  private static Option<String> doSchedule(SparkRDDWriteClient<HoodieRecordPayload> client, HoodieTableMetaClient metaClient,
                                           String indexName, MetadataPartitionType partitionType, HoodieIndexVersion version) {
    List<MetadataPartitionType> partitionTypes = Collections.singletonList(partitionType);
    if (metaClient.getTableConfig().getMetadataPartitions().isEmpty()) {
      throw new HoodieException("Metadata table is not yet initialized. Initialize FILES partition before any other partition " + Arrays.toString(partitionTypes.toArray()));
    }
    return client.scheduleIndexing(partitionTypes, Collections.singletonList(indexName));
  }

  private static Map<String, String> buildWriteConfig(HoodieTableMetaClient metaClient, Option<HoodieIndexDefinition> indexDefinitionOpt,
                                                      Option<String> indexTypeOpt) {
    Map<String, String> writeConfig = new HashMap<>();
    if (metaClient.getTableConfig().isMetadataTableAvailable()) {
      writeConfig.put(HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(), WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.name());
      writeConfig.putAll(getLockOptions(metaClient.getBasePath().toString(),
          metaClient.getBasePath().toUri().getScheme(), new TypedProperties()));

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

      if (indexTypeOpt.isPresent()) {
        String indexType = indexTypeOpt.get();
        if (indexType.equals(PARTITION_NAME_RECORD_INDEX)) {
          writeConfig.put(RECORD_INDEX_ENABLE_PROP.key(), "true");
        }
      }
    }

    indexDefinitionOpt.ifPresent(indexDefinition ->
        HoodieIndexingConfig.fromIndexDefinition(indexDefinition).getProps().forEach((key, value) -> writeConfig.put(key.toString(), value.toString())));
    return writeConfig;
  }

  static Map<String, String> getLockOptions(String tablePath, String scheme, TypedProperties lockConfig) {
    List<String> customSupportedFSs = lockConfig.getStringList(HoodieCommonConfig.HOODIE_FS_ATOMIC_CREATION_SUPPORT.key(), ",", new ArrayList<String>());
    if (scheme == null || customSupportedFSs.contains(scheme) || StorageSchemes.isAtomicCreationSupported(scheme)) {
      TypedProperties props = FileSystemBasedLockProvider.getLockConfig(tablePath);
      Map<String, String> toReturn = new HashMap<>();
      props.stringPropertyNames().stream().forEach(key -> toReturn.put(key, props.getString(key)));
      return toReturn;
    } else {
      return Collections.emptyMap();
    }
  }
}
