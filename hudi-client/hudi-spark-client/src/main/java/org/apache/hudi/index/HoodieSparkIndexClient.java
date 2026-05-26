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
import org.apache.hudi.common.config.HoodieIndexingConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.index.vector.VectorIndexOptions;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.exception.HoodieMetadataIndexException;
import org.apache.hudi.index.record.HoodieRecordIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.action.index.BaseHoodieIndexClient;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.collection.JavaConverters;

import static org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE_METADATA_INDEX_BLOOM_FILTER;
import static org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS;
import static org.apache.hudi.common.config.HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP;
import static org.apache.hudi.index.HoodieIndexUtils.indexExists;
import static org.apache.hudi.index.HoodieIndexUtils.register;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_VECTOR_INDEX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.existingIndexVersionOrDefault;

@Slf4j
public class HoodieSparkIndexClient extends BaseHoodieIndexClient {

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
    } else if (indexType.equals(PARTITION_NAME_VECTOR_INDEX)) {
      createVectorIndex(metaClient, userIndexName, columns, options);
    } else {
      createRecordIndex(metaClient, userIndexName, indexType, options);
    }
  }

  private void createRecordIndex(HoodieTableMetaClient metaClient, String userIndexName, String indexType, Map<String, String> options) {
    if (!userIndexName.equals(PARTITION_NAME_RECORD_INDEX)) {
      throw new HoodieIndexException("Record index should be named as record_index");
    }

    String fullIndexName = PARTITION_NAME_RECORD_INDEX;
    if (indexExists(metaClient, fullIndexName)) {
      throw new HoodieMetadataIndexException("Index already exists: " + userIndexName);
    }

    Map<String, String> overrideOpts = Collections.emptyMap();
    if (HoodieRecordIndex.isPartitioned(options)) {
      overrideOpts = Collections.singletonMap(HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key(), "true");
    }
    HoodieIndexVersion version = HoodieIndexVersion.getCurrentVersion(metaClient.getTableConfig().getTableVersion(), MetadataPartitionType.RECORD_INDEX);
    log.info("Creating index {} using version {}", fullIndexName, version);
    try (SparkRDDWriteClient writeClient = getWriteClient(metaClient, Option.empty(), Option.of(indexType), overrideOpts)) {
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
    log.info("Registering or updating index: {} of type: {}", indexDefinition.getIndexName(), indexDefinition.getIndexType());
    register(metaClient, indexDefinition);
  }

  private void createExpressionOrSecondaryIndex(HoodieTableMetaClient metaClient, String userIndexName, String indexType,
                                                Map<String, Map<String, String>> columns, Map<String, String> options, Map<String, String> tableProperties) throws Exception {
    HoodieIndexDefinition indexDefinition = HoodieIndexUtils.getSecondaryOrExpressionIndexDefinition(metaClient, userIndexName, indexType, columns, options, tableProperties);
    if (!metaClient.getTableConfig().getRelativeIndexDefinitionPath().isPresent()
        || !metaClient.getIndexForMetadataPartition(indexDefinition.getIndexName()).isPresent()) {
      log.info("Index definition is not present. Registering index: {} of type: {}", indexDefinition.getIndexName(), indexDefinition.getIndexType());
      register(metaClient, indexDefinition);
    }

    ValidationUtils.checkState(metaClient.getIndexMetadata().isPresent(), "Index definition is not present");

    log.info("Creating index {}", indexDefinition);
    Option<HoodieIndexDefinition> expressionIndexDefinitionOpt = Option.ofNullable(indexDefinition);
    try (SparkRDDWriteClient writeClient = getWriteClient(metaClient, expressionIndexDefinitionOpt, Option.of(indexType), Collections.emptyMap())) {
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
      log.error("Error while creating index: {}. Index will be dropped.", indexDefinition.getIndexName(), t);
      drop(metaClient, indexDefinition.getIndexName(), Option.ofNullable(indexDefinition));
      throw t;
    }
  }

  private void createVectorIndex(HoodieTableMetaClient metaClient,
                                 String userIndexName,
                                 Map<String, Map<String, String>> columns,
                                 Map<String, String> options) throws Exception {
    HoodieIndexDefinition indexDefinition = HoodieIndexUtils.getVectorIndexDefinition(metaClient, userIndexName, columns, options);
    if (!metaClient.getTableConfig().getRelativeIndexDefinitionPath().isPresent()
        || !metaClient.getIndexForMetadataPartition(indexDefinition.getIndexName()).isPresent()) {
      log.info("Index definition is not present. Registering index: {} of type: {}", indexDefinition.getIndexName(), indexDefinition.getIndexType());
      register(metaClient, indexDefinition);
    }

    ValidationUtils.checkState(metaClient.getIndexMetadata().isPresent(), "Index definition is not present");

    log.info("Creating vector index {}", indexDefinition);
    Option<HoodieIndexDefinition> indexDefinitionOpt = Option.of(indexDefinition);
    try (SparkRDDWriteClient writeClient = getWriteClient(metaClient, indexDefinitionOpt, Option.of(PARTITION_NAME_VECTOR_INDEX), Collections.emptyMap())) {
      HoodieIndexVersion currentVersion = HoodieIndexVersion.getCurrentVersion(
          metaClient.getTableConfig().getTableVersion(), MetadataPartitionType.VECTOR_INDEX);
      Option<String> indexInstantTime = doSchedule(
          writeClient, metaClient, indexDefinition.getIndexName(), MetadataPartitionType.VECTOR_INDEX, currentVersion);
      if (indexInstantTime.isPresent()) {
        writeClient.index(indexInstantTime.get());
        if (shouldMaterializeRaBitQColumnsOnCreate(indexDefinition)) {
          materializeRaBitQColumns(metaClient, indexDefinition);
        }
      } else {
        throw new HoodieMetadataIndexException("Scheduling of index action did not return any instant.");
      }
    } catch (Throwable t) {
      log.error("Error while creating vector index: {}. Index will be dropped.", indexDefinition.getIndexName(), t);
      drop(metaClient, indexDefinition.getIndexName(), Option.of(indexDefinition));
      throw t;
    }
  }

  private void drop(HoodieTableMetaClient metaClient, String indexName, Option<HoodieIndexDefinition> indexDefinitionOpt) {
    log.info("Dropping index {}", indexName);
    try (SparkRDDWriteClient writeClient = getWriteClient(metaClient, indexDefinitionOpt, Option.empty(), Collections.emptyMap())) {
      writeClient.dropIndex(Collections.singletonList(indexName));
    }
  }

  private boolean shouldMaterializeRaBitQColumnsOnCreate(HoodieIndexDefinition indexDefinition) {
    return VectorIndexOptions.DEFAULT_QUANTIZER.equals(VectorIndexOptions.getQuantizer(indexDefinition.getIndexOptions()))
        && VectorIndexOptions.shouldMaterializeRaBitQColumnsOnCreate(indexDefinition.getIndexOptions());
  }

  private void materializeRaBitQColumns(HoodieTableMetaClient metaClient, HoodieIndexDefinition indexDefinition) {
    ValidationUtils.checkState(sparkSessionOpt.isPresent(),
        "SparkSession is required to materialize RaBitQ hidden columns during index creation");

    log.info("Materializing RaBitQ hidden columns for vector index {} via insert_overwrite_table rewrite",
        indexDefinition.getIndexName());

    String basePath = metaClient.getBasePath().toString();
    Dataset<Row> rowsToRewrite = dropMetadataColumns(
        sparkSessionOpt.get().read().format("hudi").load(basePath));

    rowsToRewrite.write()
        .format("hudi")
        .options(getPropsForRewrite(metaClient))
        .option("hoodie.datasource.write.operation", WriteOperationType.INSERT_OVERWRITE_TABLE.value())
        .mode(SaveMode.Overwrite)
        .save(basePath);
  }

  private Dataset<Row> dropMetadataColumns(Dataset<Row> dataset) {
    return dataset
        .drop(HoodieRecord.RECORD_KEY_METADATA_FIELD)
        .drop(HoodieRecord.PARTITION_PATH_METADATA_FIELD)
        .drop(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD)
        .drop(HoodieRecord.FILENAME_METADATA_FIELD)
        .drop(HoodieRecord.COMMIT_TIME_METADATA_FIELD)
        .drop(HoodieRecord.OPERATION_METADATA_FIELD);
  }

  private Map<String, String> getPropsForRewrite(HoodieTableMetaClient metaClient) {
    Map<String, String> propsMap = new HashMap<>();
    metaClient.getTableConfig().getProps().forEach((k, v) -> propsMap.put(k.toString(), v.toString()));
    propsMap.put(HoodieWriteConfig.SKIP_DEFAULT_PARTITION_VALIDATION.key(), "true");
    propsMap.put(HoodieWriteConfig.TBL_NAME.key(), metaClient.getTableConfig().getTableName());
    propsMap.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), metaClient.getTableConfig().getRecordKeyFieldProp());
    propsMap.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(),
        HoodieTableConfig.getPartitionFieldPropForKeyGenerator(metaClient.getTableConfig()).orElse(""));
    propsMap.put(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), metaClient.getTableConfig().getKeyGeneratorClassName());
    metaClient.getTableConfig().getOrderingFieldsStr()
        .ifPresent(orderingFields -> {
          propsMap.put(HoodieTableConfig.ORDERING_FIELDS.key(), orderingFields);
          propsMap.put(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), orderingFields);
        });
    return propsMap;
  }

  @Override
  public void drop(HoodieTableMetaClient metaClient, String indexName, boolean ignoreIfNotExists) {
    log.info("Dropping index {}", indexName);
    Option<HoodieIndexDefinition> indexDefinitionOpt = metaClient.getIndexMetadata()
        .map(HoodieIndexMetadata::getIndexDefinitions)
        .map(definition -> definition.get(indexName));
    try (SparkRDDWriteClient writeClient = getWriteClient(metaClient, indexDefinitionOpt, Option.empty(), Collections.emptyMap())) {
      writeClient.dropIndex(Collections.singletonList(indexName));
    }
  }

  private SparkRDDWriteClient getWriteClient(HoodieTableMetaClient metaClient, Option<HoodieIndexDefinition> indexDefinitionOpt,
                                             Option<String> indexTypeOpt, Map<String, String> configs) {
    try {
      TableSchemaResolver schemaUtil = new TableSchemaResolver(metaClient);
      String schemaStr = schemaUtil.getTableSchema(false).toString();
      TypedProperties props = getProps(metaClient, indexDefinitionOpt, indexTypeOpt, schemaStr);
      if (!engineContextOpt.isPresent()) {
        engineContextOpt = Option.of(new HoodieSparkEngineContext(new JavaSparkContext(sparkSessionOpt.get().sparkContext())));
      }
      HoodieWriteConfig localWriteConfig = HoodieWriteConfig.newBuilder()
          .withPath(metaClient.getBasePath())
          .withProperties(props)
          .withEmbeddedTimelineServerEnabled(false)
          .withSchema(schemaStr)
          .withEngineType(EngineType.SPARK)
          .withProps(configs)
          .build();
      // Validate if a lock provide class is set properly.
      if (localWriteConfig.getWriteConcurrencyMode().supportsMultiWriter() && StringUtils.isNullOrEmpty(localWriteConfig.getLockProviderClass())) {
        throw new IllegalArgumentException(
            "To create index asynchronously, multi-writer configurations need to be enabled and hence 'hoodie.write.lock.provider' is expected to be set for such cases. "
                + "For single writer mode, feel free to set the config value to org.apache.hudi.client.transaction.lock.InProcessLockProvider and retry index creation");
      }
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

      // [HUDI-7472] Ensure write-config contains the existing MDT partition to prevent those from getting deleted
      metaClient.getTableConfig().getMetadataPartitions().forEach(partitionPath -> {
        if (partitionPath.equals(MetadataPartitionType.RECORD_INDEX.getPartitionPath())) {
          writeConfig.put(GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key(), "true");
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
          writeConfig.put(GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key(), "true");
        }
      }
    }

    indexDefinitionOpt.ifPresent(indexDefinition -> {
      if (!PARTITION_NAME_VECTOR_INDEX.equals(indexDefinition.getIndexType())) {
        HoodieIndexingConfig.fromIndexDefinition(indexDefinition).getProps()
            .forEach((key, value) -> writeConfig.put(key.toString(), value.toString()));
      }
    });
    return writeConfig;
  }
}
