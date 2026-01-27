/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator;
import org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.DataTypeUtils;
import org.apache.hudi.util.HoodieSchemaConverter;
import org.apache.hudi.util.SerializableSchema;
import org.apache.hudi.util.StreamerUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SORT_ASYNC_MERGE_ENABLED;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SORT_MAX_NUM_FILE_HANDLES;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.INPUT_TIME_UNIT;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_INPUT_DATE_FORMAT;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_OUTPUT_DATE_FORMAT;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_OUTPUT_TIMEZONE_FORMAT;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_TYPE_FIELD;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/**
 * Hoodie data source/sink factory.
 */
@Slf4j
public class HoodieTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

  public static final String FACTORY_ID = "hudi";

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    Configuration conf = FlinkOptions.fromMap(context.getCatalogTable().getOptions());
    String path = getTablePath(conf);
    enrichOptionsFromTableConfig(path, conf);

    // set common parameters
    conf.setString(FlinkOptions.TABLE_NAME.key(), context.getObjectIdentifier().getObjectName());
    conf.setString(FlinkOptions.DATABASE_NAME.key(), context.getObjectIdentifier().getDatabaseName());
    setupKeyGenRelatedOptions(conf, context.getCatalogTable());
    ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
    setupAdditionalOptions(conf, context.getObjectIdentifier(), schema.toPhysicalRowDataType().notNull().getLogicalType());

    // set read specific parameters
    setupReadOptions(conf);
    setupCommitsRetaining(conf);

    return new HoodieTableSource(
        SerializableSchema.create(schema),
        new StoragePath(path),
        context.getCatalogTable().getPartitionKeys(),
        conf.get(FlinkOptions.PARTITION_DEFAULT_NAME),
        conf);
  }

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    Configuration conf = FlinkOptions.fromMap(context.getCatalogTable().getOptions());
    String path = getTablePath(conf);
    enrichOptionsFromTableConfig(path, conf);

    // set common parameters
    conf.setString(FlinkOptions.TABLE_NAME.key(), context.getObjectIdentifier().getObjectName());
    conf.setString(FlinkOptions.DATABASE_NAME.key(), context.getObjectIdentifier().getDatabaseName());
    setupKeyGenRelatedOptions(conf, context.getCatalogTable());
    ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
    setupAdditionalOptions(conf, context.getObjectIdentifier(), schema.toPhysicalRowDataType().notNull().getLogicalType());

    // set write specific parameters
    sanityCheck(conf, schema);
    setupWriteOptions(conf);
    setupSortOptions(conf, context.getConfiguration());
    setupCommitsRetaining(conf);

    return new HoodieTableSink(conf, schema);
  }

  /**
   * Supplement the table config options if not specified.
   */
  private static void enrichOptionsFromTableConfig(String basePath, Configuration conf) {
    StreamerUtil.getTableConfig(basePath, HadoopConfigurations.getHadoopConf(conf))
        .ifPresent(tableConfig -> {
          if (tableConfig.contains(HoodieTableConfig.RECORDKEY_FIELDS)
              && !conf.contains(FlinkOptions.RECORD_KEY_FIELD)) {
            conf.set(FlinkOptions.RECORD_KEY_FIELD, tableConfig.getString(HoodieTableConfig.RECORDKEY_FIELDS));
          }
          if (tableConfig.contains(HoodieTableConfig.ORDERING_FIELDS)
              && !conf.contains(FlinkOptions.ORDERING_FIELDS)) {
            conf.set(FlinkOptions.ORDERING_FIELDS, tableConfig.getString(HoodieTableConfig.ORDERING_FIELDS));
          }
          if (tableConfig.contains(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE)
              && !conf.contains(FlinkOptions.HIVE_STYLE_PARTITIONING)) {
            conf.set(FlinkOptions.HIVE_STYLE_PARTITIONING, tableConfig.getBoolean(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE));
          }
          if (tableConfig.contains(HoodieTableConfig.TYPE) && conf.contains(FlinkOptions.TABLE_TYPE)) {
            if (!tableConfig.getString(HoodieTableConfig.TYPE).equals(conf.get(FlinkOptions.TABLE_TYPE))) {
              log.error("Table type conflict : {} in {} and {} in table options. Update your config to match the table type in hoodie.properties.",
                  tableConfig.getString(HoodieTableConfig.TYPE), HoodieTableConfig.HOODIE_PROPERTIES_FILE, conf.get(FlinkOptions.TABLE_TYPE));
              conf.set(FlinkOptions.TABLE_TYPE, tableConfig.getString(HoodieTableConfig.TYPE));
            }
          }
          if (tableConfig.contains(HoodieTableConfig.TYPE)
              && !conf.contains(FlinkOptions.TABLE_TYPE)) {
            conf.set(FlinkOptions.TABLE_TYPE, tableConfig.getString(HoodieTableConfig.TYPE));
          }
          if (tableConfig.contains(HoodieTableConfig.PAYLOAD_CLASS_NAME)
              && !conf.contains(FlinkOptions.PAYLOAD_CLASS_NAME)) {
            conf.set(FlinkOptions.PAYLOAD_CLASS_NAME, tableConfig.getString(HoodieTableConfig.PAYLOAD_CLASS_NAME));
          }
        });
  }

  @Override
  public String factoryIdentifier() {
    return FACTORY_ID;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Collections.singleton(FlinkOptions.PATH);
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return FlinkOptions.optionalOptions();
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  /**
   * Get table path with configuration check
   */
  private static String getTablePath(Configuration conf) {
    String path = conf.get(FlinkOptions.PATH);
    checkArgument(StringUtils.nonEmpty(path), "Option [path] should not be empty.");
    return path;
  }

  /**
   * The sanity check.
   *
   * @param conf   The table options
   * @param schema The table schema
   */
  private static void sanityCheck(Configuration conf, ResolvedSchema schema) {
    checkTableType(conf);
    checkIndexType(conf);

    if (!OptionsResolver.isAppendMode(conf)) {
      checkRecordKey(conf, schema);
    }
    StreamerUtil.checkOrderingFields(conf, schema.getColumnNames());
  }

  /**
   * Validate the index type.
   */
  private static void checkIndexType(Configuration conf) {
    String indexTypeStr = conf.get(FlinkOptions.INDEX_TYPE);
    if (StringUtils.nonEmpty(indexTypeStr)) {
      HoodieIndexConfig.INDEX_TYPE.checkValues(indexTypeStr);
    }
    if (OptionsResolver.getIndexType(conf) == HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX) {
      ValidationUtils.checkArgument(conf.get(FlinkOptions.METADATA_ENABLED),
          "Metadata table should be enabled when index.type is GLOBAL_RECORD_LEVEL_INDEX.");
    }
  }

  /**
   * Validate the table type.
   */
  private static void checkTableType(Configuration conf) {
    String tableType = conf.get(FlinkOptions.TABLE_TYPE);
    if (StringUtils.isNullOrEmpty(tableType)) {
      return;
    }
    ValidationUtils.checkArgument(
        HoodieTableType.COPY_ON_WRITE.name().equals(tableType)
            || HoodieTableType.MERGE_ON_READ.name().equals(tableType),
        () -> "Invalid table type : " + tableType + ". Table type should be either "
            + HoodieTableType.COPY_ON_WRITE.name() + "  or " + HoodieTableType.MERGE_ON_READ.name() + ".");
  }

  /**
   * Validate the record key.
   */
  private static void checkRecordKey(Configuration conf, ResolvedSchema schema) {
    List<String> fields = schema.getColumnNames();
    if (schema.getPrimaryKey().isEmpty()) {
      String[] recordKeys = OptionsResolver.getRecordKeyStr(conf).split(",");
      Arrays.stream(recordKeys)
          .filter(field -> !fields.contains(field))
          .findAny()
          .ifPresent(f -> {
            throw new HoodieValidationException("Field '" + f + "' specified in option "
                + "'" + FlinkOptions.RECORD_KEY_FIELD.key() + "' does not exist in the table schema.");
          });
    }
  }

  /**
   * Sets up the additional set of config options, for instance, related to Hive sync
   *
   * @param conf      The configuration to set up
   * @param tablePath The table path
   * @param rowType   The specified table row type
   */
  private static void setupAdditionalOptions(
      Configuration conf,
      ObjectIdentifier tablePath,
      LogicalType rowType) {
    setupHiveOptions(conf, tablePath);
    // infer avro schema from physical DDL schema
    inferAvroSchema(conf, rowType);
  }

  /**
   * Sets up options related to Hoodie key generation
   */
  private static void setupKeyGenRelatedOptions(Configuration conf, CatalogTable table) {
    List<String> pkColumns = table.getSchema().getPrimaryKey()
        .map(pk -> pk.getColumns()).orElse(Collections.emptyList());
    // PRIMARY KEY syntax has higher priority and rewrites FlinkOptions#RECORD_KEY_FIELD option
    if (!pkColumns.isEmpty()) {
      String recordKey = String.join(",", pkColumns);
      String hoodieStyleRecordKey = conf.get(FlinkOptions.RECORD_KEY_FIELD);
      if (StringUtils.nonEmpty(hoodieStyleRecordKey)
          && !hoodieStyleRecordKey.equalsIgnoreCase(recordKey)) {
        log.warn("PRIMARY KEY was set as '{}' along with '{}' = '{}'. "
                + "Value of the PRIMARY KEY will be used and the second option will be ignored.",
            recordKey, FlinkOptions.RECORD_KEY_FIELD.key(), hoodieStyleRecordKey);
      }
      conf.set(FlinkOptions.RECORD_KEY_FIELD, recordKey);
    }

    // PARTITIONED BY syntax has higher priority and rewrites FlinkOptions#PARTITION_PATH_FIELD option
    List<String> partitionKeys = table.getPartitionKeys();
    if (!partitionKeys.isEmpty()) {
      String partitionVal = String.join(",", partitionKeys);
      String hoodiePartitionVal = conf.get(FlinkOptions.PARTITION_PATH_FIELD);
      if (StringUtils.nonEmpty(hoodiePartitionVal)
          && !hoodiePartitionVal.equalsIgnoreCase(partitionVal)) {
        log.warn("PARTITIONED BY was set as '{}' along with '{}' = '{}'. "
                + "Value of the PARTITIONED BY will be used and the second option will be ignored.",
            partitionVal, FlinkOptions.PARTITION_PATH_FIELD.key(), hoodiePartitionVal);
      }
      conf.set(FlinkOptions.PARTITION_PATH_FIELD, partitionVal);
    }

    if (conf.get(FlinkOptions.INDEX_TYPE).equals(HoodieIndex.IndexType.BUCKET.name())) {
      if (conf.get(FlinkOptions.INDEX_KEY_FIELD).isEmpty()) {
        // set index keys equal to record keys if absent for bucket index
        log.info("'{}' is not set, therefore '{}' value will be used as index key instead",
            FlinkOptions.INDEX_KEY_FIELD.key(),
            FlinkOptions.RECORD_KEY_FIELD.key());
        conf.set(FlinkOptions.INDEX_KEY_FIELD, conf.get(FlinkOptions.RECORD_KEY_FIELD));
      } else {
        // check that index keys is a subset of record keys
        Set<String> recordKeySet =
            Arrays.stream(OptionsResolver.getRecordKeyStr(conf).split(",")).collect(Collectors.toSet());
        Set<String> indexKeySet =
            Arrays.stream(conf.get(FlinkOptions.INDEX_KEY_FIELD).split(",")).collect(Collectors.toSet());
        if (!recordKeySet.containsAll(indexKeySet)) {
          throw new HoodieValidationException(
              FlinkOptions.INDEX_KEY_FIELD + " should be a subset of or equal to the recordKey fields");
        }
      }
    }

    // tweak the key gen class if possible
    final String[] partitions = conf.get(FlinkOptions.PARTITION_PATH_FIELD).split(",");
    final String[] pks = OptionsResolver.getRecordKeyStr(conf).split(",");
    if (partitions.length == 1) {
      final String partitionField = partitions[0];
      if (partitionField.isEmpty()) {
        conf.set(FlinkOptions.KEYGEN_CLASS_NAME, NonpartitionedAvroKeyGenerator.class.getName());
        log.info("Table option [{}] is reset to {} because this is a non-partitioned table",
            FlinkOptions.KEYGEN_CLASS_NAME.key(), NonpartitionedAvroKeyGenerator.class.getName());
        return;
      }
      DataType partitionFieldType = table.getSchema().getFieldDataType(partitionField)
          .orElseThrow(() -> new HoodieValidationException("Field " + partitionField + " does not exist"));
      if (pks.length <= 1 && DataTypeUtils.isDatetimeType(partitionFieldType)) {
        // timestamp based key gen only supports simple primary key
        setupTimestampKeygenOptions(conf, partitionFieldType);
        return;
      }
    }
    boolean complexHoodieKey = pks.length > 1 || partitions.length > 1;
    StreamerUtil.checkKeygenGenerator(complexHoodieKey, conf);
  }

  /**
   * Sets up the keygen options when the partition path is datetime type.
   *
   * <p>The UTC timezone is used as default.
   */
  public static void setupTimestampKeygenOptions(Configuration conf, DataType fieldType) {
    if (conf.contains(FlinkOptions.KEYGEN_CLASS_NAME)) {
      // the keygen clazz has been set up explicitly, skipping
      return;
    }

    conf.set(FlinkOptions.KEYGEN_CLASS_NAME, TimestampBasedAvroKeyGenerator.class.getName());
    log.info("Table option [{}] is reset to {} because datetime partitioning turns on",
        FlinkOptions.KEYGEN_CLASS_NAME.key(), TimestampBasedAvroKeyGenerator.class.getName());
    if (DataTypeUtils.isTimestampType(fieldType)) {
      int precision = DataTypeUtils.precision(fieldType.getLogicalType());
      if (precision == 0) {
        // seconds
        conf.setString(TIMESTAMP_TYPE_FIELD.key(),
            TimestampBasedAvroKeyGenerator.TimestampType.UNIX_TIMESTAMP.name());
      } else if (precision == 3) {
        // milliseconds
        conf.setString(TIMESTAMP_TYPE_FIELD.key(),
            TimestampBasedAvroKeyGenerator.TimestampType.EPOCHMILLISECONDS.name());
      }
      String outputPartitionFormat = conf.getOptional(FlinkOptions.PARTITION_FORMAT).orElse(FlinkOptions.PARTITION_FORMAT_HOUR);
      conf.setString(TIMESTAMP_OUTPUT_DATE_FORMAT.key(), outputPartitionFormat);
    } else {
      conf.setString(TIMESTAMP_TYPE_FIELD.key(),
          TimestampBasedAvroKeyGenerator.TimestampType.SCALAR.name());
      conf.setString(INPUT_TIME_UNIT.key(), TimeUnit.DAYS.toString());

      String outputPartitionFormat = conf.getOptional(FlinkOptions.PARTITION_FORMAT).orElse(FlinkOptions.PARTITION_FORMAT_DAY);
      conf.setString(TIMESTAMP_OUTPUT_DATE_FORMAT.key(), outputPartitionFormat);
      // the option is actually useless, it only works for validation
      conf.setString(TIMESTAMP_INPUT_DATE_FORMAT.key(), FlinkOptions.PARTITION_FORMAT_DAY);
    }
    conf.setString(TIMESTAMP_OUTPUT_TIMEZONE_FORMAT.key(), "UTC");
  }

  /**
   * Sets up the compaction options from the table definition.
   */
  private static void setupCommitsRetaining(Configuration conf) {
    int commitsToRetain = conf.get(FlinkOptions.CLEAN_RETAIN_COMMITS);
    int minCommitsToKeep = conf.get(FlinkOptions.ARCHIVE_MIN_COMMITS);
    if (commitsToRetain >= minCommitsToKeep) {
      log.info("Table option [{}] is reset to {} to be greater than {}={},\n"
              + "to avoid risk of missing data from few instants in incremental pull",
          FlinkOptions.ARCHIVE_MIN_COMMITS.key(), commitsToRetain + 10,
          FlinkOptions.CLEAN_RETAIN_COMMITS.key(), commitsToRetain);
      conf.set(FlinkOptions.ARCHIVE_MIN_COMMITS, commitsToRetain + 10);
      conf.set(FlinkOptions.ARCHIVE_MAX_COMMITS, commitsToRetain + 20);
    }
  }

  /**
   * Sets up the hive options from the table definition.
   */
  private static void setupHiveOptions(Configuration conf, ObjectIdentifier tablePath) {
    if (!conf.contains(FlinkOptions.HIVE_SYNC_DB)) {
      conf.set(FlinkOptions.HIVE_SYNC_DB, tablePath.getDatabaseName());
    }
    if (!conf.contains(FlinkOptions.HIVE_SYNC_TABLE)) {
      conf.set(FlinkOptions.HIVE_SYNC_TABLE, tablePath.getObjectName());
    }
  }

  /**
   * Sets up the read options from the table definition.
   */
  private static void setupReadOptions(Configuration conf) {
    if (OptionsResolver.isIncrementalQuery(conf)) {
      conf.set(FlinkOptions.QUERY_TYPE, FlinkOptions.QUERY_TYPE_INCREMENTAL);
    }
  }

  /**
   * Sets up the write options from the table definition.
   */
  private static void setupWriteOptions(Configuration conf) {
    if (FlinkOptions.isDefaultValueDefined(conf, FlinkOptions.OPERATION)
        && OptionsResolver.isCowTable(conf)) {
      conf.set(FlinkOptions.PRE_COMBINE, true);
    }
    HoodieIndex.IndexType indexType = OptionsResolver.getIndexType(conf);
    // enable hoodie record index if the index type is configured as GLOBAL_RECORD_LEVEL_INDEX.
    if (indexType == HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX) {
      conf.setString(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key(), "true");
      conf.set(FlinkOptions.INDEX_GLOBAL_ENABLED, true);
    }
  }

  /**
   * Sets up the table exec sort options.
   */
  private static void setupSortOptions(Configuration conf, ReadableConfig contextConfig) {
    if (contextConfig.getOptional(TABLE_EXEC_SORT_MAX_NUM_FILE_HANDLES).isPresent()) {
      conf.set(TABLE_EXEC_SORT_MAX_NUM_FILE_HANDLES,
          contextConfig.get(TABLE_EXEC_SORT_MAX_NUM_FILE_HANDLES));
    }
    if (contextConfig.getOptional(TABLE_EXEC_SPILL_COMPRESSION_ENABLED).isPresent()) {
      conf.set(TABLE_EXEC_SPILL_COMPRESSION_ENABLED,
          contextConfig.get(TABLE_EXEC_SPILL_COMPRESSION_ENABLED));
    }
    if (contextConfig.getOptional(TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE).isPresent()) {
      conf.set(TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE,
          contextConfig.get(TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE));
    }
    if (contextConfig.getOptional(TABLE_EXEC_SORT_ASYNC_MERGE_ENABLED).isPresent()) {
      conf.set(TABLE_EXEC_SORT_ASYNC_MERGE_ENABLED,
          contextConfig.get(TABLE_EXEC_SORT_ASYNC_MERGE_ENABLED));
    }
  }

  /**
   * Inferences the deserialization Avro schema from the table schema (e.g. the DDL)
   * if both options {@link FlinkOptions#SOURCE_AVRO_SCHEMA_PATH} and
   * {@link FlinkOptions#SOURCE_AVRO_SCHEMA} are not specified.
   *
   * @param conf    The configuration
   * @param rowType The specified table row type
   */
  private static void inferAvroSchema(Configuration conf, LogicalType rowType) {
    if (conf.getOptional(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH).isEmpty()
        && conf.getOptional(FlinkOptions.SOURCE_AVRO_SCHEMA).isEmpty()) {
      String inferredSchema = HoodieSchemaConverter.convertToSchema(rowType, HoodieSchemaUtils.getRecordQualifiedName(conf.get(FlinkOptions.TABLE_NAME))).toString();
      conf.set(FlinkOptions.SOURCE_AVRO_SCHEMA, inferredSchema);
    }
  }
}
