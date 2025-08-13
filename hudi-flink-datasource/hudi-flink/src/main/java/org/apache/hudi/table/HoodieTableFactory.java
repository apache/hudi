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

import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator;
import org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.DataTypeUtils;
import org.apache.hudi.util.SerializableSchema;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class HoodieTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieTableFactory.class);

  public static final String FACTORY_ID = "hudi";

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    Configuration conf = FlinkOptions.fromMap(context.getCatalogTable().getOptions());
    StoragePath path = new StoragePath(conf.getOptional(FlinkOptions.PATH).orElseThrow(() ->
        new ValidationException("Option [path] should not be empty.")));
    setupTableOptions(conf.get(FlinkOptions.PATH), conf);
    ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
    setupConfOptions(conf, context.getObjectIdentifier(), context.getCatalogTable(), schema);
    return new HoodieTableSource(
        SerializableSchema.create(schema),
        path,
        context.getCatalogTable().getPartitionKeys(),
        conf.get(FlinkOptions.PARTITION_DEFAULT_NAME),
        conf);
  }

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    Configuration conf = FlinkOptions.fromMap(context.getCatalogTable().getOptions());
    checkArgument(!StringUtils.isNullOrEmpty(conf.get(FlinkOptions.PATH)),
        "Option [path] should not be empty.");
    setupTableOptions(conf.get(FlinkOptions.PATH), conf);
    ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
    sanityCheck(conf, schema);
    setupConfOptions(conf, context.getObjectIdentifier(), context.getCatalogTable(), schema);
    setupSortOptions(conf, context.getConfiguration());
    return new HoodieTableSink(conf, schema);
  }

  /**
   * Supplement the table config options if not specified.
   */
  private void setupTableOptions(String basePath, Configuration conf) {
    StreamerUtil.getTableConfig(basePath, HadoopConfigurations.getHadoopConf(conf))
        .ifPresent(tableConfig -> {
          if (tableConfig.contains(HoodieTableConfig.RECORDKEY_FIELDS)
              && !conf.contains(FlinkOptions.RECORD_KEY_FIELD)) {
            conf.set(FlinkOptions.RECORD_KEY_FIELD, tableConfig.getString(HoodieTableConfig.RECORDKEY_FIELDS));
          }
          if (tableConfig.contains(HoodieTableConfig.PRECOMBINE_FIELDS)
              && !conf.contains(FlinkOptions.PRECOMBINE_FIELDS)) {
            conf.set(FlinkOptions.PRECOMBINE_FIELDS, tableConfig.getString(HoodieTableConfig.PRECOMBINE_FIELDS));
          }
          if (tableConfig.contains(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE)
              && !conf.contains(FlinkOptions.HIVE_STYLE_PARTITIONING)) {
            conf.set(FlinkOptions.HIVE_STYLE_PARTITIONING, tableConfig.getBoolean(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE));
          }
          if (tableConfig.contains(HoodieTableConfig.TYPE) && conf.contains(FlinkOptions.TABLE_TYPE)) {
            if (!tableConfig.getString(HoodieTableConfig.TYPE).equals(conf.get(FlinkOptions.TABLE_TYPE))) {
              LOG.warn(
                  String.format("Table type conflict : %s in %s and %s in table options. Fix the table type as to be in line with the hoodie.properties.",
                      tableConfig.getString(HoodieTableConfig.TYPE), HoodieTableConfig.HOODIE_PROPERTIES_FILE,
                      conf.get(FlinkOptions.TABLE_TYPE)));
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
   * The sanity check.
   *
   * @param conf   The table options
   * @param schema The table schema
   */
  private void sanityCheck(Configuration conf, ResolvedSchema schema) {
    checkTableType(conf);
    checkIndexType(conf);

    if (!OptionsResolver.isAppendMode(conf)) {
      checkRecordKey(conf, schema);
    }
    StreamerUtil.checkPreCombineKey(conf, schema.getColumnNames());
  }

  /**
   * Validate the index type.
   */
  private void checkIndexType(Configuration conf) {
    String indexType = conf.get(FlinkOptions.INDEX_TYPE);
    if (!StringUtils.isNullOrEmpty(indexType)) {
      HoodieIndexConfig.INDEX_TYPE.checkValues(indexType);
    }
  }

  /**
   * Validate the table type.
   */
  private void checkTableType(Configuration conf) {
    String tableType = conf.get(FlinkOptions.TABLE_TYPE);
    if (StringUtils.nonEmpty(tableType)) {
      try {
        HoodieTableType.valueOf(tableType);
      } catch (IllegalArgumentException e) {
        throw new HoodieValidationException("Invalid table type: " + tableType + ". Table type should be either "
                + HoodieTableType.MERGE_ON_READ + " or " + HoodieTableType.COPY_ON_WRITE + ".");
      }
    }
  }

  /**
   * Validate the record key.
   */
  private void checkRecordKey(Configuration conf, ResolvedSchema schema) {
    List<String> fields = schema.getColumnNames();
    if (!schema.getPrimaryKey().isPresent()) {
      String[] recordKeys = conf.get(FlinkOptions.RECORD_KEY_FIELD).split(",");
      if (recordKeys.length == 1
          && FlinkOptions.RECORD_KEY_FIELD.defaultValue().equals(recordKeys[0])
          && !fields.contains(recordKeys[0])) {
        throw new HoodieValidationException("Primary key definition is required, the default primary key field "
            + "'" + FlinkOptions.RECORD_KEY_FIELD.defaultValue() + "' does not exist in the table schema, "
            + "use either PRIMARY KEY syntax or option '" + FlinkOptions.RECORD_KEY_FIELD.key() + "' to speciy.");
      }

      Arrays.stream(recordKeys)
          .filter(field -> !fields.contains(field))
          .findAny()
          .ifPresent(f -> {
            throw new HoodieValidationException("Field '" + f + "' specified in option "
                + "'" + FlinkOptions.RECORD_KEY_FIELD.key() + "' does not exist in the table schema.");
          });
    }
    if (schema.getPrimaryKey().isPresent() && conf.containsKey(FlinkOptions.RECORD_KEY_FIELD.key()))   {
      LOG.warn("PRIMARY KEY syntax and option '" + FlinkOptions.RECORD_KEY_FIELD.key() + "' was used. Priority of the PRIMARY KEY is higher!");
    }
  }

  /**
   * Sets up the config options based on the table definition, for e.g, the table name, primary key.
   *
   * @param conf      The configuration to set up
   * @param tablePath The table path
   * @param table     The catalog table
   * @param schema    The physical schema
   */
  private static void setupConfOptions(
      Configuration conf,
      ObjectIdentifier tablePath,
      CatalogTable table,
      ResolvedSchema schema) {
    // table name
    conf.setString(FlinkOptions.TABLE_NAME.key(), tablePath.getObjectName());
    // database name
    conf.setString(FlinkOptions.DATABASE_NAME.key(), tablePath.getDatabaseName());
    // hoodie key about options
    setupHoodieKeyOptions(conf, table);
    // compaction options
    setupCompactionOptions(conf);
    // hive options
    setupHiveOptions(conf, tablePath);
    // read options
    setupReadOptions(conf);
    // write options
    setupWriteOptions(conf);
    // infer avro schema from physical DDL schema
    inferAvroSchema(conf, schema.toPhysicalRowDataType().notNull().getLogicalType());
  }

  /**
   * Sets up the hoodie key options (e.g. record key and partition key) from the table definition.
   */
  private static void setupHoodieKeyOptions(Configuration conf, CatalogTable table) {
    List<String> pkColumns = table.getSchema().getPrimaryKey()
        .map(pk -> pk.getColumns()).orElse(Collections.emptyList());
    if (pkColumns.size() > 0) {
      // the PRIMARY KEY syntax always has higher priority than option FlinkOptions#RECORD_KEY_FIELD
      String recordKey = String.join(",", pkColumns);
      conf.set(FlinkOptions.RECORD_KEY_FIELD, recordKey);
    }
    List<String> partitionKeys = table.getPartitionKeys();
    if (partitionKeys.size() > 0) {
      // the PARTITIONED BY syntax always has higher priority than option FlinkOptions#PARTITION_PATH_FIELD
      conf.set(FlinkOptions.PARTITION_PATH_FIELD, String.join(",", partitionKeys));
    }
    // set index key for bucket index if not defined
    if (conf.get(FlinkOptions.INDEX_TYPE).equals(HoodieIndex.IndexType.BUCKET.name())) {
      if (conf.get(FlinkOptions.INDEX_KEY_FIELD).isEmpty()) {
        conf.set(FlinkOptions.INDEX_KEY_FIELD, conf.get(FlinkOptions.RECORD_KEY_FIELD));
      } else {
        Set<String> recordKeySet =
            Arrays.stream(conf.get(FlinkOptions.RECORD_KEY_FIELD).split(",")).collect(Collectors.toSet());
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
    final String[] pks = conf.get(FlinkOptions.RECORD_KEY_FIELD).split(",");
    if (partitions.length == 1) {
      final String partitionField = partitions[0];
      if (partitionField.isEmpty()) {
        conf.set(FlinkOptions.KEYGEN_CLASS_NAME, NonpartitionedAvroKeyGenerator.class.getName());
        LOG.info("Table option [{}] is reset to {} because this is a non-partitioned table",
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
    LOG.info("Table option [{}] is reset to {} because datetime partitioning turns on",
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
  private static void setupCompactionOptions(Configuration conf) {
    int commitsToRetain = conf.get(FlinkOptions.CLEAN_RETAIN_COMMITS);
    int minCommitsToKeep = conf.get(FlinkOptions.ARCHIVE_MIN_COMMITS);
    if (commitsToRetain >= minCommitsToKeep) {
      LOG.info("Table option [{}] is reset to {} to be greater than {}={},\n"
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
  }

  /**
   * Sets up the table exec sort options.
   */
  private void setupSortOptions(Configuration conf, ReadableConfig contextConfig) {
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
    if (!conf.getOptional(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH).isPresent()
        && !conf.getOptional(FlinkOptions.SOURCE_AVRO_SCHEMA).isPresent()) {
      String inferredSchema = AvroSchemaConverter.convertToSchema(rowType, AvroSchemaUtils.getAvroRecordQualifiedName(conf.get(FlinkOptions.TABLE_NAME))).toString();
      conf.set(FlinkOptions.SOURCE_AVRO_SCHEMA, inferredSchema);
    }
  }
}
