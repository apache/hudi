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
import org.apache.hudi.client.model.ModelUtils;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.bucket.expression.utils.ExpressionUtils;
import org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator;
import org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.metrics.FlinkEnvironmentContext;
import org.apache.hudi.sink.clustering.LSMClusteringScheduleMode;
import org.apache.hudi.table.format.mor.lsm.FlinkLsmUtils;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.DataTypeUtils;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;
import static org.apache.hudi.config.HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY;
import static org.apache.hudi.config.HoodieIndexConfig.BUCKET_INDEX_PARTITION_BUCKET_EXPR_PREFIX;

/**
 * Hoodie data source/sink factory.
 */
public class HoodieTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieTableFactory.class);

  public static final String FACTORY_ID = "hudi";

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    Configuration conf = FlinkOptions.fromMap(context.getCatalogTable().getOptions());
    String jobName = context.getConfiguration().get(PipelineOptions.NAME);
    Path path = new Path(conf.getOptional(FlinkOptions.PATH).orElseThrow(() ->
        new ValidationException("Option [path] should not be empty.")));
    setupTableOptions(conf.getString(FlinkOptions.PATH), conf);
    ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
    setupBaizeOptions(jobName, conf);
    setupConfOptions(conf, context.getObjectIdentifier(), context.getCatalogTable(), schema);
    FlinkEnvironmentContext.init();
    return new HoodieTableSource(
        schema,
        path,
        context.getCatalogTable().getPartitionKeys(),
        conf.getString(FlinkOptions.PARTITION_DEFAULT_NAME),
        conf);
  }

  private void setupBaizeOptions(String jobName, Configuration conf) {
    if (!StringUtils.isNullOrEmpty(jobName)) {
      conf.setString("hoodie.metrics.pushgateway.job.name", jobName);
      conf.setBoolean("hoodie.metrics.on", conf.getBoolean(FlinkOptions.METRIC_ON));
      conf.setString("hoodie.metrics.reporter.type",conf.getString(FlinkOptions.REPORT_TYPE));
    }
  }

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    Configuration conf = FlinkOptions.fromMap(context.getCatalogTable().getOptions());
    String jobName = context.getConfiguration().get(PipelineOptions.NAME);
    checkArgument(!StringUtils.isNullOrEmpty(conf.getString(FlinkOptions.PATH)),
        "Option [path] should not be empty.");
    setupTableOptions(conf.getString(FlinkOptions.PATH), conf);
    setupLSMWriteConfig(conf);
    ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
    sanityCheck(conf, schema);
    setupBaizeOptions(jobName, conf);
    setupConfOptions(conf, context.getObjectIdentifier(), context.getCatalogTable(), schema);
    validateLSMClusteringConfig(conf);
    FlinkEnvironmentContext.init();
    return new HoodieTableSink(conf, schema);
  }

  private void setupLSMWriteConfig(Configuration conf) {
    if (OptionsResolver.isLSMBasedLogFormat(conf)) {
      FlinkLsmUtils.setupLSMConfig(conf);
    }
  }

  private void validateLSMClusteringConfig(Configuration conf) {
    Map<String, String> confMap = conf.toMap();
    if (!confMap.containsKey(HoodieTableConfig.HOODIE_LOG_FORMAT.key())) {
      return;
    }
    if (!confMap.get(HoodieTableConfig.HOODIE_LOG_FORMAT.key()).equalsIgnoreCase(HoodieTableConfig.LSM_HOODIE_TABLE_LOG_FORMAT)) {
      return;
    }
    boolean inlineLSMClustering = Boolean.parseBoolean(confMap.get(HoodieClusteringConfig.LSM_INLINE_CLUSTERING.key()));
    boolean scheduleInlineLSMClustering = Boolean.parseBoolean(confMap.get(HoodieClusteringConfig.LSM_SCHEDULE_INLINE_CLUSTERING.key()));
    boolean asyncLSMClustering = conf.getBoolean(FlinkOptions.LSM_CLUSTERING_ASYNC_ENABLED);
    String scheduleLSMClusteringMod = conf.getString(FlinkOptions.LSM_CLUSTERING_SCHEDULE_MODE);
    boolean streamOrAsyncScheduleClustering = scheduleLSMClusteringMod.equalsIgnoreCase(LSMClusteringScheduleMode.STREAM.name())
        || scheduleLSMClusteringMod.equalsIgnoreCase(LSMClusteringScheduleMode.ASYNC.name());

    // Can't turn on both async clustering and inline clustering in Flink job.
    ValidationUtils.checkArgument(!(inlineLSMClustering && asyncLSMClustering), String.format("Either of inline clustering (%s) or "
            + "async clustering (%s) can be enabled. Both can't be set to true at the same time in flink job. %s,%s", HoodieClusteringConfig.LSM_INLINE_CLUSTERING.key(),
        FlinkOptions.LSM_CLUSTERING_ASYNC_ENABLED.key(), inlineLSMClustering, asyncLSMClustering));

    // Can't turn on inline schedule clustering when the mod of lsm schedule clustering is async/sync  in Flink job.
    ValidationUtils.checkArgument(!(streamOrAsyncScheduleClustering && (inlineLSMClustering || scheduleInlineLSMClustering)),
        String.format("Either of inline schedule clustering (%s/%s) or async/sync schedule clustering (%s) can be enabled. Both can't be set to true at the same time in flink job. %s,%s,%s",
            HoodieClusteringConfig.LSM_INLINE_CLUSTERING.key(), HoodieClusteringConfig.LSM_SCHEDULE_INLINE_CLUSTERING.key(),
            FlinkOptions.LSM_CLUSTERING_SCHEDULE_MODE.key(), inlineLSMClustering, scheduleInlineLSMClustering, scheduleLSMClusteringMod));
  }

  /**
   * Supplement the table config options if not specified.
   */
  private void setupTableOptions(String basePath, Configuration conf) {
    Option<HoodieTableConfig> option = StreamerUtil.getTableConfig(basePath, HadoopConfigurations.getHadoopConf(conf));
    option.ifPresent(tableConfig -> {
      if (tableConfig.contains(HoodieTableConfig.RECORDKEY_FIELDS)
          && !conf.contains(FlinkOptions.RECORD_KEY_FIELD)) {
        conf.setString(FlinkOptions.RECORD_KEY_FIELD, tableConfig.getString(HoodieTableConfig.RECORDKEY_FIELDS));
      }
      if (tableConfig.contains(HoodieTableConfig.PRECOMBINE_FIELD)
          && !conf.contains(FlinkOptions.PRECOMBINE_FIELD)) {
        conf.setString(FlinkOptions.PRECOMBINE_FIELD, tableConfig.getString(HoodieTableConfig.PRECOMBINE_FIELD));
      }
      if (tableConfig.contains(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE)
          && !conf.contains(FlinkOptions.HIVE_STYLE_PARTITIONING)) {
        conf.setBoolean(FlinkOptions.HIVE_STYLE_PARTITIONING, tableConfig.getBoolean(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE));
      }
      if (tableConfig.contains(HoodieTableConfig.TYPE) && conf.contains(FlinkOptions.TABLE_TYPE)) {
        if (!tableConfig.getString(HoodieTableConfig.TYPE).equals(conf.get(FlinkOptions.TABLE_TYPE))) {
          LOG.warn(
              String.format("Table type conflict : %s in %s and %s in table options. Fix the table type as to be in line with the hoodie.properties.",
                  tableConfig.getString(HoodieTableConfig.TYPE), HoodieTableConfig.HOODIE_PROPERTIES_FILE,
                  conf.get(FlinkOptions.TABLE_TYPE)));
          conf.setString(FlinkOptions.TABLE_TYPE, tableConfig.getString(HoodieTableConfig.TYPE));
        }
      }
      if (tableConfig.contains(HoodieTableConfig.TYPE)
          && !conf.contains(FlinkOptions.TABLE_TYPE)) {
        conf.setString(FlinkOptions.TABLE_TYPE, tableConfig.getString(HoodieTableConfig.TYPE));
      }
      if (tableConfig.isLSMBasedLogFormat() && tableConfig.contains(HoodieTableConfig.RECORD_MERGER_STRATEGY)
          && !conf.contains(FlinkOptions.TABLE_RECORD_MERGER_STRATEGY)) {
        conf.set(FlinkOptions.TABLE_RECORD_MERGER_STRATEGY, tableConfig.getString(HoodieTableConfig.RECORD_MERGER_STRATEGY));
        conf.set(FlinkOptions.RECORD_MERGER_STRATEGY, tableConfig.getString(HoodieTableConfig.RECORD_MERGER_STRATEGY));
        // 覆盖RECORD_MERGER_IMPLS，此时用户指定的impls不会生效
        conf.set(FlinkOptions.RECORD_MERGER_IMPLS, ModelUtils.getSupportedRecordMerger());
      }
    });

    // HoodieTable 表不存在
    if (!option.isPresent() && OptionsResolver.isLSMBasedLogFormat(conf)) {
      FlinkLsmUtils.setupMergerConfig(conf);
    }
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
    if (!OptionsResolver.isAppendMode(conf)) {
      checkRecordKey(conf, schema);
    }
    StreamerUtil.checkPreCombineKey(conf, schema.getColumnNames());
    StreamerUtil.checkBasePathAndStoragePath(conf);
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
        throw new HoodieValidationException("Primary key definition is required, use either PRIMARY KEY syntax "
            + "or option '" + FlinkOptions.RECORD_KEY_FIELD.key() + "' to specify.");
      }

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
    // 兼容base path
    conf.setString(HoodieWriteConfig.BASE_PATH.key(), conf.getString(FlinkOptions.PATH));
    // set partition bucket expression
    if (conf.getString(FlinkOptions.INDEX_TYPE).equals(HoodieIndex.IndexType.BUCKET.name())
        && conf.getBoolean(FlinkOptions.BUCKET_INDEX_PARTITION_LEVEL)) {
      Properties props = new Properties();
      for (String key : table.getOptions().keySet()) {
        if (key.startsWith(BUCKET_INDEX_PARTITION_BUCKET_EXPR_PREFIX)) {
          props.setProperty(key, table.getOptions().get(key));
        }
      }
      conf.setString(FlinkOptions.BUCKET_INDEX_PARTITION_BUCKET_EXPR, ExpressionUtils.generateBucketExpression(props));
    }
    // async rollback
    if (conf.getBoolean(FlinkOptions.ROLLBACK_ASYNC_ENABLE)) {
      conf.setString(FAILED_WRITES_CLEANER_POLICY.key(), HoodieFailedWritesCleaningPolicy.LAZY.name());
    }
  }

  /**
   * Sets up the hoodie key options (e.g. record key and partition key) from the table definition.
   */
  private static void setupHoodieKeyOptions(Configuration conf, CatalogTable table) {
    List<String> pkColumns = table.getSchema().getPrimaryKey()
        .map(UniqueConstraint::getColumns).orElse(Collections.emptyList());
    if (pkColumns.size() > 0) {
      // the PRIMARY KEY syntax always has higher priority than option FlinkOptions#RECORD_KEY_FIELD
      String recordKey = String.join(",", pkColumns);
      conf.setString(FlinkOptions.RECORD_KEY_FIELD, recordKey);
    }
    List<String> partitionKeys = table.getPartitionKeys();
    if (partitionKeys.size() > 0) {
      // the PARTITIONED BY syntax always has higher priority than option FlinkOptions#PARTITION_PATH_FIELD
      conf.setString(FlinkOptions.PARTITION_PATH_FIELD, String.join(",", partitionKeys));
    }
    // set index key for bucket index if not defined
    if (conf.getString(FlinkOptions.INDEX_TYPE).equals(HoodieIndex.IndexType.BUCKET.name())) {
      if (conf.getString(FlinkOptions.INDEX_KEY_FIELD).isEmpty()) {
        conf.setString(FlinkOptions.INDEX_KEY_FIELD, conf.getString(FlinkOptions.RECORD_KEY_FIELD));
      } else {
        Set<String> recordKeySet =
            Arrays.stream(conf.getString(FlinkOptions.RECORD_KEY_FIELD).split(",")).collect(Collectors.toSet());
        Set<String> indexKeySet =
            Arrays.stream(conf.getString(FlinkOptions.INDEX_KEY_FIELD).split(",")).collect(Collectors.toSet());
        if (!recordKeySet.containsAll(indexKeySet)) {
          throw new HoodieValidationException(
              FlinkOptions.INDEX_KEY_FIELD + " should be a subset of or equal to the recordKey fields");
        }
      }
    }

    // tweak the key gen class if possible
    final String[] partitions = conf.getString(FlinkOptions.PARTITION_PATH_FIELD).split(",");
    final String[] pks = conf.getString(FlinkOptions.RECORD_KEY_FIELD).split(",");
    if (partitions.length == 1) {
      final String partitionField = partitions[0];
      if (partitionField.isEmpty()) {
        conf.setString(FlinkOptions.KEYGEN_CLASS_NAME, NonpartitionedAvroKeyGenerator.class.getName());
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

    conf.setString(FlinkOptions.KEYGEN_CLASS_NAME, TimestampBasedAvroKeyGenerator.class.getName());
    LOG.info("Table option [{}] is reset to {} because datetime partitioning turns on",
        FlinkOptions.KEYGEN_CLASS_NAME.key(), TimestampBasedAvroKeyGenerator.class.getName());
    if (DataTypeUtils.isTimestampType(fieldType)) {
      int precision = DataTypeUtils.precision(fieldType.getLogicalType());
      if (precision == 0) {
        // seconds
        conf.setString(KeyGeneratorOptions.Config.TIMESTAMP_TYPE_FIELD_PROP,
            TimestampBasedAvroKeyGenerator.TimestampType.UNIX_TIMESTAMP.name());
      } else if (precision == 3) {
        // milliseconds
        conf.setString(KeyGeneratorOptions.Config.TIMESTAMP_TYPE_FIELD_PROP,
            TimestampBasedAvroKeyGenerator.TimestampType.EPOCHMILLISECONDS.name());
      }
      String outputPartitionFormat = conf.getOptional(FlinkOptions.PARTITION_FORMAT).orElse(FlinkOptions.PARTITION_FORMAT_HOUR);
      conf.setString(KeyGeneratorOptions.Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP, outputPartitionFormat);
    } else {
      conf.setString(KeyGeneratorOptions.Config.TIMESTAMP_TYPE_FIELD_PROP,
          TimestampBasedAvroKeyGenerator.TimestampType.SCALAR.name());
      conf.setString(KeyGeneratorOptions.Config.INPUT_TIME_UNIT, TimeUnit.DAYS.toString());

      String outputPartitionFormat = conf.getOptional(FlinkOptions.PARTITION_FORMAT).orElse(FlinkOptions.PARTITION_FORMAT_DAY);
      conf.setString(KeyGeneratorOptions.Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP, outputPartitionFormat);
      // the option is actually useless, it only works for validation
      conf.setString(KeyGeneratorOptions.Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP, FlinkOptions.PARTITION_FORMAT_DAY);
    }
    conf.setString(KeyGeneratorOptions.Config.TIMESTAMP_OUTPUT_TIMEZONE_FORMAT_PROP, "UTC");
  }

  /**
   * Sets up the compaction options from the table definition.
   */
  private static void setupCompactionOptions(Configuration conf) {
    int commitsToRetain = conf.getInteger(FlinkOptions.CLEAN_RETAIN_COMMITS);
    int minCommitsToKeep = conf.getInteger(FlinkOptions.ARCHIVE_MIN_COMMITS);
    if (commitsToRetain >= minCommitsToKeep) {
      LOG.info("Table option [{}] is reset to {} to be greater than {}={},\n"
              + "to avoid risk of missing data from few instants in incremental pull",
          FlinkOptions.ARCHIVE_MIN_COMMITS.key(), commitsToRetain + 10,
          FlinkOptions.CLEAN_RETAIN_COMMITS.key(), commitsToRetain);
      conf.setInteger(FlinkOptions.ARCHIVE_MIN_COMMITS, commitsToRetain + 10);
      conf.setInteger(FlinkOptions.ARCHIVE_MAX_COMMITS, commitsToRetain + 20);
    }
  }

  /**
   * Sets up the hive options from the table definition.
   */
  private static void setupHiveOptions(Configuration conf, ObjectIdentifier tablePath) {
    if (!conf.contains(FlinkOptions.HIVE_SYNC_DB)) {
      conf.setString(FlinkOptions.HIVE_SYNC_DB, tablePath.getDatabaseName());
    }
    if (!conf.contains(FlinkOptions.HIVE_SYNC_TABLE)) {
      conf.setString(FlinkOptions.HIVE_SYNC_TABLE, tablePath.getObjectName());
    }
  }

  /**
   * Sets up the read options from the table definition.
   */
  private static void setupReadOptions(Configuration conf) {
    if (OptionsResolver.isIncrementalQuery(conf)) {
      conf.setString(FlinkOptions.QUERY_TYPE, FlinkOptions.QUERY_TYPE_INCREMENTAL);
    }
  }

  /**
   * Sets up the write options from the table definition.
   */
  private static void setupWriteOptions(Configuration conf) {
    if (FlinkOptions.isDefaultValueDefined(conf, FlinkOptions.OPERATION)
        && OptionsResolver.isCowTable(conf)) {
      conf.setBoolean(FlinkOptions.PRE_COMBINE, true);
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
      String inferredSchema = AvroSchemaConverter.convertToSchema(rowType, AvroSchemaUtils.getAvroRecordQualifiedName(conf.getString(FlinkOptions.TABLE_NAME))).toString();
      conf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA, inferredSchema);
    }
  }
}
