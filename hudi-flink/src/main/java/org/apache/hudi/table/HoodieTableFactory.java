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

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.hive.MultiPartKeysValueExtractor;
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;
import org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator;
import org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.DataTypeUtils;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Hoodie data source/sink factory.
 */
public class HoodieTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieTableFactory.class);

  public static final String FACTORY_ID = "hudi";

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    helper.validate();

    Configuration conf = (Configuration) helper.getOptions();
    ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
    sanityCheck(conf, schema);
    setupConfOptions(conf, context.getObjectIdentifier().getObjectName(), context.getCatalogTable(), schema);

    Path path = new Path(conf.getOptional(FlinkOptions.PATH).orElseThrow(() ->
        new ValidationException("Option [path] should not be empty.")));
    return new HoodieTableSource(
        schema,
        path,
        context.getCatalogTable().getPartitionKeys(),
        conf.getString(FlinkOptions.PARTITION_DEFAULT_NAME),
        conf);
  }

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    Configuration conf = FlinkOptions.fromMap(context.getCatalogTable().getOptions());
    ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
    sanityCheck(conf, schema);
    setupConfOptions(conf, context.getObjectIdentifier().getObjectName(), context.getCatalogTable(), schema);
    return new HoodieTableSink(conf, schema);
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
    List<String> fields = schema.getColumnNames();

    // validate record key in pk absence.
    if (!schema.getPrimaryKey().isPresent()) {
      Arrays.stream(conf.get(FlinkOptions.RECORD_KEY_FIELD).split(","))
          .filter(field -> !fields.contains(field))
          .findAny()
          .ifPresent(f -> {
            throw new ValidationException("Field '" + f + "' does not exist in the table schema."
                + "Please define primary key or modify 'hoodie.datasource.write.recordkey.field' option.");
          });
    }

    // validate pre_combine key
    String preCombineField = conf.get(FlinkOptions.PRECOMBINE_FIELD);
    if (!fields.contains(preCombineField)) {
      throw new ValidationException("Field " + preCombineField + " does not exist in the table schema."
          + "Please check 'write.precombine.field' option.");
    }
  }

  /**
   * Sets up the config options based on the table definition, for e.g the table name, primary key.
   *
   * @param conf      The configuration to setup
   * @param tableName The table name
   * @param table     The catalog table
   * @param schema    The physical schema
   */
  private static void setupConfOptions(
      Configuration conf,
      String tableName,
      CatalogTable table,
      ResolvedSchema schema) {
    // table name
    conf.setString(FlinkOptions.TABLE_NAME.key(), tableName);
    // hoodie key about options
    setupHoodieKeyOptions(conf, table);
    // compaction options
    setupCompactionOptions(conf);
    // hive options
    setupHiveOptions(conf);
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
    if (complexHoodieKey && FlinkOptions.isDefaultValueDefined(conf, FlinkOptions.KEYGEN_CLASS_NAME)) {
      conf.setString(FlinkOptions.KEYGEN_CLASS_NAME, ComplexAvroKeyGenerator.class.getName());
      LOG.info("Table option [{}] is reset to {} because record key or partition path has two or more fields",
          FlinkOptions.KEYGEN_CLASS_NAME.key(), ComplexAvroKeyGenerator.class.getName());
    }
  }

  /**
   * Sets up the keygen options when the partition path is datetime type.
   *
   * <p>The UTC timezone is used as default.
   */
  public static void setupTimestampKeygenOptions(Configuration conf, DataType fieldType) {
    conf.setString(FlinkOptions.KEYGEN_CLASS_NAME, TimestampBasedAvroKeyGenerator.class.getName());
    LOG.info("Table option [{}] is reset to {} because datetime partitioning turns on",
        FlinkOptions.KEYGEN_CLASS_NAME.key(), TimestampBasedAvroKeyGenerator.class.getName());
    if (DataTypeUtils.isTimestampType(fieldType)) {
      int precision = DataTypeUtils.precision(fieldType.getLogicalType());
      if (precision == 0) {
        // seconds
        conf.setString(TimestampBasedAvroKeyGenerator.Config.TIMESTAMP_TYPE_FIELD_PROP,
            TimestampBasedAvroKeyGenerator.TimestampType.UNIX_TIMESTAMP.name());
      } else if (precision == 3) {
        // milliseconds
        conf.setString(TimestampBasedAvroKeyGenerator.Config.TIMESTAMP_TYPE_FIELD_PROP,
            TimestampBasedAvroKeyGenerator.TimestampType.EPOCHMILLISECONDS.name());
      }
      String partitionFormat = conf.getOptional(FlinkOptions.PARTITION_FORMAT).orElse(FlinkOptions.PARTITION_FORMAT_HOUR);
      conf.setString(TimestampBasedAvroKeyGenerator.Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP, partitionFormat);
    } else {
      conf.setString(TimestampBasedAvroKeyGenerator.Config.TIMESTAMP_TYPE_FIELD_PROP,
          TimestampBasedAvroKeyGenerator.TimestampType.DATE_STRING.name());
      String partitionFormat = conf.getOptional(FlinkOptions.PARTITION_FORMAT).orElse(FlinkOptions.PARTITION_FORMAT_DAY);
      conf.setString(TimestampBasedAvroKeyGenerator.Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP, partitionFormat);
    }
    conf.setString(TimestampBasedAvroKeyGenerator.Config.TIMESTAMP_OUTPUT_TIMEZONE_FORMAT_PROP, "UTC");
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
    if (StreamerUtil.allowDuplicateInserts(conf)) {
      // no need for compaction if insert duplicates is allowed
      conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, false);
      conf.setBoolean(FlinkOptions.COMPACTION_SCHEDULE_ENABLED, false);
    }
  }

  /**
   * Sets up the hive options from the table definition.
   */
  private static void setupHiveOptions(Configuration conf) {
    if (!conf.getBoolean(FlinkOptions.HIVE_STYLE_PARTITIONING)
        && FlinkOptions.isDefaultValueDefined(conf, FlinkOptions.HIVE_SYNC_PARTITION_EXTRACTOR_CLASS_NAME)) {
      conf.setString(FlinkOptions.HIVE_SYNC_PARTITION_EXTRACTOR_CLASS_NAME, MultiPartKeysValueExtractor.class.getName());
    }
  }

  /**
   * Sets up the read options from the table definition.
   */
  private static void setupReadOptions(Configuration conf) {
    if (!conf.getBoolean(FlinkOptions.READ_AS_STREAMING)
        && (conf.getOptional(FlinkOptions.READ_START_COMMIT).isPresent() || conf.getOptional(FlinkOptions.READ_END_COMMIT).isPresent())) {
      conf.setString(FlinkOptions.QUERY_TYPE, FlinkOptions.QUERY_TYPE_INCREMENTAL);
    }
  }

  /**
   * Sets up the write options from the table definition.
   */
  private static void setupWriteOptions(Configuration conf) {
    if (FlinkOptions.isDefaultValueDefined(conf, FlinkOptions.OPERATION)
        && HoodieTableType.valueOf(conf.getString(FlinkOptions.TABLE_TYPE)) == HoodieTableType.COPY_ON_WRITE) {
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
      String inferredSchema = AvroSchemaConverter.convertToSchema(rowType).toString();
      conf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA, inferredSchema);
    }
  }
}
