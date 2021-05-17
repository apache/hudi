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

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;
import org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator;
import org.apache.hudi.util.AvroSchemaConverter;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
    TableSchema schema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
    validateRequiredFields(conf, schema);
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
    TableSchema schema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
    validateRequiredFields(conf, schema);
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

  /** Validate required options. For e.g, record key and pre_combine key.
   *
   * @param conf The table options
   * @param schema The table schema
   */
  private void validateRequiredFields(Configuration conf, TableSchema schema) {
    List<String> fields = Arrays.stream(schema.getFieldNames()).collect(Collectors.toList());

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
   * Setup the config options based on the table definition, for e.g the table name, primary key.
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
      TableSchema schema) {
    // table name
    conf.setString(FlinkOptions.TABLE_NAME.key(), tableName);
    // hoodie key about options
    setupHoodieKeyOptions(conf, table);
    // cleaning options
    setupCleaningOptions(conf);
    // infer avro schema from physical DDL schema
    inferAvroSchema(conf, schema.toRowDataType().notNull().getLogicalType());
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
    if (partitions.length == 1 && partitions[0].equals("")) {
      conf.setString(FlinkOptions.KEYGEN_CLASS, NonpartitionedAvroKeyGenerator.class.getName());
      LOG.info("Table option [{}] is reset to {} because this is a non-partitioned table",
          FlinkOptions.KEYGEN_CLASS.key(), NonpartitionedAvroKeyGenerator.class.getName());
      return;
    }
    final String[] pks = conf.getString(FlinkOptions.RECORD_KEY_FIELD).split(",");
    boolean complexHoodieKey = pks.length > 1 || partitions.length > 1;
    if (complexHoodieKey && FlinkOptions.isDefaultValueDefined(conf, FlinkOptions.KEYGEN_CLASS)) {
      conf.setString(FlinkOptions.KEYGEN_CLASS, ComplexAvroKeyGenerator.class.getName());
      LOG.info("Table option [{}] is reset to {} because record key or partition path has two or more fields",
          FlinkOptions.KEYGEN_CLASS.key(), ComplexAvroKeyGenerator.class.getName());
    }
  }

  /**
   * Sets up the cleaning options from the table definition.
   */
  private static void setupCleaningOptions(Configuration conf) {
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
   * Inferences the deserialization Avro schema from the table schema (e.g. the DDL)
   * if both options {@link FlinkOptions#READ_AVRO_SCHEMA_PATH} and
   * {@link FlinkOptions#READ_AVRO_SCHEMA} are not specified.
   *
   * @param conf    The configuration
   * @param rowType The specified table row type
   */
  private static void inferAvroSchema(Configuration conf, LogicalType rowType) {
    if (!conf.getOptional(FlinkOptions.READ_AVRO_SCHEMA_PATH).isPresent()
        && !conf.getOptional(FlinkOptions.READ_AVRO_SCHEMA).isPresent()) {
      String inferredSchema = AvroSchemaConverter.convertToSchema(rowType).toString();
      conf.setString(FlinkOptions.READ_AVRO_SCHEMA, inferredSchema);
    }
  }
}
