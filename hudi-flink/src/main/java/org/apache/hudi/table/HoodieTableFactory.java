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
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;
import org.apache.hudi.util.AvroSchemaConverter;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hoodie data source/sink factory.
 */
public class HoodieTableFactory implements TableSourceFactory<RowData>, TableSinkFactory<RowData> {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieTableFactory.class);

  public static final String FACTORY_ID = "hudi";

  @Override
  public TableSource<RowData> createTableSource(TableSourceFactory.Context context) {
    Configuration conf = FlinkOptions.fromMap(context.getTable().getOptions());
    TableSchema schema = TableSchemaUtils.getPhysicalSchema(context.getTable().getSchema());
    setupConfOptions(conf, context.getObjectIdentifier().getObjectName(), context.getTable(), schema);
    // enclosing the code within a try catch block so that we can log the error message.
    // Flink 1.11 did a bad compatibility for the old table factory, it uses the old factory
    // to create the source/sink and catches all the exceptions then tries the new factory.
    //
    // log the error message first so that there is a chance to show the real failure cause.
    try {
      Path path = new Path(conf.getOptional(FlinkOptions.PATH).orElseThrow(() ->
          new ValidationException("Option [path] should not be empty.")));
      return new HoodieTableSource(
          schema,
          path,
          context.getTable().getPartitionKeys(),
          conf.getString(FlinkOptions.PARTITION_DEFAULT_NAME),
          conf);
    } catch (Throwable throwable) {
      LOG.error("Create table source error", throwable);
      throw new HoodieException(throwable);
    }
  }

  @Override
  public TableSink<RowData> createTableSink(TableSinkFactory.Context context) {
    Configuration conf = FlinkOptions.fromMap(context.getTable().getOptions());
    TableSchema schema = TableSchemaUtils.getPhysicalSchema(context.getTable().getSchema());
    setupConfOptions(conf, context.getObjectIdentifier().getObjectName(), context.getTable(), schema);
    return new HoodieTableSink(conf, schema);
  }

  @Override
  public Map<String, String> requiredContext() {
    Map<String, String> context = new HashMap<>();
    context.put(FactoryUtil.CONNECTOR.key(), FACTORY_ID);
    return context;
  }

  @Override
  public List<String> supportedProperties() {
    // contains format properties.
    return Collections.singletonList("*");
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

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
    List<String> partitions = table.getPartitionKeys();
    if (partitions.size() > 0) {
      // the PARTITIONED BY syntax always has higher priority than option FlinkOptions#PARTITION_PATH_FIELD
      conf.setString(FlinkOptions.PARTITION_PATH_FIELD, String.join(",", partitions));
    }
    // tweak the key gen class if possible
    boolean complexHoodieKey = pkColumns.size() > 1 || partitions.size() > 1;
    if (complexHoodieKey && FlinkOptions.isDefaultValueDefined(conf, FlinkOptions.KEYGEN_CLASS)) {
      conf.setString(FlinkOptions.KEYGEN_CLASS, ComplexAvroKeyGenerator.class.getName());
      LOG.info("Table option [{}] is reset to {} because record key or partition path has two or more fields",
          FlinkOptions.KEYGEN_CLASS.key(), ComplexAvroKeyGenerator.class.getName());
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
