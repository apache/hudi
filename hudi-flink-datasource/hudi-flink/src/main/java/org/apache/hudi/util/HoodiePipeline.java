/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.util;

import org.apache.hudi.adapter.Utils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.HoodieTableFactory;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *  A tool class to construct hoodie flink pipeline.
 *
 *  <p>How to use ?</p>
 *  Method {@link #builder(String)} returns a pipeline builder. The builder
 *  can then define the hudi table columns, primary keys and partitions.
 *
 *  <p>An example:</p>
 *  <pre>
 *    HoodiePipeline.Builder builder = HoodiePipeline.builder("myTable");
 *    DataStreamSink<?> sinkStream = builder
 *        .column("f0 int")
 *        .column("f1 varchar(10)")
 *        .column("f2 varchar(20)")
 *        .pk("f0,f1")
 *        .partition("f2")
 *        .sink(input, false);
 *  </pre>
 */
public class HoodiePipeline {

  private static final Logger LOG = LogManager.getLogger(HoodiePipeline.class);

  /**
   * Returns the builder for hoodie pipeline construction.
   */
  public static Builder builder(String tableName) {
    return new Builder(tableName);
  }

  /**
   * Builder for hudi source/sink pipeline construction.
   */
  public static class Builder {
    private final String tableName;
    private final List<String> columns;
    private final Map<String, String> options;

    private String pk;
    private List<String> partitions;

    private Builder(String tableName) {
      this.tableName = tableName;
      this.columns = new ArrayList<>();
      this.options = new HashMap<>();
      this.partitions = new ArrayList<>();
    }

    /**
     * Add a table column definition.
     *
     * @param column the column format should be in the form like 'f0 int'
     */
    public Builder column(String column) {
      this.columns.add(column);
      return this;
    }

    /**
     * Add primary keys.
     */
    public Builder pk(String... pks) {
      this.pk = String.join(",", pks);
      return this;
    }

    /**
     * Add partition fields.
     */
    public Builder partition(String... partitions) {
      this.partitions = new ArrayList<>(Arrays.asList(partitions));
      return this;
    }

    /**
     * Add a config option.
     */
    public Builder option(ConfigOption<?> option, Object val) {
      this.options.put(option.key(), val.toString());
      return this;
    }

    public Builder option(String key, Object val) {
      this.options.put(key, val.toString());
      return this;
    }

    public Builder options(Map<String, String> options) {
      this.options.putAll(options);
      return this;
    }

    public DataStreamSink<?> sink(DataStream<RowData> input, boolean bounded) {
      TableDescriptor tableDescriptor = getTableDescriptor();
      return HoodiePipeline.sink(input, tableDescriptor.getTableId(), tableDescriptor.getResolvedCatalogTable(), bounded);
    }

    public TableDescriptor getTableDescriptor() {
      EnvironmentSettings environmentSettings = EnvironmentSettings
              .newInstance()
              .build();
      TableEnvironmentImpl tableEnv = TableEnvironmentImpl.create(environmentSettings);
      String sql = getCreateHoodieTableDDL(this.tableName, this.columns, this.options, this.pk, this.partitions);
      tableEnv.executeSql(sql);
      String currentCatalog = tableEnv.getCurrentCatalog();
      ResolvedCatalogTable catalogTable = null;
      String defaultDatabase = null;
      try {
        Catalog catalog = tableEnv.getCatalog(currentCatalog).get();
        defaultDatabase = catalog.getDefaultDatabase();
        catalogTable = (ResolvedCatalogTable) catalog.getTable(new ObjectPath(defaultDatabase, this.tableName));
      } catch (TableNotExistException e) {
        throw new HoodieException("Create table " + this.tableName + " exception", e);
      }
      ObjectIdentifier tableId = ObjectIdentifier.of(currentCatalog, defaultDatabase, this.tableName);
      return new TableDescriptor(tableId, catalogTable);
    }

    public DataStream<RowData> source(StreamExecutionEnvironment execEnv) {
      TableDescriptor tableDescriptor = getTableDescriptor();
      return HoodiePipeline.source(execEnv, tableDescriptor.tableId, tableDescriptor.getResolvedCatalogTable());
    }
  }

  private static String getCreateHoodieTableDDL(
      String tableName,
      List<String> fields,
      Map<String, String> options,
      String pkField,
      List<String> partitionField) {
    StringBuilder builder = new StringBuilder();
    builder.append("create table ")
           .append(tableName)
           .append("(\n");
    for (String field : fields) {
      builder.append("  ")
             .append(field)
             .append(",\n");
    }
    builder.append("  PRIMARY KEY(")
           .append(pkField)
           .append(") NOT ENFORCED\n")
           .append(")\n");
    if (!partitionField.isEmpty()) {
      String partitons = partitionField
              .stream()
              .map(partitionName -> "`" + partitionName + "`")
              .collect(Collectors.joining(","));
      builder.append("PARTITIONED BY (")
             .append(partitons)
             .append(")\n");
    }
    builder.append("with ('connector' = 'hudi'");
    options.forEach((k, v) -> builder
            .append(",\n")
            .append("  '")
            .append(k)
            .append("' = '")
            .append(v)
            .append("'"));
    builder.append("\n)");
    return builder.toString();
  }

  /**
   * Returns the data stream sink with given catalog table.
   *
   * @param input        The input datastream
   * @param tablePath    The table path to the hoodie table in the catalog
   * @param catalogTable The hoodie catalog table
   * @param isBounded    A flag indicating whether the input data stream is bounded
   */
  private static DataStreamSink<?> sink(DataStream<RowData> input, ObjectIdentifier tablePath, ResolvedCatalogTable catalogTable, boolean isBounded) {
    FactoryUtil.DefaultDynamicTableContext context = Utils.getTableContext(tablePath, catalogTable, Configuration.fromMap(catalogTable.getOptions()));
    HoodieTableFactory hoodieTableFactory = new HoodieTableFactory();
    return ((DataStreamSinkProvider) hoodieTableFactory.createDynamicTableSink(context)
            .getSinkRuntimeProvider(new SinkRuntimeProviderContext(isBounded)))
            .consumeDataStream(input);
  }

  /**
   * Returns the data stream source with given catalog table.
   *
   * @param execEnv      The execution environment
   * @param tablePath    The table path to the hoodie table in the catalog
   * @param catalogTable The hoodie catalog table
   */
  private static DataStream<RowData> source(StreamExecutionEnvironment execEnv, ObjectIdentifier tablePath, ResolvedCatalogTable catalogTable) {
    FactoryUtil.DefaultDynamicTableContext context = Utils.getTableContext(tablePath, catalogTable, Configuration.fromMap(catalogTable.getOptions()));
    HoodieTableFactory hoodieTableFactory = new HoodieTableFactory();
    DataStreamScanProvider dataStreamScanProvider = (DataStreamScanProvider) ((ScanTableSource) hoodieTableFactory
            .createDynamicTableSource(context))
            .getScanRuntimeProvider(new ScanRuntimeProviderContext());
    return  dataStreamScanProvider.produceDataStream(execEnv);
  }

  /***
   *  A POJO that contains tableId and resolvedCatalogTable.
   */
  public static class TableDescriptor {
    private final ObjectIdentifier tableId;
    private final ResolvedCatalogTable resolvedCatalogTable;

    public TableDescriptor(ObjectIdentifier tableId, ResolvedCatalogTable resolvedCatalogTable) {
      this.tableId = tableId;
      this.resolvedCatalogTable = resolvedCatalogTable;
    }

    public ObjectIdentifier getTableId() {
      return tableId;
    }

    public ResolvedCatalogTable getResolvedCatalogTable() {
      return resolvedCatalogTable;
    }
  }
}
