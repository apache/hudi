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

package org.apache.hudi.sync.common.util;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The engine-neutral description of how a Hudi table is registered in a Hive-style metastore.
 *
 * <p>Registering a Hudi table means agreeing on a handful of things that have nothing to do with any
 * particular engine: which input format carries the table type, which columns are partition columns
 * and which are data columns, and which properties make the table recognisable to Spark SQL. Those
 * decisions were previously made independently by {@code HMSDDLExecutor} (for hive-sync), by Spark
 * SQL's {@code createHiveDataSourceTable}, and by Flink's {@code TableOptionProperties}. Each
 * arrived at nearly the same answer, and "nearly" is the problem: a table that disagrees on any one
 * of them is a table another engine either cannot read or does not recognise as Hudi at all.
 *
 * <p>This class exposes a normalized form of those decisions in Hudi and JDK types only. It
 * deliberately carries no Hive types: the metastore column type is a {@link HoodieSchemaField} here,
 * and the input/output format and serde are class <em>names</em> rather than classes, so a caller that has no
 * {@code hive-metastore} on its classpath -- the Trino connector, for one -- can consume it and
 * translate to its own metastore types. {@code hudi-hadoop-mr} holds the only existing copy of the
 * format-name mapping ({@code HoodieInputFormatUtils}), and it is not reachable from such a caller,
 * so the names are repeated here as constants and pinned by
 * {@code TestHoodieMetastoreTableDescriptorFormatNames} against the values hive-sync emits.
 *
 * <p>The Spark datasource properties come from {@link SparkDataSourceTableUtils} rather than being
 * rebuilt, since that class is already engine-neutral and its Spark compatibility is already
 * covered by round-tripping the schema JSON through Spark's own {@code StructType.fromJson}.
 *
 * <p>Only the Parquet base file format is described. Every engine that creates a table today creates
 * a Parquet one, and reproducing the ORC/HFile/Lance/Vortex branches of
 * {@code HoodieInputFormatUtils} without a caller for them would be speculative; a non-Parquet base
 * file format is rejected rather than guessed at.
 */
public final class HoodieMetastoreTableDescriptor {

  /**
   * Input format for a Copy-on-Write table, and for the read-optimized view of a Merge-on-Read one.
   *
   * <p>The input format is the only record of the table type in the metastore, so these four names
   * are load-bearing well beyond format selection. Trino's {@code HiveUtil#isHudiTable} recognises
   * a Hudi table by this field alone.
   */
  public static final String PARQUET_INPUT_FORMAT_CLASS = "org.apache.hudi.hadoop.HoodieParquetInputFormat";

  /** Input format for the real-time (snapshot) view of a Merge-on-Read table. */
  public static final String PARQUET_REALTIME_INPUT_FORMAT_CLASS = "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat";

  public static final String PARQUET_OUTPUT_FORMAT_CLASS = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";

  public static final String PARQUET_SERDE_CLASS = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";

  /**
   * Marks the table as external. Set alongside the metastore's own table type, not instead of it:
   * Hive treats the two as independent, and a table with {@code EXTERNAL_TABLE} but no
   * {@code EXTERNAL=TRUE} parameter is still dropped destructively by some metastore versions.
   */
  public static final String EXTERNAL_PARAMETER = "EXTERNAL";

  public static final String EXTERNAL_PARAMETER_VALUE = "TRUE";

  /**
   * Written into the <em>serde</em> parameters, which is where {@code HMSDDLExecutor} puts it. It is
   * a serde-level setting, and a table carrying it in its table parameters instead differs from
   * every hive-synced table without failing in any way that is visible until an engine compares the
   * two.
   */
  public static final String SERIALIZATION_FORMAT_PARAMETER = "serialization.format";

  public static final String SERIALIZATION_FORMAT_VALUE = "1";

  /**
   * Matches {@code hoodie.datasource.hive_sync.schema_string_length_thresh}, the chunk size the
   * Spark-reconstructible schema is split into across {@code spark.sql.sources.schema.part.N}.
   * Spark reassembles by concatenating the parts in order, so the value only has to be stable, not
   * any particular number -- but keeping it equal to the hive-sync default keeps a Trino-created
   * table byte-identical to a hive-synced one.
   */
  public static final int DEFAULT_SCHEMA_STRING_LENGTH_THRESHOLD = 4000;

  private final List<HoodieSchemaField> dataFields;
  private final List<HoodieSchemaField> partitionFields;
  private final String inputFormatClassName;
  private final String outputFormatClassName;
  private final String serdeClassName;
  private final Map<String, String> tableParameters;
  private final Map<String, String> serdeParameters;
  private final boolean external;

  private HoodieMetastoreTableDescriptor(
      List<HoodieSchemaField> dataFields,
      List<HoodieSchemaField> partitionFields,
      String inputFormatClassName,
      String outputFormatClassName,
      String serdeClassName,
      Map<String, String> tableParameters,
      Map<String, String> serdeParameters,
      boolean external) {
    this.dataFields = Collections.unmodifiableList(dataFields);
    this.partitionFields = Collections.unmodifiableList(partitionFields);
    this.inputFormatClassName = inputFormatClassName;
    this.outputFormatClassName = outputFormatClassName;
    this.serdeClassName = serdeClassName;
    this.tableParameters = Collections.unmodifiableMap(tableParameters);
    this.serdeParameters = Collections.unmodifiableMap(serdeParameters);
    this.external = external;
  }

  /**
   * Describes the snapshot view of a table: the whole table for Copy-on-Write, the real-time view
   * for Merge-on-Read. This is what an engine registering a single table wants; the separate
   * read-optimized ({@code _ro}) registration hive-sync also performs for Merge-on-Read is the only
   * case that needs {@code readAsOptimized}, so it is not exposed here.
   *
   * @param tableSchema the table schema <em>including</em> Hudi's meta fields, as
   *     {@link org.apache.hudi.common.schema.HoodieSchemaUtils#addMetadataFields} produces. Supplies
   *     both the column names and their types, so a caller cannot register columns that disagree
   *     with the schema it wrote to {@code hoodie.properties}.
   * @param partitionFieldNames partition columns in partition-path order. A name absent from
   *     {@code tableSchema} is typed as a string, matching {@code HiveSchemaUtil#getPartitionKeyType};
   *     real tables built with some key generators do not carry their partition fields in the
   *     schema, and rejecting those would make an existing table impossible to register.
   * @param basePath the table's base path, recorded as the metastore location and as the serde
   *     {@code path}
   * @param external whether to describe the table as external; see {@link #isExternal()}
   */
  public static HoodieMetastoreTableDescriptor forSnapshotView(
      HoodieSchema tableSchema,
      List<String> partitionFieldNames,
      HoodieTableType tableType,
      String basePath,
      boolean external) {
    if (tableType == null) {
      throw new IllegalArgumentException("tableType is required");
    }
    // The snapshot view of a Merge-on-Read table is its real-time view: the one that merges log
    // files over the base files. Registering it with the Copy-on-Write input format instead does not
    // fail, it silently reads only the base files.
    boolean useRealtimeInputFormat = tableType == HoodieTableType.MERGE_ON_READ;
    return forView(tableSchema, partitionFieldNames, tableType, basePath, external,
        useRealtimeInputFormat, false, "", DEFAULT_SCHEMA_STRING_LENGTH_THRESHOLD, false,
        Collections.emptyMap());
  }

  /**
   * The general form, retaining every knob hive-sync varies. Callers registering one table should
   * prefer {@link #forSnapshotView}.
   *
   * @param useRealtimeInputFormat whether to use the real-time input format. Independent of
   *     {@code tableType} only because hive-sync registers a Merge-on-Read table twice, once each
   *     way; for Copy-on-Write it must be false, as there is no real-time view to read.
   * @param readAsOptimized recorded as {@code hoodie.query.as.ro.table}. True only for the
   *     read-optimized view of a Merge-on-Read table.
   * @param sparkVersion written as {@code spark.sql.create.version}; empty omits the property, as
   *     hive-sync does by default
   * @param includeFieldDocs whether column comments are carried into the serialized Spark schema
   * @param extraTableParameters additional table parameters, applied last so a caller can override
   *     anything derived here
   */
  public static HoodieMetastoreTableDescriptor forView(
      HoodieSchema tableSchema,
      List<String> partitionFieldNames,
      HoodieTableType tableType,
      String basePath,
      boolean external,
      boolean useRealtimeInputFormat,
      boolean readAsOptimized,
      String sparkVersion,
      int schemaStringLengthThreshold,
      boolean includeFieldDocs,
      Map<String, String> extraTableParameters) {
    if (tableSchema == null) {
      throw new IllegalArgumentException("tableSchema is required");
    }
    if (basePath == null || basePath.isEmpty()) {
      throw new IllegalArgumentException("basePath is required");
    }
    if (tableType == null) {
      throw new IllegalArgumentException("tableType is required");
    }
    if (useRealtimeInputFormat && tableType == HoodieTableType.COPY_ON_WRITE) {
      throw new IllegalArgumentException(
          "A Copy-on-Write table has no real-time view, so it cannot use the real-time input format");
    }
    List<String> partitionNames = partitionFieldNames == null
        ? Collections.emptyList() : new ArrayList<>(partitionFieldNames);

    Map<String, HoodieSchemaField> fieldsByName = new HashMap<>();
    for (HoodieSchemaField field : tableSchema.getFields()) {
      fieldsByName.put(field.name(), field);
    }

    // Partition columns first, in the order given, so the metastore's partition key order matches the
    // partition path. A metastore reorders neither list, so this order is the table's for good.
    List<HoodieSchemaField> partitionColumns = new ArrayList<>(partitionNames.size());
    Set<String> seenPartitionNames = new HashSet<>();
    for (String partitionName : partitionNames) {
      if (!seenPartitionNames.add(partitionName)) {
        throw new IllegalArgumentException("Partition column '" + partitionName + "' is listed more than once");
      }
      HoodieSchemaField field = fieldsByName.get(partitionName);
      if (field == null) {
        // Not an error: see the parameter documentation on forSnapshotView.
        field = HoodieSchemaField.of(partitionName, HoodieSchema.create(HoodieSchemaType.STRING));
      }
      partitionColumns.add(field);
    }

    // Everything else, in schema order, so the meta fields keep the leading positions
    // addMetadataFields gave them.
    List<HoodieSchemaField> dataColumns = new ArrayList<>();
    for (HoodieSchemaField field : tableSchema.getFields()) {
      if (!seenPartitionNames.contains(field.name())) {
        dataColumns.add(field);
      }
    }

    Map<String, String> tableParameters = new LinkedHashMap<>();
    if (external) {
      tableParameters.put(EXTERNAL_PARAMETER, EXTERNAL_PARAMETER_VALUE);
    }
    // Emitted unconditionally. hive-sync gates these on
    // hoodie.datasource.hive_sync.sync_as_datasource, which defaults to true; a table without
    // spark.sql.sources.provider is not recognised by Spark SQL as a Hudi datasource table at all,
    // so there is no reason for a newly created table to omit them.
    tableParameters.putAll(SparkDataSourceTableUtils.getSparkTableProperties(
        partitionNames, sparkVersion, schemaStringLengthThreshold, tableSchema, includeFieldDocs));
    if (extraTableParameters != null) {
      tableParameters.putAll(extraTableParameters);
    }

    Map<String, String> serdeParameters = new LinkedHashMap<>(
        SparkDataSourceTableUtils.getSparkSerdeProperties(readAsOptimized, basePath));
    serdeParameters.put(SERIALIZATION_FORMAT_PARAMETER, SERIALIZATION_FORMAT_VALUE);

    return new HoodieMetastoreTableDescriptor(
        dataColumns,
        partitionColumns,
        inputFormatClassName(tableType, useRealtimeInputFormat),
        PARQUET_OUTPUT_FORMAT_CLASS,
        PARQUET_SERDE_CLASS,
        tableParameters,
        serdeParameters,
        external);
  }

  /**
   * The input format that records this table type. Equivalent to
   * {@code HoodieInputFormatUtils#getInputFormatClassName(HoodieFileFormat.PARQUET, realtime)},
   * which lives in {@code hudi-hadoop-mr} and drags in Hadoop MapReduce.
   */
  public static String inputFormatClassName(HoodieTableType tableType, boolean useRealtimeInputFormat) {
    return useRealtimeInputFormat ? PARQUET_REALTIME_INPUT_FORMAT_CLASS : PARQUET_INPUT_FORMAT_CLASS;
  }

  /** Data columns, in schema order, excluding partition columns. */
  public List<HoodieSchemaField> getDataFields() {
    return dataFields;
  }

  /** Partition columns, in partition-path order. */
  public List<HoodieSchemaField> getPartitionFields() {
    return partitionFields;
  }

  public String getInputFormatClassName() {
    return inputFormatClassName;
  }

  public String getOutputFormatClassName() {
    return outputFormatClassName;
  }

  public String getSerdeClassName() {
    return serdeClassName;
  }

  /** Table-level parameters, including the Spark datasource properties. */
  public Map<String, String> getTableParameters() {
    return tableParameters;
  }

  /** Serde-level parameters: {@code path}, {@code hoodie.query.as.ro.table}, {@code serialization.format}. */
  public Map<String, String> getSerdeParameters() {
    return serdeParameters;
  }

  /**
   * Whether the table is external, meaning the metastore does not own its data and must not delete
   * it when the table is dropped.
   */
  public boolean isExternal() {
    return external;
  }
}
