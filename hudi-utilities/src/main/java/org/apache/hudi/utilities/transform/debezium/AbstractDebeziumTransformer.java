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

package org.apache.hudi.utilities.transform.debezium;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.debezium.DebeziumConstants;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.config.DebeziumTransformerConfig;
import org.apache.hudi.utilities.transform.Transformer;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.config.HoodieErrorTableConfig.ERROR_TABLE_ENABLED;
import static org.apache.hudi.utilities.streamer.BaseErrorTableWriter.ERROR_TABLE_CURRUPT_RECORD_COL_NAME;

/**
 * Base {@link Transformer} that flattens a Debezium change-event envelope into a Hudi row.
 *
 * <p>A Debezium change event is a nested record of the form
 * {@code {op, ts_ms, before:{...}, after:{...}, source:{...}}}. This transformer:
 * <ul>
 *   <li>selects the {@code before} image for deletes and the {@code after} image otherwise,
 *       and explodes it to the row's top level;</li>
 *   <li>surfaces the common Debezium metadata columns (operation type, processing/origin
 *       timestamps, shard) along with any database-specific metadata columns supplied by the
 *       subclass;</li>
 *   <li>optionally nests the metadata columns under a single {@code _debezium_metadata} struct
 *       (see {@link DebeziumTransformerConfig#ENABLE_NESTED_FIELDS});</li>
 *   <li>optionally preserves the error-table corrupt-record column when the error table is
 *       enabled;</li>
 *   <li>applies an optional database-specific post-processing step (e.g. ordering/sequence
 *       columns, LSN defaulting);</li>
 *   <li>normalizes column nullability (see
 *       {@link DebeziumTransformerConfig#SCHEMA_AS_NULLABLE}).</li>
 * </ul>
 *
 * <p>The flattened column names are defined in {@link DebeziumConstants}; the matching
 * {@code DebeziumAvroPayload} implementations rely on these names for merge/ordering semantics.
 *
 * <p>Subclasses configure the database-specific behavior purely through the constructor; there is
 * no abstract method to implement.
 */
public class AbstractDebeziumTransformer implements Transformer {

  private static final String DATA_FIELD = "__data";

  private static final List<Column> DEFAULT_ROOT_LEVEL_METADATA_COLUMNS = Arrays.asList(
      new Column(DebeziumConstants.INCOMING_OP_FIELD).alias(DebeziumConstants.FLATTENED_OP_COL_NAME));

  private static final List<Column> DEFAULT_NESTED_METADATA_COLUMNS = Arrays.asList(
      new Column(DebeziumConstants.INCOMING_TS_MS_FIELD).alias(DebeziumConstants.UPSTREAM_PROCESSING_TS_COL_NAME),
      new Column(DebeziumConstants.INCOMING_SOURCE_NAME_FIELD).alias(DebeziumConstants.FLATTENED_SHARD_NAME),
      new Column(DebeziumConstants.INCOMING_SOURCE_TS_MS_FIELD).alias(DebeziumConstants.FLATTENED_TS_COL_NAME));

  private final List<Column> typeSpecificMetadataColumns;
  private final Option<Function<Dataset<Row>, Dataset<Row>>> postProcessingOption;
  private final boolean nestedFieldsEnabledByDefault;

  protected AbstractDebeziumTransformer(
      List<Column> typeSpecificMetadataColumns,
      Option<Function<Dataset<Row>, Dataset<Row>>> postProcessingOption) {
    this(typeSpecificMetadataColumns, postProcessingOption, false);
  }

  /**
   * @param typeSpecificMetadataColumns database-specific metadata columns (already aliased to their
   *                                    flattened output names).
   * @param postProcessingOption        optional post-flatten transformation applied to the result.
   * @param nestedFieldsEnabledByDefault default used for
   *                                     {@link DebeziumTransformerConfig#ENABLE_NESTED_FIELDS} when
   *                                     the property is not set explicitly. Lets a subclass (e.g.
   *                                     Postgres) opt into nested metadata by default.
   */
  protected AbstractDebeziumTransformer(
      List<Column> typeSpecificMetadataColumns,
      Option<Function<Dataset<Row>, Dataset<Row>>> postProcessingOption,
      boolean nestedFieldsEnabledByDefault) {
    this.typeSpecificMetadataColumns = typeSpecificMetadataColumns;
    this.postProcessingOption = postProcessingOption;
    this.nestedFieldsEnabledByDefault = nestedFieldsEnabledByDefault;
  }

  @Override
  public Dataset<Row> apply(JavaSparkContext javaSparkContext, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties props) {
    if (rowDataset.columns().length == 0) {
      return rowDataset;
    }
    // Pick selective debezium meta fields: pick the row values from before field for delete record
    // and row values from after field for insert or update records.
    rowDataset = rowDataset
        .withColumn(DATA_FIELD,
            functions.when(new Column(DebeziumConstants.INCOMING_OP_FIELD).equalTo(DebeziumConstants.DELETE_OP),
                new Column(DebeziumConstants.INCOMING_BEFORE_FIELD))
                .otherwise(new Column(DebeziumConstants.INCOMING_AFTER_FIELD)))
        .drop(DebeziumConstants.INCOMING_AFTER_FIELD, DebeziumConstants.INCOMING_BEFORE_FIELD);

    List<Column> allColumns = new ArrayList<>();
    boolean enableNestedFields = isNestedFieldsEnabled(props);

    // When nested fields are enabled, only _change_operation_type and root-level metadata columns should be at root level
    if (enableNestedFields) {
      Column lsnColumn = null;
      List<Column> otherMetadata = new ArrayList<>();

      // Extract LSN column to root level, keep other metadata nested
      for (Column col : typeSpecificMetadataColumns) {
        String colStr = col.toString();
        if (colStr.contains(DebeziumConstants.FLATTENED_LSN_COL_NAME)) {
          lsnColumn = col;
        } else {
          otherMetadata.add(col);
        }
      }

      List<Column> nestedMetadataFields = new ArrayList<>();
      nestedMetadataFields.addAll(DEFAULT_NESTED_METADATA_COLUMNS);
      nestedMetadataFields.addAll(otherMetadata);

      // Only add schema field if it exists in the source struct (not all databases have this field)
      if (hasSchemaField(rowDataset)) {
        nestedMetadataFields.add(new Column(DebeziumConstants.INCOMING_SOURCE_SCHEMA_FIELD).alias(DebeziumConstants.FLATTENED_SCHEMA_NAME));
      }

      rowDataset = rowDataset.withColumn(DebeziumConstants.DEBEZIUM_METADATA_FIELD,
          functions.struct(nestedMetadataFields.toArray(new Column[]{})));
      allColumns.add(new Column(DebeziumConstants.DEBEZIUM_METADATA_FIELD));

      allColumns.addAll(DEFAULT_ROOT_LEVEL_METADATA_COLUMNS);
      if (lsnColumn != null) {
        // Add LSN column to root level
        allColumns.add(lsnColumn);
      }
    } else {
      // When nested fields are disabled, all metadata fields are at root level
      allColumns.addAll(DEFAULT_ROOT_LEVEL_METADATA_COLUMNS);
      allColumns.addAll(DEFAULT_NESTED_METADATA_COLUMNS);
      allColumns.addAll(typeSpecificMetadataColumns);
    }

    allColumns.add(new Column(String.format("%s.*", DATA_FIELD)));

    if (ConfigUtils.getBooleanWithAltKeys(props, ERROR_TABLE_ENABLED)) {
      if (!Arrays.stream(rowDataset.columns()).collect(Collectors.toList())
          .contains(ERROR_TABLE_CURRUPT_RECORD_COL_NAME)) {
        rowDataset = rowDataset.withColumn(ERROR_TABLE_CURRUPT_RECORD_COL_NAME, functions.lit(null));
      }
      allColumns.add(new Column(ERROR_TABLE_CURRUPT_RECORD_COL_NAME));
    }

    Dataset<Row> flattened = rowDataset.select(allColumns.toArray(new Column[]{}));
    Dataset<Row> debeziumDataset = postProcessingOption.map(postProcessing -> postProcessing.apply(flattened)).orElse(flattened);

    if (ConfigUtils.getBooleanWithAltKeys(props, DebeziumTransformerConfig.SCHEMA_AS_NULLABLE)) {
      return convertColumnsToNullable(sparkSession, debeziumDataset);
    }

    Set<String> nonNullableColumns = new HashSet<>();
    for (StructField field : rowDataset.schema().fields()) {
      if (field.dataType() instanceof StructType && DATA_FIELD.equals(field.name())) {
        nonNullableColumns.addAll(Arrays.stream(((StructType) field.dataType()).fields())
            .filter(dataField -> !dataField.nullable())
            .map(StructField::name)
            .collect(Collectors.toSet()));
      }
    }

    // Apply correct nullability to the transformed schema: a column stays non-nullable only if it
    // was a non-nullable source column; every other column (including Debezium metadata columns) is nullable.
    StructField[] updatedStructFields = Arrays.stream(debeziumDataset.schema().fields())
        .map(field -> nonNullableColumns.contains(field.name())
          ? new StructField(field.name(), field.dataType(), false, field.metadata())
          : new StructField(field.name(), field.dataType(), true, field.metadata()))
        .toArray(StructField[]::new);

    return sparkSession.createDataFrame(debeziumDataset.rdd(), new StructType(updatedStructFields));
  }

  /**
   * Resolves whether to nest the metadata columns. An explicitly set property always wins; when the
   * property is absent the per-subclass default ({@link #nestedFieldsEnabledByDefault}) is used.
   */
  private boolean isNestedFieldsEnabled(TypedProperties props) {
    return ConfigUtils.getRawValueWithAltKeys(props, DebeziumTransformerConfig.ENABLE_NESTED_FIELDS)
        .map(value -> Boolean.parseBoolean(value.toString()))
        .orElse(nestedFieldsEnabledByDefault);
  }

  /**
   * Rebuilds the dataset with every column marked nullable.
   */
  private static Dataset<Row> convertColumnsToNullable(SparkSession sparkSession, Dataset<Row> dataset) {
    StructField[] modifiedStructFields = Arrays.stream(dataset.schema().fields())
        .map(field -> new StructField(field.name(), field.dataType(), true, field.metadata()))
        .toArray(StructField[]::new);
    return sparkSession.createDataFrame(dataset.rdd(), new StructType(modifiedStructFields));
  }

  private static boolean hasSchemaField(Dataset<Row> rowDataset) {
    return Arrays.stream(rowDataset.schema().fields())
        .filter(field -> DebeziumConstants.INCOMING_SOURCE_FIELD.equals(field.name()) && field.dataType() instanceof StructType)
        .findFirst()
        .map(field -> {
          StructType sourceType = (StructType) field.dataType();
          return Arrays.stream(sourceType.fields())
              .anyMatch(sourceField -> "schema".equals(sourceField.name()));
        })
        .orElse(false);
  }
}
