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
 * <p>The layout and nullability behavior are configured through {@link DebeziumTransformerConfig}.
 *
 * <p>Subclasses configure the database-specific behavior purely through the constructor; there is
 * no abstract method to implement.
 */
public class AbstractDebeziumTransformer implements Transformer {

  private static final String DATA_FIELD = "__data";
  // Bare name of the optional {@code schema} field inside the Debezium {@code source} struct
  // (INCOMING_SOURCE_SCHEMA_FIELD is the fully-qualified {@code source.schema} path).
  private static final String SOURCE_SCHEMA_FIELD_NAME = "schema";

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
   * @param nestedFieldsEnabledByDefault the subclass default for the metadata layout. Resolution
   *                                     order at runtime: an explicitly set
   *                                     {@code hoodie.streamer.transformer.debezium.nested.fields.enable}
   *                                     ({@link DebeziumTransformerConfig#ENABLE_NESTED_FIELDS})
   *                                     always wins; when the property is absent this default is
   *                                     used. Lets a subclass opt into nested metadata by default.
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
    Dataset<Row> withDataField = selectBeforeOrAfterImage(rowDataset);
    List<Column> outputColumns = buildOutputColumns(withDataField, props);
    Dataset<Row> withErrorCol = applyErrorTablePassthrough(withDataField, outputColumns, props);
    Dataset<Row> flattened = withErrorCol.select(outputColumns.toArray(new Column[]{}));
    Dataset<Row> postProcessed = postProcessingOption.map(postProcessing -> postProcessing.apply(flattened)).orElse(flattened);
    return applyNullabilityRules(sparkSession, withDataField, postProcessed, props);
  }

  /**
   * Selects the {@code before} image for deletes and the {@code after} image otherwise into a single
   * {@code __data} struct column, then drops the original {@code before}/{@code after} columns.
   */
  private static Dataset<Row> selectBeforeOrAfterImage(Dataset<Row> rowDataset) {
    return rowDataset
        .withColumn(DATA_FIELD,
            functions.when(new Column(DebeziumConstants.INCOMING_OP_FIELD).equalTo(DebeziumConstants.DELETE_OP),
                new Column(DebeziumConstants.INCOMING_BEFORE_FIELD))
                .otherwise(new Column(DebeziumConstants.INCOMING_AFTER_FIELD)))
        .drop(DebeziumConstants.INCOMING_AFTER_FIELD, DebeziumConstants.INCOMING_BEFORE_FIELD);
  }

  /**
   * Builds the flattened output column list: the metadata columns (flat at the root or grouped under
   * the {@code _debezium_metadata} struct, per {@link DebeziumTransformerConfig#ENABLE_NESTED_FIELDS})
   * followed by the exploded {@code __data} image.
   */
  private List<Column> buildOutputColumns(Dataset<Row> withDataField, TypedProperties props) {
    List<Column> outputColumns = new ArrayList<>();
    if (isNestedFieldsEnabled(props)) {
      outputColumns.addAll(buildNestedMetadataColumns(withDataField));
    } else {
      // When nested fields are disabled, all metadata fields are at the root level.
      outputColumns.addAll(DEFAULT_ROOT_LEVEL_METADATA_COLUMNS);
      outputColumns.addAll(DEFAULT_NESTED_METADATA_COLUMNS);
      outputColumns.addAll(typeSpecificMetadataColumns);
    }
    // Explode the selected before/after image to the row's top level.
    outputColumns.add(new Column(String.format("%s.*", DATA_FIELD)));
    return outputColumns;
  }

  /**
   * Assembles the metadata columns for the nested layout: the operation-type column and the
   * log-position column (e.g. the Postgres LSN) stay at the root level so payload ordering keeps
   * working, while every other metadata column is grouped under the {@code _debezium_metadata} struct.
   */
  private List<Column> buildNestedMetadataColumns(Dataset<Row> withDataField) {
    Column lsnColumn = null;
    List<Column> nestedMetadataFields = new ArrayList<>(DEFAULT_NESTED_METADATA_COLUMNS);
    // Keep the log-position (LSN) column at the root level; nest the rest of the type-specific metadata.
    for (Column col : typeSpecificMetadataColumns) {
      if (col.toString().contains(DebeziumConstants.FLATTENED_LSN_COL_NAME)) {
        lsnColumn = col;
      } else {
        nestedMetadataFields.add(col);
      }
    }
    // Only add the schema field if it exists in the source struct (not all databases have this field).
    if (hasSchemaField(withDataField)) {
      nestedMetadataFields.add(new Column(DebeziumConstants.INCOMING_SOURCE_SCHEMA_FIELD).alias(DebeziumConstants.FLATTENED_SCHEMA_NAME));
    }

    List<Column> outputColumns = new ArrayList<>();
    outputColumns.add(functions.struct(nestedMetadataFields.toArray(new Column[]{}))
        .alias(DebeziumConstants.DEBEZIUM_METADATA_FIELD));
    outputColumns.addAll(DEFAULT_ROOT_LEVEL_METADATA_COLUMNS);
    if (lsnColumn != null) {
      outputColumns.add(lsnColumn);
    }
    return outputColumns;
  }

  /**
   * When the error table is enabled, ensures the corrupt-record column is present (adding a null one
   * if the input lacks it) and includes it in {@code outputColumns} so it is preserved downstream.
   */
  private static Dataset<Row> applyErrorTablePassthrough(Dataset<Row> dataset, List<Column> outputColumns, TypedProperties props) {
    if (!ConfigUtils.getBooleanWithAltKeys(props, ERROR_TABLE_ENABLED)) {
      return dataset;
    }
    Dataset<Row> withCorruptCol = dataset;
    if (!Arrays.asList(dataset.columns()).contains(ERROR_TABLE_CURRUPT_RECORD_COL_NAME)) {
      withCorruptCol = dataset.withColumn(ERROR_TABLE_CURRUPT_RECORD_COL_NAME, functions.lit(null));
    }
    outputColumns.add(new Column(ERROR_TABLE_CURRUPT_RECORD_COL_NAME));
    return withCorruptCol;
  }

  /**
   * Normalizes column nullability on the flattened dataset. When
   * {@link DebeziumTransformerConfig#SCHEMA_AS_NULLABLE} is set every column is marked nullable;
   * otherwise a column stays non-nullable only if Spark already infers it non-nullable or if it was a
   * non-nullable source data column. This preserves the non-nullability of Debezium metadata columns
   * (e.g. {@code _change_operation_type}) that Spark infers as non-nullable.
   *
   * @param withDataField the dataset carrying the {@code __data} struct, used to recover which source
   *                      data columns were non-nullable before flattening.
   */
  private Dataset<Row> applyNullabilityRules(SparkSession sparkSession, Dataset<Row> withDataField,
                                             Dataset<Row> debeziumDataset, TypedProperties props) {
    if (ConfigUtils.getBooleanWithAltKeys(props, DebeziumTransformerConfig.SCHEMA_AS_NULLABLE)) {
      return convertColumnsToNullable(sparkSession, debeziumDataset);
    }

    Set<String> nonNullableColumns = new HashSet<>();
    for (StructField field : withDataField.schema().fields()) {
      if (field.dataType() instanceof StructType && DATA_FIELD.equals(field.name())) {
        nonNullableColumns.addAll(Arrays.stream(((StructType) field.dataType()).fields())
            .filter(dataField -> !dataField.nullable())
            .map(StructField::name)
            .collect(Collectors.toSet()));
      }
    }

    StructField[] updatedStructFields = Arrays.stream(debeziumDataset.schema().fields())
        .map(field -> field.nullable() && !nonNullableColumns.contains(field.name())
          ? new StructField(field.name(), field.dataType(), true, field.metadata())
          : new StructField(field.name(), field.dataType(), false, field.metadata()))
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

  /**
   * Returns whether the source struct carries a {@code schema} field (present for Postgres, absent
   * for MySQL).
   */
  private static boolean hasSchemaField(Dataset<Row> rowDataset) {
    return getSourceStruct(rowDataset)
        .map(source -> Arrays.stream(source.fields()).anyMatch(field -> SOURCE_SCHEMA_FIELD_NAME.equals(field.name())))
        .orElse(false);
  }

  /**
   * Locates the Debezium {@code source} struct in the dataset schema, if present.
   */
  private static Option<StructType> getSourceStruct(Dataset<Row> rowDataset) {
    return Option.ofNullable(Arrays.stream(rowDataset.schema().fields())
        .filter(field -> DebeziumConstants.INCOMING_SOURCE_FIELD.equals(field.name()) && field.dataType() instanceof StructType)
        .map(field -> (StructType) field.dataType())
        .findFirst()
        .orElse(null));
  }
}
