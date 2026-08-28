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

package org.apache.hudi.utilities.transform.debezium;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.debezium.DebeziumConstants;
import org.apache.hudi.common.util.Option;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload.DEBEZIUM_TOASTED_VALUE;

/**
 * Transformer that flattens an Oracle Debezium CDC envelope into a flat Hudi-ready record with
 * Oracle-specific metadata and partial-update tracking.
 *
 * <p>On top of the shared {@link AbstractDebeziumTransformer} flattening this adds:
 * <ul>
 *   <li>Oracle metadata columns {@code _event_scn} / {@code _event_commit_scn} (from
 *       {@code source.scn} / {@code source.commit_scn}).</li>
 *   <li>A composite event-time ordering column {@code _event_ordering} = zero-padded
 *       {@code commit_scn.scn}, used as the ordering field for merging.</li>
 *   <li>A {@code _changed_columns} field for update events: the comma-separated names of data columns
 *       whose before/after values differ, excluding columns whose after image is the toasted
 *       (unavailable) sentinel. It is null for inserts, snapshots and deletes.</li>
 *   <li>A {@code _hoodie_is_deleted} flag (true for deletes).</li>
 * </ul>
 *
 * <p>Only the supported Debezium operation types are retained: {@code c} (insert), {@code u}
 * (update), {@code r} (snapshot) and {@code d} (delete); any other operation is dropped.
 *
 * <p>The flattened output is consumed either by {@code OracleDebeziumAvroPayload} (payload-based
 * tables) or, on table version 9, by the built-in {@code EVENT_TIME_ORDERING} +
 * {@code FILL_UNCHANGED} merge configuration inferred from that payload class.
 */
public class OracleDebeziumTransformer extends AbstractDebeziumTransformer {

  // Operation type constants.
  static final String INSERT_OP = "c";
  static final String UPDATE_OP = "u";
  static final String SNAPSHOT_OP = "r";

  private static final List<Column> ORACLE_METADATA = Arrays.asList(
      new Column(DebeziumConstants.INCOMING_SOURCE_SCN_FIELD).cast(DataTypes.StringType).alias(DebeziumConstants.FLATTENED_SCN_COL_NAME),
      new Column(DebeziumConstants.INCOMING_SOURCE_COMMIT_SCN_FIELD).cast(DataTypes.StringType).alias(DebeziumConstants.FLATTENED_COMMIT_SCN_COL_NAME));

  public OracleDebeziumTransformer() {
    super(ORACLE_METADATA, Option.of(OracleDebeziumTransformer::applyOrdering));
  }

  @Override
  public Dataset<Row> apply(
      JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties props) {
    if (rowDataset.columns().length == 0) {
      return rowDataset;
    }

    // Filter to supported operation types only (c, r, u, d).
    rowDataset = rowDataset.filter(
        functions.col(DebeziumConstants.INCOMING_OP_FIELD)
            .isin(INSERT_OP, SNAPSHOT_OP, UPDATE_OP, DebeziumConstants.DELETE_OP));

    StructType afterSchema = (StructType) rowDataset.schema()
        .apply(DebeziumConstants.INCOMING_AFTER_FIELD).dataType();
    String[] dataFieldNames = afterSchema.fieldNames();

    // Compute _changed_columns for updates: track field names where before and after values differ,
    // excluding fields where the after image contains a toasted (unavailable) value.
    Column toastedLit = functions.lit(DEBEZIUM_TOASTED_VALUE);
    List<Column> changedColConditions = new ArrayList<>();
    for (String fieldName : dataFieldNames) {
      Column afterCol = functions.col(DebeziumConstants.INCOMING_AFTER_FIELD + "." + fieldName);
      Column beforeCol = functions.col(DebeziumConstants.INCOMING_BEFORE_FIELD + "." + fieldName);
      // A column is "changed" when before != after (using null-safe comparison).
      Column isDifferent = afterCol.isNull().and(beforeCol.isNotNull())
          .or(afterCol.isNotNull().and(beforeCol.isNull()))
          .or(afterCol.isNotNull().and(beforeCol.isNotNull()).and(afterCol.notEqual(beforeCol)));
      // Only check for the toasted sentinel on string-typed columns; non-string columns (numeric, etc.)
      // cannot hold the sentinel value and comparing them against a string literal is type-unsafe.
      // Use coalesce(..., false) because when afterCol is null, equalTo returns null (Spark's
      // three-valued logic), which would poison and cause the change to be missed.
      boolean isStringField = afterSchema.apply(fieldName).dataType() == DataTypes.StringType;
      Column isChanged = isStringField
          ? isDifferent.and(functions.not(functions.coalesce(afterCol.equalTo(toastedLit), functions.lit(false))))
          : isDifferent;
      changedColConditions.add(
          functions.when(isChanged, functions.lit(fieldName))
              .otherwise(functions.lit(null).cast(DataTypes.StringType)));
    }
    Column changedColumnsConcat = functions.concat_ws(",",
        changedColConditions.toArray(new Column[0]));
    Column changedColsForUpdate = functions.when(changedColumnsConcat.equalTo(""),
            functions.lit(null).cast(DataTypes.StringType))
        .otherwise(changedColumnsConcat);

    // Materialize computed values as top-level columns first, so the expression tree is evaluated
    // once per row rather than duplicated across both struct rebuilds.
    String tmpChangedCol = "__tmp_changed_columns";
    String tmpIsDeletedCol = "__tmp_hoodie_is_deleted";

    // _changed_columns is only meaningful for update operations; null for inserts/deletes/snapshots.
    Column changedCols = functions.when(
            functions.col(DebeziumConstants.INCOMING_OP_FIELD).equalTo(UPDATE_OP), changedColsForUpdate)
        .otherwise(functions.lit(null).cast(DataTypes.StringType));

    // _hoodie_is_deleted: true for deletes, false otherwise.
    Column isDeleted = functions.col(DebeziumConstants.INCOMING_OP_FIELD)
        .equalTo(DebeziumConstants.DELETE_OP);

    rowDataset = rowDataset
        .withColumn(tmpChangedCol, changedCols)
        .withColumn(tmpIsDeletedCol, isDeleted);

    // Reference the materialized columns in struct rebuilds so they are not re-derived.
    Column changedColsRef = functions.col(tmpChangedCol);
    Column isDeletedRef = functions.col(tmpIsDeletedCol);

    // Add computed columns to both before and after structs so they survive the flattening in
    // AbstractDebeziumTransformer.apply() (which selects __data.*).
    rowDataset = rebuildStructWithExtraFields(rowDataset, DebeziumConstants.INCOMING_AFTER_FIELD,
        dataFieldNames, changedColsRef, isDeletedRef);
    rowDataset = rebuildStructWithExtraFields(rowDataset, DebeziumConstants.INCOMING_BEFORE_FIELD,
        dataFieldNames, changedColsRef, isDeletedRef);

    // Drop temp columns before passing to super — they are now inside the structs.
    rowDataset = rowDataset.drop(tmpChangedCol, tmpIsDeletedCol);

    return super.apply(jsc, sparkSession, rowDataset, props);
  }

  private static Dataset<Row> rebuildStructWithExtraFields(Dataset<Row> dataset, String structName,
                                                           String[] dataFieldNames, Column changedCols, Column isDeleted) {
    List<Column> fields = new ArrayList<>();
    for (String fieldName : dataFieldNames) {
      fields.add(functions.col(structName + "." + fieldName).alias(fieldName));
    }
    fields.add(changedCols.alias(DebeziumConstants.CHANGED_COLUMNS_FIELD));
    fields.add(isDeleted.alias(HoodieRecord.HOODIE_IS_DELETED_FIELD));
    return dataset.withColumn(structName, functions.struct(fields.toArray(new Column[0])));
  }

  private static Dataset<Row> applyOrdering(Dataset<Row> dataset) {
    boolean isNested = Arrays.asList(dataset.columns()).contains(DebeziumConstants.DEBEZIUM_METADATA_FIELD);

    Column commitScnCol = isNested
        ? dataset.col(DebeziumConstants.DEBEZIUM_METADATA_FIELD + "." + DebeziumConstants.FLATTENED_COMMIT_SCN_COL_NAME)
        : dataset.col(DebeziumConstants.FLATTENED_COMMIT_SCN_COL_NAME);
    Column scnCol = isNested
        ? dataset.col(DebeziumConstants.DEBEZIUM_METADATA_FIELD + "." + DebeziumConstants.FLATTENED_SCN_COL_NAME)
        : dataset.col(DebeziumConstants.FLATTENED_SCN_COL_NAME);

    Column paddedCommitScn = functions.lpad(
        functions.coalesce(commitScnCol, functions.lit("0")), 20, "0");
    Column paddedScn = functions.lpad(
        functions.coalesce(scnCol, functions.lit("0")), 20, "0");
    return dataset.withColumn(DebeziumConstants.FLATTENED_ORDERING_COL_NAME,
        functions.concat(paddedCommitScn, functions.lit("."), paddedScn));
  }
}
