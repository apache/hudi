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

package org.apache.hudi.utilities.sources.debezium;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.debezium.DebeziumConstants;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.avro.HoodieAvroUtils.METADATA_FIELD_SCHEMA;
import static org.apache.spark.sql.functions.callUDF;

/**
 * Source for incrementally ingesting debezium generated change logs for Mysql DB.
 */
public class MysqlDebeziumSource extends DebeziumSource {

  private final SQLContext sqlContext;
  private final String generateUniqueSeqUdfFn = "mysql_generate_order_key";

  public MysqlDebeziumSource(TypedProperties props,
                             JavaSparkContext sparkContext,
                             SparkSession sparkSession,
                             SchemaProvider schemaProvider,
                             HoodieIngestionMetrics metrics) {
    super(props, sparkContext, sparkSession, schemaProvider, metrics);
    this.sqlContext = sparkSession.sqlContext();
    sqlContext.udf().register(generateUniqueSeqUdfFn, (UDF2<String, Long, String>) MysqlDebeziumSource::generateUniqueSequence, DataTypes.StringType);
  }

  /**
   * Debezium Kafka Payload has a nested structure (see https://debezium.io/documentation/reference/1.4/connectors/mysql.html).
   * This function flattens this nested structure for the Mysql data, and also extracts a subset of Debezium metadata fields.
   *
   * @param rowDataset Dataset containing Debezium Payloads
   * @return New dataset with flattened columns
   */
  @Override
  protected Dataset<Row>
  processDataset(Dataset<Row> rowDataset) {
    Dataset<Row> flattenedDataset = rowDataset;
    if (rowDataset.columns().length > 0) {
      // Only flatten for non-empty schemas
      Dataset<Row> insertedOrUpdatedData = rowDataset
          .selectExpr(
              String.format("%s as %s", DebeziumConstants.INCOMING_OP_FIELD, DebeziumConstants.FLATTENED_OP_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_TS_MS_FIELD, DebeziumConstants.UPSTREAM_PROCESSING_TS_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_NAME_FIELD, DebeziumConstants.FLATTENED_SHARD_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_TS_MS_FIELD, DebeziumConstants.FLATTENED_TS_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_FILE_FIELD, DebeziumConstants.FLATTENED_FILE_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_POS_FIELD, DebeziumConstants.FLATTENED_POS_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_ROW_FIELD, DebeziumConstants.FLATTENED_ROW_COL_NAME),
              String.format("%s.*", DebeziumConstants.INCOMING_AFTER_FIELD)
          )
          .filter(rowDataset.col(DebeziumConstants.INCOMING_OP_FIELD).notEqual(DebeziumConstants.DELETE_OP));

      Dataset<Row> deletedData = rowDataset
          .selectExpr(
              String.format("%s as %s", DebeziumConstants.INCOMING_OP_FIELD, DebeziumConstants.FLATTENED_OP_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_TS_MS_FIELD, DebeziumConstants.UPSTREAM_PROCESSING_TS_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_NAME_FIELD, DebeziumConstants.FLATTENED_SHARD_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_TS_MS_FIELD, DebeziumConstants.FLATTENED_TS_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_FILE_FIELD, DebeziumConstants.FLATTENED_FILE_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_POS_FIELD, DebeziumConstants.FLATTENED_POS_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_ROW_FIELD, DebeziumConstants.FLATTENED_ROW_COL_NAME),
              String.format("%s.*", DebeziumConstants.INCOMING_BEFORE_FIELD)
          )
          .filter(rowDataset.col(DebeziumConstants.INCOMING_OP_FIELD).equalTo(DebeziumConstants.DELETE_OP));

      flattenedDataset = insertedOrUpdatedData.union(deletedData);
    }

    return flattenedDataset.withColumn(DebeziumConstants.ADDED_SEQ_COL_NAME,
            callUDF(generateUniqueSeqUdfFn, flattenedDataset.col(DebeziumConstants.FLATTENED_FILE_COL_NAME),
                flattenedDataset.col(DebeziumConstants.FLATTENED_POS_COL_NAME)));
  }

  @Override
  protected List<Pair<Dataset<Row>, Schema>> processDatasetForPartialUpdates(Dataset<Row> rowDataset, Schema schema) {
    List<Pair<Dataset<Row>, Schema>> flattenedDataset = new ArrayList<>();
    if (rowDataset.columns().length > 0) {
      // Only flatten for non-empty schemas
      Dataset<Row> insertedData = rowDataset
          .selectExpr(
              String.format("%s as %s", DebeziumConstants.INCOMING_OP_FIELD, DebeziumConstants.FLATTENED_OP_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_TS_MS_FIELD, DebeziumConstants.UPSTREAM_PROCESSING_TS_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_NAME_FIELD, DebeziumConstants.FLATTENED_SHARD_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_TS_MS_FIELD, DebeziumConstants.FLATTENED_TS_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_FILE_FIELD, DebeziumConstants.FLATTENED_FILE_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_POS_FIELD, DebeziumConstants.FLATTENED_POS_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_ROW_FIELD, DebeziumConstants.FLATTENED_ROW_COL_NAME),
              String.format("%s.*", DebeziumConstants.INCOMING_AFTER_FIELD)
          )
          .filter(rowDataset.col(DebeziumConstants.INCOMING_OP_FIELD).equalTo(DebeziumConstants.INSERT_OP));
      flattenedDataset.add(Pair.of(insertedData, generateInsertSchema(schema)));

      Dataset<Row> deletedData = rowDataset
          .selectExpr(
              String.format("%s as %s", DebeziumConstants.INCOMING_OP_FIELD, DebeziumConstants.FLATTENED_OP_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_TS_MS_FIELD, DebeziumConstants.UPSTREAM_PROCESSING_TS_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_NAME_FIELD, DebeziumConstants.FLATTENED_SHARD_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_TS_MS_FIELD, DebeziumConstants.FLATTENED_TS_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_FILE_FIELD, DebeziumConstants.FLATTENED_FILE_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_POS_FIELD, DebeziumConstants.FLATTENED_POS_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_ROW_FIELD, DebeziumConstants.FLATTENED_ROW_COL_NAME),
              String.format("%s.*", DebeziumConstants.INCOMING_BEFORE_FIELD)
          )
          .filter(rowDataset.col(DebeziumConstants.INCOMING_OP_FIELD).equalTo(DebeziumConstants.DELETE_OP));
      flattenedDataset.add(Pair.of(deletedData, generateDeleteSchema(schema)));

      Dataset<Row> updatedData = rowDataset
          .selectExpr(
              String.format("%s as %s", DebeziumConstants.INCOMING_OP_FIELD, DebeziumConstants.FLATTENED_OP_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_TS_MS_FIELD, DebeziumConstants.UPSTREAM_PROCESSING_TS_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_NAME_FIELD, DebeziumConstants.FLATTENED_SHARD_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_TS_MS_FIELD, DebeziumConstants.FLATTENED_TS_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_FILE_FIELD, DebeziumConstants.FLATTENED_FILE_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_POS_FIELD, DebeziumConstants.FLATTENED_POS_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_ROW_FIELD, DebeziumConstants.FLATTENED_ROW_COL_NAME),
              String.format("%s.*", DebeziumConstants.INCOMING_BEFORE_FIELD)
          )
          .filter(rowDataset.col(DebeziumConstants.INCOMING_OP_FIELD).equalTo(DebeziumConstants.UPDATE_OP));
      flattenedDataset.add(Pair.of(updatedData, generateUpdateSchema(schema)));
    }

    return flattenedDataset.stream().map(p -> {
      Dataset<Row> data = p.getLeft();
      data.withColumn(
          DebeziumConstants.ADDED_SEQ_COL_NAME,
          callUDF(
              generateUniqueSeqUdfFn,
              data.col(DebeziumConstants.FLATTENED_FILE_COL_NAME),
              data.col(DebeziumConstants.FLATTENED_POS_COL_NAME))
      );
      return Pair.of(data, p.getRight());
    }).collect(Collectors.toList());
  }

  private Schema generateInsertSchema(Schema schema) {
    return schema;
  }

  private Schema generateDeleteSchema(Schema schema) {
    List<String> fields = Arrays.asList(
        DebeziumConstants.FLATTENED_OP_COL_NAME,
        DebeziumConstants.UPSTREAM_PROCESSING_TS_COL_NAME,
        DebeziumConstants.FLATTENED_SHARD_NAME,
        DebeziumConstants.FLATTENED_TS_COL_NAME,
        DebeziumConstants.FLATTENED_FILE_COL_NAME,
        DebeziumConstants.FLATTENED_POS_COL_NAME,
        DebeziumConstants.FLATTENED_ROW_COL_NAME);
    List<Schema.Field> schemaFields = Arrays.asList(
        new Schema.Field(DebeziumConstants.FLATTENED_OP_COL_NAME, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE),
        new Schema.Field(DebeziumConstants.UPSTREAM_PROCESSING_TS_COL_NAME, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE),
        new Schema.Field(DebeziumConstants.UPSTREAM_PROCESSING_TS_COL_NAME, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE),
        new Schema.Field(DebeziumConstants.UPSTREAM_PROCESSING_TS_COL_NAME, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE),
        new Schema.Field(DebeziumConstants.UPSTREAM_PROCESSING_TS_COL_NAME, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE),
        new Schema.Field(DebeziumConstants.UPSTREAM_PROCESSING_TS_COL_NAME, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE),
        new Schema.Field(DebeziumConstants.UPSTREAM_PROCESSING_TS_COL_NAME, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE));
    Schema deleteSchema = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), false);
    deleteSchema.setFields(schemaFields);
    return deleteSchema;
  }

  private Schema generateUpdateSchema(Schema schema) {
    String changedColumnsStr = props.getString("header.changed.name");
    List<String> changedColumns = Arrays.asList(changedColumnsStr.split(","));
    List<Schema.Field> schemaFields = Arrays.asList(
        new Schema.Field(DebeziumConstants.FLATTENED_OP_COL_NAME, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE),
        new Schema.Field(DebeziumConstants.UPSTREAM_PROCESSING_TS_COL_NAME, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE),
        new Schema.Field(DebeziumConstants.UPSTREAM_PROCESSING_TS_COL_NAME, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE),
        new Schema.Field(DebeziumConstants.UPSTREAM_PROCESSING_TS_COL_NAME, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE),
        new Schema.Field(DebeziumConstants.UPSTREAM_PROCESSING_TS_COL_NAME, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE),
        new Schema.Field(DebeziumConstants.UPSTREAM_PROCESSING_TS_COL_NAME, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE),
        new Schema.Field(DebeziumConstants.UPSTREAM_PROCESSING_TS_COL_NAME, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE));
    List<Schema.Field> updateFields = schema.getFields().stream().filter(f -> changedColumns.contains(f.name())).collect(Collectors.toList());
    updateFields.addAll(schemaFields);
    Schema updateSchema = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), false);
    updateSchema.setFields(updateFields);
    return updateSchema;
  }

  private static String generateUniqueSequence(String fileId, Long pos) {
    return fileId.substring(fileId.lastIndexOf('.') + 1).concat("." + pos);
  }
}
