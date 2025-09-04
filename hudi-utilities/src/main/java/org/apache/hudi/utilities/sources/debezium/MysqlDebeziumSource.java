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
import org.apache.hudi.utilities.exception.HoodieReadFromSourceException;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.callUDF;

/**
 * Source for incrementally ingesting debezium generated change logs for Mysql DB.
 */
public class MysqlDebeziumSource extends DebeziumSource {

  private final SQLContext sqlContext;
  private final String generateUniqueSeqUdfFn = "mysql_generate_order_key";

  public MysqlDebeziumSource(TypedProperties props, JavaSparkContext sparkContext,
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
  protected Dataset<Row> processDataset(Dataset<Row> rowDataset) {
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

  private static String generateUniqueSequence(String fileId, Long pos) {
    // Minimal validations to ensure fileId and pos are valid.
    if (fileId == null || fileId.trim().isEmpty() || pos == null || pos < 0) {
      throw new HoodieReadFromSourceException(
          String.format("Invalid binlog file information from Debezium: fileId=%s, pos=%s", fileId, pos));
    }

    return fileId.substring(fileId.lastIndexOf('.') + 1).concat("." + pos);
  }
}
