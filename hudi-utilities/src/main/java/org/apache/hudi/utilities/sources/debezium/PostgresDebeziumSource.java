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
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Source for incrementally ingesting debezium generated change logs for PostgresDB.
 */
public class PostgresDebeziumSource extends DebeziumSource {

  public PostgresDebeziumSource(TypedProperties props, JavaSparkContext sparkContext,
                                SparkSession sparkSession,
                                SchemaProvider schemaProvider,
                                HoodieIngestionMetrics metrics) {
    super(props, sparkContext, sparkSession, schemaProvider, metrics);
  }

  /**
   * Debezium Kafka Payload has a nested structure (see https://debezium.io/documentation/reference/1.4/connectors/postgresql.html#postgresql-create-events).
   * This function flattens this nested structure for the Postgres data, and also extracts a subset of Debezium metadata fields.
   *
   * @param rowDataset Dataset containing Debezium Payloads
   * @return New dataset with flattened columns
   */
  @Override
  protected Dataset<Row> processDataset(Dataset<Row> rowDataset) {
    if (rowDataset.columns().length > 0) {
      // Pick selective debezium and postgres meta fields: pick the row values from before field for delete record
      // and row values from after field for insert or update records.
      Dataset<Row> insertedOrUpdatedData = rowDataset
          .selectExpr(
              String.format("%s as %s", DebeziumConstants.INCOMING_OP_FIELD, DebeziumConstants.FLATTENED_OP_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_TS_MS_FIELD, DebeziumConstants.UPSTREAM_PROCESSING_TS_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_NAME_FIELD, DebeziumConstants.FLATTENED_SHARD_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_SCHEMA_FIELD, DebeziumConstants.FLATTENED_SCHEMA_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_TS_MS_FIELD, DebeziumConstants.FLATTENED_TS_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_TXID_FIELD, DebeziumConstants.FLATTENED_TX_ID_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_LSN_FIELD, DebeziumConstants.FLATTENED_LSN_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_XMIN_FIELD, DebeziumConstants.FLATTENED_XMIN_COL_NAME),
              String.format("%s.*", DebeziumConstants.INCOMING_AFTER_FIELD)
          )
          .filter(rowDataset.col(DebeziumConstants.INCOMING_OP_FIELD).notEqual(DebeziumConstants.DELETE_OP));

      Dataset<Row> deletedData = rowDataset
          .selectExpr(
              String.format("%s as %s", DebeziumConstants.INCOMING_OP_FIELD, DebeziumConstants.FLATTENED_OP_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_TS_MS_FIELD, DebeziumConstants.UPSTREAM_PROCESSING_TS_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_NAME_FIELD, DebeziumConstants.FLATTENED_SHARD_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_SCHEMA_FIELD, DebeziumConstants.FLATTENED_SCHEMA_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_TS_MS_FIELD, DebeziumConstants.FLATTENED_TS_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_TXID_FIELD, DebeziumConstants.FLATTENED_TX_ID_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_LSN_FIELD, DebeziumConstants.FLATTENED_LSN_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INCOMING_SOURCE_XMIN_FIELD, DebeziumConstants.FLATTENED_XMIN_COL_NAME),
              String.format("%s.*", DebeziumConstants.INCOMING_BEFORE_FIELD)
          )
          .filter(rowDataset.col(DebeziumConstants.INCOMING_OP_FIELD).equalTo(DebeziumConstants.DELETE_OP));

      return insertedOrUpdatedData.union(deletedData);
    } else {
      return rowDataset;
    }
  }
}

