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

import org.apache.hudi.common.model.debezium.DebeziumConstants;
import org.apache.hudi.common.util.Option;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.when;

/**
 * {@link AbstractDebeziumTransformer} for Postgres Debezium change events.
 *
 * <p>Surfaces the Postgres-specific source metadata ({@code txId}, {@code lsn}, {@code xmin}) as the
 * flattened {@code _event_tx_id}, {@code _event_lsn} and {@code _event_xmin} columns. The
 * {@code _event_lsn} column is the ordering field used by {@code PostgresDebeziumAvroPayload}.
 *
 * <p>Post-processing defaults a null {@code _event_lsn} to {@code 0} for snapshot records, since the
 * LSN is not populated for rows produced by Debezium incremental snapshots
 * (see <a href="https://debezium.io/blog/2021/10/07/incremental-snapshots/">incremental snapshots</a>).
 *
 * <p>Nested metadata is enabled by default for Postgres (the {@code _event_lsn} and operation-type
 * columns stay at the root level so payload ordering keeps working); set
 * {@code hoodie.streamer.transformer.debezium.nested.fields.enable=false} to flatten all metadata.
 */
public class PostgresDebeziumTransformer extends AbstractDebeziumTransformer {

  // Snapshot operation type emitted by Debezium ("read").
  private static final String SNAPSHOT_OP = "r";

  private static final List<Column> POSTGRES_METADATA = Arrays.asList(
      new Column(DebeziumConstants.INCOMING_SOURCE_TXID_FIELD).alias(DebeziumConstants.FLATTENED_TX_ID_COL_NAME),
      new Column(DebeziumConstants.INCOMING_SOURCE_LSN_FIELD).alias(DebeziumConstants.FLATTENED_LSN_COL_NAME),
      new Column(DebeziumConstants.INCOMING_SOURCE_XMIN_FIELD).alias(DebeziumConstants.FLATTENED_XMIN_COL_NAME));

  public PostgresDebeziumTransformer() {
    super(POSTGRES_METADATA, Option.of(PostgresDebeziumTransformer::useDefaultValuesForLsnIfNull), true);
  }

  /**
   * Defaults a null {@code _event_lsn} to {@code 0} for snapshot records. The LSN is null when a
   * table is added via a Debezium incremental snapshot; leaving it null would break LSN-based
   * ordering in the payload.
   *
   * @param dataset flattened Postgres Debezium dataset.
   * @return dataset where null {@code _event_lsn} values on snapshot rows are replaced with 0.
   */
  private static Dataset<Row> useDefaultValuesForLsnIfNull(Dataset<Row> dataset) {
    if (!Arrays.asList(dataset.columns()).contains(DebeziumConstants.FLATTENED_LSN_COL_NAME)) {
      return dataset;
    }

    return dataset.withColumn(DebeziumConstants.FLATTENED_LSN_COL_NAME, when(
        dataset.col(DebeziumConstants.FLATTENED_OP_COL_NAME).equalTo(SNAPSHOT_OP),
        expr(String.format("coalesce(%s, cast(0 as long))", DebeziumConstants.FLATTENED_LSN_COL_NAME))
    ).otherwise(dataset.col(DebeziumConstants.FLATTENED_LSN_COL_NAME)));
  }
}
