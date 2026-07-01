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
import org.apache.spark.sql.functions;

import java.util.Arrays;
import java.util.List;

/**
 * {@link AbstractDebeziumTransformer} for MySQL Debezium change events.
 *
 * <p>Surfaces the MySQL binlog coordinates ({@code file}, {@code pos}, {@code row}) as the flattened
 * {@code _event_bin_file}, {@code _event_pos} and {@code _event_row} columns, and derives the
 * {@code _event_seq} ordering column as {@code "<binlog-file-suffix>.<pos>"} (e.g. {@code "000001.100"}
 * for a binlog file {@code "mysql-bin.000001"} at position {@code 100}). {@code _event_seq} is the
 * precombine/ordering field consumed by {@code MySqlDebeziumAvroPayload}.
 *
 * <p>Metadata is flattened to the root level by default; set
 * {@code hoodie.streamer.transformer.debezium.nested.fields.enable=true} to group it under a
 * {@code _debezium_metadata} struct instead.
 */
public class MysqlDebeziumTransformer extends AbstractDebeziumTransformer {

  private static final List<Column> MYSQL_METADATA = Arrays.asList(
      new Column(DebeziumConstants.INCOMING_SOURCE_FILE_FIELD).alias(DebeziumConstants.FLATTENED_FILE_COL_NAME),
      new Column(DebeziumConstants.INCOMING_SOURCE_POS_FIELD).alias(DebeziumConstants.FLATTENED_POS_COL_NAME),
      new Column(DebeziumConstants.INCOMING_SOURCE_ROW_FIELD).alias(DebeziumConstants.FLATTENED_ROW_COL_NAME));

  public MysqlDebeziumTransformer() {
    super(MYSQL_METADATA, Option.of(MysqlDebeziumTransformer::applySeqNo));
  }

  /**
   * Builds the {@code _event_seq} ordering column from the binlog file and position. The file column
   * holds a name like {@code "mysql-bin.000001"}; only the numeric suffix after the last dot is used,
   * yielding a sequence such as {@code "000001.100"}. Handles both the flat and nested metadata
   * layouts (reading {@code file}/{@code pos} from the {@code _debezium_metadata} struct when nested).
   *
   * @param dataset flattened MySQL Debezium dataset.
   * @return dataset with the {@code _event_seq} column added.
   */
  private static Dataset<Row> applySeqNo(Dataset<Row> dataset) {
    boolean isNested = Arrays.asList(dataset.columns()).contains(DebeziumConstants.DEBEZIUM_METADATA_FIELD);

    Column fileCol = isNested
        ? dataset.col(DebeziumConstants.DEBEZIUM_METADATA_FIELD + "." + DebeziumConstants.FLATTENED_FILE_COL_NAME)
        : dataset.col(DebeziumConstants.FLATTENED_FILE_COL_NAME);

    Column posCol = isNested
        ? dataset.col(DebeziumConstants.DEBEZIUM_METADATA_FIELD + "." + DebeziumConstants.FLATTENED_POS_COL_NAME)
        : dataset.col(DebeziumConstants.FLATTENED_POS_COL_NAME);

    return dataset.withColumn(DebeziumConstants.ADDED_SEQ_COL_NAME, functions.concat(
        functions.substring_index(fileCol, ".", -1),
        functions.lit("."),
        posCol));
  }
}
