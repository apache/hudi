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

package org.apache.hudi.common.model.debezium;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Constants used by {@link DebeziumSource} and {@link DebeziumAvroPayload}.
 */
public class DebeziumConstants {

  // INPUT COLUMNS
  public static final String INCOMING_BEFORE_FIELD = "before";
  public static final String INCOMING_AFTER_FIELD = "after";
  public static final String INCOMING_SOURCE_FIELD = "source";
  public static final String INCOMING_OP_FIELD = "op";
  public static final String INCOMING_TS_MS_FIELD = "ts_ms";

  public static final String INCOMING_SOURCE_NAME_FIELD = "source.name";
  public static final String INCOMING_SOURCE_TS_MS_FIELD = "source.ts_ms";
  public static final String INCOMING_SOURCE_TXID_FIELD = "source.txId";

  // INPUT COLUMNS SPECIFIC TO MYSQL
  public static final String INCOMING_SOURCE_FILE_FIELD = "source.file";
  public static final String INCOMING_SOURCE_POS_FIELD = "source.pos";
  public static final String INCOMING_SOURCE_ROW_FIELD = "source.row";

  // INPUT COLUMNS SPECIFIC TO POSTGRES
  public static final String INCOMING_SOURCE_LSN_FIELD = "source.lsn";
  public static final String INCOMING_SOURCE_XMIN_FIELD = "source.xmin";

  // OUTPUT COLUMNS
  public static final String FLATTENED_OP_COL_NAME = "_change_operation_type";
  public static final String UPSTREAM_PROCESSING_TS_COL_NAME = "_upstream_event_processed_ts_ms";
  public static final String FLATTENED_SHARD_NAME = "db_shard_source_partition";
  public static final String FLATTENED_TS_COL_NAME = "_event_origin_ts_ms";
  public static final String FLATTENED_TX_ID_COL_NAME = "_event_tx_id";

  // OUTPUT COLUMNS SPECIFIC TO MYSQL
  public static final String FLATTENED_FILE_COL_NAME = "_event_bin_file";
  public static final String FLATTENED_POS_COL_NAME = "_event_pos";
  public static final String FLATTENED_ROW_COL_NAME = "_event_row";
  public static final String ADDED_SEQ_COL_NAME = "_event_seq";

  // OUTPUT COLUMNS SPECIFIC TO POSTGRES
  public static final String FLATTENED_LSN_COL_NAME = "_event_lsn";
  public static final String FLATTENED_XMIN_COL_NAME = "_event_xmin";

  // Other Constants
  public static final String DELETE_OP = "d";

  // List of meta data columns
  public static List<String> META_COLUMNS = Collections.unmodifiableList(Arrays.asList(
      FLATTENED_OP_COL_NAME,
      UPSTREAM_PROCESSING_TS_COL_NAME,
      FLATTENED_TS_COL_NAME,
      FLATTENED_TX_ID_COL_NAME,
      FLATTENED_LSN_COL_NAME,
      FLATTENED_XMIN_COL_NAME,
      FLATTENED_SHARD_NAME
  ));
}

