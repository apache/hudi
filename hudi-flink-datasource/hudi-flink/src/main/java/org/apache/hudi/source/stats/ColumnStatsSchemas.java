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

package org.apache.hudi.source.stats;

import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.util.HoodieSchemaConverter;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.stream.Stream;

/**
 * Utility class for column stats schema.
 */
public class ColumnStatsSchemas {
  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------
  public static final DataType METADATA_DATA_TYPE = getMetadataDataType();
  public static final DataType COL_STATS_DATA_TYPE = getColStatsDataType();
  public static final int[] COL_STATS_TARGET_POS = getColStatsTargetPos();

  // the column schema:
  // |- file_name: string
  // |- min_val: row
  // |- max_val: row
  // |- null_cnt: long
  // |- val_cnt: long
  // |- column_name: string
  public static final int ORD_FILE_NAME = 0;
  public static final int ORD_MIN_VAL = 1;
  public static final int ORD_MAX_VAL = 2;
  public static final int ORD_NULL_CNT = 3;
  public static final int ORD_VAL_CNT = 4;
  public static final int ORD_COL_NAME = 5;

  private static DataType getMetadataDataType() {
    return HoodieSchemaConverter.convertToDataType(HoodieSchema.fromAvroSchema(HoodieMetadataRecord.SCHEMA$));
  }

  private static DataType getColStatsDataType() {
    int pos = HoodieMetadataRecord.SCHEMA$.getField(HoodieMetadataPayload.SCHEMA_FIELD_ID_COLUMN_STATS).pos();
    return METADATA_DATA_TYPE.getChildren().get(pos);
  }

  // the column schema:
  // |- file_name: string
  // |- min_val: row
  // |- max_val: row
  // |- null_cnt: long
  // |- val_cnt: long
  // |- column_name: string
  private static int[] getColStatsTargetPos() {
    RowType colStatsRowType = (RowType) COL_STATS_DATA_TYPE.getLogicalType();
    return Stream.of(
            HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME,
            HoodieMetadataPayload.COLUMN_STATS_FIELD_MIN_VALUE,
            HoodieMetadataPayload.COLUMN_STATS_FIELD_MAX_VALUE,
            HoodieMetadataPayload.COLUMN_STATS_FIELD_NULL_COUNT,
            HoodieMetadataPayload.COLUMN_STATS_FIELD_VALUE_COUNT,
            HoodieMetadataPayload.COLUMN_STATS_FIELD_COLUMN_NAME)
        .mapToInt(colStatsRowType::getFieldIndex)
        .toArray();
  }
}
