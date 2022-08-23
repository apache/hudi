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

package org.apache.hudi.common.table.cdc;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.exception.HoodieException;

public class CDCUtils {

  /* the `op` column represents how a record is changed. */
  public static final String CDC_OPERATION_TYPE = "op";

  /* the `ts_ms` column represents when a record is changed. */
  public static final String CDC_COMMIT_TIMESTAMP = "ts_ms";

  /* the pre-image before one record is changed */
  public static final String CDC_BEFORE_IMAGE = "before";

  /* the post-image after one record is changed */
  public static final String CDC_AFTER_IMAGE = "after";

  /* the key of the changed record */
  public static final String CDC_RECORD_KEY = "record_key";

  public static final String[] CDC_COLUMNS = new String[] {
      CDC_OPERATION_TYPE,
      CDC_COMMIT_TIMESTAMP,
      CDC_BEFORE_IMAGE,
      CDC_AFTER_IMAGE
  };

  /**
   * This is the standard CDC output format.
   * Also, this is the schema of cdc log file in the case `hoodie.table.cdc.supplemental.logging.mode` is 'cdc_data_before_after'.
   */
  public static final String CDC_SCHEMA_STRING = "{\"type\":\"record\",\"name\":\"Record\","
      + "\"fields\":["
      + "{\"name\":\"op\",\"type\":[\"string\",\"null\"]},"
      + "{\"name\":\"ts_ms\",\"type\":[\"string\",\"null\"]},"
      + "{\"name\":\"before\",\"type\":[\"string\",\"null\"]},"
      + "{\"name\":\"after\",\"type\":[\"string\",\"null\"]}"
      + "]}";

  public static final Schema CDC_SCHEMA = new Schema.Parser().parse(CDC_SCHEMA_STRING);

  /**
   * The schema of cdc log file in the case `hoodie.table.cdc.supplemental.logging.mode` is 'cdc_data_before'.
   */
  public static final String CDC_SCHEMA_OP_RECORDKEY_BEFORE_STRING = "{\"type\":\"record\",\"name\":\"Record\","
      + "\"fields\":["
      + "{\"name\":\"op\",\"type\":[\"string\",\"null\"]},"
      + "{\"name\":\"record_key\",\"type\":[\"string\",\"null\"]},"
      + "{\"name\":\"before\",\"type\":[\"string\",\"null\"]}"
      + "]}";

  public static final Schema CDC_SCHEMA_OP_RECORDKEY_BEFORE =
      new Schema.Parser().parse(CDC_SCHEMA_OP_RECORDKEY_BEFORE_STRING);

  /**
   * The schema of cdc log file in the case `hoodie.table.cdc.supplemental.logging.mode` is 'op_key'.
   */
  public static final String CDC_SCHEMA_OP_AND_RECORDKEY_STRING = "{\"type\":\"record\",\"name\":\"Record\","
      + "\"fields\":["
      + "{\"name\":\"op\",\"type\":[\"string\",\"null\"]},"
      + "{\"name\":\"record_key\",\"type\":[\"string\",\"null\"]}"
      + "]}";

  public static final Schema CDC_SCHEMA_OP_AND_RECORDKEY =
      new Schema.Parser().parse(CDC_SCHEMA_OP_AND_RECORDKEY_STRING);

  public static final Schema schemaBySupplementalLoggingMode(String supplementalLoggingMode) {
    switch (supplementalLoggingMode) {
      case HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE_WITH_BEFORE_AFTER:
        return CDC_SCHEMA;
      case HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE_WITH_BEFORE:
        return CDC_SCHEMA_OP_RECORDKEY_BEFORE;
      case HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE_MINI:
        return CDC_SCHEMA_OP_AND_RECORDKEY;
      default:
        throw new HoodieException("not support this supplemental logging mode: " + supplementalLoggingMode);
    }
  }

  /**
   * Build the cdc record which has all the cdc fields when `hoodie.table.cdc.supplemental.logging.mode` is 'cdc_data_before_after'.
   */
  public static GenericData.Record cdcRecord(
      String op, String commitTime, GenericRecord before, GenericRecord after) {
    String beforeJsonStr = recordToJson(before);
    String afterJsonStr = recordToJson(after);
    return cdcRecord(op, commitTime, beforeJsonStr, afterJsonStr);
  }

  public static GenericData.Record cdcRecord(
      String op, String commitTime, String before, String after) {
    GenericData.Record record = new GenericData.Record(CDC_SCHEMA);
    record.put(CDC_OPERATION_TYPE, op);
    record.put(CDC_COMMIT_TIMESTAMP, commitTime);
    record.put(CDC_BEFORE_IMAGE, before);
    record.put(CDC_AFTER_IMAGE, after);
    return record;
  }

  /**
   * Build the cdc record when `hoodie.table.cdc.supplemental.logging.mode` is 'cdc_data_before'.
   */
  public static GenericData.Record cdcRecord(String op, String recordKey, GenericRecord before) {
    GenericData.Record record = new GenericData.Record(CDC_SCHEMA_OP_RECORDKEY_BEFORE);
    record.put(CDC_OPERATION_TYPE, op);
    record.put(CDC_RECORD_KEY, recordKey);
    String beforeJsonStr = recordToJson(before);
    record.put(CDC_BEFORE_IMAGE, beforeJsonStr);
    return record;
  }

  /**
   * Build the cdc record when `hoodie.table.cdc.supplemental.logging.mode` is 'op_key'.
   */
  public static GenericData.Record cdcRecord(String op, String recordKey) {
    GenericData.Record record = new GenericData.Record(CDC_SCHEMA_OP_AND_RECORDKEY);
    record.put(CDC_OPERATION_TYPE, op);
    record.put(CDC_RECORD_KEY, recordKey);
    return record;
  }

  public static String recordToJson(GenericRecord record) {
    return GenericData.get().toString(record);
  }
}
