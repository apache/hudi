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

import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.Arrays;
import java.util.List;

/**
 * Utilities for change log capture.
 */
public class HoodieCDCUtils {

  public static final String CDC_LOGFILE_SUFFIX = "-cdc";

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
   * The schema of cdc log file in the case `hoodie.table.cdc.supplemental.logging.mode` is 'cdc_op_key'.
   */
  public static final String CDC_SCHEMA_OP_AND_RECORDKEY_STRING = "{\"type\":\"record\",\"name\":\"Record\","
      + "\"fields\":["
      + "{\"name\":\"op\",\"type\":[\"string\",\"null\"]},"
      + "{\"name\":\"record_key\",\"type\":[\"string\",\"null\"]}"
      + "]}";

  public static final Schema CDC_SCHEMA_OP_AND_RECORDKEY =
      new Schema.Parser().parse(CDC_SCHEMA_OP_AND_RECORDKEY_STRING);

  public static Schema schemaBySupplementalLoggingMode(
      HoodieCDCSupplementalLoggingMode supplementalLoggingMode,
      Schema tableSchema) {
    if (supplementalLoggingMode == HoodieCDCSupplementalLoggingMode.OP_KEY) {
      return CDC_SCHEMA_OP_AND_RECORDKEY;
    } else if (supplementalLoggingMode == HoodieCDCSupplementalLoggingMode.WITH_BEFORE) {
      return createCDCSchema(tableSchema, false);
    } else if (supplementalLoggingMode == HoodieCDCSupplementalLoggingMode.WITH_BEFORE_AFTER) {
      return createCDCSchema(tableSchema, true);
    } else {
      throw new HoodieException("not support this supplemental logging mode: " + supplementalLoggingMode);
    }
  }

  private static Schema createCDCSchema(Schema tableSchema, boolean withAfterImage) {
    Schema imageSchema = AvroSchemaUtils.createNullableSchema(tableSchema);
    Schema.Field opField = new Schema.Field(CDC_OPERATION_TYPE,
        AvroSchemaUtils.createNullableSchema(Schema.Type.STRING), "", JsonProperties.NULL_VALUE);
    Schema.Field beforeField = new Schema.Field(
        CDC_BEFORE_IMAGE, imageSchema, "", JsonProperties.NULL_VALUE);
    List<Schema.Field> fields;
    if (withAfterImage) {
      Schema.Field tsField = new Schema.Field(CDC_COMMIT_TIMESTAMP,
          AvroSchemaUtils.createNullableSchema(Schema.Type.STRING), "", JsonProperties.NULL_VALUE);
      Schema.Field afterField = new Schema.Field(
          CDC_AFTER_IMAGE, imageSchema, "", JsonProperties.NULL_VALUE);
      fields = Arrays.asList(opField, tsField, beforeField, afterField);
    } else {
      Schema.Field keyField = new Schema.Field(CDC_RECORD_KEY,
          AvroSchemaUtils.createNullableSchema(Schema.Type.STRING), "", JsonProperties.NULL_VALUE);
      fields = Arrays.asList(opField, keyField, beforeField);
    }

    Schema mergedSchema = Schema.createRecord("CDC", null, "", false);
    mergedSchema.setFields(fields);
    return mergedSchema;
  }

  /**
   * Build the cdc record which has all the cdc fields when `hoodie.table.cdc.supplemental.logging.mode` is 'cdc_data_before_after'.
   */
  public static GenericData.Record cdcRecord(Schema cdcSchema, String op, String commitTime,
                                             GenericRecord before, GenericRecord after) {
    GenericData.Record record = new GenericData.Record(cdcSchema);
    record.put(CDC_OPERATION_TYPE, op);
    record.put(CDC_COMMIT_TIMESTAMP, commitTime);
    record.put(CDC_BEFORE_IMAGE, before);
    record.put(CDC_AFTER_IMAGE, after);
    return record;
  }

  /**
   * Build the cdc record when `hoodie.table.cdc.supplemental.logging.mode` is 'cdc_data_before'.
   */
  public static GenericData.Record cdcRecord(Schema cdcSchema, String op,
                                             String recordKey, GenericRecord before) {
    GenericData.Record record = new GenericData.Record(cdcSchema);
    record.put(CDC_OPERATION_TYPE, op);
    record.put(CDC_RECORD_KEY, recordKey);
    record.put(CDC_BEFORE_IMAGE, before);
    return record;
  }

  /**
   * Build the cdc record when `hoodie.table.cdc.supplemental.logging.mode` is 'cdc_op_key'.
   */
  public static GenericData.Record cdcRecord(Schema cdcSchema, String op, String recordKey) {
    GenericData.Record record = new GenericData.Record(cdcSchema);
    record.put(CDC_OPERATION_TYPE, op);
    record.put(CDC_RECORD_KEY, recordKey);
    return record;
  }

  public static String recordToJson(GenericRecord record) {
    return GenericData.get().toString(record);
  }
}
