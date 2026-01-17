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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.Arrays;
import java.util.List;

/**
 * Utilities for change log capture.
 */
public class HoodieCDCUtils {

  public static final String CDC_LOGFILE_SUFFIX = ".cdc";

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
   * The schema of cdc log file in the case `hoodie.table.cdc.supplemental.logging.mode` is {@link HoodieCDCSupplementalLoggingMode#OP_KEY_ONLY}.
   */
  public static final String CDC_SCHEMA_OP_AND_RECORDKEY_STRING = "{\"type\":\"record\",\"name\":\"Record\","
      + "\"fields\":["
      + "{\"name\":\"op\",\"type\":[\"string\",\"null\"]},"
      + "{\"name\":\"record_key\",\"type\":[\"string\",\"null\"]}"
      + "]}";

  public static final HoodieSchema CDC_SCHEMA_OP_AND_RECORDKEY =
      HoodieSchema.parse(CDC_SCHEMA_OP_AND_RECORDKEY_STRING);

  public static HoodieSchema schemaBySupplementalLoggingMode(
      HoodieCDCSupplementalLoggingMode supplementalLoggingMode,
      HoodieSchema tableSchema) {
    if (supplementalLoggingMode == HoodieCDCSupplementalLoggingMode.OP_KEY_ONLY) {
      return CDC_SCHEMA_OP_AND_RECORDKEY;
    } else if (supplementalLoggingMode == HoodieCDCSupplementalLoggingMode.DATA_BEFORE) {
      return createCDCSchema(tableSchema, false);
    } else if (supplementalLoggingMode == HoodieCDCSupplementalLoggingMode.DATA_BEFORE_AFTER) {
      return createCDCSchema(tableSchema, true);
    } else {
      throw new HoodieException("not support this supplemental logging mode: " + supplementalLoggingMode);
    }
  }

  private static HoodieSchema createCDCSchema(HoodieSchema tableSchema, boolean withAfterImage) {
    HoodieSchema imageSchema = HoodieSchemaUtils.createNullableSchema(tableSchema);
    HoodieSchema nullableString = HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.STRING));

    HoodieSchemaField opField = HoodieSchemaField.of(CDC_OPERATION_TYPE, nullableString, "", HoodieSchema.NULL_VALUE);
    HoodieSchemaField beforeField = HoodieSchemaField.of(CDC_BEFORE_IMAGE, imageSchema, "", HoodieSchema.NULL_VALUE);

    List<HoodieSchemaField> fields;
    if (withAfterImage) {
      HoodieSchemaField tsField = HoodieSchemaField.of(CDC_COMMIT_TIMESTAMP, nullableString, "", HoodieSchema.NULL_VALUE);
      HoodieSchemaField afterField = HoodieSchemaField.of(CDC_AFTER_IMAGE, imageSchema, "", HoodieSchema.NULL_VALUE);
      fields = Arrays.asList(opField, tsField, beforeField, afterField);
    } else {
      HoodieSchemaField keyField = HoodieSchemaField.of(CDC_RECORD_KEY, nullableString, "", HoodieSchema.NULL_VALUE);
      fields = Arrays.asList(opField, keyField, beforeField);
    }

    return HoodieSchema.createRecord("CDC", tableSchema.getNamespace().orElse(null), "", fields);
  }

  /**
   * Build the cdc record which has all the cdc fields when `hoodie.table.cdc.supplemental.logging.mode` is {@link HoodieCDCSupplementalLoggingMode#DATA_BEFORE_AFTER}.
   */
  public static GenericData.Record cdcRecord(HoodieSchema cdcSchema, String op, String commitTime,
                                             GenericRecord before, GenericRecord after) {
    GenericData.Record record = new GenericData.Record(cdcSchema.toAvroSchema());
    record.put(CDC_OPERATION_TYPE, op);
    record.put(CDC_COMMIT_TIMESTAMP, commitTime);
    record.put(CDC_BEFORE_IMAGE, before);
    record.put(CDC_AFTER_IMAGE, after);
    return record;
  }

  /**
   * Build the cdc record when `hoodie.table.cdc.supplemental.logging.mode` is {@link HoodieCDCSupplementalLoggingMode#DATA_BEFORE}.
   */
  public static GenericData.Record cdcRecord(HoodieSchema cdcSchema, String op,
                                             String recordKey, GenericRecord before) {
    GenericData.Record record = new GenericData.Record(cdcSchema.toAvroSchema());
    record.put(CDC_OPERATION_TYPE, op);
    record.put(CDC_RECORD_KEY, recordKey);
    record.put(CDC_BEFORE_IMAGE, before);
    return record;
  }

  /**
   * Build the cdc record when `hoodie.table.cdc.supplemental.logging.mode` is {@link HoodieCDCSupplementalLoggingMode#OP_KEY_ONLY}.
   */
  public static GenericData.Record cdcRecord(HoodieSchema cdcSchema, String op, String recordKey) {
    GenericData.Record record = new GenericData.Record(cdcSchema.toAvroSchema());
    record.put(CDC_OPERATION_TYPE, op);
    record.put(CDC_RECORD_KEY, recordKey);
    return record;
  }

  public static String recordToJson(GenericRecord record) {
    return GenericData.get().toString(record);
  }
}
