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
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieCDCUtils {
  @ParameterizedTest
  @CsvSource({
      "OP_KEY_ONLY, FLOAT, true, false",
      "OP_KEY_ONLY, DOUBLE, false, false",
      "OP_KEY_ONLY, INT8, false, false",
      "DATA_BEFORE, FLOAT, true, true",
      "DATA_BEFORE, DOUBLE, false, true",
      "DATA_BEFORE, INT8, false, true",
      "DATA_BEFORE_AFTER, FLOAT, true, true",
      "DATA_BEFORE_AFTER, DOUBLE, false, true",
      "DATA_BEFORE_AFTER, INT8, false, true",
      "DATA_BEFORE, ARRAY, true, false",
      "DATA_BEFORE_AFTER, ARRAY, false, false"
  })
  void testSchemaBySupplementalLoggingMode(HoodieCDCSupplementalLoggingMode mode,
                                          String type, boolean nullable, boolean rejected) {
    HoodieSchema fieldSchema = type.equals("ARRAY") ? HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.FLOAT))
        : HoodieSchema.createVector(2, HoodieSchema.Vector.VectorElementType.valueOf(type));
    HoodieSchema schema = HoodieSchema.createRecord("data", null, null, Collections.singletonList(
        HoodieSchemaField.of("embedding", nullable ? HoodieSchema.createNullable(fieldSchema) : fieldSchema)));
    if (rejected) {
      HoodieNotSupportedException error = assertThrows(HoodieNotSupportedException.class,
          () -> HoodieCDCUtils.schemaBySupplementalLoggingMode(mode, schema));
      assertTrue(error.getMessage().contains(mode.name()));
      assertTrue(error.getMessage().contains("embedding"));
      assertTrue(error.getMessage().contains(HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE.key() + "=OP_KEY_ONLY"));
    } else {
      HoodieSchema cdcSchema = HoodieCDCUtils.schemaBySupplementalLoggingMode(mode, schema);
      if (mode == HoodieCDCSupplementalLoggingMode.OP_KEY_ONLY) {
        assertSame(HoodieCDCUtils.CDC_SCHEMA_OP_AND_RECORDKEY, cdcSchema);
      } else {
        assertEquals(HoodieSchema.createNullable(schema), cdcSchema.getField(HoodieCDCUtils.CDC_BEFORE_IMAGE).get().schema());
        if (mode == HoodieCDCSupplementalLoggingMode.DATA_BEFORE_AFTER) {
          assertEquals(HoodieSchema.createNullable(schema), cdcSchema.getField(HoodieCDCUtils.CDC_AFTER_IMAGE).get().schema());
        }
      }
    }
  }
}
