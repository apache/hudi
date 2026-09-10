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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieCDCUtils {
  @ParameterizedTest
  @CsvSource({
      "OP_KEY_ONLY, true, FLOAT, true, false",
      "DATA_BEFORE, true, FLOAT, true, true",
      "DATA_BEFORE_AFTER, true, FLOAT, true, true",
      "DATA_BEFORE_AFTER, true, DOUBLE, false, true",
      "DATA_BEFORE_AFTER, true, INT8, false, true",
      "DATA_BEFORE, false, FLOAT, true, false",
      "DATA_BEFORE_AFTER, false, FLOAT, true, false",
      "DATA_BEFORE, true, ARRAY, true, false",
      "DATA_BEFORE_AFTER, true, ARRAY, false, false"
  })
  void testValidateCdcSchema(HoodieCDCSupplementalLoggingMode mode, boolean enabled,
                            String type, boolean nullable, boolean rejected) {
    HoodieSchema fieldSchema = type.equals("ARRAY") ? HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.FLOAT))
        : HoodieSchema.createVector(2, HoodieSchema.Vector.VectorElementType.valueOf(type));
    HoodieSchema schema = HoodieSchema.createRecord("data", null, null, Collections.singletonList(
        HoodieSchemaField.of("embedding", nullable ? HoodieSchema.createNullable(fieldSchema) : fieldSchema)));
    HoodieTableConfig config = new HoodieTableConfig();
    config.setValue(HoodieTableConfig.CDC_ENABLED, Boolean.toString(enabled));
    config.setValue(HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE, mode.name());
    if (rejected) {
      HoodieNotSupportedException error = assertThrows(HoodieNotSupportedException.class,
          () -> HoodieCDCUtils.validateCdcSchema(config, schema));
      assertTrue(error.getMessage().contains(mode.name()));
      assertTrue(error.getMessage().contains("embedding"));
      assertTrue(error.getMessage().contains(HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE.key() + "=OP_KEY_ONLY"));
    } else {
      assertDoesNotThrow(() -> HoodieCDCUtils.validateCdcSchema(config, schema));
    }
  }
}
