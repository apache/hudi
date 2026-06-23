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

package org.apache.hudi.io.storage.hadoop;

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieVariantReconstruction {

  private static HoodieSchema recordWithVariant(HoodieSchema variantSchema) {
    return HoodieSchema.createRecord("test_record", "org.apache.hudi.test", null,
        Collections.singletonList(HoodieSchemaField.of("v", variantSchema)));
  }

  private static HoodieStorage storageWithReadingShredded(Path tmp, boolean enabled) {
    HoodieStorage storage = HoodieTestUtils.getStorage(tmp.toString());
    storage.getConf().set(HoodieStorageConfig.PARQUET_VARIANT_ALLOW_READING_SHREDDED.key(),
        Boolean.toString(enabled));
    return storage;
  }

  @Test
  void failsFastWhenShreddedColumnPresentButReadingDisabled(@TempDir Path tmp) {
    HoodieSchema fileSchema = recordWithVariant(
        HoodieSchema.createVariantShredded(HoodieSchema.create(HoodieSchemaType.INT)));
    HoodieSchema requestedSchema = recordWithVariant(HoodieSchema.createVariant());

    HoodieException ex = assertThrows(HoodieException.class, () ->
        HoodieVariantReconstruction.create(fileSchema, requestedSchema,
            storageWithReadingShredded(tmp, false)));
    // The flag check must run after shredded-column detection: dropping typed_value silently would
    // corrupt rows whose payload lives there, so a disabled read of a shredded file fails fast.
    assertTrue(ex.getMessage().contains("reading shredded variants is disabled"), ex.getMessage());
  }

  @Test
  void returnsNullWhenNoShreddedColumnEvenIfReadingDisabled(@TempDir Path tmp) {
    HoodieSchema schema = recordWithVariant(HoodieSchema.createVariant());
    // No shredded variant column to reconstruct: nothing to do regardless of the flag.
    assertNull(HoodieVariantReconstruction.create(schema, schema,
        storageWithReadingShredded(tmp, false)));
  }
}
