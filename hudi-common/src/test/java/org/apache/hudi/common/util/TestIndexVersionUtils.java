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

package org.apache.hudi.common.util;

import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestIndexVersionUtils {

  static HoodieIndexMetadata loadIndexDefFromResource(String resourceName) {
    try {
      String resourcePath = TestIndexVersionUtils.class.getClassLoader().getResource(resourceName).toString();
      return HoodieIndexMetadata.fromJson(new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(new java.net.URI(resourcePath)))));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testPopulateVersionFieldIfMissing() {
    Function<String, HoodieIndexMetadata> getIndexDef = (idxFileName) ->
        loadIndexDefFromResource(idxFileName);
    HoodieIndexMetadata loadedDef = getIndexDef.apply("indexMissingVersion1.json");
    assertEquals(2, loadedDef.getIndexDefinitions().size());
    // Apply the function fixing the missing version field
    // Table version 9 with missing version field is not acceptable for secondary index as it should always write the version field.
    assertThrows(IllegalArgumentException.class, () -> IndexVersionUtils.populateIndexVersionIfMissing(HoodieTableVersion.NINE,
        Option.of(getIndexDef.apply("indexMissingVersion1.json"))));

    // If it is table version 8, secondary index def missing version field will be fixed.
    HoodieIndexMetadata loadedDef2 = getIndexDef.apply("indexMissingVersion1.json");
    IndexVersionUtils.populateIndexVersionIfMissing(HoodieTableVersion.EIGHT, Option.of(loadedDef2));

    assertEquals(HoodieIndexVersion.V1, loadedDef2.getIndexDefinitions().get("column_stats").getVersion());
    assertEquals(HoodieIndexVersion.V1, loadedDef2.getIndexDefinitions().get("secondary_index_idx_price").getVersion());
    validateAllFieldsExcludingVersion(loadedDef2);
  }

  private static void validateAllFieldsExcludingVersion(HoodieIndexMetadata loadedDef) {
    HoodieIndexDefinition colStatsDef = loadedDef.getIndexDefinitions().get("column_stats");
    assertEquals("column_stats", colStatsDef.getIndexName());
    assertEquals("column_stats", colStatsDef.getIndexType());
    assertEquals(Collections.emptyMap(), colStatsDef.getIndexOptions());
    assertEquals(Arrays.asList(
        "_hoodie_commit_time",
        "_hoodie_partition_path",
        "_hoodie_record_key",
        "key",
        "secKey",
        "partition",
        "intField",
        "city",
        "textField1",
        "textField2",
        "textField3",
        "textField4",
        "decimalField",
        "longField",
        "incrLongField",
        "round"), colStatsDef.getSourceFields());

    HoodieIndexDefinition secIdxDef = loadedDef.getIndexDefinitions().get("secondary_index_idx_price");
    assertEquals("secondary_index_idx_price", secIdxDef.getIndexName());
    assertEquals("secondary_index", secIdxDef.getIndexType());
    assertEquals("identity", secIdxDef.getIndexFunction());
    assertEquals(Collections.singletonList("price"), secIdxDef.getSourceFields());
    assertEquals(Collections.emptyMap(), secIdxDef.getIndexOptions());
  }
} 