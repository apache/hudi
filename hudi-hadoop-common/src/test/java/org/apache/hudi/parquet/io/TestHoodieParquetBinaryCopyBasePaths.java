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

package org.apache.hudi.parquet.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieParquetBinaryCopyBasePaths {

  @Test
  void testLegacyArrayPathsAreConverted() {
    TestableBinaryCopy copy = new TestableBinaryCopy();
    String[] genericLegacyPath = {"unknown", "bag", "array_element"};
    assertTrue(copy.convertLegacy3LevelArray(genericLegacyPath));
    assertArrayEquals(new String[] {"unknown", "list", "element"}, genericLegacyPath);

    copy.requiredSchema = MessageTypeParser.parseMessageType(
        "message record { optional group values (LIST) { repeated group list { optional binary element; } } }");
    String[] avroLegacyPath = {"values", "bag", "array"};
    assertTrue(copy.convertLegacy3LevelArray(avroLegacyPath));
    assertArrayEquals(new String[] {"values", "list", "element"}, avroLegacyPath);

    String[] unchanged = {"values", "list", "element"};
    assertFalse(copy.convertLegacy3LevelArray(unchanged));
  }

  @Test
  void testLegacyMapPathsAreConvertedAndUnknownPathsAreIgnored() {
    TestableBinaryCopy copy = new TestableBinaryCopy();
    copy.requiredSchema = MessageTypeParser.parseMessageType(
        "message record { optional group properties (MAP) { repeated group key_value { "
            + "required binary key; optional binary value; } } }");

    String[] path = {"properties", "map", "value"};
    assertTrue(copy.convertLegacyMap(path));
    assertArrayEquals(new String[] {"properties", "key_value", "value"}, path);

    assertFalse(copy.convertLegacyMap(new String[] {"missing", "map", "value"}));
  }

  private static class TestableBinaryCopy extends HoodieParquetBinaryCopyBase {
    private TestableBinaryCopy() {
      super(new Configuration());
    }

    @Override
    protected Map<String, String> finalizeMetadata() {
      return Collections.emptyMap();
    }
  }
}
