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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.exception.HoodieNotSupportedException;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestNativeLogFooterMetadata {

  @Test
  void testWriteThenReadRoundTrip() {
    Map<HeaderMetadataType, String> header = new HashMap<>();
    header.put(HeaderMetadataType.INSTANT_TIME, "001");
    header.put(HeaderMetadataType.SCHEMA, "{\"type\":\"record\",\"name\":\"test\",\"fields\":[]}");
    header.put(HeaderMetadataType.IS_PARTIAL, "true");

    // Footer produced by the write path must be readable by the read path.
    Map<String, String> footer = NativeLogFooterMetadata.toFooterMetadata(header);
    assertEquals(String.valueOf(HoodieLogFormat.CURRENT_VERSION), footer.get("hudi.log.format.VERSION"));
    assertEquals("001", footer.get("hudi.log.format.INSTANT_TIME"));
    assertEquals("{\"type\":\"record\",\"name\":\"test\",\"fields\":[]}",
        footer.get("hudi.log.format.SCHEMA"));
    assertEquals("true", footer.get("hudi.log.format.IS_PARTIAL"));
    assertEquals(4, footer.size());

    Map<HeaderMetadataType, String> parsed = NativeLogFooterMetadata.fromFooterMetadata(footer);
    assertEquals("001", parsed.get(HeaderMetadataType.INSTANT_TIME));
    assertEquals("{\"type\":\"record\",\"name\":\"test\",\"fields\":[]}", parsed.get(HeaderMetadataType.SCHEMA));
    assertEquals("true", parsed.get(HeaderMetadataType.IS_PARTIAL));
    // VERSION is injected on write and surfaced on read.
    assertEquals(String.valueOf(HoodieLogFormat.CURRENT_VERSION),
        parsed.get(HeaderMetadataType.VERSION));
  }

  @Test
  void testNullHeaderValuesAreDropped() {
    Map<HeaderMetadataType, String> header = new HashMap<>();
    header.put(HeaderMetadataType.INSTANT_TIME, "001");
    header.put(HeaderMetadataType.SCHEMA, null);

    Map<HeaderMetadataType, String> parsed =
        NativeLogFooterMetadata.fromFooterMetadata(NativeLogFooterMetadata.toFooterMetadata(header));
    assertEquals("001", parsed.get(HeaderMetadataType.INSTANT_TIME));
    assertFalse(parsed.containsKey(HeaderMetadataType.SCHEMA));
  }

  @Test
  void testMissingFooterMetadataReturnsEmptyHeader() {
    assertTrue(NativeLogFooterMetadata.fromFooterMetadata(new HashMap<>()).isEmpty());
  }

  @Test
  void testUnknownHeaderTypesAreIgnored() {
    Map<String, String> footer = new HashMap<>();
    footer.put(NativeLogFooterMetadata.getFooterMetadataKey(HeaderMetadataType.VERSION), "2");
    footer.put(NativeLogFooterMetadata.getFooterMetadataKey(HeaderMetadataType.INSTANT_TIME), "001");
    footer.put(NativeLogFooterMetadata.FOOTER_METADATA_KEY_PREFIX + "some_future_key", "v");

    Map<HeaderMetadataType, String> parsed = NativeLogFooterMetadata.fromFooterMetadata(footer);
    assertEquals("001", parsed.get(HeaderMetadataType.INSTANT_TIME));
    assertEquals(2, parsed.size());
  }

  @Test
  void testNewerFormatVersionIsRejected() {
    Map<String, String> footer = new HashMap<>();
    footer.put(NativeLogFooterMetadata.getFooterMetadataKey(HeaderMetadataType.VERSION),
        String.valueOf(HoodieLogFormat.CURRENT_VERSION + 1));
    footer.put(NativeLogFooterMetadata.getFooterMetadataKey(HeaderMetadataType.INSTANT_TIME), "001");

    assertThrows(HoodieNotSupportedException.class,
        () -> NativeLogFooterMetadata.fromFooterMetadata(footer));
  }
}
