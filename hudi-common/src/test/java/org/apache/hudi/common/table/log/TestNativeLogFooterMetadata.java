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
    assertTrue(footer.containsKey(NativeLogFooterMetadata.FOOTER_METADATA_KEY));

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
  void testMissingFooterKeyReturnsEmptyHeader() {
    assertTrue(NativeLogFooterMetadata.fromFooterMetadata(new HashMap<>()).isEmpty());
  }

  @Test
  void testUnknownHeaderTypesAreIgnored() {
    Map<String, String> footer = new HashMap<>();
    footer.put(NativeLogFooterMetadata.FOOTER_METADATA_KEY,
        "{\"VERSION\":\"2\",\"INSTANT_TIME\":\"001\",\"SOME_FUTURE_KEY\":\"v\"}");

    Map<HeaderMetadataType, String> parsed = NativeLogFooterMetadata.fromFooterMetadata(footer);
    assertEquals("001", parsed.get(HeaderMetadataType.INSTANT_TIME));
    assertEquals(2, parsed.size());
  }

  @Test
  void testNewerFormatVersionIsRejected() {
    Map<String, String> footer = new HashMap<>();
    footer.put(NativeLogFooterMetadata.FOOTER_METADATA_KEY,
        "{\"VERSION\":\"" + (HoodieLogFormat.CURRENT_VERSION + 1) + "\",\"INSTANT_TIME\":\"001\"}");

    assertThrows(HoodieNotSupportedException.class,
        () -> NativeLogFooterMetadata.fromFooterMetadata(footer));
  }
}
