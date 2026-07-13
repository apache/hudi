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

package org.apache.hudi.utilities.sources.helpers.unstructured;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the never-throw contract and status mapping of the Tika-backed parser.
 * Fixtures are generated in-line (no binary files in the repo).
 */
public class TestTikaDocumentParser {

  private final TikaDocumentParser parser = new TikaDocumentParser();

  private ParseResult parse(byte[] content, String fileName, int maxChars) {
    return parser.parse(new ByteArrayInputStream(content), fileName, maxChars);
  }

  @Test
  public void testPlainTextAndHtmlSuccessWithMetadata() {
    ParseResult text = parse("hudi ingests unstructured data".getBytes(StandardCharsets.UTF_8),
        "note.txt", 1000);
    assertEquals(ParseResult.ParseStatus.SUCCESS, text.getStatus());
    assertTrue(text.getText().contains("unstructured"));
    assertFalse(text.getMetadata().isEmpty()); // content-type at minimum

    ParseResult html = parse("<html><title>t</title><body><p>lakehouse body</p></body></html>"
        .getBytes(StandardCharsets.UTF_8), "page.html", 1000);
    assertEquals(ParseResult.ParseStatus.SUCCESS, html.getStatus());
    assertTrue(html.getText().contains("lakehouse body"));
    assertFalse(html.getText().contains("<p>")); // markup stripped
  }

  @Test
  public void testTruncationAtCharCap() {
    byte[] longText = new byte[5000];
    java.util.Arrays.fill(longText, (byte) 'a');
    ParseResult result = parse(longText, "big.txt", 100);
    assertEquals(ParseResult.ParseStatus.TRUNCATED, result.getStatus());
    assertTrue(result.getText().length() <= 100 + 1);
  }

  @Test
  public void testCorruptAndBinaryInputsNeverThrow() {
    // Claims to be PDF (magic bytes) but is corrupt: parser error -> FAILED, not thrown
    ParseResult corrupt = parse("%PDF-1.4 this is not really a pdf".getBytes(StandardCharsets.UTF_8),
        "corrupt.pdf", 1000);
    assertEquals(ParseResult.ParseStatus.FAILED, corrupt.getStatus());
    assertNotNull(corrupt.getError());

    // Unrecognizable binary: no text, no error -> EMPTY
    ParseResult binary = parse(new byte[] {0x00, 0x11, 0x22, 0x33, (byte) 0xff}, "blob.bin", 1000);
    assertEquals(ParseResult.ParseStatus.EMPTY, binary.getStatus());
    assertEquals("", binary.getText());
  }
}
