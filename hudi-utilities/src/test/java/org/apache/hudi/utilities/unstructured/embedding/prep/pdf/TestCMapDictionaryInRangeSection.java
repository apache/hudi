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

package org.apache.hudi.utilities.unstructured.embedding.prep.pdf;

import org.apache.fontbox.cmap.CMap;
import org.apache.fontbox.cmap.CMapParser;
import org.apache.pdfbox.io.RandomAccessReadBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * CMapParser ends a dictionary at its first key that is not a name, so dictionary delimiters do not pair up as the
 * scanner's containers do: an open dictionary does not hide a later bfrange section, and a dictionary delimiter inside a
 * section is refused.
 */
class TestCMapDictionaryInRangeSection {

  /** One code more than the cap: 0x000000 to 0x040000. */
  private static final String OVER_CAP_RANGE = "<000000> <040000> <0041> endbfrange";
  private static final int LAST_CODE = 0x040000;
  private static final String WELL_FORMED_HEADER = "/CIDInit /ProcSet findresource begin\n12 dict begin\nbegincmap\n"
      + "/CIDSystemInfo << /Registry (Adobe) /Ordering (UCS) /Supplement 0 >> def\n/CMapName /Test-UCS def\n"
      + "1 begincodespacerange\n<000000> <FFFFFF>\nendcodespacerange\n";

  @ParameterizedTest
  @ValueSource(strings = {
      "<< 1 1 beginbfrange ",                                // a dictionary left open before the section
      "<< /Key 1 2 1 beginbfrange ",                         // a number in key position ends it
      "2 beginbfrange <00> <05> << <00> ",                   // a dictionary destination ends at a hex key
      "2 beginbfrange <00> <05> [ << <00> ] ",               // inside a short destination array (ignored by fontbox)
      "2 beginbfrange <00> <05> [ >> <00> ] ",               // a stray close inside a short destination array
      "2 beginbfrange <00> <05> [ endbfrange ] ",            // an end operator inside an array is an array element
      "2 beginbfrange <00> <05> [ [ endbfrange ] ] "
  })
  void dictionaryDelimitersDoNotHideTheRangeCodes(String prefix) throws Exception {
    String cmap = prefix + OVER_CAP_RANGE;
    assertNotNull(parseWithFontbox(cmap).toUnicode(LAST_CODE, 3), "fontbox expands the range of: " + cmap);
    StreamStructureScanner.Refusal failure =
        assertThrows(StreamStructureScanner.Refusal.class, () -> scan(cmap), cmap);
    assertEquals(PdfFailureCode.DECODE_LIMIT, failure.code());
  }

  @Test
  void wellFormedCMapWithADictionaryOutsideTheSectionPasses() throws Exception {
    String cmap = WELL_FORMED_HEADER + "1 beginbfrange\n<000001> <040000> <0041>\nendbfrange\n"
        + "/Extra << /Key 1 >> def\nendcmap\nCMapName currentdict /CMap defineresource pop\nend\nend\n";
    assertNotNull(parseWithFontbox(cmap).toUnicode(LAST_CODE, 3));
    scan(cmap);                                            // exactly the cap
  }

  @Test
  void wellFormedRangeWithAnArrayDestinationPassesAtTheCap() throws Exception {
    String cmap = WELL_FORMED_HEADER + "2 beginbfrange\n<000001> <000002> [<0041> <0042>]\n<000003> <040000> <0041>\n"
        + "endbfrange\nendcmap\n";
    CMap parsed = parseWithFontbox(cmap);
    assertEquals("B", parsed.toUnicode(2, 3));
    assertNotNull(parsed.toUnicode(LAST_CODE, 3));
    scan(cmap);                                            // 2 + 262,142 codes: exactly the cap
  }

  @Test
  void wellFormedCMapWithADictionaryOutsideTheSectionStillCounts() {
    String cmap = WELL_FORMED_HEADER + "1 beginbfrange\n" + OVER_CAP_RANGE + "\nendcmap\n";
    assertThrows(StreamStructureScanner.Refusal.class, () -> scan(cmap));
  }

  private static CMap parseWithFontbox(String cmap) throws Exception {
    try (RandomAccessReadBuffer buffer = new RandomAccessReadBuffer(cmap.getBytes(StandardCharsets.ISO_8859_1))) {
      return new CMapParser(false).parse(buffer);
    }
  }

  private static void scan(String cmap) throws Exception {
    try (StreamStructureScanner.CMapRanges scanner = new StreamStructureScanner.CMapRanges("test CMap")) {
      scanner.write(cmap.getBytes(StandardCharsets.ISO_8859_1));
    }
  }
}
