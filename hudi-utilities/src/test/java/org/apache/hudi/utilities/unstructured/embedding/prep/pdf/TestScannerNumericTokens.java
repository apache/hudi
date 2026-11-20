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

import org.apache.pdfbox.contentstream.operator.Operator;
import org.apache.pdfbox.cos.COSArray;
import org.apache.pdfbox.cos.COSNumber;
import org.apache.pdfbox.pdfparser.PDFStreamParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Numeric admission counts the tokens PDFBox consumes, retaining bounded scanner state. */
class TestScannerNumericTokens {

  @ParameterizedTest
  @ValueSource(strings = {"1+1 ", "1.2.3 ", "1+1+1 "})
  void adjacentNumericTokensAboveTheCapAreRefused(String numbers) throws Exception {
    String content = "1 ".repeat(65535) + numbers + "n\n";
    int expected = numbers.equals("1+1+1 ") ? 65538 : 65537;
    assertEquals(expected, maximumPendingOperands(content));
    assertEquals(PdfFailureCode.DECODE_LIMIT,
        assertThrows(StreamStructureScanner.Refusal.class, () -> scan(content)).code());
  }

  @ParameterizedTest
  @ValueSource(strings = {"1+1 ", "1.2.3 ", "1-1 ", "--1 ", "+ ", "- ", ". "})
  void numericBoundariesAtTheCapPass(String numbers) throws Exception {
    int tokens = maximumPendingOperands(numbers + "n\n");
    String content = "1 ".repeat(65536 - tokens) + numbers + "n\n";
    assertEquals(65536, maximumPendingOperands(content));
    scan(content);
  }

  @ParameterizedTest
  @ValueSource(strings = {"0", "000", "-0", "0.0"})
  void oneLongNumericTokenStillCountsAsAnOperand(String suffix) throws Exception {
    String content = "1 ".repeat(65536) + "0".repeat(65) + suffix + " n\n";
    assertEquals(65537, maximumPendingOperands(content));
    assertThrows(StreamStructureScanner.Refusal.class, () -> scan(content));
  }

  @ParameterizedTest
  @ValueSource(strings = {"0", ".0", "-0"})
  void longNumericTokenAtTheCapPasses(String suffix) throws Exception {
    String content = "1 ".repeat(65535) + "0".repeat(65) + suffix + " n\n";
    assertEquals(65536, maximumPendingOperands(content));
    scan(content);
  }

  @ParameterizedTest
  @ValueSource(strings = {"/Name1+2", "(1+2.3)", "<312b32>", "[1 2]", "<</A 1>>", "true", "false", "null"})
  void namesStringsHexContainersAndBooleanOperandsKeepTheirBoundaries(String operand) throws Exception {
    String at = "1 ".repeat(65535) + operand + " n\n";
    assertEquals(65536, maximumPendingOperands(at));
    scan(at);
    String over = "1 " + at;
    assertEquals(65537, maximumPendingOperands(over));
    assertThrows(StreamStructureScanner.Refusal.class, () -> scan(over));
  }

  @ParameterizedTest
  @ValueSource(strings = {"q1 ", "d01 ", "op2 "})
  void numbersFollowingOrdinaryOperatorsAreStillCounted(String prefix) throws Exception {
    assertEquals(1, maximumPendingOperands(prefix));
    String over = prefix + "1 ".repeat(65536) + "n\n";
    assertEquals(65537, maximumPendingOperands(over));
    assertThrows(StreamStructureScanner.Refusal.class, () -> scan(over));
  }

  @ParameterizedTest
  @ValueSource(strings = {"d0 ", "d1 ", "false1 ", "true1 ", "null1 ", "BI1 "})
  void pdfboxOperatorNamesKeepTheirNumericCharacters(String prefix) throws Exception {
    assertEquals(0, maximumPendingOperands(prefix));
    String at = prefix + "1 ".repeat(65536) + "n\n";
    assertEquals(65536, maximumPendingOperands(at));
    scan(at);
  }

  @ParameterizedTest
  @CsvSource({"1e2,100.0", "2.0E3,2000.0", "1e+2,100.0", "-1.25,-1.25", "+2.5,2.5"})
  void containerNumbersKeepBaseParserGrammar(String number, float value) throws Exception {
    PDFStreamParser parser = new PDFStreamParser(("[" + number + "] n").getBytes(StandardCharsets.US_ASCII));
    try {
      COSArray parsed = (COSArray) parser.parseNextToken();
      assertEquals(1, parsed.size());
      assertEquals(value, ((COSNumber) parsed.getObject(0)).floatValue());
    } finally {
      parser.close();
    }
    scan("[" + "1 ".repeat(65535) + number + "] n");
    assertThrows(StreamStructureScanner.Refusal.class,
        () -> scan("[" + "1 ".repeat(65536) + number + "] n"));
  }

  @Test
  void cmapTokensKeepTheirOwnGrammarAndEndOfStreamCheck() throws Exception {
    try (StreamStructureScanner.CMapRanges scanner = new StreamStructureScanner.CMapRanges("CMap boundary")) {
      scanner.write("1 beginbfrange <000000> <03FFFF> /Destination".getBytes(StandardCharsets.US_ASCII));
    }
    assertThrows(StreamStructureScanner.Refusal.class, () -> {
      try (StreamStructureScanner.CMapRanges scanner = new StreamStructureScanner.CMapRanges("CMap boundary")) {
        scanner.write("1 beginbfrange <000000> <040000> /Destination".getBytes(StandardCharsets.US_ASCII));
      }
    });
  }

  private static int maximumPendingOperands(String content) throws Exception {
    PDFStreamParser parser = new PDFStreamParser(content.getBytes(StandardCharsets.US_ASCII));
    int pending = 0;
    int maximum = 0;
    try {
      Object token;
      while ((token = parser.parseNextToken()) != null) {
        if (token instanceof Operator) {
          pending = 0;
        } else {
          maximum = Math.max(maximum, ++pending);
        }
      }
    } finally {
      parser.close();
    }
    System.out.println("numeric-token parser maximum pending operands=" + maximum);
    return maximum;
  }

  private static void scan(String content) {
    try (StreamStructureScanner.Operands scanner = new StreamStructureScanner.Operands("numeric-token test content", Long.MAX_VALUE)) {
      byte[] bytes = content.getBytes(StandardCharsets.US_ASCII);
      scanner.write(bytes, 0, bytes.length);
    }
  }
}
