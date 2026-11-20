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

import org.apache.pdfbox.cos.COSArray;
import org.apache.pdfbox.cos.COSDictionary;
import org.apache.pdfbox.cos.COSInteger;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.cos.COSStream;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDResources;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The graphics depth limit of {@link GuardedPageStripper}: {@code maxDepth} nested {@code q} or marked-content sequences
 * pass and one more fails the page, and the graphics states of the enclosing streams count while a form is drawn.
 */
class TestGraphicsDepthLimit {

  private static final int MAX_DEPTH = 16;
  private static final String TEXT = "BT /F1 10 Tf 50 700 Td (deep) Tj ET\n";

  @Test
  void graphicsStateNestingAtTheLimitPasses() throws IOException {
    assertEquals("deep", extract(repeat("q ", MAX_DEPTH) + TEXT));
  }

  @Test
  void graphicsStateNestingAboveTheLimitFails() {
    assertDepthFailure(repeat("q ", MAX_DEPTH + 1) + TEXT);
  }

  @Test
  void markedContentNestingAtTheLimitPasses() throws IOException {
    assertEquals("deep", extract(repeat("/A BMC ", MAX_DEPTH) + TEXT));
  }

  @Test
  void markedContentNestingAboveTheLimitFails() {
    assertDepthFailure(repeat("/A BMC ", MAX_DEPTH + 1) + TEXT);
  }

  @Test
  void nestedFormsShareTheLimit() {
    // each stream alone stays far below the limit; together the page, two forms and their q exceed it
    assertDepthFailure(repeat("q ", 6) + "/X1 Do\n", repeat("q ", 6) + "/X2 Do\n", repeat("q ", 6) + TEXT);
  }

  @Test
  void nestedTransparencyGroupsShareTheLimit() {
    assertDepthFailure(true, repeat("q ", 6) + "/X1 Do\n", repeat("q ", 6) + "/X2 Do\n", repeat("q ", 6) + TEXT);
  }

  @Test
  void nestedFormsAtTheLimitPass() throws IOException {
    // a drawn form holds one graphics state of its own: 5 + (1 + 5) + (1 + 4) = 16 states above the page's base state
    assertEquals("deep", extract(false, repeat("q ", 5) + "/X1 Do\n", repeat("q ", 5) + "/X2 Do\n", repeat("q ", 4) + TEXT));
    assertDepthFailure(repeat("q ", 5) + "/X1 Do\n", repeat("q ", 5) + "/X2 Do\n", repeat("q ", 5) + TEXT);
  }

  @Test
  void depthOfAFinishedFormIsReleased() throws IOException {
    // two forms drawn one after the other, each close to the limit: only the one being drawn counts
    String form = repeat("q ", MAX_DEPTH - 1) + TEXT;
    assertTrue(extract(false, "/X1 Do\n/X2 Do\n", form, form).contains("deep"));
  }

  @Test
  void nestedEntriesWithoutStateSavesRespectTheDepthLimit() throws IOException {
    for (boolean groups : new boolean[] {false, true}) {
      assertEquals("deep", extract(groups, entryStreams(MAX_DEPTH)));
      assertDepthFailure(groups, entryStreams(MAX_DEPTH + 1));
    }
  }

  private static String[] entryStreams(int depth) {
    String[] streams = new String[depth + 1];
    for (int i = 0; i < depth; i++) {
      streams[i] = "/X" + (i + 1) + " Do\n";
    }
    streams[depth] = TEXT;
    return streams;
  }

  private static void assertDepthFailure(String... streams) {
    assertDepthFailure(false, streams);
  }

  private static void assertDepthFailure(boolean groups, String... streams) {
    PageFailure failure = assertThrows(PageFailure.class, () -> extract(groups, streams));
    assertEquals(PdfFailureCode.GRAPHICS_DEPTH_LIMIT, failure.code());
  }

  private static String extract(String pageOps) throws IOException {
    return extract(false, pageOps);
  }

  /**
   * Extracts a page whose content is {@code streams[0]}; {@code streams[i]} for i > 0 is form {@code /Xi}, which the page and
   * every form can draw. With {@code groups}, every form is a transparency group.
   */
  private static String extract(boolean groups, String... streams) throws IOException {
    try (PDDocument document = new PDDocument()) {
      PDResources resources = PdfFixtures.helvetica();
      COSDictionary xobjects = new COSDictionary();
      resources.getCOSObject().setItem(COSName.XOBJECT, xobjects);
      for (int i = 1; i < streams.length; i++) {
        COSStream form = stream(document, streams[i]);
        form.setItem(COSName.TYPE, COSName.XOBJECT);
        form.setItem(COSName.SUBTYPE, COSName.FORM);
        COSArray box = new COSArray();
        for (int v : new int[] {0, 0, 612, 792}) {
          box.add(COSInteger.get(v));
        }
        form.setItem(COSName.BBOX, box);
        form.setItem(COSName.RESOURCES, resources.getCOSObject());
        if (groups) {
          COSDictionary group = new COSDictionary();
          group.setItem(COSName.S, COSName.TRANSPARENCY);
          form.setItem(COSName.GROUP, group);
        }
        xobjects.setItem(COSName.getPDFName("X" + i), form);
      }
      PDPage page = new PDPage();
      page.setResources(resources);
      page.getCOSObject().setItem(COSName.CONTENTS, stream(document, streams[0]));
      document.addPage(page);
      BoundedPageWriter writer = new BoundedPageWriter(10_000);
      new GuardedPageStripper(1000, 1000, MAX_DEPTH).extract(document, page, writer);
      assertFalse(writer.truncated());
      return writer.text().trim();
    }
  }

  private static COSStream stream(PDDocument document, String ops) throws IOException {
    COSStream stream = document.getDocument().createCOSStream();
    try (OutputStream out = stream.createRawOutputStream()) {
      out.write(ops.getBytes(StandardCharsets.US_ASCII));
    }
    return stream;
  }

  private static String repeat(String s, int n) {
    StringBuilder sb = new StringBuilder(s.length() * n);
    for (int i = 0; i < n; i++) {
      sb.append(s);
    }
    return sb.toString();
  }
}
