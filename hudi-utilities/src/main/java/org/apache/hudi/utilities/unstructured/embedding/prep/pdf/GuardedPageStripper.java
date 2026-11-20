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
import org.apache.pdfbox.cos.COSBase;
import org.apache.pdfbox.cos.COSDictionary;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.cos.COSNumber;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.graphics.form.PDFormXObject;
import org.apache.pdfbox.pdmodel.graphics.form.PDTransparencyGroup;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.text.TextPosition;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

/**
 * Extracts ONE page under the page caps of the in-JVM page source. A FRESH instance per page: PDFBox's
 * private marked-content deque is never cleared between pages (PDFTextStripper.java:162), so a reused stripper would
 * accumulate it.
 * <ul>
 *   <li>content-stream order ({@code sortByPosition=false}), beads off, duplicate-overlap suppression off (no per-glyph map);
 *       paragraph end {@code \n\n}, line separator {@code \n}, page end empty;</li>
 *   <li>more than {@code maxGlyphs} glyphs, or more than {@code maxChars} chars of glyph text after
 *       {@code /ActualText} substitution, fails the page with {@code PAGE_GLYPH_LIMIT} (the caller passes
 *       {@code max.page.glyphs} for both, so a page between {@code max.page.chars} and that is truncated, not failed);</li>
 *   <li>more than {@code maxDepth} graphics states saved above the page's base state, or marked-content sequences nested
 *       deeper than {@code maxDepth}, fail the page with {@code GRAPHICS_DEPTH_LIMIT}. PDFBox gives every form and
 *       transparency group a stack of its own, starting from one copy of the current state, so the states held by the
 *       enclosing streams count too: a drawn form holds one state, as if it were wrapped in {@code q}.</li>
 * </ul>
 * The caps throw an unchecked {@link PageFailure}, which PDFBox's operator error handling does not swallow.
 */
final class GuardedPageStripper extends PDFTextStripper {

  private final int maxGlyphs;
  private final int maxChars;
  private final int maxDepth;
  private final StreamDecodeGuard fontGuard;
  private long glyphs;
  private long chars;
  private int markedDepth;
  /** Graphics states held by the streams enclosing the one being processed. */
  private int outerStates;
  private int skippedContentStreams;

  GuardedPageStripper(int maxGlyphs, int maxChars, int maxDepth) {
    this(maxGlyphs, maxChars, maxDepth, null);
  }

  GuardedPageStripper(int maxGlyphs, int maxChars, int maxDepth, StreamDecodeGuard fontGuard) {
    this.fontGuard = fontGuard;
    this.maxGlyphs = maxGlyphs;
    this.maxChars = maxChars;
    this.maxDepth = maxDepth;
    setSortByPosition(false);
    setShouldSeparateByBeads(false);
    setSuppressDuplicateOverlappingText(false);
    setParagraphEnd("\n\n");
    setLineSeparator("\n");
    setPageEnd("");
  }

  /** Extracts {@code page} of {@code document} into {@code out}; {@code processPage} is public. */
  void extract(PDDocument document, PDPage page, Writer out) throws IOException {
    this.document = document;
    this.output = out;
    processPage(page);
  }

  /** Invoked form streams whose extraction failed with an I/O error PDFBox handles by skipping the form. */
  int skippedContentStreams() {
    return skippedContentStreams;
  }

  @Override
  protected void processOperator(Operator operator, List<COSBase> operands) throws IOException {
    if (fontGuard != null && !operands.isEmpty() && operands.get(0) instanceof COSName) {
      String operation = operator.getName();
      if ("gs".equals(operation)) {
        fontGuard.checkFontOperation(getResources(), (COSName) operands.get(0), true);
      } else if ("Tf".equals(operation) && operands.size() >= 2 && operands.get(1) instanceof COSNumber) {
        fontGuard.checkFontOperation(getResources(), (COSName) operands.get(0), false);
      }
    }
    super.processOperator(operator, operands);
  }

  @Override
  protected void processTextPosition(TextPosition text) {
    if (++glyphs > maxGlyphs) {
      throw new PageFailure(PdfFailureCode.PAGE_GLYPH_LIMIT, "more than " + maxGlyphs + " glyphs on the page");
    }
    super.processTextPosition(text);
    String unicode = text.getUnicode();
    chars += unicode == null ? 0 : unicode.length();
    if (chars > maxChars) {
      throw new PageFailure(PdfFailureCode.PAGE_GLYPH_LIMIT, "more than " + maxChars + " chars of glyph text on the page");
    }
  }

  @Override
  public void saveGraphicsState() {
    super.saveGraphicsState();
    if (outerStates + getGraphicsStackSize() - 1 > maxDepth) {
      throw new PageFailure(PdfFailureCode.GRAPHICS_DEPTH_LIMIT, "more than " + maxDepth + " nested graphics states");
    }
  }

  @Override
  public void showForm(PDFormXObject form) throws IOException {
    int enclosing = enterNestedStream();
    try {
      super.showForm(form);
    } catch (IOException e) {
      skippedContentStreams++;
      throw e;
    } finally {
      outerStates = enclosing;
    }
  }

  @Override
  protected void processTransparencyGroup(PDTransparencyGroup group) throws IOException {
    int enclosing = enterNestedStream();
    try {
      super.processTransparencyGroup(group);
    } catch (IOException e) {
      skippedContentStreams++;
      throw e;
    } finally {
      outerStates = enclosing;
    }
  }

  /** Adds the current stack to the enclosing states before PDFBox starts a nested stream; returns the value to restore. */
  private int enterNestedStream() {
    int enclosing = outerStates;
    if (outerStates + getGraphicsStackSize() > maxDepth) {
      throw new PageFailure(PdfFailureCode.GRAPHICS_DEPTH_LIMIT, "more than " + maxDepth + " nested graphics states");
    }
    outerStates += getGraphicsStackSize();
    return enclosing;
  }

  @Override
  public void beginMarkedContentSequence(COSName tag, COSDictionary properties) {
    super.beginMarkedContentSequence(tag, properties);
    if (++markedDepth > maxDepth) {
      throw new PageFailure(PdfFailureCode.GRAPHICS_DEPTH_LIMIT, "marked content nested deeper than " + maxDepth);
    }
  }

  @Override
  public void endMarkedContentSequence() {
    super.endMarkedContentSequence();
    if (markedDepth > 0) {
      markedDepth--;
    }
  }
}
