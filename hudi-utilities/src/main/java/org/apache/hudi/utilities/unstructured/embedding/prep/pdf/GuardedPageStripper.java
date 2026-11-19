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

import org.apache.hudi.utilities.unstructured.embedding.source.CloseOnFailure;

import org.apache.pdfbox.cos.COSArray;
import org.apache.pdfbox.cos.COSBase;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.cos.COSStream;
import org.apache.pdfbox.filter.FlateFilterDecoderStream;
import org.apache.pdfbox.io.NonSeekableRandomAccessReadInputStream;
import org.apache.pdfbox.io.RandomAccessRead;
import org.apache.pdfbox.io.RandomAccessReadBuffer;
import org.apache.pdfbox.io.SequenceRandomAccessRead;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.ResourceCache;
import org.apache.pdfbox.pdmodel.graphics.form.PDFormXObject;
import org.apache.pdfbox.pdmodel.graphics.form.PDTransparencyGroup;
import org.apache.pdfbox.text.PDFTextStripper;

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

/** Extracts one PDF page in content-stream order with the configured text separators; it applies no limit of its own. */
final class GuardedPageStripper extends PDFTextStripper {
  private int skippedContentStreams;

  GuardedPageStripper() {
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
    processPage(new LossAwarePage(page));
  }

  int skippedContentStreams() {
    return skippedContentStreams;
  }

  @Override
  public void showForm(PDFormXObject form) throws IOException {
    try {
      super.showForm(form);
    } catch (IOException e) {
      skippedContentStreams++;
      throw e;
    }
  }

  @Override
  protected void processTransparencyGroup(PDTransparencyGroup group) throws IOException {
    try {
      super.processTransparencyGroup(group);
    } catch (IOException e) {
      skippedContentStreams++;
      throw e;
    }
  }

  /** Keeps PDFBox's scalar-Flate and array-separator behavior while observing streams it otherwise skips silently. */
  private final class LossAwarePage extends PDPage {
    private final ResourceCache resourceCache;

    private LossAwarePage(PDPage page) {
      super(page.getCOSObject());
      resourceCache = page.getResourceCache();
    }

    @Override
    public ResourceCache getResourceCache() {
      return resourceCache;
    }

    @Override
    public RandomAccessRead getContentsForStreamParsing() throws IOException {
      COSStream stream = getCOSObject().getCOSStream(COSName.CONTENTS);
      if (stream != null && COSName.FLATE_DECODE.equals(stream.getFilters())) {
        try {
          InputStream raw = stream.createRawInputStream();
          try (CloseOnFailure<IOException> owner = new CloseOnFailure<>(raw::close)) {
            RandomAccessRead decoded = new NonSeekableRandomAccessReadInputStream(new FlateFilterDecoderStream(raw));
            owner.succeeded();
            return decoded;
          }
        } catch (IOException failure) {
          skippedContentStreams++;
          return separator();
        }
      }
      return getContentsForRandomAccess();
    }

    @Override
    public RandomAccessRead getContentsForRandomAccess() throws IOException {
      COSStream stream = getCOSObject().getCOSStream(COSName.CONTENTS);
      if (stream != null) {
        RandomAccessRead decoded = open(stream);
        return decoded == null ? separator() : decoded;
      }
      COSArray contents = getCOSObject().getCOSArray(COSName.CONTENTS);
      List<RandomAccessRead> readers = new ArrayList<>();
      if (contents != null) {
        for (int index = 0; index < contents.size(); index++) {
          COSBase value = contents.getObject(index);
          if (value instanceof COSStream) {
            RandomAccessRead decoded = open((COSStream) value);
            if (decoded != null) {
              readers.add(decoded);
              readers.add(separator());
            }
          }
        }
      }
      return readers.isEmpty() ? new RandomAccessReadBuffer(new byte[0]) : new SequenceRandomAccessRead(readers);
    }

    private RandomAccessRead open(COSStream stream) {
      try {
        return stream.createView();
      } catch (IOException failure) {
        skippedContentStreams++;
        return null;
      }
    }

    private RandomAccessRead separator() {
      return new RandomAccessReadBuffer(new byte[] {'\n'});
    }
  }
}
