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

import org.apache.pdfbox.contentstream.PDContentStream;
import org.apache.pdfbox.contentstream.operator.Operator;
import org.apache.pdfbox.cos.COSBase;
import org.apache.pdfbox.io.RandomAccessRead;
import org.apache.pdfbox.io.RandomAccessReadView;
import org.apache.pdfbox.pdfparser.PDFStreamParser;
import org.apache.pdfbox.pdmodel.PDResources;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.util.Matrix;

import java.io.IOException;
import java.io.InputStream;

/**
 * PDFBox's own content parser with the decode guard's structure budgets enforced on the tokens it parses, so the budgets
 * hold on exactly what text extraction parses next, whatever the guard's pre-scan made of the same bytes:
 * <ul>
 *   <li>at most {@code maxOperands} operands pending before an operator (PDFStreamEngine keeps them in one list), counting
 *       an inline image's parameters too;</li>
 *   <li>at most {@code maxOperands} objects parsed inside one top-level array or dictionary operand, nested ones
 *       included, refused while it is being built;</li>
 *   <li>at most {@code maxTokenBytes} bytes read for one token, counted from its first byte after whitespace and comments
 *       (inside an array or dictionary, from each element's): a string or an inline image with its data is built from no
 *       more than the guard's per-body budget, as the pre-scan's body budget intends;</li>
 *   <li>at most {@code maxTokenBytes} retained-body bytes across all operands pending before an operator, including
 *       nested arrays and dictionaries. Skipped whitespace and comments are excluded, and the budget resets at each
 *       operator, so an unfiltered stream may contain many independently bounded operand batches;</li>
 *   <li>at most {@code maxContentBytes} bytes read in all. This applies where PDFBox reads bytes the guard did not decode
 *       the same way: a page whose /Contents is one stream filtered by exactly /FlateDecode, which
 *       PDPage#getContentsForStreamParsing inflates without its /DecodeParms predictor.</li>
 * </ul>
 * A budget passed fails the page with {@code DECODE_LIMIT}. Only reading happens here; nothing is extracted.
 */
final class BoundedContentParser extends PDFStreamParser implements AutoCloseable {

  private final BudgetedRead read;
  private final int maxOperands;
  private int pendingOperands;
  private int nestedObjects;

  private BoundedContentParser(BudgetedContent content, int maxOperands) throws IOException {
    super(content);
    this.read = content.read;
    this.maxOperands = maxOperands;
  }

  /**
   * @param content         a page or form, as PDFBox would parse it
   * @param maxOperands     operands pending at an operator, and objects inside one top-level operand
   * @param maxTokenBytes   bytes one token may be read from
   * @param maxContentBytes bytes the whole content may be read from ({@code Long.MAX_VALUE}: unbounded here)
   */
  static BoundedContentParser of(PDContentStream content, int maxOperands, long maxTokenBytes, long maxContentBytes)
      throws IOException {
    return new BoundedContentParser(new BudgetedContent(content, maxTokenBytes, maxContentBytes), maxOperands);
  }

  @Override
  public Object parseNextToken() throws IOException {
    nestedObjects = 0;
    Object token = super.parseNextToken();
    if (token instanceof Operator) {
      pendingOperands = 0;
      read.resetOperands();
    } else if (token != null && ++pendingOperands > maxOperands) {
      throw refusal("more than " + maxOperands + " operands before an operator");
    }
    return token;
  }

  @Override
  protected void skipSpaces() throws IOException {
    read.skipping = true;                              // whitespace and comments are skipped, never built into a token
    try {
      super.skipSpaces();
    } finally {
      read.skipping = false;
    }
    read.startToken();
  }

  @Override
  protected COSBase parseDirObject() throws IOException {
    if (++nestedObjects > maxOperands) {
      throw refusal("an array or dictionary operand with more than " + maxOperands + " elements");
    }
    return super.parseDirObject();
  }

  private static PageFailure refusal(String what) {
    return new PageFailure(PdfFailureCode.DECODE_LIMIT, what + " as PDFBox parses the content");
  }

  /** {@code content} with its parsing view wrapped in a {@link BudgetedRead}; everything else delegates. */
  private static final class BudgetedContent implements PDContentStream {
    private final PDContentStream content;
    private final long maxTokenBytes;
    private final long maxContentBytes;
    private BudgetedRead read;

    BudgetedContent(PDContentStream content, long maxTokenBytes, long maxContentBytes) {
      this.content = content;
      this.maxTokenBytes = maxTokenBytes;
      this.maxContentBytes = maxContentBytes;
    }

    @Override
    public RandomAccessRead getContentsForStreamParsing() throws IOException {
      read = new BudgetedRead(content.getContentsForStreamParsing(), maxTokenBytes, maxContentBytes);
      return read;
    }

    @Override
    public InputStream getContents() throws IOException {
      return content.getContents();
    }

    @Override
    public RandomAccessRead getContentsForRandomAccess() throws IOException {
      return content.getContentsForRandomAccess();
    }

    @Override
    public PDResources getResources() {
      return content.getResources();
    }

    @Override
    public PDRectangle getBBox() {
      return content.getBBox();
    }

    @Override
    public Matrix getMatrix() {
      return content.getMatrix();
    }
  }

  /**
   * A read-only view that refuses to read past a byte budget for the whole content and for the token being parsed. Every
   * method delegates, the stateful defaults (peek, rewind, skip, available) included: PDFBox's non-seekable Flate reader
   * keeps its own buffer behind them.
   */
  private static final class BudgetedRead implements RandomAccessRead {
    private final RandomAccessRead source;
    private final long maxTokenBytes;
    private final long maxContentBytes;
    private long tokenStart;
    private long lastPosition;
    /** Bytes read into the current operand batch; nested token boundaries do not release its objects. */
    private long retainedBytes;
    /** While set, only the whole-content budget applies: the bytes being read are skipped, not kept. */
    private boolean skipping;

    BudgetedRead(RandomAccessRead source, long maxTokenBytes, long maxContentBytes) throws IOException {
      this.source = source;
      this.lastPosition = source.getPosition();
      this.maxTokenBytes = maxTokenBytes;
      this.maxContentBytes = maxContentBytes;
    }

    void startToken() throws IOException {
      tokenStart = source.getPosition();
      lastPosition = tokenStart;
    }

    void resetOperands() {
      retainedBytes = 0;
    }

    private void checkBudget() throws IOException {
      long position = source.getPosition();
      long added = Math.max(0, position - lastPosition);
      lastPosition = position;
      if (!skipping) {
        if (added > maxTokenBytes - retainedBytes) {
          throw refusal("an operand batch with more than " + maxTokenBytes + " retained-body bytes");
        }
        retainedBytes += added;
      }
      if (position > maxContentBytes) {
        throw refusal("content of more than " + maxContentBytes + " bytes");
      }
      if (!skipping && position - tokenStart > maxTokenBytes) {
        throw refusal("a string, inline image or other token of more than " + maxTokenBytes + " bytes");
      }
    }

    @Override
    public int read() throws IOException {
      int b = source.read();
      checkBudget();
      return b;
    }

    @Override
    public int read(byte[] b) throws IOException {
      int n = source.read(b);
      checkBudget();
      return n;
    }

    @Override
    public int read(byte[] b, int offset, int length) throws IOException {
      int n = source.read(b, offset, length);
      checkBudget();
      return n;
    }

    @Override
    public void readFully(byte[] b) throws IOException {
      source.readFully(b);
      checkBudget();
    }

    @Override
    public void readFully(byte[] b, int offset, int length) throws IOException {
      source.readFully(b, offset, length);
      checkBudget();
    }

    @Override
    public void skip(int length) throws IOException {
      source.skip(length);
      checkBudget();
    }

    @Override
    public int peek() throws IOException {
      return source.peek();
    }

    @Override
    public void rewind(int bytes) throws IOException {
      source.rewind(bytes);
    }

    @Override
    public int available() throws IOException {
      return source.available();
    }

    @Override
    public long getPosition() throws IOException {
      return source.getPosition();
    }

    @Override
    public void seek(long position) throws IOException {
      source.seek(position);
      checkBudget();
    }

    @Override
    public long length() throws IOException {
      return source.length();
    }

    @Override
    public boolean isClosed() {
      return source.isClosed();
    }

    @Override
    public boolean isEOF() throws IOException {
      return source.isEOF();
    }

    @Override
    public RandomAccessReadView createView(long startPosition, long streamLength) throws IOException {
      return source.createView(startPosition, streamLength);
    }

    @Override
    public void close() throws IOException {
      source.close();
    }
  }
}
