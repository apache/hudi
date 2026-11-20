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

import org.apache.hudi.utilities.unstructured.embedding.text.Utf16;

import java.io.Writer;

/**
 * The stripper's output for one page, keeping at most {@code maxChars} chars (first check):
 * the rest is dropped at a code-point boundary and the page is marked truncated.
 */
final class BoundedPageWriter extends Writer {

  private final int maxChars;
  private final StringBuilder text = new StringBuilder();
  private boolean truncated;

  BoundedPageWriter(int maxChars) {
    this.maxChars = maxChars;
  }

  @Override
  public void write(char[] buffer, int offset, int length) {
    if (truncated) {
      return;
    }
    int room = maxChars - text.length();
    if (length <= room) {
      text.append(buffer, offset, length);
      return;
    }
    int keep = Utf16.floor(buffer, offset, room);
    text.append(buffer, offset, keep);
    truncated = true;
  }

  @Override
  public void write(String s, int offset, int length) {
    if (truncated) {
      return;
    }
    int room = maxChars - text.length();
    if (length <= room) {
      text.append(s, offset, offset + length);
      return;
    }
    int keep = room > 0 ? Utf16.floor(s, offset + room, true) - offset : 0;
    text.append(s, offset, offset + keep);
    truncated = true;
  }

  @Override
  public void flush() {
    // the text is kept in memory: nothing to flush
  }

  @Override
  public void close() {
    // the text is kept in memory: nothing to release
  }

  String text() {
    return text.toString();
  }

  boolean truncated() {
    return truncated;
  }
}
