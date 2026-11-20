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

/** The raw text of one extracted page (before normalization), truncation and skipped-content loss. */
public final class PageText {

  private final String text;
  private final boolean truncated;
  private final int skippedContentStreams;

  /**
   * @param text      raw page text, at most {@code max.page.chars} chars
   * @param truncated whether text beyond {@code max.page.chars} was dropped
   */
  public PageText(String text, boolean truncated) {
    this(text, truncated, 0);
  }

  /**
   * @param text raw page text, at most {@code max.page.chars} chars
   * @param truncated whether text beyond {@code max.page.chars} was dropped
   * @param skippedContentStreams content streams PDFBox could not decode on this partially decodable page
   */
  public PageText(String text, boolean truncated, int skippedContentStreams) {
    this.text = text;
    this.truncated = truncated;
    this.skippedContentStreams = skippedContentStreams;
  }

  public String text() {
    return text;
  }

  public boolean truncated() {
    return truncated;
  }

  public int skippedContentStreams() {
    return skippedContentStreams;
  }
}
