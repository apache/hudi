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

/**
 * One record of the chunk iterator: an OK chunk, or the single marker row of a file that
 * yielded no OK chunk. Immutable; holds exactly the contract's fields.
 */
public final class PdfChunk {

  /** A record's status: an OK chunk, or the marker row of a file that FAILED or had NO_TEXT. */
  public enum Status { OK, FAILED, NO_TEXT }

  private final String sourcePathSha;
  private final long chunkId;
  private final String text;
  private final int tokenCount;
  private final String tokenizerId;
  private final int wordCount;
  private final int charCount;
  private final int overlapChars;
  private final int pageStart;
  private final int pageEnd;
  private final Status status;
  private final String statusDetail;

  PdfChunk(String sourcePathSha, long chunkId, String text, int tokenCount, String tokenizerId, int wordCount, int overlapChars,
           int pageStart, int pageEnd, Status status, String statusDetail) {
    this.sourcePathSha = sourcePathSha;
    this.chunkId = chunkId;
    this.text = text;
    this.tokenCount = tokenCount;
    this.tokenizerId = tokenizerId;
    this.wordCount = wordCount;
    this.charCount = text == null ? 0 : text.length();
    this.overlapChars = overlapChars;
    this.pageStart = pageStart;
    this.pageEnd = pageEnd;
    this.status = status;
    this.statusDetail = statusDetail;
  }

  static PdfChunk marker(String sourcePathSha, String tokenizerId, Status status, String detail) {
    return new PdfChunk(sourcePathSha, 0L, null, 0, tokenizerId, 0, 0, 0, 0, status, detail);
  }

  /** The same chunk carrying {@code detail} (the loss summary of the last OK chunk). */
  PdfChunk withStatusDetail(String detail) {
    return new PdfChunk(sourcePathSha, chunkId, text, tokenCount, tokenizerId, wordCount, overlapChars, pageStart, pageEnd, status, detail);
  }

  public String sourcePathSha() {
    return sourcePathSha;
  }

  /** 0, 1, 2, … in reading order for one source_path_sha; 0 on a marker row. */
  public long chunkId() {
    return chunkId;
  }

  /** Normalized text, an exact slice of the normalized stream; null on marker rows. */
  public String text() {
    return text;
  }

  public int tokenCount() {
    return tokenCount;
  }

  public String tokenizerId() {
    return tokenizerId;
  }

  public int wordCount() {
    return wordCount;
  }

  public int charCount() {
    return charCount;
  }

  /** Length of the text prefix repeated from the previous chunk. */
  public int overlapChars() {
    return overlapChars;
  }

  /** 1-based physical page of the first char; 0 on marker rows. */
  public int pageStart() {
    return pageStart;
  }

  /** 1-based physical page of the last char; 0 on marker rows. */
  public int pageEnd() {
    return pageEnd;
  }

  public Status status() {
    return status;
  }

  /** The failure code of a FAILED marker, or the loss summary on the last OK chunk; null otherwise. */
  public String statusDetail() {
    return statusDetail;
  }
}
