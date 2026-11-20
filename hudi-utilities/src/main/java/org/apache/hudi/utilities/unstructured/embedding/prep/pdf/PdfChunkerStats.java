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
 * Per-file counters of one chunk iterator, primitives only. Printed in the per-file summary WARN.
 */
public final class PdfChunkerStats {

  long pagesSeen;
  long pagesFailed;
  long pagesTruncated;
  long pagesEmpty;
  long glyphLimited;
  long decodeLimited;
  long decodeFailed;
  long streamsSkipped;
  long graphicsLimited;
  long chunksEmitted;
  long forcedSplits;
  long stagedBytes;
  long elapsedMillis;

  public long pagesSeen() {
    return pagesSeen;
  }

  /** Pages that failed with {@code PAGE_ERROR}. */
  public long pagesFailed() {
    return pagesFailed;
  }

  public long pagesTruncated() {
    return pagesTruncated;
  }

  public long pagesEmpty() {
    return pagesEmpty;
  }

  public long glyphLimited() {
    return glyphLimited;
  }

  public long decodeLimited() {
    return decodeLimited;
  }

  /** Pages whose content streams all failed to decode. */
  public long decodeFailed() {
    return decodeFailed;
  }

  /** Undecodable content streams skipped on partially decodable pages, including repeated uses. */
  public long streamsSkipped() {
    return streamsSkipped;
  }

  public long graphicsLimited() {
    return graphicsLimited;
  }

  /** OK chunks cut (returned or held back). */
  public long chunksEmitted() {
    return chunksEmitted;
  }

  public long forcedSplits() {
    return forcedSplits;
  }

  public long stagedBytes() {
    return stagedBytes;
  }

  /** Milliseconds from the first {@code hasNext()} to the end of the file. */
  public long elapsedMillis() {
    return elapsedMillis;
  }

  @Override
  public String toString() {
    return "pagesSeen=" + pagesSeen + " pagesFailed=" + pagesFailed + " pagesTruncated=" + pagesTruncated + " pagesEmpty="
        + pagesEmpty + " glyphLimited=" + glyphLimited + " decodeLimited=" + decodeLimited + " graphicsLimited="
        + graphicsLimited + " decodeFailed=" + decodeFailed + " streamsSkipped=" + streamsSkipped
        + " chunksEmitted=" + chunksEmitted + " forcedSplits=" + forcedSplits + " stagedBytes="
        + stagedBytes + " elapsedMillis=" + elapsedMillis;
  }
}
