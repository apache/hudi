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

package org.apache.hudi.utilities.unstructured.embedding.audio;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** An immutable {@link TranscriptChunk}, for chunker implementations. */
public final class TranscriptChunkValue implements TranscriptChunk {

  private final long ordinal;
  private final String text;
  private final int tokenCount;
  private final long startMs;
  private final long endMs;
  private final List<String> speakers;
  private final long firstSegment;
  private final long lastSegment;

  /**
   * @param timeMs   the chunk's start and end, in ms from the audio's start
   * @param segments the first and last segment the chunk's utterances come from
   */
  public TranscriptChunkValue(long ordinal, String text, int tokenCount, Range timeMs, List<String> speakers, Range segments) {
    this.ordinal = ordinal;
    this.text = Objects.requireNonNull(text, "text");
    this.tokenCount = tokenCount;
    this.startMs = timeMs.first;
    this.endMs = timeMs.last;
    this.speakers = Collections.unmodifiableList(speakers);
    this.firstSegment = segments.first;
    this.lastSegment = segments.last;
  }

  /** A closed range [first, last]. */
  public static final class Range {
    private final long first;
    private final long last;

    public Range(long first, long last) {
      this.first = first;
      this.last = last;
    }
  }

  @Override
  public long ordinal() {
    return ordinal;
  }

  @Override
  public String text() {
    return text;
  }

  @Override
  public int tokenCount() {
    return tokenCount;
  }

  @Override
  public long startMs() {
    return startMs;
  }

  @Override
  public long endMs() {
    return endMs;
  }

  @Override
  public List<String> speakers() {
    return speakers;
  }

  @Override
  public long firstSegment() {
    return firstSegment;
  }

  @Override
  public long lastSegment() {
    return lastSegment;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TranscriptChunkValue)) {
      return false;
    }
    TranscriptChunkValue that = (TranscriptChunkValue) o;
    return ordinal == that.ordinal && tokenCount == that.tokenCount && startMs == that.startMs && endMs == that.endMs
        && firstSegment == that.firstSegment && lastSegment == that.lastSegment && text.equals(that.text)
        && speakers.equals(that.speakers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ordinal, text, tokenCount, startMs, endMs, speakers, firstSegment, lastSegment);
  }

  @Override
  public String toString() {
    return "TranscriptChunk{" + ordinal + ", segments " + firstSegment + ".." + lastSegment + ", [" + startMs + ", " + endMs
        + "] ms, " + tokenCount + " tokens, " + speakers + "}";
  }
}
