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

import org.apache.hudi.common.util.ValidationUtils;

import java.io.Serializable;
import java.util.Objects;

/**
 * One utterance of a transcript: who spoke, what was said, and when (milliseconds on the video's timeline).
 */
public final class TranscriptSegment implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String speakerLabel;
  private final String text;
  private final long startMs;
  private final long endMs;
  private final Float confidence;

  /**
   * @param speakerLabel the service's speaker label (e.g. {@code spk_0}); a chunk carries at least speaker and content
   * @param text         what was said; not null
   * @param startMs      start of the utterance, {@code >= 0}
   * @param endMs        end of the utterance, {@code >= startMs}
   * @param confidence   the service's confidence in {@code [0, 1]}, or null when the service returns none
   */
  public TranscriptSegment(String speakerLabel, String text, long startMs, long endMs, Float confidence) {
    ValidationUtils.checkArgument(speakerLabel != null && !speakerLabel.isEmpty(), "speakerLabel must be non-empty");
    ValidationUtils.checkArgument(text != null, "text must not be null");
    ValidationUtils.checkArgument(startMs >= 0, () -> "startMs must be >= 0: " + startMs);
    ValidationUtils.checkArgument(endMs >= startMs, () -> "endMs " + endMs + " must be >= startMs " + startMs);
    ValidationUtils.checkArgument(confidence == null || (confidence >= 0f && confidence <= 1f),
        () -> "confidence must be in [0, 1]: " + confidence);
    this.speakerLabel = speakerLabel;
    this.text = text;
    this.startMs = startMs;
    this.endMs = endMs;
    this.confidence = confidence;
  }

  public String getSpeakerLabel() {
    return speakerLabel;
  }

  public String getText() {
    return text;
  }

  public long getStartMs() {
    return startMs;
  }

  public long getEndMs() {
    return endMs;
  }

  /** The service's confidence, or null when it returns none. */
  public Float getConfidence() {
    return confidence;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TranscriptSegment)) {
      return false;
    }
    TranscriptSegment that = (TranscriptSegment) o;
    return startMs == that.startMs && endMs == that.endMs && speakerLabel.equals(that.speakerLabel)
        && text.equals(that.text) && Objects.equals(confidence, that.confidence);
  }

  @Override
  public int hashCode() {
    return Objects.hash(speakerLabel, text, startMs, endMs, confidence);
  }

  @Override
  public String toString() {
    return "TranscriptSegment{" + speakerLabel + " [" + startMs + ", " + endMs + "] ms, confidence=" + confidence + "}";
  }
}
