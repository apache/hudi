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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.ClosableIterator;

import java.util.Collections;

/**
 * TEST DOUBLE (no default names it; the defaults are the real implementations, {@link AudioConfig}).
 *
 * <p>DUMMY {@link SegmentTranscriber}, id {@value #ID}: hears nothing. Each segment yields ONE utterance spanning the segment,
 * {@code "Utterance of audio segment <k>."}, spoken by the v1 file-scoped speaker {@link SpeakerLabels#FILE_SPEAKER}
 * ({@link #fileScopedSpeakers()} is true). Deterministic, so a resumed or rerun stage writes the same text.
 */
public class StubSegmentTranscriber implements SegmentTranscriber {

  /** The stub's engine id; part of the audio logic line, so of every audio table's preprocessing style. */
  public static final String ID = "stub/fixed-utterance-v0";

  /** The utterance of segment {@code index}. */
  public static String utterance(long index) {
    return "Utterance of audio segment " + index + ".";
  }

  @Override
  public String id() {
    return ID;
  }

  @Override
  public boolean fileScopedSpeakers() {
    return true;
  }

  @Override
  public void validate(TypedProperties props) {
    // the stub reads no settings, so there is nothing to validate
  }

  @Override
  public ClosableIterator<TranscriptSegment> transcribe(AudioSegment segment, TypedProperties props) {
    return ClosableIterator.wrap(Collections.singletonList(new TranscriptSegment(SpeakerLabels.FILE_SPEAKER,
        utterance(segment.index()), segment.startMs(), segment.endMs(), null)).iterator());
  }
}
