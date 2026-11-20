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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The transcript segment value type: it keeps its speaker, text and times, its confidence may be null, and it refuses an empty or
 * null speaker, a null text, a negative or inverted time span and a confidence above 1.
 */
class TestTranscriptSegment {

  @Test
  void transcriptSegmentKeepsSpeakerTextAndTimesWithNullableConfidence() {
    TranscriptSegment segment = new TranscriptSegment("spk_0", "hello world", 1200, 2500, null);
    assertEquals("spk_0", segment.getSpeakerLabel());
    assertEquals("hello world", segment.getText());
    assertEquals(1200, segment.getStartMs());
    assertEquals(2500, segment.getEndMs());
    assertNull(segment.getConfidence());
    assertEquals(0.75f, new TranscriptSegment("spk_1", "", 0, 0, 0.75f).getConfidence());

    assertThrows(IllegalArgumentException.class, () -> new TranscriptSegment("", "x", 0, 1, null));
    assertThrows(IllegalArgumentException.class, () -> new TranscriptSegment(null, "x", 0, 1, null));
    assertThrows(IllegalArgumentException.class, () -> new TranscriptSegment("spk_0", null, 0, 1, null));
    assertThrows(IllegalArgumentException.class, () -> new TranscriptSegment("spk_0", "x", -1, 1, null));
    assertThrows(IllegalArgumentException.class, () -> new TranscriptSegment("spk_0", "x", 5, 4, null));
    assertThrows(IllegalArgumentException.class, () -> new TranscriptSegment("spk_0", "x", 0, 1, 1.5f));
  }
}
