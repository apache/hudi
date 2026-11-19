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
import org.apache.hudi.utilities.unstructured.embedding.audio.whisper.WhisperSegmentTranscriber;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** The default implementations AudioConfig names by string stay the classes they name. */
class TestAudioConfigDefaults {

  @Test
  void defaultTranscriberIsWhisper() {
    String defaultTranscriber = AudioConfig.DEFAULT_TRANSCRIBER_CLASS;
    assertEquals(WhisperSegmentTranscriber.class.getName(), defaultTranscriber);
  }

  @Test
  void segmentSourceHasNoDefault() {
    assertFalse(AudioConfigKeys.SEGMENT_SOURCE_CLASS.hasDefaultValue(),
        "no segment source ships here, so the key must not name one by default");
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> AudioConfig.from(new TypedProperties()).newSegmentSource());
    assertEquals("Config " + AudioConfigKeys.SEGMENT_SOURCE_CLASS.key() + " must be set", e.getMessage());
  }
}
