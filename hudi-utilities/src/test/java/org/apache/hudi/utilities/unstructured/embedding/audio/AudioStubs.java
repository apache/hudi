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

/**
 * The audio stubs as test doubles: the defaults are the real FFmpeg source, whisper and the transcript packer, so a test that
 * wants the stubs names them.
 */
public final class AudioStubs {

  private AudioStubs() {
  }

  /** Names the three stub classes in {@code props}; returns {@code props}. */
  public static TypedProperties named(TypedProperties props) {
    props.setProperty(AudioConfigKeys.SEGMENT_SOURCE_CLASS.key(), StubAudioSegmentSource.class.getName());
    props.setProperty(AudioConfigKeys.TRANSCRIBER_CLASS.key(), StubSegmentTranscriber.class.getName());
    props.setProperty(AudioConfigKeys.CHUNKER_CLASS.key(), StubTranscriptChunker.class.getName());
    return props;
  }

  /** Names the stub chunker only (a test with its own source and transcriber whose oracle is the stub's packing). */
  public static TypedProperties stubChunker(TypedProperties props) {
    props.setProperty(AudioConfigKeys.CHUNKER_CLASS.key(), StubTranscriptChunker.class.getName());
    return props;
  }
}
