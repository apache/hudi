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
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;

import java.util.Iterator;

/**
 * Normalises and packs the utterances of one video's segments into {@link TranscriptChunk}s, under the same
 * packing and tokenizer rules as the PDF chunker (hard capped word limit, default 256 tokens, at most 128
 * tokens of overlap; keys {@code ...chunk.max.tokens}, {@code ...chunk.overlap.tokens}, {@code ...chunk.tokenizer.*}).
 * The default is {@link TranscriptPacker}.
 *
 * <p>Contract:
 * <ul>
 *   <li>{@link #logicVersion()} names the packing and normalisation logic; it is part of the audio logic version;</li>
 *   <li>a speaker turn is a paragraph boundary and an utterance end a sentence boundary for packing; an utterance that
 *       does not fit alone is split at words, as the PDF packer splits an oversized sentence;</li>
 *   <li>the end of {@code utterances} ends the last chunk: no overlap is carried past it (data prep packs a video whole, in the
 *       prep run that reads it);</li>
 *   <li>bounded: the chunker pulls utterances only while it needs text and holds a working set fixed by the table's chunk
 *       settings, never by the video: for {@link TranscriptPacker}, the PDF chunker's pull rule (pending text below two
 *       windows of {@code chunk.window.max.chars} before a pull), the overlap and one utterance read ahead — never the
 *       video's whole transcript;</li>
 *   <li>any failure, a {@link Tokenizer} failure included, is a content failure of the video (its quarantine entry in data prep),
 *       except an {@link IllegalArgumentException} from {@link #chunks} itself, which refuses the chunker's settings and fails
 *       the task; the caller always closes the iterator.</li>
 * </ul>
 */
public interface TranscriptChunker {

  /** Logic version of normalisation and packing, e.g. {@code transcript-pack-v2}. */
  String logicVersion();

  /**
   * Packs {@code utterances} into chunks numbered from {@code firstOrdinal}.
   *
   * @param utterances   the utterances of the video, in time order, each tagged with its segment index
   * @param firstOrdinal the ordinal of the first chunk (data prep: 0, a video is packed whole)
   * @param tokenizer    the table's tokenizer
   * @param props        the job's properties
   * @return the chunks in time order; the caller closes it
   */
  ClosableIterator<TranscriptChunk> chunks(Iterator<SegmentUtterance> utterances, long firstOrdinal, Tokenizer tokenizer,
                                           TypedProperties props);

  /** One utterance together with the index of the {@link AudioSegment} it came from. */
  interface SegmentUtterance {

    /** Index of the segment the utterance came from. */
    long segmentIndex();

    /** The utterance, as the transcriber returned it. */
    TranscriptSegment utterance();
  }
}
