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

package org.apache.hudi.utilities.unstructured.memory;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioSegment;
import org.apache.hudi.utilities.unstructured.embedding.audio.SegmentTranscriber;
import org.apache.hudi.utilities.unstructured.embedding.audio.SpeakerLabels;
import org.apache.hudi.utilities.unstructured.embedding.audio.TranscriptSegment;

import org.apache.spark.TaskContext;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The speech memory cells' deterministic in-heap transcriber (the whisper model is not available to the tests), id {@value #ID}.
 * Like a real engine it reads each segment's whole PCM payload (at most {@code segment.max.bytes}, 1 MiB by default); it then says
 * ONE utterance spanning the segment, {@value #WORDS} words drawn from a hash of the payload and the segment index, spoken by
 * {@link SpeakerLabels#FILE_SPEAKER}. At the cells' {@code chunk.max.tokens} = 64 with no overlap, one utterance (41 tokens with its
 * label) fits a chunk and two do not, so the transcript packer makes one speech row per segment.
 *
 * <p>It counts the segments each prep task transcribes (by its partition id) with the times of the task's first and last
 * segment: local Spark runs every task in the cell's child JVM, which prints them after its rounds.
 */
public class SegmentCountingTranscriber implements SegmentTranscriber {

  public static final String ID = "memory-cell/hash-words-v0";
  static final int WORDS = 40;
  private static final String[] VOCABULARY = ("river stone lantern harbor meadow signal copper window garden thunder "
      + "silver orchard canyon ember willow anchor beacon falcon prairie glacier").split(" ");
  private static final Map<Integer, long[]> TASKS = new ConcurrentHashMap<>();

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
  }

  @Override
  public ClosableIterator<TranscriptSegment> transcribe(AudioSegment segment, TypedProperties props) throws IOException {
    long hash = segment.index();
    byte[] buffer = new byte[64 * 1024];
    try (InputStream in = segment.openPayload()) {
      int n;
      while ((n = in.read(buffer)) > 0) {
        for (int i = 0; i < n; i++) {
          hash = hash * 31 + buffer[i];
        }
      }
    }
    StringBuilder text = new StringBuilder("segment ").append(segment.index());
    for (int w = 1; w < WORDS; w++) {
      hash = hash * 6364136223846793005L + 1442695040888963407L;
      text.append(' ').append(VOCABULARY[(int) Math.floorMod(hash >>> 33, (long) VOCABULARY.length)]);
    }
    text.append('.');
    record();
    return ClosableIterator.wrap(Collections.singletonList(new TranscriptSegment(SpeakerLabels.FILE_SPEAKER, text.toString(),
        segment.startMs(), segment.endMs(), null)).iterator());
  }

  /** Counts one segment for the running task: {segments, first nanos, last nanos}. */
  private static void record() {
    TaskContext task = TaskContext.get();
    int partition = task == null ? -1 : task.partitionId();
    long now = System.nanoTime();
    TASKS.compute(partition, (p, stats) -> stats == null ? new long[] {1, now, now} : new long[] {stats[0] + 1, stats[1], now});
  }

  /** Per prep task (partition id): {segments transcribed, nano time of its first segment, of its last}. */
  public static Map<Integer, long[]> tasks() {
    return new TreeMap<>(TASKS);
  }
}
