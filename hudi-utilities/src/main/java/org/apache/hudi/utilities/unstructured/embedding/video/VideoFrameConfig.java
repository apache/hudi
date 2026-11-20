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

package org.apache.hudi.utilities.unstructured.embedding.video;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;

import java.util.Locale;

/**
 * The frame-extraction settings of {@link FfmpegKeyframeVideoFrameSource}, read and checked in one place: the strategy's
 * {@code validateConfig} (called by the video preprocessor's) and its {@code frames} both call {@link #from}, so a bad value fails
 * on the driver of data prep, naming the key. The interval, the dedup keys and the pixel guard are preprocessing style
 * ({@code prep.video.*}). A change is not detected; use a new table for a different preprocessing style. The thread count
 * changes no output and is not style.
 */
public final class VideoFrameConfig {

  /** How consecutive kept frames are compared. */
  public enum DedupMethod {
    /** 64-bit difference hash of the decoded frame; a frame within the Hamming distance of the last kept one is dropped. */
    DHASH,
    /** SHA-256 of the encoded frame; a frame whose encoded bytes equal the last kept one's is dropped. */
    SHA256,
    /** No content dedup (a keyframe chosen by consecutive targets is still taken once). */
    NONE
  }

  /** The largest interval: one day (the source works in microseconds; this keeps every target far from overflow). */
  static final long MAX_INTERVAL_MS = 24L * 60 * 60 * 1000;

  private final long intervalMs;
  private final DedupMethod dedupMethod;
  private final int maxDistance;
  private final int threads;
  private final long maxPixels;

  private VideoFrameConfig(long intervalMs, DedupMethod dedupMethod, int maxDistance, int threads, long maxPixels) {
    this.maxPixels = maxPixels;
    this.intervalMs = intervalMs;
    this.dedupMethod = dedupMethod;
    this.maxDistance = maxDistance;
    this.threads = threads;
  }

  /**
   * @throws IllegalArgumentException naming the offending key
   */
  public static VideoFrameConfig from(TypedProperties props) {
    long intervalMs = parseLong(props, EmbeddingTableConfig.PREP_VIDEO_KEYFRAME_INTERVAL_MS.key(),
        EmbeddingTableConfig.DEFAULT_PREP_VIDEO_KEYFRAME_INTERVAL_MS);
    if (intervalMs <= 0 || intervalMs > MAX_INTERVAL_MS) {
      throw new IllegalArgumentException("Config " + EmbeddingTableConfig.PREP_VIDEO_KEYFRAME_INTERVAL_MS.key()
          + " must be in [1, " + MAX_INTERVAL_MS + "] (one day), got " + intervalMs);
    }
    String method = ConfigUtils.getStringWithAltKeys(props, EmbeddingTableConfig.PREP_VIDEO_DEDUP_METHOD, EmbeddingTableConfig.DEFAULT_PREP_VIDEO_DEDUP_METHOD).trim();
    DedupMethod dedupMethod;
    try {
      dedupMethod = DedupMethod.valueOf(method.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Config " + EmbeddingTableConfig.PREP_VIDEO_DEDUP_METHOD.key()
          + " must be one of dhash, sha256, none, got " + method);
    }
    long maxDistance = parseLong(props, EmbeddingTableConfig.PREP_VIDEO_DEDUP_MAX_DISTANCE.key(),
        EmbeddingTableConfig.DEFAULT_PREP_VIDEO_DEDUP_MAX_DISTANCE);
    if (maxDistance < 0 || maxDistance > Long.SIZE) {
      throw new IllegalArgumentException("Config " + EmbeddingTableConfig.PREP_VIDEO_DEDUP_MAX_DISTANCE.key()
          + " must be in [0, 64], got " + maxDistance);
    }
    long threads = parseLong(props, EmbeddingTableConfig.VIDEO_FFMPEG_THREADS.key(), EmbeddingTableConfig.DEFAULT_VIDEO_FFMPEG_THREADS);
    if (threads < 1 || threads > EmbeddingTableConfig.MAX_VIDEO_FFMPEG_THREADS) {
      throw new IllegalArgumentException("Config " + EmbeddingTableConfig.VIDEO_FFMPEG_THREADS.key() + " must be in [1, "
          + EmbeddingTableConfig.MAX_VIDEO_FFMPEG_THREADS + "], got " + threads);
    }
    long maxPixels = parseLong(props, EmbeddingTableConfig.PREP_VIDEO_MAX_PIXELS.key(), EmbeddingTableConfig.DEFAULT_PREP_VIDEO_MAX_PIXELS);
    if (maxPixels <= 0) {
      throw new IllegalArgumentException("Config " + EmbeddingTableConfig.PREP_VIDEO_MAX_PIXELS.key() + " must be positive, got "
          + maxPixels);
    }
    return new VideoFrameConfig(intervalMs, dedupMethod, (int) maxDistance, (int) threads, maxPixels);
  }

  private static long parseLong(TypedProperties props, String key, long defaultValue) {
    String value = ConfigUtils.getStringWithAltKeys(props, ConfigProperty.key(key).noDefaultValue(), null);
    if (value == null || value.trim().isEmpty()) {
      return defaultValue;
    }
    try {
      return Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Config " + key + " must be an integer, got " + value, e);
    }
  }

  /** X of "the closest keyframe every X": the target spacing in milliseconds. */
  public long getIntervalMs() {
    return intervalMs;
  }

  public DedupMethod getDedupMethod() {
    return dedupMethod;
  }

  /** The largest dHash Hamming distance that still counts as a repeat ({@link DedupMethod#DHASH} only). */
  public int getMaxDistance() {
    return maxDistance;
  }

  /** The most pixels a video's frames may have: a larger video is refused before any frame is decoded. */
  public long getMaxPixels() {
    return maxPixels;
  }

  /** FFmpeg decoder threads ({@code thread_count}) per video. */
  public int getThreads() {
    return threads;
  }
}
