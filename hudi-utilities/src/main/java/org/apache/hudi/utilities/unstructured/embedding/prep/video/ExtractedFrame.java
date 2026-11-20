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

package org.apache.hudi.utilities.unstructured.embedding.prep.video;

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;

/**
 * One frame a {@link FrameExtractionStrategy} kept: where it sits in the video, its dedup key and its encoded image. The image is
 * behind a {@link BlobBytesSupplier} of a known size, so the prep framework stores it as the frame row's {@code prepared_image}
 * without the strategy holding more than this one frame (aux {@code frame_index},
 * {@code frame_timestamp_ms}, {@code frame_dedup_key}).
 */
public final class ExtractedFrame {

  private final long timeMs;
  private final long frameIndex;
  private final boolean keyframe;
  private final int width;
  private final int height;
  private final String mimeType;
  private final long sizeBytes;
  private final String dedupKey;
  private final BlobBytesSupplier image;

  /**
   * @param timeMs     presentation time from the container's start time, clamped at 0, {@code >= 0}
   * @param frameIndex the dense kept-frame ordinal, {@code >= 0}
   * @param keyframe   whether the frame is a key (sync) frame
   * @param dedupKey   the key the strategy's dedup compared the frame by; not empty
   * @param image      the encoded image; not null
   */
  public ExtractedFrame(long timeMs, long frameIndex, boolean keyframe, String dedupKey, EncodedImage image) {
    ValidationUtils.checkArgument(timeMs >= 0, () -> "timeMs must be >= 0: " + timeMs);
    ValidationUtils.checkArgument(frameIndex >= 0, () -> "frameIndex must be >= 0: " + frameIndex);
    ValidationUtils.checkArgument(dedupKey != null && !dedupKey.isEmpty(), "dedupKey must be non-empty");
    ValidationUtils.checkArgument(image != null, "image must not be null");
    this.timeMs = timeMs;
    this.frameIndex = frameIndex;
    this.keyframe = keyframe;
    this.width = image.width;
    this.height = image.height;
    this.mimeType = image.mimeType;
    this.sizeBytes = image.sizeBytes;
    this.dedupKey = dedupKey;
    this.image = image.bytes;
  }

  public long getTimeMs() {
    return timeMs;
  }

  public long getFrameIndex() {
    return frameIndex;
  }

  public boolean isKeyframe() {
    return keyframe;
  }

  public int getWidth() {
    return width;
  }

  public int getHeight() {
    return height;
  }

  public String getMimeType() {
    return mimeType;
  }

  /** Exact size of the encoded image in bytes. */
  public long getSizeBytes() {
    return sizeBytes;
  }

  public String getDedupKey() {
    return dedupKey;
  }

  /** Opens the encoded image; each call is a new stream the caller closes. */
  public BlobBytesSupplier getImage() {
    return image;
  }

  /** The encoded image of a frame: its size in pixels, MIME type, exact byte size and the supplier that opens it. */
  public static final class EncodedImage {
    private final int width;
    private final int height;
    private final String mimeType;
    private final long sizeBytes;
    private final BlobBytesSupplier bytes;

    /**
     * @param width     width in pixels of the encoded image, {@code > 0}
     * @param height    height in pixels of the encoded image, {@code > 0}
     * @param mimeType  MIME type of the encoded image (e.g. {@code image/jpeg}); not empty
     * @param sizeBytes exact size of the encoded image in bytes, {@code >= 0}
     * @param bytes     opens the encoded image; not null
     */
    public EncodedImage(int width, int height, String mimeType, long sizeBytes, BlobBytesSupplier bytes) {
      ValidationUtils.checkArgument(width > 0 && height > 0, () -> "frame size must be positive: " + width + "x" + height);
      ValidationUtils.checkArgument(mimeType != null && !mimeType.isEmpty(), "mimeType must be non-empty");
      ValidationUtils.checkArgument(sizeBytes >= 0, () -> "sizeBytes must be >= 0: " + sizeBytes);
      ValidationUtils.checkArgument(bytes != null, "image must not be null");
      this.width = width;
      this.height = height;
      this.mimeType = mimeType;
      this.sizeBytes = sizeBytes;
      this.bytes = bytes;
    }
  }
}
