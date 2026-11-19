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

import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlobInputStream;

import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.FrameGrabber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The duration a video's container declares, read by FFmpeg's header probe over a seekable view of the blob
 * ({@link SeekableBlobInputStream}, nothing buffered): no frame and no audio sample is decoded, no grabbed frame is
 * requested. Used by the audio stub, which segments a clip by its duration alone.
 *
 * <p>Failures: a storage failure under the probe is a {@link BlobReadException} (transient); a container FFmpeg cannot open,
 * or one that declares no positive duration, is a {@link VideoDecodeException} (the clip's data, permanent).
 */
public final class ClipDuration {

  private static final Logger LOG = LoggerFactory.getLogger(ClipDuration.class);
  private static final long MICROS_PER_MILLI = 1000L;

  private ClipDuration() {
  }

  /**
   * @param bytes the video; must be a {@link SeekableBlobBytesSupplier} (an mp4 index may sit at the end of the file)
   * @param video names the video in failure messages
   * @return the declared duration in milliseconds, {@code > 0}
   * @throws BlobReadException  when the bytes cannot be read
   * @throws VideoDecodeException when the container cannot be probed or declares no duration
   */
  public static long millis(BlobBytesSupplier bytes, String video) throws BlobReadException {
    if (!(bytes instanceof SeekableBlobBytesSupplier)) {
      throw new IllegalStateException("video " + video + ": the duration probe needs random access to the blob, got "
          + (bytes == null ? "no bytes" : bytes.getClass().getName()));
    }
    SeekableBlobInputStream stream;
    try {
      stream = new SeekableBlobInputStream(((SeekableBlobBytesSupplier) bytes).openSeekable());
    } catch (IOException e) {
      throw e instanceof BlobReadException ? (BlobReadException) e : new BlobReadException(e);
    }
    FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(stream);
    // the probe opens the stream's decoders; one thread, as for frames (FFmpeg's 0 means one per core)
    grabber.setVideoOption("threads", "1");
    try {
      grabber.start();
      long micros = grabber.getLengthInTime();
      if (stream.storageFailure() != null) {
        // the decoder's read callback swallows a failed read and may still report a duration: never trust it then
        throw new VideoDecodeException("video " + video + " was probed over a failed read");
      }
      if (micros <= 0) {
        throw new VideoDecodeException("video " + video + " declares no duration (" + micros + " us)");
      }
      return Math.max(1L, micros / MICROS_PER_MILLI);
    } catch (FrameGrabber.Exception | RuntimeException e) {
      IOException storage = stream.storageFailure();
      if (storage != null) {
        BlobReadException failure = storage instanceof BlobReadException ? (BlobReadException) storage
            : new BlobReadException(storage);
        failure.addSuppressed(e);
        throw failure;
      }
      throw e instanceof VideoDecodeException ? (VideoDecodeException) e
          : new VideoDecodeException("video " + video + " cannot be probed for its duration", e);
    } finally {
      try {
        // stops and releases the native contexts; closes the stream it was given
        grabber.close();
      } catch (FrameGrabber.Exception | RuntimeException e) {
        // the duration is already known or the failure above is the one that matters, but a stream left open must leave a trace
        LOG.warn("The FFmpeg grabber or blob stream of video {} failed to close", video, e);
      }
    }
  }
}
