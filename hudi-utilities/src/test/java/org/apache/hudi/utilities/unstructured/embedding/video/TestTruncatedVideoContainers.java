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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.prep.video.ExtractedFrame;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A video cut mid-stream in each claimed container: mkv, webm, mov and avi, with mp4 as the control. Behaviour: the file is a
 * content failure of the FFmpeg frame source — a {@link VideoDecodeException}, the file's FAILED row — never a short run of
 * frames presented as the whole video. An mp4 or mov cut anywhere before its index (moov, written last) cannot be opened. A
 * matroska or webm file keeps its declared duration in its header and is refused when its packets end before it; an avi is
 * refused when its RIFF chunk declares more bytes than the file holds (FFmpeg re-derives a cut avi's duration from what is
 * left). An MP4 or MOV with its index at the front (faststart) opens when cut, and is refused like a Matroska file. The cut clips
 * are the codec matrix's (and their remuxes in {@code /video/truncation/}), at 90% and 50% of their bytes; every whole clip
 * decodes, including ones whose streams all start late and an AVI whose RIFF size was never patched.
 */
class TestTruncatedVideoContainers {

  private static final String[] RESOURCES = {"/video/codec-matrix/", "/video/truncation/"};

  @TempDir
  Path dir;

  @ParameterizedTest(name = "{0} at {1}%")
  @CsvSource({
      // index (moov) written last: the cut file has none and cannot be opened
      "c01-h264-aac.mp4, 90, cannot be opened",
      "c01-h264-aac.mp4, 50, cannot be opened",
      "c13-h264-aac.mov, 90, cannot be opened",
      "c13-h264-aac.mov, 50, cannot be opened",
      "c14-hevc-aac.mov, 90, cannot be opened",
      // index at the front: the cut file opens, and its packets stop before the duration the index declares
      "c01-faststart.mp4, 90, the file is truncated",
      "c01-faststart.mp4, 50, the file is truncated",
      "c13-faststart.mov, 90, the file is truncated",
      "c13-faststart.mov, 50, the file is truncated",
      "c08-h264-mp3.mkv, 90, the file is truncated",
      "c08-h264-mp3.mkv, 50, the file is truncated",
      "c06-hevc-opus.mkv, 90, the file is truncated",
      "c06-hevc-opus.mkv, 50, the file is truncated",
      "c08-offset-1500ms.mkv, 50, the file is truncated",
      "c10-vp9-opus.webm, 90, the file is truncated",
      "c10-vp9-opus.webm, 50, the file is truncated",
      "c12-av1-opus.webm, 90, the file is truncated",
      "c12-av1-opus.webm, 50, the file is truncated",
      "c18-mpeg4-mp3.avi, 90, the file is truncated",
      "c18-mpeg4-mp3.avi, 50, the file is truncated"})
  void videoCutMidStreamIsAContentFailure(String clip, int percent, String reason) throws IOException {
    byte[] whole = clip(clip);
    Path cut = Files.write(dir.resolve(clip), Arrays.copyOf(whole, whole.length * percent / 100));
    String extension = clip.substring(clip.lastIndexOf('.') + 1);
    List<Long> frames = new ArrayList<>();
    VideoDecodeException failure = assertThrows(VideoDecodeException.class,
        () -> extract(cut, extension, frames), () -> clip + " at " + percent + "%: " + frames.size() + " frames");
    assertTrue(failure.getMessage().contains(reason), failure.getMessage());
    assertTrue(failure.getMessage().startsWith("video source_path_sha fid-" + extension + " (" + extension + ", "), failure.getMessage());
  }

  /** The control: each claimed container's whole clip decodes all six keyframes through the same checks. */
  @ParameterizedTest
  @CsvSource({"c01-h264-aac.mp4", "c08-h264-mp3.mkv", "c06-hevc-opus.mkv", "c09-vp9-none.mkv", "c10-vp9-opus.webm",
      "c12-av1-opus.webm", "c13-h264-aac.mov", "c14-hevc-aac.mov", "c18-mpeg4-mp3.avi", "t02-h264-aac-video-delayed.mp4",
      "c01-offset-1500ms.mp4", "c08-offset-1500ms.mkv", "c01-faststart.mp4", "c13-faststart.mov", "c18-unpatched-riff.avi"})
  void wholeClipDecodesEveryFrame(String clip) throws IOException {
    Path file = Files.write(dir.resolve(clip), clip(clip));
    List<Long> frames = new ArrayList<>();
    extract(file, clip.substring(clip.lastIndexOf('.') + 1), frames);
    assertEquals(6, frames.size(), clip + ": " + frames);
  }

  /** Drains the frame source over {@code video}, adding each frame's time to {@code out} as it comes. */
  private static void extract(Path video, String extension, List<Long> out) throws IOException {
    TypedProperties props = new TypedProperties();
    // every kept keyframe is a frame: the six keyframes of a whole clip are six frames
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_DEDUP_METHOD.key(), "none");
    try (ClosableIterator<ExtractedFrame> frames = new FfmpegKeyframeVideoFrameSource().frames(VideoClips.supplier(video),
        new FileMeta("fid-" + extension, extension, Files.size(video)), props)) {
      while (frames.hasNext()) {
        out.add(frames.next().getTimeMs());
      }
    }
    assertFalse(out.isEmpty(), video.toString());
  }

  private static byte[] clip(String name) throws IOException {
    for (String folder : RESOURCES) {
      try (InputStream in = TestTruncatedVideoContainers.class.getResourceAsStream(folder + name)) {
        if (in != null) {
          return in.readAllBytes();
        }
      }
    }
    return fail("no clip " + name);
  }
}
