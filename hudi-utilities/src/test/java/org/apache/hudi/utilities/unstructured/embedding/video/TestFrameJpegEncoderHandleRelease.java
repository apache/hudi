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

import org.apache.hudi.utilities.unstructured.embedding.image.ImageProviderLeakSpies;

import com.sun.imageio.plugins.leakspy.JdkNamedSpyWriterSpi;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Handle release of the JPEG writer the video frame encoder ({@link FrameJpegEncoder}) chooses: a JDK-named spy JPEG writer,
 * ordered ahead of the JDK's own, is the chosen one, so a test can see whether it was disposed.
 */
class TestFrameJpegEncoderHandleRelease {

  /** Control: the spy reaches the chosen path, and the encoder's close disposes it. */
  @Test
  void chosenWriterIsDisposedByClose() {
    try (ImageProviderLeakSpies spies = ImageProviderLeakSpies.installJpeg(false, false)) {
      FrameJpegEncoder encoder = new FrameJpegEncoder();
      assertEquals(1, spies.spyWriter().created().size(), "the encoder chose the JDK-named spy JPEG writer");
      JdkNamedSpyWriterSpi.SpyWriter chosen = spies.spyWriter().created().get(0);
      assertFalse(chosen.disposed(), "the chosen writer is the encoder's until it closes");
      encoder.close();
      assertTrue(chosen.disposed(), "FrameJpegEncoder.close: the chosen ImageWriter " + chosen + " was not disposed");
    }
  }

  @Test
  void chosenWriterIsDisposedWhenALaterProviderFails() {
    try (ImageProviderLeakSpies spies = ImageProviderLeakSpies.installJpeg(true, false)) {
      FrameJpegEncoder[] built = new FrameJpegEncoder[1];
      ImageProviderLeakSpies.runEitherOutcome(() -> built[0] = new FrameJpegEncoder());
      if (built[0] != null) {
        // the walk skipped the failing provider: the encoder owns the chosen writer and releases it on close
        built[0].close();
      }
      assertEquals(1, spies.spyWriter().created().size(), "the spy JPEG writer was chosen");
      JdkNamedSpyWriterSpi.SpyWriter chosen = spies.spyWriter().created().get(0);
      assertTrue(chosen.disposed(), "FrameJpegEncoder.jdkJpegWriter: the chosen ImageWriter " + chosen
          + " was not disposed when a later provider's createWriterInstance threw");
    }
  }
}
