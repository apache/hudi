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

package org.apache.hudi.utilities.unstructured.embedding.prep;

import org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Source failure classification terminates for repeated cause identities. */
class TestVideoFailureClassification {

  @Test
  void ordinaryStorageFailureIsRecognized() throws Exception {
    assertTrue(storageFailure(new BlobReadException(new IOException("read failed"))));
  }

  @Test
  void cyclicStorageFailureIsRecognized() throws Exception {
    BoundedIOException first = new BoundedIOException();
    IOException second = new IOException("read failed");
    first.initCause(second);
    second.initCause(first);
    assertTrue(storageFailure(new BlobReadException(first)));
  }

  private static boolean storageFailure(Exception failure) throws Exception {
    Method method = VideoFramePreprocessor.class.getDeclaredMethod("isStorageFailure", Exception.class);
    method.setAccessible(true);
    return (Boolean) method.invoke(null, failure);
  }

  /** Bounds the regression itself if a classifier revisits the cycle. */
  private static final class BoundedIOException extends IOException {
    private static final long serialVersionUID = 1L;
    private int visits;

    @Override
    public synchronized Throwable getCause() {
      if (++visits > 16) {
        throw new AssertionError("failure classification revisited a cause identity");
      }
      return super.getCause();
    }
  }
}
