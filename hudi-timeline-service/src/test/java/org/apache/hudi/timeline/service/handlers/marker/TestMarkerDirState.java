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

package org.apache.hudi.timeline.service.handlers.marker;

import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import io.javalin.http.Context;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.util.MarkerUtils.MARKERS_FILENAME_PREFIX;
import static org.apache.hudi.common.util.MarkerUtils.MARKER_TYPE_FILENAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link MarkerDirState}.
 */
public class TestMarkerDirState extends HoodieCommonTestHarness {
  private static final String MARKER_NAME =
      "2016/68b3ad4d-9d43-4e4c-8f4b-4c9b6d3e8b3a-0_1-0-1_00000000000001.parquet.marker.CREATE";
  private static final String OTHER_MARKER_NAME =
      "2016/0b8f4c52-3f7e-4a63-9a43-6c2d1e5b7a90-0_2-0-1_00000000000001.parquet.marker.CREATE";
  private static final String REQUEST_ID = "request-1";

  private CloseFailingHoodieStorage storage;
  private String markerDir;

  @BeforeEach
  void setUp() throws IOException {
    initPath();
    storage = new CloseFailingHoodieStorage(basePath, new Configuration());
    markerDir = basePath + "/.hoodie/.temp/00000000000001";
  }

  @Test
  void testMarkerCreationRequestProcessing() throws Exception {
    MarkerDirState dirState = createMarkerDirState();
    MarkerCreationFuture future = createFuture();
    int fileIndex = dirState.getNextFileIndexToUse().get();

    dirState.processMarkerCreationRequests(Collections.singletonList(future), fileIndex);

    assertTrue(future.isSuccessful());
    assertEquals("true", future.get());
    assertEquals(Collections.singleton(MARKER_NAME), dirState.getAllMarkers());
    assertEquals(MARKER_NAME + "\n", readMarkersFileContent(fileIndex));
    // The file index must be released after the batch is processed
    assertEquals(Option.of(fileIndex), dirState.getNextFileIndexToUse());
  }

  @Test
  void testMarkerCreationRequestsFailWhenFlushFails() {
    MarkerDirState dirState = createMarkerDirState();
    storage.setShouldFailClose(true);
    MarkerCreationFuture future = createFuture();
    int fileIndex = dirState.getNextFileIndexToUse().get();

    dirState.processMarkerCreationRequests(Collections.singletonList(future), fileIndex);

    assertTrue(future.isCompletedExceptionally());
    ExecutionException exception = assertThrows(ExecutionException.class, future::get);
    assertInstanceOf(HoodieIOException.class, exception.getCause());
    // The marker without durable persistence must not be acknowledged as existing
    assertTrue(dirState.getAllMarkers().isEmpty());
    // The file index must be released even if the flush fails
    assertEquals(Option.of(fileIndex), dirState.getNextFileIndexToUse());
  }

  @Test
  void testMarkerRecreationAfterFlushFailure() throws Exception {
    MarkerDirState dirState = createMarkerDirState();
    storage.setShouldFailClose(true);
    MarkerCreationFuture failedFuture = createFuture();
    dirState.processMarkerCreationRequests(Collections.singletonList(failedFuture), 0);
    assertTrue(failedFuture.isCompletedExceptionally());

    // A retried request for the same marker must succeed once the storage recovers
    storage.setShouldFailClose(false);
    MarkerCreationFuture retriedFuture = createFuture();
    dirState.processMarkerCreationRequests(Collections.singletonList(retriedFuture), 0);

    assertTrue(retriedFuture.isSuccessful());
    assertEquals("true", retriedFuture.get());
    assertEquals(Collections.singleton(MARKER_NAME), dirState.getAllMarkers());
    assertEquals(MARKER_NAME + "\n", readMarkersFileContent(0));
  }

  @Test
  void testRetryOfRequestThatCreatedMarkerSucceeds() throws Exception {
    MarkerDirState dirState = createMarkerDirState();
    MarkerCreationFuture future = createFuture(MARKER_NAME, REQUEST_ID);
    dirState.processMarkerCreationRequests(Collections.singletonList(future), 0);
    assertEquals("true", future.get());

    // The client resends the request with the same request ID when the response is lost
    MarkerCreationFuture retriedFuture = createFuture(MARKER_NAME, REQUEST_ID);
    dirState.processMarkerCreationRequests(Collections.singletonList(retriedFuture), 0);

    assertTrue(retriedFuture.isSuccessful());
    assertEquals("true", retriedFuture.get());
    assertEquals(Collections.singleton(MARKER_NAME), dirState.getAllMarkers());
    assertEquals(MARKER_NAME + "\n", readMarkersFileContent(0));
  }

  @Test
  void testRetryInTheSameBatchSucceeds() throws Exception {
    MarkerDirState dirState = createMarkerDirState();
    MarkerCreationFuture future = createFuture(MARKER_NAME, REQUEST_ID);
    MarkerCreationFuture retriedFuture = createFuture(MARKER_NAME, REQUEST_ID);

    dirState.processMarkerCreationRequests(Arrays.asList(future, retriedFuture), 0);

    assertEquals("true", future.get());
    assertEquals("true", retriedFuture.get());
    assertEquals(MARKER_NAME + "\n", readMarkersFileContent(0));
  }

  @Test
  void testOtherRequestForExistingMarkerFails() throws Exception {
    MarkerDirState dirState = createMarkerDirState();
    MarkerCreationFuture future = createFuture(MARKER_NAME, REQUEST_ID);
    dirState.processMarkerCreationRequests(Collections.singletonList(future), 0);
    assertEquals("true", future.get());

    MarkerCreationFuture otherRequestFuture = createFuture(MARKER_NAME, "request-2");
    MarkerCreationFuture requestWithoutIdFuture = createFuture(MARKER_NAME, null);
    dirState.processMarkerCreationRequests(Arrays.asList(otherRequestFuture, requestWithoutIdFuture), 0);

    assertFalse(otherRequestFuture.isSuccessful());
    assertEquals("false", otherRequestFuture.get());
    assertFalse(requestWithoutIdFuture.isSuccessful());
    assertEquals("false", requestWithoutIdFuture.get());
  }

  @Test
  void testRetryFailsForMarkerReadFromFileSystem() throws Exception {
    MarkerDirState dirState = createMarkerDirState();
    MarkerCreationFuture future = createFuture(MARKER_NAME, REQUEST_ID);
    dirState.processMarkerCreationRequests(Collections.singletonList(future), 0);
    assertEquals("true", future.get());

    // A restarted timeline server reads the markers from the file system and does not know which
    // request created them, so it cannot tell a retry from a duplicate
    MarkerDirState restartedDirState = createMarkerDirState();
    MarkerCreationFuture retriedFuture = createFuture(MARKER_NAME, REQUEST_ID);
    restartedDirState.processMarkerCreationRequests(Collections.singletonList(retriedFuture), 0);

    assertEquals("false", retriedFuture.get());
  }

  @Test
  void testRetryWaitsForTheBatchPersistingTheMarker() throws Exception {
    assertRetryWaitsForTheBatchPersistingTheMarker(false);
  }

  @Test
  void testRetryRecreatesTheMarkerWhenTheBatchPersistingItFails() throws Exception {
    assertRetryWaitsForTheBatchPersistingTheMarker(true);
  }

  private void assertRetryWaitsForTheBatchPersistingTheMarker(boolean shouldFirstBatchFail) throws Exception {
    MarkerDirState dirState = createMarkerDirState(2);
    CountDownLatch closeStarted = new CountDownLatch(1);
    CountDownLatch releaseClose = new CountDownLatch(1);
    storage.blockNextClose(closeStarted, releaseClose, shouldFirstBatchFail);
    MarkerCreationFuture future = createFuture(MARKER_NAME, REQUEST_ID);
    Thread firstBatch = new Thread(
        () -> dirState.processMarkerCreationRequests(Collections.singletonList(future), 0));
    firstBatch.start();
    assertTrue(closeStarted.await(30, TimeUnit.SECONDS));

    // The retry arrives while the first batch is still persisting the marker. It must not be
    // acknowledged before the marker is durable, so it goes back to the queue.
    MarkerCreationFuture retriedFuture = createFuture(MARKER_NAME, REQUEST_ID);
    dirState.processMarkerCreationRequests(Collections.singletonList(retriedFuture), 1);
    assertFalse(retriedFuture.isDone());

    releaseClose.countDown();
    firstBatch.join(TimeUnit.SECONDS.toMillis(30));
    assertEquals(shouldFirstBatchFail, future.isCompletedExceptionally());
    assertEquals(Collections.singletonList(retriedFuture), dirState.fetchPendingMarkerCreationRequests());

    dirState.processMarkerCreationRequests(Collections.singletonList(retriedFuture), 1);
    assertEquals("true", retriedFuture.get());
    assertEquals(Collections.singleton(MARKER_NAME), dirState.getAllMarkers());
  }

  @Test
  void testFlushFailureKeepsMarkerPersistedByEarlierBatch() throws Exception {
    MarkerDirState dirState = createMarkerDirState();
    MarkerCreationFuture future = createFuture(MARKER_NAME, REQUEST_ID);
    dirState.processMarkerCreationRequests(Collections.singletonList(future), 0);
    assertEquals("true", future.get());

    storage.setShouldFailClose(true);
    MarkerCreationFuture retriedFuture = createFuture(MARKER_NAME, REQUEST_ID);
    MarkerCreationFuture otherMarkerFuture = createFuture(OTHER_MARKER_NAME, "request-2");
    dirState.processMarkerCreationRequests(Arrays.asList(retriedFuture, otherMarkerFuture), 0);

    assertTrue(otherMarkerFuture.isCompletedExceptionally());
    // Only the marker that the failed batch added is removed
    assertEquals(Collections.singleton(MARKER_NAME), dirState.getAllMarkers());
  }

  private MarkerDirState createMarkerDirState() {
    return createMarkerDirState(1);
  }

  private MarkerDirState createMarkerDirState(int markerBatchNumThreads) {
    return new MarkerDirState(
        markerDir, markerBatchNumThreads, Option.empty(), storage, Registry.getRegistry("TestMarkerDirState"), 1);
  }

  private MarkerCreationFuture createFuture() {
    return new MarkerCreationFuture(mock(Context.class), markerDir, MARKER_NAME);
  }

  private MarkerCreationFuture createFuture(String markerName, String requestId) {
    return new MarkerCreationFuture(mock(Context.class), markerDir, markerName, requestId);
  }

  private String readMarkersFileContent(int fileIndex) throws IOException {
    return FileIOUtils.readAsUTFString(
        storage.open(new StoragePath(markerDir, MARKERS_FILENAME_PREFIX + fileIndex)));
  }

  /**
   * Storage whose created streams for {@code MARKERS} index files fail at close() time,
   * simulating an object store that uploads the file content in close().
   */
  private static class CloseFailingHoodieStorage extends HoodieHadoopStorage {
    private boolean shouldFailClose = false;
    private CountDownLatch closeStarted;
    private CountDownLatch releaseClose;
    private boolean shouldFailBlockedClose;

    CloseFailingHoodieStorage(String path, Configuration conf) {
      super(path, conf);
    }

    void setShouldFailClose(boolean shouldFailClose) {
      this.shouldFailClose = shouldFailClose;
    }

    /**
     * Makes the close() of the next {@code MARKERS} index file wait for {@code releaseClose},
     * after counting down {@code closeStarted}, and then fail if {@code shouldFail} is set.
     */
    void blockNextClose(CountDownLatch closeStarted, CountDownLatch releaseClose, boolean shouldFail) {
      this.closeStarted = closeStarted;
      this.releaseClose = releaseClose;
      this.shouldFailBlockedClose = shouldFail;
    }

    @Override
    public OutputStream create(StoragePath path) throws IOException {
      OutputStream stream = super.create(path);
      String fileName = path.getName();
      if (fileName.startsWith(MARKERS_FILENAME_PREFIX) && !fileName.equals(MARKER_TYPE_FILENAME)) {
        if (releaseClose != null) {
          CountDownLatch started = closeStarted;
          CountDownLatch release = releaseClose;
          boolean shouldFail = shouldFailBlockedClose;
          releaseClose = null;
          return new BlockingCloseStream(stream, started, release, shouldFail);
        }
        if (shouldFailClose) {
          return new CloseFailingStream(stream);
        }
      }
      return stream;
    }
  }

  private static class CloseFailingStream extends FilterOutputStream {
    CloseFailingStream(OutputStream delegate) {
      super(delegate);
    }

    @Override
    public void close() throws IOException {
      super.close();
      throw new IOException("Simulated failure to persist the file at close() time");
    }
  }

  private static class BlockingCloseStream extends FilterOutputStream {
    private final CountDownLatch closeStarted;
    private final CountDownLatch releaseClose;
    private final boolean shouldFail;

    BlockingCloseStream(OutputStream delegate, CountDownLatch closeStarted,
                        CountDownLatch releaseClose, boolean shouldFail) {
      super(delegate);
      this.closeStarted = closeStarted;
      this.releaseClose = releaseClose;
      this.shouldFail = shouldFail;
    }

    @Override
    public void close() throws IOException {
      closeStarted.countDown();
      try {
        releaseClose.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
      super.close();
      if (shouldFail) {
        throw new IOException("Simulated failure to persist the file at close() time");
      }
    }
  }
}
