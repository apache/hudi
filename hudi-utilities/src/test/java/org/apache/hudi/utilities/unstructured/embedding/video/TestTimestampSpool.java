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

import org.apache.hudi.utilities.unstructured.embedding.prep.PrepFailures;
import org.apache.hudi.utilities.unstructured.embedding.source.PrepPlatformException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;

class TestTimestampSpool {
  @TempDir
  Path directory;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void orderAndCleanupOnExhaustionOrEarlyClose(boolean consume) throws Exception {
    try (TimestampSpool spool = new TimestampSpool(directory)) {
      for (int i = 0; i < 10; i++) {
        spool.add(i, i + 17);
      }
      if (consume) {
        for (int i = 0; i < 10; i++) {
          assertArrayEquals(new long[] {i, i + 17}, spool.next());
        }
        assertNull(spool.next());
        assertEmpty();
      }
    }
    assertEmpty();
  }

  @Test
  void emptyScheduleCreatesNoFile() throws Exception {
    try (TimestampSpool spool = new TimestampSpool(directory)) {
      assertNull(spool.next());
    }
    assertEmpty();
  }

  @Test
  void scratchWriteFailureFailsTheRound() throws Exception {
    Path file = Files.write(directory.resolve("file"), new byte[] {1});
    try (TimestampSpool spool = new TimestampSpool(file)) {
      PrepPlatformException failure = assertThrows(PrepPlatformException.class, () -> spool.add(1, 2));
      assertSame(failure, PrepFailures.roundFailure(failure));
    }
  }

  @Test
  void truncatedScheduleFailsTheRoundAndDeletesItsFile() throws Exception {
    try (TimestampSpool spool = new TimestampSpool(directory)) {
      for (int i = 0; i < 8192; i++) {
        spool.add(i, i + 17);
      }
      assertArrayEquals(new long[] {0, 17}, spool.next());
      Path path;
      try (Stream<Path> paths = Files.list(directory)) {
        path = paths.findFirst().orElseThrow();
      }
      try (FileChannel file = FileChannel.open(path, StandardOpenOption.WRITE)) {
        file.truncate(0);
      }
      PrepPlatformException failure = assertThrows(PrepPlatformException.class, () -> {
        while (spool.next() != null) {
          // Consume any records already buffered before the local file was shortened.
        }
      });
      assertSame(failure, PrepFailures.roundFailure(failure));
      assertEmpty();
    }
  }

  @Test
  void concurrentCloseCannotBeOvertakenByLazyFileCreation() throws Exception {
    TimestampSpool spool = new TimestampSpool(directory);
    CountDownLatch creating = new CountDownLatch(1);
    CountDownLatch releaseCreation = new CountDownLatch(1);
    CountDownLatch closeStarted = new CountDownLatch(1);
    CountDownLatch closeFinished = new CountDownLatch(1);
    ExecutorService workers = Executors.newFixedThreadPool(2);
    try {
      Future<?> add = workers.submit(() -> {
        try (MockedStatic<Files> files = mockStatic(Files.class, call -> {
          // Stubbing createTempFile must not itself create a setup file.
          return call.getMethod().getName().equals("createTempFile") ? null : call.callRealMethod();
        })) {
          files.when(() -> Files.createTempFile(directory, "hudi-video-timestamps-", ".bin")).thenAnswer(call -> {
            creating.countDown();
            assertTrue(releaseCreation.await(5, TimeUnit.SECONDS));
            return call.callRealMethod();
          });
          spool.add(1, 2);
        }
      });
      assertTrue(creating.await(5, TimeUnit.SECONDS));
      Future<?> close = workers.submit(() -> {
        closeStarted.countDown();
        try {
          spool.close();
        } finally {
          closeFinished.countDown();
        }
      });
      assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
      // Let an uncoordinated close complete before the lazy creation; a serialized close waits for its current owner.
      closeFinished.await(1, TimeUnit.SECONDS);
      releaseCreation.countDown();
      add.get(5, TimeUnit.SECONDS);
      close.get(5, TimeUnit.SECONDS);
      assertEmpty();
    } finally {
      releaseCreation.countDown();
      workers.shutdown();
      assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
      spool.close();
    }
  }

  private void assertEmpty() throws Exception {
    try (Stream<Path> files = Files.list(directory)) {
      assertEquals(0, files.count());
    }
  }
}
