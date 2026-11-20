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

package org.apache.hudi.utilities.unstructured.storage;

import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.exception.HoodieException;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Runs one task's out-of-line writes ({@link OutOfLineWriter#writeAt}: a marker, then the file) on {@link #THREADS} background
 * threads, so a task prepares its next payloads while the timeline server batches the markers (every
 * {@code hoodie.markers.timeline_server_based.batch.interval_ms}, 50 ms by default) and storage takes the PUTs. The bytes of
 * the payloads submitted and not yet written are bounded by {@link #MAX_PENDING_BYTES} (one larger payload is admitted alone):
 * a caller blocks until enough earlier writes finish. The caller holds one more payload while it waits, so a task holds at most
 * about twice that. The number of pending writes is also bounded by {@link #MAX_PENDING_WRITES}, so small payloads cannot
 * accumulate an unbounded number of queued callbacks.
 *
 * <p>The first failure of a write is kept and thrown, as it was raised (later ones suppressed into it), by the next
 * {@link #submit} or by {@link #awaitAll}, which the task calls before it reports its end: a write that failed fails the task, as
 * a write on the task's own thread does. {@link #close} drops the writes not yet started and waits, through interrupts, for
 * those running to end, so no file of the task is created after it ends, unless a write is stuck past close's bound (below): a
 * file such a write creates later is left behind under the table's blob folder. A rollback of the round deletes every marked file.
 *
 * <p>Every wait of the task on the writes is bounded by {@link #WAIT_MILLIS} (10 minutes). {@link #awaitAll}'s clock starts when
 * it begins and again whenever a write ends, so it gives up once no write has ended for one bound. {@link #submit} waits for room
 * in attempts of one bound and gives up after an attempt in which no write ended, so it goes on for between one and almost two
 * bounds with no write ending. Neither gives up while writes keep ending, so a slow but healthy drain of many queued writes is
 * waited for however long it takes. {@link #close}, which interrupts the writes, waits one bound from its start. A stuck write (a
 * store or marker call that never returns and ignores interrupts) would otherwise hold the task's executor slot without bound;
 * after the bound the wait throws a {@link HoodieException} naming the writes still running ("did not end within"), failing the
 * task. The task then closes the pool, which waits once more, so from the start of the wait that gave up, or the last write's
 * end if later, the task ends within about two bounds (after {@link #awaitAll}) or three (after {@link #submit}).
 */
public final class OutOfLineWritePool implements Closeable {

  /** Writes in flight at once per task. */
  static final int THREADS = 4;
  /** 4 MiB: the bytes of the payloads waiting to be written, per task. */
  public static final int MAX_PENDING_BYTES = 4 * 1024 * 1024;

  private static final AtomicInteger POOLS = new AtomicInteger();

  /** One running batch and one queued batch; small payloads also have bounded queue overhead. */
  static final int MAX_PENDING_WRITES = 2 * THREADS;

  private final Semaphore pendingBytes = new Semaphore(MAX_PENDING_BYTES);
  private final Semaphore pendingWrites = new Semaphore(MAX_PENDING_WRITES);
  private final AtomicReference<Throwable> failure = new AtomicReference<>();
  private final ExecutorService writeThreads;
  private int submitted;
  private final AtomicInteger finished = new AtomicInteger();
  private final Object done = new Object();

  /**
   * How long {@link #awaitAll} goes on with no write ending, the length of each of {@link #submit}'s attempts to get room, and how
   * long {@link #close} waits from its start, before a wait gives up on the writes still running (a stuck store call).
   */
  static final long WAIT_MILLIS = TimeUnit.MINUTES.toMillis(10);

  /** The writes running now, by thread, with what they write, for the message of a wait that gives up on them. */
  private final Map<String, String> runningWrites = new ConcurrentHashMap<>();
  private final long waitMillis;

  public OutOfLineWritePool() {
    this(WAIT_MILLIS);
  }

  /** @param waitMillis the bound of each wait on the writes ({@link #WAIT_MILLIS} in production) */
  @VisibleForTesting
  OutOfLineWritePool(long waitMillis) {
    this.waitMillis = waitMillis;
    int pool = POOLS.incrementAndGet();
    AtomicInteger thread = new AtomicInteger();
    this.writeThreads = new ThreadPoolExecutor(THREADS, THREADS, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), runnable -> {
      Thread t = new Thread(runnable, "hudi-out-of-line-write-" + pool + "-" + thread.incrementAndGet());
      t.setDaemon(true);
      return t;
    });
  }

  /** One write: it throws what the task should fail with. */
  @FunctionalInterface
  public interface Write {
    void run() throws IOException, InterruptedException;
  }

  /**
   * Runs {@code write}, which holds {@code bytes} bytes until it ends, in the background; first throws the failure of an
   * earlier write, if any. Waits for room while earlier writes keep ending, in attempts of {@link #WAIT_MILLIS} each, and gives up
   * after an attempt in which none ended: between one and almost two bounds with no write ending, counted from the start of the
   * wait or the last write's end, whichever is later.
   *
   * @throws HoodieException when interrupted while waiting for room (the interrupt is kept), when no write ended within the
   *                         bound while it waited (naming each write still running), or when the pool is closed
   */
  public void submit(long bytes, Write write) {
    submit(bytes, null, write);
  }

  /** As {@link #submit(long, Write)}, naming what {@code write} writes ({@code label}) in a wait that gives up on it. */
  public void submit(long bytes, String label, Write write) {
    rethrowFailure();
    if (writeThreads.isShutdown()) {
      throw new HoodieException("The out-of-line write pool is closed");
    }
    int permits = (int) Math.min(MAX_PENDING_BYTES, Math.max(1L, bytes));
    acquire(pendingWrites, 1);
    boolean bytesAcquired = false;
    boolean counted = false;
    boolean accepted = false;
    try {
      acquire(pendingBytes, permits);
      bytesAcquired = true;
      rethrowFailure();
      submitted++;
      counted = true;
      writeThreads.execute(new Pending(permits, label, write));
      accepted = true;
    } catch (RejectedExecutionException e) {
      throw new HoodieException("The out-of-line write pool is closed", e);
    } finally {
      if (!accepted) {
        if (counted) {
          submitted--;
        }
        if (bytesAcquired) {
          pendingBytes.release(permits);
        }
        pendingWrites.release();
      }
    }
  }

  /** Acquires either kind of room, retaining the existing bound on a wait without progress. */
  private void acquire(Semaphore room, int permits) {
    try {
      int ended = finished.get();
      while (!room.tryAcquire(permits, waitMillis, TimeUnit.MILLISECONDS)) {
        int now = finished.get();
        if (now == ended) {
          throw stuckWrites("while the task waited for room to write");
        }
        ended = now;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new HoodieException("Interrupted while waiting to write an out-of-line payload", e);
    }
  }

  /** A submitted write and the permits it holds until it ends or is dropped. */
  private final class Pending implements Runnable {
    private final int permits;
    private final String label;
    private final Write write;

    Pending(int permits, String label, Write write) {
      this.permits = permits;
      this.label = label;
      this.write = write;
    }

    @Override
    public void run() {
      String thread = Thread.currentThread().getName();
      runningWrites.put(thread, label == null ? "a write" : label);
      try {
        write.run();
        // by design: any failure of a background write, an Error included, is kept: the first one, later ones suppressed on it
      } catch (Throwable t) { // NOSONAR (by design: see above)
        if (!failure.compareAndSet(null, t) && failure.get() != t) {
          failure.get().addSuppressed(t);
        }
      } finally {
        runningWrites.remove(thread);
        end();
      }
    }

    /** Gives back the permits and counts the write as ended: it ran, or close dropped it. */
    void end() {
      pendingBytes.release(permits);
      pendingWrites.release();
      finished.incrementAndGet();
      synchronized (done) {
        done.notifyAll();
      }
    }
  }

  /** Throws the first failure of a write so far, if any. */
  public void checkFailure() {
    rethrowFailure();
  }

  /**
   * Waits for every write submitted so far, then throws the first failure, if any. Gives up once no write ended for
   * {@link #WAIT_MILLIS}: the bound starts again each time one ends.
   *
   * @throws HoodieException when interrupted (the interrupt is kept), or when no write ended within the bound (naming each write
   *                         still running)
   */
  public void awaitAll() {
    long boundNanos = TimeUnit.MILLISECONDS.toNanos(waitMillis);
    long deadline = System.nanoTime() + boundNanos;
    int ended = finished.get();
    synchronized (done) {
      while (finished.get() < submitted) {
        int now = finished.get();
        if (now != ended) {
          // a write ended: progress, so the bound starts again
          ended = now;
          deadline = System.nanoTime() + boundNanos;
        }
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
          throw stuckWrites("while the task waited for them");
        }
        try {
          TimeUnit.NANOSECONDS.timedWait(done, remaining);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new HoodieException("Interrupted while waiting for out-of-line writes", e);
        }
      }
    }
    rethrowFailure();
  }

  /**
   * Stops taking writes, drops those not yet started (after {@link #awaitAll} there are none; otherwise the task is failing;
   * a drop is recorded as a failure, so a later {@link #awaitAll} or {@link #checkFailure} throws) and asks those running to
   * stop, then waits for them to end, through interrupts: a store or a marker call may ignore an interrupt, and its file must
   * not be created after the task ends. It waits at most {@link #WAIT_MILLIS}: a write still running then (a stuck store
   * call) would hold the task's executor slot without bound, so close gives up on it and throws, failing the task; a file that
   * write creates later is left behind under the table's blob folder. The interrupt is kept. Write failures are not thrown here.
   *
   * @throws HoodieException naming each write still running after the bound (what it writes, and its thread)
   */
  @Override
  public void close() {
    List<Runnable> dropped = writeThreads.shutdownNow();
    if (!dropped.isEmpty()) {
      // their files were never written: a barrier after this close fails rather than pass without them
      HoodieException notWritten = new HoodieException(dropped.size() + " out-of-line writes were dropped by close before they ran");
      if (!failure.compareAndSet(null, notWritten)) {
        failure.get().addSuppressed(notWritten);
      }
    }
    for (Runnable each : dropped) {
      // never run: its permits go back and it counts as ended, so no awaitAll or submit waits for it
      ((Pending) each).end();
    }
    boolean interrupted = false;
    boolean terminated = false;
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(waitMillis);
    while (!terminated) {
      long remaining = deadline - System.nanoTime();
      if (remaining <= 0) {
        break;
      }
      try {
        terminated = writeThreads.awaitTermination(remaining, TimeUnit.NANOSECONDS);
        // by design: every write thread must end before close returns, up to the bound; the interrupt is restored after the loop
      } catch (InterruptedException e) { // NOSONAR (by design: see above)
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    if (!terminated) {
      throw stuckWrites("of close");
    }
  }

  /**
   * The failure of a wait that gave up on the writes still running ({@link #waitMillis} with no write ending in awaitAll, an
   * attempt of it with no write ending in submit, or close's bound), naming each (what it writes, and its thread); {@code when}
   * says which wait it was.
   */
  private HoodieException stuckWrites(String when) {
    List<String> stuck = new ArrayList<>();
    runningWrites.forEach((thread, label) -> stuck.add(label + " on " + thread));
    return new HoodieException("out-of-line writes did not end within " + waitMillis + " ms " + when
        + (stuck.isEmpty() ? "" : " (" + String.join(", ", stuck) + ")") + ": the task fails; a file such a write creates later "
        + "is left behind under the table's blob folder");
  }

  private void rethrowFailure() {
    Throwable first = failure.get();
    if (first == null) {
      return;
    }
    if (first instanceof RuntimeException) {
      throw (RuntimeException) first;
    }
    if (first instanceof Error) {
      throw (Error) first;
    }
    throw new HoodieException("An out-of-line write failed", first);
  }
}
