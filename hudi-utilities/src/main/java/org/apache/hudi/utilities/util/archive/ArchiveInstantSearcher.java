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

package org.apache.hudi.utilities.util.archive;

import org.apache.hudi.common.util.CustomizedThreadFactory;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.util.CloseableExecutor;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * Finds a batch of instants in a table's archived timeline with a bounded, multi-threaded walk over
 * the archive files.
 *
 * <p><b>The three-state result is the safety property.</b> {@link InstantSearchState#NOT_FOUND} means
 * every byte that could have held the instant was read cleanly and it was not there;
 * {@link InstantSearchState#INDETERMINATE} means the search could not cover what it needed to. Only
 * the first is a claim about the table. Collapsing them is a data-loss bug, because downstream the
 * absence of an instant licenses treating a file as unreferenced, and therefore a delete.
 *
 * <p><b>Why not just call {@code ArchivedTimelineLoaderV1.loadInstants}.</b> That loader has three
 * ways to truncate a range while reporting success, none of them visible through the
 * {@code ArchivedTimelineLoader} API:
 *
 * <ul>
 *   <li>A corrupt block is <em>skipped, not raised</em>. {@code HoodieLogFileReader.readBlock}
 *       returns a {@code HoodieCorruptBlock} rather than throwing when a block fails its
 *       sync-marker check, and the loader's {@code if (block instanceof HoodieAvroDataBlock)} then
 *       drops it. Every instant in that block vanishes silently.
 *   <li>The early {@code break} that stops reading older archive files once one yields no in-range
 *       instants is driven by a {@code HashSet} mutated from inside a <em>parallel</em> stream. A
 *       lost {@code add} makes a non-empty file look empty and skips every older file behind it.
 *   <li>An archive file that cannot be read at all is swallowed by {@code continue} when its name
 *       matches an in-progress merge-archive plan.
 * </ul>
 *
 * <p>This class reports the first, does not perform the second at all, and keeps the third
 * deliberately -- a file named by an in-progress merge plan is expected to be unreadable and its
 * content still lives in the source files. It also bounds memory, which the loader does not.
 *
 * <p><b>The consumption window.</b> Files are walked newest first. A file enters the window only
 * when {@code used == 0 || used + size <= budgetBytes}; the first disjunct keeps one file always in
 * flight, so a file larger than the whole budget still runs, alone. A worker that cannot afford the
 * next file <em>waits for it</em> rather than stepping over it, which is what makes the invariant
 * below hold and means no run has to double back at the end for a file it skipped.
 *
 * <p><b>Invariant:</b> the {@code CONSUMING} and {@code CONSUMED} files together are always exactly
 * a prefix of the file list, and everything beyond it is {@code NOT_CONSUMED}. No file is ever
 * skipped. Note this is not contiguity of {@code CONSUMING} on its own -- files finish out of order,
 * so a {@code CONSUMED} file may sit between two in-flight ones.
 *
 * <p><b>Termination.</b> Every file is at least probed unless the walk stopped early, and the walk
 * stops early only once every target is found -- at which point no {@code NOT_FOUND} remains to
 * justify. There is deliberately no ordering-based early break: merge-archive can produce files
 * whose name suffix does not order the same way as their instants, and an unsound break is exactly
 * the bug being replaced.
 *
 * <p><b>Scope.</b> This searches the v6 archive layout and reports <em>where</em> instants live; it
 * decodes nothing. Turning a record into commit metadata belongs to the consumer.
 */
public final class ArchiveInstantSearcher {

  private static final Logger LOG = LoggerFactory.getLogger(ArchiveInstantSearcher.class);

  /** Bytes of archive file allowed in flight at once, unless one file alone exceeds it. */
  public static final long DEFAULT_BUDGET_BYTES = 2L * 1024L * 1024L * 1024L;

  private final ArchiveFileCatalog catalog;
  private final ArchiveFileScanner scanner;
  private final int threads;
  private final long budgetBytes;

  public ArchiveInstantSearcher(ArchiveFileCatalog catalog, ArchiveFileScanner scanner) {
    this(catalog, scanner, defaultThreads(), DEFAULT_BUDGET_BYTES);
  }

  /**
   * @param threads     upper bound on workers; the pool is sized to the file count when that is
   *                    smaller
   * @param budgetBytes bytes of archive file allowed in flight at once
   */
  public ArchiveInstantSearcher(ArchiveFileCatalog catalog, ArchiveFileScanner scanner, int threads,
                                long budgetBytes) {
    ValidationUtils.checkArgument(catalog != null, "catalog is required");
    ValidationUtils.checkArgument(scanner != null, "scanner is required");
    ValidationUtils.checkArgument(threads > 0, "threads must be positive, got " + threads);
    ValidationUtils.checkArgument(budgetBytes > 0L,
        "budgetBytes must be positive, got " + budgetBytes);
    this.catalog = catalog;
    this.scanner = scanner;
    this.threads = threads;
    this.budgetBytes = budgetBytes;
  }

  public static int defaultThreads() {
    return 2 * Runtime.getRuntime().availableProcessors();
  }

  /**
   * Locates each target instant in the archived timeline.
   *
   * @param targetInstants the instants to look for; duplicates collapse to one result entry
   * @param onHit          invoked, <b>on worker threads</b>, for every record whose instant is a
   *                       target -- including repeat records for an instant already found in the
   *                       same file, because the v6 archive writes a requested, an inflight and a
   *                       completed entry per instant and only one carries write stats. Must be
   *                       thread-safe. Records are handed over rather than accumulated, so a large
   *                       batch does not have to fit in memory.
   *
   * @return one entry per distinct target instant. A {@link InstantSearchState#FOUND} entry carries
   *         the newest archive file that held the instant.
   *
   * @throws HoodieIOException when the archive directory cannot be listed, since a search that does
   *         not know which files exist cannot report absence
   * @throws HoodieException when {@code onHit} itself failed. That is a caller bug, and reporting it
   *         as archive damage would hide it.
   */
  public Map<String, InstantLocation> search(Collection<String> targetInstants,
                                             BiConsumer<String, GenericRecord> onHit) {
    ValidationUtils.checkArgument(onHit != null, "onHit consumer is required");
    if (targetInstants == null || targetInstants.isEmpty()) {
      return Collections.emptyMap();
    }
    LiveTargets targets = LiveTargets.of(targetInstants);

    List<ArchiveFile> files;
    try {
      files = catalog.list();
    } catch (IOException e) {
      throw new HoodieIOException("Could not list the archive files to search", e);
    }
    if (files.isEmpty()) {
      // Nothing to read, so nothing was found, cleanly. That is an honest NOT_FOUND.
      return resolve(targets, Collections.emptyList());
    }

    Window window = new Window(files, budgetBytes);
    Queue<InstantRange> damage = new ConcurrentLinkedQueue<>();
    AtomicReference<Throwable> consumerFailure = new AtomicReference<>();
    AtomicReference<Throwable> workerFailure = new AtomicReference<>();

    // Guarding here rather than trusting scanners not to swallow it: a consumer that fails must
    // surface as a caller bug even if the scanner in use catches broadly and reports damage.
    BiConsumer<String, GenericRecord> guarded = (instantTime, record) -> {
      try {
        onHit.accept(instantTime, record);
      } catch (RuntimeException | Error t) {
        consumerFailure.compareAndSet(null, t);
        window.stop();
        throw t;
      }
    };

    boolean interrupted = false;
    int poolSize = Math.min(threads, files.size());
    try (CloseableExecutor pool = CloseableExecutor.fixedPool(
        poolSize, new CustomizedThreadFactory("archive-instant-search", true))) {
      List<Callable<Void>> workers = new ArrayList<>(poolSize);
      for (int i = 0; i < poolSize; i++) {
        workers.add(() -> {
          runWorker(window, targets, guarded, damage, consumerFailure);
          return null;
        });
      }
      for (Future<Void> future : pool.service().invokeAll(workers)) {
        try {
          future.get();
        } catch (ExecutionException e) {
          workerFailure.compareAndSet(null, e.getCause());
        } catch (CancellationException e) {
          interrupted = true;
        }
      }
    } catch (InterruptedException e) {
      interrupted = true;
      window.stop();
      Thread.currentThread().interrupt();
    }

    rethrowIfFailed(consumerFailure.get(),
        "The onHit consumer failed while searching the archived timeline");
    rethrowIfFailed(workerFailure.get(), "An archived timeline search worker failed");

    if (interrupted) {
      // Half a walk is not a view anything may act on, so nothing still missing may be called absent.
      LOG.warn("The archived timeline search was interrupted; every instant not already found is "
          + "reported INDETERMINATE");
      damage.add(InstantRange.all());
    }
    return resolve(targets, damage);
  }

  private void runWorker(Window window,
                         LiveTargets targets,
                         BiConsumer<String, GenericRecord> onHit,
                         Queue<InstantRange> damage,
                         AtomicReference<Throwable> consumerFailure) {
    while (true) {
      ArchiveFile file;
      try {
        file = window.claim();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      if (file == null) {
        return;
      }
      try {
        process(file, targets, onHit, damage);
      } catch (RuntimeException | Error t) {
        if (consumerFailure.get() != null) {
          throw t;
        }
        // A scanner is contracted not to throw. One that does has told us nothing about what the
        // file held, so its whole coverage is unreadable.
        LOG.warn("The scanner failed unexpectedly on archive file {}; treating it as unreadable",
            file.path(), t);
        damage.add(InstantRange.all());
      } finally {
        window.release(file);
        if (targets.allFound()) {
          window.stop();
        }
      }
    }
  }

  private void process(ArchiveFile file,
                       LiveTargets targets,
                       BiConsumer<String, GenericRecord> onHit,
                       Queue<InstantRange> damage) {
    Option<InstantRange> probed = scanner.probe(file);
    // A failed probe is "I could not find out", never "it covers nothing" -- so scan it.
    InstantRange coverage = probed.isPresent() ? probed.get() : InstantRange.all();
    if (probed.isPresent() && !targets.hasLiveIn(coverage)) {
      LOG.debug("Skipping archive file {}: its range {} holds no instant still being searched for",
          file.path(), coverage);
      return;
    }
    ScanOutcome outcome = scanner.scan(file, coverage, targets, onHit);
    if (!outcome.isClean()) {
      LOG.warn("Archive file {} could not be read in full ({}); any instant in {} it did not yield "
          + "is reported INDETERMINATE rather than absent",
          file.path(), outcome.reason(), coverage);
      damage.add(coverage);
    }
  }

  private static void rethrowIfFailed(Throwable failure, String message) {
    if (failure == null) {
      return;
    }
    if (failure instanceof Error) {
      throw (Error) failure;
    }
    throw new HoodieException(message, failure);
  }

  private static Map<String, InstantLocation> resolve(LiveTargets targets,
                                                      Collection<InstantRange> damage) {
    Map<String, InstantLocation> resolved = new HashMap<>();
    for (String instantTime : targets.instants()) {
      Option<StoragePath> location = targets.locationOf(instantTime);
      if (location.isPresent()) {
        resolved.put(instantTime, InstantLocation.found(location.get()));
      } else if (coveredByDamage(damage, instantTime)) {
        resolved.put(instantTime, InstantLocation.indeterminate());
      } else {
        resolved.put(instantTime, InstantLocation.notFound());
      }
    }
    return resolved;
  }

  private static boolean coveredByDamage(Collection<InstantRange> damage, String instantTime) {
    for (InstantRange range : damage) {
      if (range.contains(instantTime)) {
        return true;
      }
    }
    return false;
  }

  /**
   * The elastic consumption window: hands out files in order, and holds a worker at the next file
   * until the byte budget can afford it.
   *
   * <p>Deadlock-free by construction. If {@code used > 0} then some file is {@code CONSUMING} and
   * its owner is between claim and release, hence not waiting; if {@code used == 0} the admission
   * predicate holds, so nobody waits. Progress is always available.
   *
   * <p>Fairness is not needed. {@code notifyAll} on a monitor is not FIFO, but whichever worker wins
   * takes the <em>next</em> index, so only the identity of the thread parked on a large file
   * changes.
   */
  private static final class Window {

    private final List<ArchiveFile> files;
    private final long budgetBytes;
    private int cursor;
    private long used;
    private boolean stopped;

    Window(List<ArchiveFile> files, long budgetBytes) {
      this.files = files;
      this.budgetBytes = budgetBytes;
    }

    /** @return the next file to read, or null once the walk is over */
    synchronized ArchiveFile claim() throws InterruptedException {
      while (true) {
        if (stopped || cursor == files.size()) {
          return null;
        }
        ArchiveFile file = files.get(cursor);
        long cost = file.budgetCost();
        // used == 0 is what lets a file larger than the whole budget through, as sole occupant.
        if (used == 0L || used + cost <= budgetBytes) {
          cursor++;
          used += cost;
          file.setState(ConsumeState.CONSUMING);
          return file;
        }
        wait();
      }
    }

    synchronized void release(ArchiveFile file) {
      used -= file.budgetCost();
      file.setState(ConsumeState.CONSUMED);
      notifyAll();
    }

    /** Ends the walk: every target is found, or nothing further may be trusted. */
    synchronized void stop() {
      stopped = true;
      notifyAll();
    }
  }
}
