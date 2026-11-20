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

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Synthetic archive files for testing the searcher without storage.
 *
 * <p>The point of the seam: a three-gigabyte file under a two-gigabyte budget, a corrupt block on
 * demand, and a scan that blocks until the test says otherwise are all things no real fixture can
 * produce.
 */
final class FakeArchive {

  static final long GB = 1024L * 1024L * 1024L;

  private static final Schema RECORD_SCHEMA = SchemaBuilder.record("FakeArchivedMetaEntry")
      .fields()
      .name(HoodieTableMetaClient.COMMIT_TIME_KEY).type().stringType().noDefault()
      .endRecord();

  private FakeArchive() {
  }

  static GenericRecord record(String instantTime) {
    GenericRecord record = new GenericData.Record(RECORD_SCHEMA);
    record.put(HoodieTableMetaClient.COMMIT_TIME_KEY, instantTime);
    return record;
  }

  /** What one synthetic file holds and how reading it behaves. */
  static final class FileSpec {
    final String name;
    final long sizeBytes;
    final List<String> instants = new ArrayList<>();
    boolean probeFails;
    String damageReason;
    /** Held by scan() until released, so a test can drive completion order. */
    CountDownLatch scanGate;

    FileSpec(String name, long sizeBytes) {
      this.name = name;
      this.sizeBytes = sizeBytes;
    }

    FileSpec holding(String... instantTimes) {
      instants.addAll(Arrays.asList(instantTimes));
      return this;
    }

    /** Records the same instant several times, as the v6 archive does per lifecycle state. */
    FileSpec holdingRepeated(String instantTime, int times) {
      for (int i = 0; i < times; i++) {
        instants.add(instantTime);
      }
      return this;
    }

    FileSpec withFailingProbe() {
      probeFails = true;
      return this;
    }

    FileSpec damaged(String reason) {
      damageReason = reason;
      return this;
    }

    FileSpec gated() {
      scanGate = new CountDownLatch(1);
      return this;
    }
  }

  static FileSpec spec(String name, long sizeBytes) {
    return new FileSpec(name, sizeBytes);
  }

  /** An archive whose files the test declares outright. */
  static final class Fixture implements ArchiveFileCatalog, ArchiveFileScanner {

    private final Map<String, FileSpec> specs = new LinkedHashMap<>();
    private final List<ArchiveFile> files = new ArrayList<>();
    private final Map<String, Integer> scanCounts = new ConcurrentHashMap<>();
    private final Map<String, Integer> probeCounts = new ConcurrentHashMap<>();
    private final List<String> events = Collections.synchronizedList(new ArrayList<>());
    private final List<String> invariantViolations = Collections.synchronizedList(new ArrayList<>());
    private IOException listFailure;

    Fixture(FileSpec... fileSpecs) {
      for (FileSpec spec : fileSpecs) {
        specs.put(spec.name, spec);
        files.add(new ArchiveFile(new StoragePath("/archive/" + spec.name), spec.sizeBytes));
      }
    }

    Fixture failingList(IOException failure) {
      this.listFailure = failure;
      return this;
    }

    List<ArchiveFile> archiveFiles() {
      return files;
    }

    StoragePath pathOf(String name) {
      return new StoragePath("/archive/" + name);
    }

    /** "enter:f0", "exit:f0", ... in the order they happened. */
    List<String> events() {
      synchronized (events) {
        return new ArrayList<>(events);
      }
    }

    int scanCount(String name) {
      return scanCounts.getOrDefault(name, 0);
    }

    int probeCount(String name) {
      return probeCounts.getOrDefault(name, 0);
    }

    /**
     * Any moment at which a NOT_CONSUMED file was seen ahead of a file already in or past the
     * window -- which would mean a file had been skipped.
     */
    List<String> invariantViolations() {
      synchronized (invariantViolations) {
        return new ArrayList<>(invariantViolations);
      }
    }

    /**
     * CONSUMING and CONSUMED together must always be exactly a prefix of the file list: no file may
     * be NOT_CONSUMED while an older one is already in or past the window.
     *
     * <p>The states are read one at a time without the window's lock, which is private to the
     * searcher, so the walk sees each file at a different moment. That makes the direction of the
     * walk decide whether a reported violation is real, and only <b>oldest first</b> is sound. Once
     * the walk sees a file in or past the window, that file's claim has already happened; the window
     * claims files strictly in list order under its lock, so every newer file was claimed before it;
     * and the walk reads every newer file after that. A newer file read as NOT_CONSUMED is therefore
     * a file the window really skipped.
     *
     * <p>Walking newest first is not sound, even though each single read can only under-report
     * progress. The walk can read a newer file just before a worker claims it, lose the CPU while
     * other workers claim and release the files behind it, and then read an older file as CONSUMED.
     * That pairs a stale reading with a fresh one and reports a skip that never happened, which is
     * how {@code neverSkipsAFileUnderContention} failed under load.
     */
    void checkPrefixInvariant(String where) {
      boolean sawClaimed = false;
      for (int i = files.size() - 1; i >= 0; i--) {
        ArchiveFile file = files.get(i);
        if (file.state() != ConsumeState.NOT_CONSUMED) {
          sawClaimed = true;
        } else if (sawClaimed) {
          invariantViolations.add(where + ": " + file.path().getName()
              + " is NOT_CONSUMED while an older file is already in or past the window");
          return;
        }
      }
    }

    Map<String, Integer> scanCounts() {
      return new HashMap<>(scanCounts);
    }

    void release(String name) {
      specs.get(name).scanGate.countDown();
    }

    boolean awaitEntered(String name, long timeoutMillis) throws InterruptedException {
      long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
      while (System.nanoTime() < deadline) {
        if (events().contains("enter:" + name)) {
          return true;
        }
        Thread.sleep(5L);
      }
      return events().contains("enter:" + name);
    }

    @Override
    public List<ArchiveFile> list() throws IOException {
      if (listFailure != null) {
        throw listFailure;
      }
      return files;
    }

    @Override
    public Option<InstantRange> probe(ArchiveFile file) {
      FileSpec spec = specOf(file);
      probeCounts.merge(spec.name, 1, Integer::sum);
      if (spec.probeFails || spec.instants.isEmpty()) {
        return Option.empty();
      }
      List<String> sorted = new ArrayList<>(spec.instants);
      Collections.sort(sorted);
      return Option.of(InstantRange.of(sorted.get(0), sorted.get(sorted.size() - 1)));
    }

    @Override
    public ScanOutcome scan(ArchiveFile file,
                            InstantRange coverage,
                            LiveTargets targets,
                            BiConsumer<String, GenericRecord> onHit) {
      FileSpec spec = specOf(file);
      scanCounts.merge(spec.name, 1, Integer::sum);
      events.add("enter:" + spec.name);
      checkPrefixInvariant("enter " + spec.name);
      try {
        if (spec.scanGate != null) {
          spec.scanGate.await();
        }
        for (String instantTime : spec.instants) {
          if (targets.isTarget(instantTime)) {
            targets.markFound(instantTime, file.path());
            onHit.accept(instantTime, record(instantTime));
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return ScanOutcome.damaged("interrupted");
      } finally {
        checkPrefixInvariant("exit " + spec.name);
        events.add("exit:" + spec.name);
      }
      return spec.damageReason == null
          ? ScanOutcome.clean()
          : ScanOutcome.damaged(spec.damageReason);
    }

    private FileSpec specOf(ArchiveFile file) {
      return specs.get(file.path().getName());
    }
  }
}
