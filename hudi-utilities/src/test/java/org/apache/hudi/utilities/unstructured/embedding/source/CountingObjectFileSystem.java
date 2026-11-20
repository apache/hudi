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

package org.apache.hudi.utilities.unstructured.embedding.source;

import org.apache.hudi.utilities.sources.blob.BucketFolderFileSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A bucket file system ({@link BucketFolderFileSystem}, scheme {@value #SCHEME}) that counts, per object, the requests an object
 * store would serve: status requests a caller sends (a HEAD; the local file system's own status check inside an open, which
 * stands for the object store's own open request, is not counted), opens, and read runs (each read that does not continue the
 * previous one: a new ranged GET); and records the read-ahead set on each opened stream, which a local file's own stream does not
 * take.
 */
public class CountingObjectFileSystem extends BucketFolderFileSystem {

  public static final String SCHEME = "countfs";

  private static final Map<String, Counts> COUNTS = new ConcurrentHashMap<>();
  /** Set while this file system opens an object: the status check the open makes itself is not the caller's request. */
  private static final ThreadLocal<Boolean> OPENING = ThreadLocal.withInitial(() -> false);

  /** What one object was asked. */
  public static final class Counts {
    public final AtomicInteger statuses = new AtomicInteger();
    public final AtomicInteger opens = new AtomicInteger();
    public final AtomicInteger readRuns = new AtomicInteger();
    public final AtomicLong bytesRead = new AtomicLong();
    /** The read-ahead last set on a stream of the object; -1 when none was set. */
    public final AtomicLong readahead = new AtomicLong(-1L);
  }

  /** Registers the scheme in {@code conf}, its buckets as folders under {@code root}. */
  public static void installCounting(Configuration conf, java.nio.file.Path root) {
    BucketFolderFileSystem.install(conf, root);
    conf.set("fs." + SCHEME + ".impl", CountingObjectFileSystem.class.getName());
    conf.setBoolean("fs." + SCHEME + ".impl.disable.cache", true);
    conf.set("fs." + SCHEME + ".root", root.toAbsolutePath().toString());
  }

  public static String path(String bucket, String key) {
    return SCHEME + "://" + bucket + "/" + key;
  }

  /** The counts of {@code countfs://bucket/key}, zeros when it was never asked. */
  public static Counts counts(String path) {
    return COUNTS.computeIfAbsent(path, p -> new Counts());
  }

  public static void clear() {
    COUNTS.clear();
  }

  @Override
  protected String scheme() {
    return SCHEME;
  }

  private static String key(Path f) {
    return f.toUri().getScheme() + "://" + f.toUri().getAuthority() + f.toUri().getPath();
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    if (!OPENING.get()) {
      counts(key(f)).statuses.incrementAndGet();
    }
    return super.getFileStatus(f);
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    Counts counts = counts(key(f));
    counts.opens.incrementAndGet();
    FSDataInputStream opened;
    OPENING.set(true);
    try {
      opened = super.open(f, bufferSize);
    } finally {
      OPENING.set(false);
    }
    return new FSDataInputStream(new CountingStream(opened, counts));
  }

  /** A stream that counts read runs and takes a read-ahead, as an object store's stream does. */
  private static final class CountingStream extends FSInputStream implements CanSetReadahead {
    private final FSDataInputStream in;
    private final Counts counts;
    private long nextContiguous = -1L;

    CountingStream(FSDataInputStream in, Counts counts) {
      this.in = in;
      this.counts = counts;
    }

    @Override
    public void seek(long pos) throws IOException {
      in.seek(pos);
    }

    @Override
    public long getPos() throws IOException {
      return in.getPos();
    }

    @Override
    public boolean seekToNewSource(long targetPos) {
      return false;
    }

    @Override
    public int read() throws IOException {
      byte[] one = new byte[1];
      int n = read(one, 0, 1);
      return n < 0 ? -1 : one[0] & 0xff;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      long pos = in.getPos();
      int n = in.read(buffer, offset, length);
      if (n > 0) {
        if (pos != nextContiguous) {
          counts.readRuns.incrementAndGet();
        }
        counts.bytesRead.addAndGet(n);
        nextContiguous = pos + n;
      }
      return n;
    }

    @Override
    public void setReadahead(Long readahead) {
      counts.readahead.set(readahead == null ? -1L : readahead);
    }

    @Override
    public void close() throws IOException {
      in.close();
    }
  }
}
