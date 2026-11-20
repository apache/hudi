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

package org.apache.hudi.utilities.testutils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TEST-ONLY local file system under the scheme {@value #SCHEME} ({@code handlefs:///<absolute local path>}) that records every
 * stream it hands out until that stream is closed, so a test can assert that the code under test released every handle it
 * opened, on every path. It can also inject a fault into the streams of one file: a failed seek, a failed read (a number of
 * times), or a failed close of a written file (after the file is closed underneath, as a storage client that fails the
 * upload of a completed write does).
 */
public class HandleCountingFileSystem extends RawLocalFileSystem {

  public static final String SCHEME = "handlefs";
  private static final URI ROOT = URI.create(SCHEME + ":///");

  /** A fault injected into the streams of one file. */
  public enum Fault {
    SEEK, READ, CLOSE_WRITE
  }

  private static final AtomicInteger IDS = new AtomicInteger();
  private static final Map<Integer, String> OPEN = new ConcurrentHashMap<>();
  private static final AtomicInteger OPENED = new AtomicInteger();
  private static final Map<String, Map<Fault, AtomicInteger>> FAULTS = new ConcurrentHashMap<>();

  /** Registers the scheme in {@code conf}, uncached, so every lookup builds a fresh instance over the same registry. */
  public static Configuration install(Configuration conf) {
    conf.set("fs." + SCHEME + ".impl", HandleCountingFileSystem.class.getName());
    conf.setBoolean("fs." + SCHEME + ".impl.disable.cache", true);
    return conf;
  }

  /** The {@value #SCHEME} URL of a local file or folder. */
  public static String url(java.nio.file.Path local) {
    return SCHEME + "://" + local.toAbsolutePath().toUri().getPath();
  }

  /** The next {@code times} uses of {@code fault} on the streams of {@code local}, or of any file under it, fail. */
  public static void inject(java.nio.file.Path local, Fault fault, int times) {
    FAULTS.computeIfAbsent(local.toAbsolutePath().toString(), path -> new ConcurrentHashMap<>())
        .put(fault, new AtomicInteger(times));
  }

  /** The streams handed out and not closed yet, each named by its direction and file, sorted. */
  public static List<String> unclosed() {
    List<String> open = new ArrayList<>(OPEN.values());
    Collections.sort(open);
    return open;
  }

  /** How many streams were handed out since the last {@link #reset}. */
  public static int opened() {
    return OPENED.get();
  }

  public static void reset() {
    OPEN.clear();
    FAULTS.clear();
    OPENED.set(0);
  }

  private static boolean fires(String file, Fault fault) {
    for (Map.Entry<String, Map<Fault, AtomicInteger>> faults : FAULTS.entrySet()) {
      AtomicInteger left = faults.getValue().get(fault);
      String key = faults.getKey();
      boolean under = file.equals(key) || file.startsWith(key.endsWith(File.separator) ? key : key + File.separator);
      if (under && left != null && left.getAndDecrement() > 0) {
        return true;
      }
    }
    return false;
  }

  private static int register(String description) {
    int id = IDS.incrementAndGet();
    OPEN.put(id, description + " #" + id);
    OPENED.incrementAndGet();
    return id;
  }

  @Override
  public URI getUri() {
    return ROOT;
  }

  @Override
  public String getScheme() {
    return SCHEME;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    String file = pathToFile(f).getAbsolutePath();
    FSDataInputStream in = super.open(f, bufferSize);
    return new FSDataInputStream(new TrackedInput(in, file, register("read " + file)));
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication,
                                   long blockSize, Progressable progress) throws IOException {
    String file = pathToFile(f).getAbsolutePath();
    FSDataOutputStream out = super.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
    return new FSDataOutputStream(new TrackedOutput(out, file, register("write " + file)), null);
  }

  /** The create {@code FileSystem.create(Path, boolean)} reaches: the local file system overrides it directly. */
  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize,
                                   Progressable progress) throws IOException {
    String file = pathToFile(f).getAbsolutePath();
    FSDataOutputStream out = super.create(f, overwrite, bufferSize, replication, blockSize, progress);
    return new FSDataOutputStream(new TrackedOutput(out, file, register("write " + file)), null);
  }

  /** A read stream that leaves the registry when it is closed. */
  private static final class TrackedInput extends FSInputStream {
    private final FSDataInputStream in;
    private final String file;
    private final int id;

    TrackedInput(FSDataInputStream in, String file, int id) {
      this.in = in;
      this.file = file;
      this.id = id;
    }

    @Override
    public void seek(long pos) throws IOException {
      if (fires(file, Fault.SEEK)) {
        throw new IOException("injected seek failure of " + file);
      }
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
      if (fires(file, Fault.READ)) {
        throw new IOException("injected read failure of " + file);
      }
      return in.read();
    }

    @Override
    public int read(byte[] buffer, int offset, int len) throws IOException {
      if (fires(file, Fault.READ)) {
        throw new IOException("injected read failure of " + file);
      }
      return in.read(buffer, offset, len);
    }

    @Override
    public int available() throws IOException {
      return in.available();
    }

    @Override
    public void close() throws IOException {
      OPEN.remove(id);
      in.close();
    }
  }

  /** A write stream that leaves the registry when it is closed. */
  private static final class TrackedOutput extends OutputStream {
    private final FSDataOutputStream out;
    private final String file;
    private final int id;

    TrackedOutput(FSDataOutputStream out, String file, int id) {
      this.out = out;
      this.file = file;
      this.id = id;
    }

    @Override
    public void write(int b) throws IOException {
      out.write(b);
    }

    @Override
    public void write(byte[] buffer, int offset, int len) throws IOException {
      out.write(buffer, offset, len);
    }

    @Override
    public void flush() throws IOException {
      out.flush();
    }

    @Override
    public void close() throws IOException {
      boolean wasOpen = OPEN.remove(id) != null;
      out.close();
      if (wasOpen && fires(file, Fault.CLOSE_WRITE)) {
        throw new IOException("injected close failure of " + file);
      }
    }
  }
}
