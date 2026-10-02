/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * The local file system with a recorder of every file system call, for tests that assert which
 * calls a code path makes: how many times it lists the timeline, whether it opens a file once per
 * task or once per job, whether an engine task touches the {@code .hoodie} folder at all.
 *
 * <p>Each call is recorded with its operation, path and thread, and whether it ran in the current
 * scope, a test-defined notion such as "inside a Spark task" or "inside the compaction operator"
 * (see {@link #setScope}). Calls in scope also keep their call stack. Only the outermost call is
 * recorded, so {@code exists} delegating to {@code getFileStatus} counts once. Compound operations
 * that a store serves as several requests ({@code globStatus}, {@code listFiles},
 * {@code listStatusIterator}) are not recorded themselves; the directory listings and status calls
 * they make are, when they make them. Tests filter the recorded calls with the predicates on
 * {@link Call}, for example {@code getCalls(Call.inScope().and(Call.underMetaFolder()))}.
 *
 * <p>Usage: {@link #register} the file system on the Hadoop configuration the code under test uses
 * (with Flink options, set {@code "hadoop." + FILE_IMPL_KEY} and {@code "hadoop." + DISABLE_CACHE_KEY}
 * instead), {@link #reset}, run the work and inspect {@link #getCalls}. Assert that a call the work
 * must make was recorded before asserting that another one was not, so that the test fails rather
 * than passes when the recorder is not in use.
 *
 * <p>The recorder state is static because Hadoop creates a new instance per {@code FileSystem.get}
 * call once caching is disabled, which is needed for the implementation to be picked up regardless
 * of what the file system cache already holds.
 */
public class RecordingLocalFileSystem extends RawLocalFileSystem {

  public static final String FILE_IMPL_KEY = "fs.file.impl";
  public static final String DISABLE_CACHE_KEY = "fs.file.impl.disable.cache";

  private static final String META_FOLDER = "/.hoodie";
  private static final int MAX_DESCRIBED_STACKS = 3;
  private static final BooleanSupplier NO_SCOPE = () -> false;

  private static final Queue<Call> CALLS = new ConcurrentLinkedQueue<>();
  private static final ThreadLocal<Boolean> IN_RECORDED_CALL = ThreadLocal.withInitial(() -> false);
  private static final Map<Configuration, Map<String, String>> VALUES_BEFORE_REGISTER = new WeakHashMap<>();
  private static volatile BooleanSupplier scope = NO_SCOPE;

  /**
   * One file system call.
   */
  public static final class Call {
    private final String operation;
    private final String path;
    private final String threadName;
    private final boolean inScope;
    private final boolean underMetaFolder;
    private final Throwable stack;

    private Call(String operation, Path path, boolean inScope) {
      this.operation = operation;
      this.path = String.valueOf(path);
      this.threadName = Thread.currentThread().getName();
      this.inScope = inScope;
      this.underMetaFolder = isMetaFolderPath(path);
      this.stack = inScope ? new Throwable(operation + " " + path) : null;
    }

    public String getOperation() {
      return operation;
    }

    public String getPath() {
      return path;
    }

    public String getThreadName() {
      return threadName;
    }

    public boolean isInScope() {
      return inScope;
    }

    public boolean isUnderMetaFolder() {
      return underMetaFolder;
    }

    /**
     * The call stack, kept for calls in scope only.
     */
    public Throwable getStack() {
      return stack;
    }

    public static Predicate<Call> inScope() {
      return Call::isInScope;
    }

    /**
     * Calls to a table's {@code .hoodie} folder or anything under it, the metadata table included.
     */
    public static Predicate<Call> underMetaFolder() {
      return Call::isUnderMetaFolder;
    }

    public static Predicate<Call> operation(String... operations) {
      List<String> names = Arrays.asList(operations);
      return call -> names.contains(call.getOperation());
    }

    public static Predicate<Call> pathEndsWith(String suffix) {
      return call -> call.getPath().endsWith(suffix);
    }

    public static Predicate<Call> pathContains(String part) {
      return call -> call.getPath().contains(part);
    }

    @Override
    public String toString() {
      return operation + " " + path + " [" + threadName + "]";
    }
  }

  /**
   * Restores the scope that was set before {@link #withScope}.
   */
  public static final class Scope implements AutoCloseable {
    private final BooleanSupplier previous;

    private Scope(BooleanSupplier previous) {
      this.previous = previous;
    }

    @Override
    public void close() {
      scope = previous;
    }
  }

  /**
   * Makes the {@code file} scheme resolve to this file system on the given configuration.
   */
  public static void register(Configuration conf) {
    synchronized (VALUES_BEFORE_REGISTER) {
      VALUES_BEFORE_REGISTER.computeIfAbsent(conf, c -> {
        Map<String, String> values = new HashMap<>();
        values.put(FILE_IMPL_KEY, c.getRaw(FILE_IMPL_KEY));
        values.put(DISABLE_CACHE_KEY, c.getRaw(DISABLE_CACHE_KEY));
        return values;
      });
    }
    conf.setClass(FILE_IMPL_KEY, RecordingLocalFileSystem.class, FileSystem.class);
    conf.setBoolean(DISABLE_CACHE_KEY, true);
  }

  /**
   * Reverts {@link #register}, restoring the {@code file} scheme settings the configuration had
   * before. Does nothing when the configuration is not registered.
   */
  public static void unregister(Configuration conf) {
    Map<String, String> values;
    synchronized (VALUES_BEFORE_REGISTER) {
      values = VALUES_BEFORE_REGISTER.remove(conf);
    }
    if (values == null) {
      return;
    }
    conf.unset(FILE_IMPL_KEY);
    conf.unset(DISABLE_CACHE_KEY);
    values.forEach((key, value) -> {
      if (value != null) {
        conf.set(key, value);
      }
    });
  }

  /**
   * Sets how to tell whether the current thread runs in scope, for example inside an engine task.
   */
  public static void setScope(BooleanSupplier inScope) {
    scope = inScope;
  }

  /**
   * Sets the scope until the returned {@link Scope} is closed.
   */
  public static Scope withScope(BooleanSupplier inScope) {
    Scope restore = new Scope(scope);
    scope = inScope;
    return restore;
  }

  public static void reset() {
    CALLS.clear();
  }

  public static List<Call> getCalls(Predicate<Call> filter) {
    return CALLS.stream().filter(filter).collect(Collectors.toList());
  }

  public static long count(Predicate<Call> filter) {
    return CALLS.stream().filter(filter).count();
  }

  /**
   * Describes the recorded calls that match {@code filter}: every distinct operation and path, then
   * the first few call stacks.
   */
  public static String describe(Predicate<Call> filter) {
    List<Call> calls = getCalls(filter);
    StringBuilder sb = new StringBuilder();
    sb.append(calls.size()).append(" matching file system call(s):\n");
    calls.stream()
        .map(call -> call.getOperation() + " " + call.getPath())
        .distinct()
        .forEach(line -> sb.append("  ").append(line).append('\n'));
    calls.stream().filter(call -> call.getStack() != null).limit(MAX_DESCRIBED_STACKS).forEach(call -> {
      StringWriter writer = new StringWriter();
      call.getStack().printStackTrace(new PrintWriter(writer));
      sb.append("Stack of ").append(call).append(":\n").append(writer);
    });
    return sb.toString();
  }

  static boolean isMetaFolderPath(Path path) {
    if (path == null) {
      return false;
    }
    String str = path.toUri().getPath();
    return str.contains(META_FOLDER + "/") || str.endsWith(META_FOLDER);
  }

  /**
   * Runs {@code call}, recording it when it is the outermost recorded call on this thread. Nested
   * calls (for example {@code exists} delegating to {@code getFileStatus}) are not recorded twice.
   */
  private static <T> T record(String operation, Path path, IOCall<T> call) throws IOException {
    if (IN_RECORDED_CALL.get()) {
      return call.run();
    }
    CALLS.add(new Call(operation, path, scope.getAsBoolean()));
    IN_RECORDED_CALL.set(true);
    try {
      return call.run();
    } finally {
      IN_RECORDED_CALL.set(false);
    }
  }

  @FunctionalInterface
  private interface IOCall<T> {
    T run() throws IOException;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return record("open", f, () -> super.open(f, bufferSize));
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize,
                                   Progressable progress) throws IOException {
    return record("create", f, () -> super.create(f, overwrite, bufferSize, replication, blockSize, progress));
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication,
                                   long blockSize, Progressable progress) throws IOException {
    return record("create", f, () -> super.create(f, permission, overwrite, bufferSize, replication, blockSize, progress));
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize,
                                               short replication, long blockSize, Progressable progress) throws IOException {
    return record("createNonRecursive", f,
        () -> super.createNonRecursive(f, permission, flags, bufferSize, replication, blockSize, progress));
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, boolean overwrite, int bufferSize,
                                               short replication, long blockSize, Progressable progress) throws IOException {
    return record("createNonRecursive", f,
        () -> super.createNonRecursive(f, permission, overwrite, bufferSize, replication, blockSize, progress));
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    return record("append", f, () -> super.append(f, bufferSize, progress));
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return record("rename", src, () -> super.rename(src, dst));
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return record("delete", f, () -> super.delete(f, recursive));
  }

  @Override
  public boolean mkdirs(Path f) throws IOException {
    return record("mkdirs", f, () -> super.mkdirs(f));
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return record("mkdirs", f, () -> super.mkdirs(f, permission));
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    return record("getFileStatus", f, () -> super.getFileStatus(f));
  }

  @Override
  public boolean exists(Path f) throws IOException {
    return record("exists", f, () -> super.exists(f));
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    return record("listStatus", f, () -> super.listStatus(f));
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f) throws IOException {
    return record("listLocatedStatus", f, () -> super.listLocatedStatus(f));
  }

  @Override
  public boolean truncate(Path f, long newLength) throws IOException {
    return record("truncate", f, () -> super.truncate(f, newLength));
  }

  @Override
  public void setTimes(Path p, long mtime, long atime) throws IOException {
    record("setTimes", p, () -> {
      super.setTimes(p, mtime, atime);
      return null;
    });
  }
}
