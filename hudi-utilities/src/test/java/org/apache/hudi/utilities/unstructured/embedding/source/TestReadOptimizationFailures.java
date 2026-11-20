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

import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.unstructured.embedding.image.SeekableBlobImageInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.SocketTimeoutException;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Speculative reads may retry ordinary I/O, but never erase a JVM failure. */
class TestReadOptimizationFailures {

  @ParameterizedTest
  @CsvSource({"image,false,0", "image,false,1", "image,false,2", "image,true,0", "image,true,1", "image,true,2"})
  void speculativeReadsPropagateCauseAndSuppressedJvmFailures(String kind, boolean partial, int form) throws Exception {
    LinkageError fatal = new LinkageError("source runtime unavailable");
    FailingBlob source = new FailingBlob(wrapped(fatal, form), partial);
    try (ReadView view = view(kind, source)) {
      assertSame(fatal, assertThrows(LinkageError.class, view::read));
      assertEquals(partial ? 2 : 1, source.reads, "a JVM failure never starts a fallback read");
    }
    assertEquals(1, source.closes, "the reader retains its source owner when a failure propagates");
  }

  @ParameterizedTest
  @CsvSource({"image,false", "image,true"})
  void ordinarySpeculativeIoFailureStillFallsBack(String kind, boolean partial) throws Exception {
    FailingBlob source = new FailingBlob(new IOException("speculative read unavailable"), partial);
    try (ReadView view = view(kind, source)) {
      assertEquals(7, view.read());
    }
    assertEquals(1, source.closes);
  }

  @ParameterizedTest
  @ValueSource(strings = {"image"})
  void virtualMachineFailureIsNotFallback(String kind) throws Exception {
    InternalError fatal = new InternalError("source runtime unavailable");
    try (ReadView view = view(kind, new FailingBlob(new IOException("source read failed", fatal), false))) {
      assertSame(fatal, assertThrows(InternalError.class, view::read));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"image"})
  void causeCycleDoesNotHideSuppressedJvmFailure(String kind) throws Exception {
    LinkageError fatal = new LinkageError("source runtime unavailable");
    IOException first = new IOException("first wrapper");
    IOException second = new IOException("second wrapper");
    first.initCause(second);
    second.initCause(first);
    second.addSuppressed(fatal);
    try (ReadView view = view(kind, new FailingBlob(first, false))) {
      assertSame(fatal, assertThrows(LinkageError.class, view::read));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"image"})
  void wrappedInputNestingKeepsItsExistingFallback(String kind) throws Exception {
    IOException failure = new IOException("input nesting", new StackOverflowError("input nesting"));
    try (ReadView view = view(kind, new FailingBlob(failure, false))) {
      assertEquals(7, view.read());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 0, 1, 2})
  void reflectiveBuilderPreservesSynchronousAndWrappedJvmFailures(int form) throws Exception {
    LinkageError fatal = new LinkageError("builder runtime unavailable");
    Throwable failure = form < 0 ? fatal : wrapped(fatal, form);
    FakeBuilder builder = new FakeBuilder(failure);
    assertSame(fatal, assertThrows(LinkageError.class, () -> api().open(new FakeFileSystem(builder),
        new Path("file:///test/object"), new FileStatus(), 4, 4096)));
  }

  @Test
  void reflectiveBuilderKeepsUnsupportedApiFallbackAndIoFailure() throws Exception {
    assertNull(api().open(new FakeFileSystem(new FakeBuilder(new IllegalArgumentException("unsupported status"))),
        new Path("file:///test/object"), new FileStatus(), 4, 4096));
    IOException failure = new IOException("object cannot be opened");
    assertSame(failure, assertThrows(IOException.class, () -> api().open(new FakeFileSystem(new FakeBuilder(failure)),
        new Path("file:///test/object"), new FileStatus(), 4, 4096)));
    assertNull(SourceObjectOpener.openWithKnownLength(new FakeFileSystem(new FakeBuilder(null)),
        new Path("file:///test/object"), 4, 4096), "a non-S3A filesystem keeps its ordinary open");
  }

  @Test
  void asyncUnsupportedArgumentDoesNotHideSuppressedJvmFailure() throws Exception {
    LinkageError fatal = new LinkageError("async runtime unavailable");
    IllegalArgumentException wrapper = new IllegalArgumentException("unsupported status");
    wrapper.addSuppressed(fatal);
    assertSame(fatal, assertThrows(LinkageError.class, () -> await(wrapper)));
    assertNull(await(new IllegalArgumentException("ordinary unsupported status")));
  }

  @Test
  void readaheadPreservesJvmFailureAndOrdinaryUnsupportedControl() throws Exception {
    Method setter = SourceObjectOpener.class.getDeclaredMethod("setReadahead", FSDataInputStream.class, long.class);
    setter.setAccessible(true);
    LinkageError fatal = new LinkageError("readahead runtime unavailable");
    FSDataInputStream broken = mock(FSDataInputStream.class);
    doThrow(wrapped(fatal, 1)).when(broken).setReadahead(1024L);
    assertSame(fatal, assertThrows(LinkageError.class, () -> invoke(setter, null, broken, 1024L)));
    FSDataInputStream unsupported = mock(FSDataInputStream.class);
    doThrow(new UnsupportedOperationException("readahead is unavailable")).when(unsupported).setReadahead(1024L);
    invoke(setter, null, unsupported, 1024L);
  }

  @Test
  void readaheadJvmFailureClosesTheOpenedStream() throws Exception {
    LinkageError fatal = new LinkageError("read-ahead runtime unavailable");
    FSDataInputStream stream = mock(FSDataInputStream.class);
    doThrow(fatal).when(stream).setReadahead(1024L);
    FakeFileSystem fs = new FakeFileSystem(new FakeBuilder(null));
    fs.stream = stream;
    HoodieStorage storage = mock(HoodieStorage.class);
    when(storage.getFileSystem()).thenReturn(fs);
    assertSame(fatal, assertThrows(LinkageError.class, () -> SourceObjectOpener.open(storage,
        new StoragePath("file:///test/object"), 4096, null, 1024)));
    org.mockito.Mockito.verify(stream).close();
  }

  private static Exception wrapped(Error fatal, int form) {
    if (form == 0) {
      return new IOException("source read failed", fatal);
    }
    Exception wrapper = form == 1 ? new IllegalStateException("source read failed") : new SocketTimeoutException("read timed out");
    wrapper.addSuppressed(fatal);
    return wrapper;
  }

  private static SourceObjectOpener.OpenFileApi api() throws Exception {
    Constructor<SourceObjectOpener.OpenFileApi> constructor = SourceObjectOpener.OpenFileApi.class
        .getDeclaredConstructor(Method.class, Method.class, Method.class, Method.class);
    constructor.setAccessible(true);
    return constructor.newInstance(FakeFileSystem.class.getMethod("startOpen", Path.class),
        FakeBuilder.class.getMethod("withFileStatus", FileStatus.class),
        FakeBuilder.class.getMethod("opt", String.class, String.class), FakeBuilder.class.getMethod("build"));
  }

  private static Object await(Throwable failure) throws Exception {
    CompletableFuture<FSDataInputStream> future = new CompletableFuture<>();
    future.completeExceptionally(failure);
    Method method = SourceObjectOpener.OpenFileApi.class.getDeclaredMethod("await", CompletableFuture.class, Path.class);
    method.setAccessible(true);
    return invoke(method, null, future, new Path("file:///test/object"));
  }

  private static Object invoke(Method method, Object receiver, Object... arguments) throws Exception {
    try {
      return method.invoke(receiver, arguments);
    } catch (InvocationTargetException wrapper) {
      if (wrapper.getCause() instanceof Error) {
        throw (Error) wrapper.getCause();
      }
      throw (Exception) wrapper.getCause();
    }
  }

  private static ReadView view(String kind, FailingBlob source) throws Exception {
    if ("image".equals(kind)) {
      SeekableBlobImageInputStream stream = new SeekableBlobImageInputStream(source, 4);
      return new ReadView(stream, stream::read);
    }
    Class<?> type = Class.forName("org.apache.hudi.utilities.unstructured.embedding.video.BlockBufferedBlob");
    Constructor<?> constructor = type.getDeclaredConstructor(SeekableBlob.class, int.class);
    constructor.setAccessible(true);
    SeekableBlob blob = (SeekableBlob) constructor.newInstance(source, 4);
    return new ReadView(blob, () -> {
      byte[] one = new byte[1];
      return blob.read(one, 0, 1) > 0 ? one[0] : -1;
    });
  }

  @FunctionalInterface
  private interface Read {
    int read() throws Exception;
  }

  private static final class ReadView implements AutoCloseable {
    private final Closeable owner;
    private final Read read;

    private ReadView(Closeable owner, Read read) {
      this.owner = owner;
      this.read = read;
    }

    int read() throws Exception {
      return read.read();
    }

    @Override
    public void close() throws IOException {
      owner.close();
    }
  }

  private static final class FailingBlob implements SeekableBlob {
    private final Exception failure;
    private final boolean partial;
    private long pos;
    private int reads;
    private int closes;

    private FailingBlob(Exception failure, boolean partial) {
      this.failure = failure;
      this.partial = partial;
    }

    @Override
    public long length() {
      return 4;
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void seek(long position) {
      pos = position;
    }

    @Override
    public int read(byte[] bytes, int offset, int len) throws IOException {
      reads++;
      if (reads == (partial ? 2 : 1)) {
        if (failure instanceof IOException) {
          throw (IOException) failure;
        }
        throw (RuntimeException) failure;
      }
      int count = (int) Math.min(partial && reads == 1 ? 1 : len, length() - pos);
      if (count <= 0) {
        return -1;
      }
      java.util.Arrays.fill(bytes, offset, offset + count, (byte) 7);
      pos += count;
      return count;
    }

    @Override
    public void close() {
      closes++;
    }
  }

  public static final class FakeFileSystem extends FilterFileSystem {
    private final FakeBuilder builder;
    private FSDataInputStream stream;

    private FakeFileSystem(FakeBuilder builder) {
      this.builder = builder;
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) {
      return stream;
    }

    public FakeBuilder startOpen(Path path) {
      return builder;
    }
  }

  public static final class FakeBuilder {
    private final Throwable failure;

    private FakeBuilder(Throwable failure) {
      this.failure = failure;
    }

    public FakeBuilder withFileStatus(FileStatus status) {
      return this;
    }

    public FakeBuilder opt(String key, String value) {
      return this;
    }

    public CompletableFuture<FSDataInputStream> build() throws IOException {
      if (failure instanceof Error) {
        throw (Error) failure;
      }
      if (failure instanceof IOException) {
        throw (IOException) failure;
      }
      if (failure instanceof RuntimeException) {
        throw (RuntimeException) failure;
      }
      return CompletableFuture.completedFuture(null);
    }
  }

  // ------------------------------------------------- S3AFileStatus probe (RV3217-3)

  private static FileStatus probe(ClassLoader loader, String statusClass) throws IOException {
    FileSystem local = FileSystem.getLocal(new Configuration());
    return SourceObjectOpener.s3aStatus(loader, statusClass, local, new Path("file:///probe/object"), 4);
  }

  /** An absent status class or constructor is an optional API missing: the open falls back to a status request. */
  @Test
  void absentStatusClassOrConstructorFallsBack() throws IOException {
    ClassLoader loader = TestReadOptimizationFailures.class.getClassLoader();
    assertNull(probe(loader, "org.apache.hudi.absent.S3AFileStatus"));
    assertNull(probe(loader, StatusWithoutConstructor.class.getName()));
    // control: a status class with the S3A constructor is used
    assertEquals(4, probe(loader, ConstructibleStatus.class.getName()).getLen());
  }

  /** A class that cannot be linked while it is looked up (a missing dependency) is also the optional API missing. */
  @Test
  void statusClassLinkageFailureDuringLookupFallsBack() throws IOException {
    ClassLoader unlinkable = new ClassLoader(TestReadOptimizationFailures.class.getClassLoader()) {
      @Override
      protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (name.equals("org.apache.hudi.unlinkable.S3AFileStatus")) {
          throw new NoClassDefFoundError("org/apache/hudi/unlinkable/Dependency (injected)");
        }
        return super.loadClass(name, resolve);
      }
    };
    assertNull(probe(unlinkable, "org.apache.hudi.unlinkable.S3AFileStatus"));
  }

  /** A JVM failure inside the status constructor is the round's failure, not a missing API. */
  @Test
  void statusConstructorJvmFailureIsNotFallback() {
    OutOfMemoryError thrown = assertThrows(OutOfMemoryError.class,
        () -> probe(TestReadOptimizationFailures.class.getClassLoader(), ThrowingStatus.class.getName()));
    assertEquals("status constructor (injected)", thrown.getMessage());
  }

  /** A status class whose initializer fails is not a missing API either: it is looked up uninitialized and fails when constructed. */
  @Test
  void statusInitializerFailureIsNotFallback() {
    // LinkageError: ExceptionInInitializerError on the first use in a JVM, NoClassDefFoundError on any later one
    assertThrows(LinkageError.class,
        () -> probe(TestReadOptimizationFailures.class.getClassLoader(), FailingInitializerStatus.class.getName()));
  }

  public static class StatusWithoutConstructor extends FileStatus {
  }

  public static class ConstructibleStatus extends FileStatus {
    public ConstructibleStatus(long length, long modificationTime, Path path, long blockSize, String eTag, String versionId,
                               String owner) {
      super(length, false, 1, blockSize, modificationTime, path);
    }
  }

  public static class ThrowingStatus extends FileStatus {
    public ThrowingStatus(long length, long modificationTime, Path path, long blockSize, String eTag, String versionId,
                          String owner) {
      throw new OutOfMemoryError("status constructor (injected)");
    }
  }

  public static class FailingInitializerStatus extends FileStatus {
    private static final int VALUE = fail();

    public FailingInitializerStatus(long length, long modificationTime, Path path, long blockSize, String eTag, String versionId,
                                    String owner) {
      super(length + VALUE, false, 1, blockSize, modificationTime, path);
    }

    private static int fail() {
      throw new IllegalStateException("status initializer (injected)");
    }
  }
}
