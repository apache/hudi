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

package org.apache.hudi.utilities.unstructured.embedding.streamer;

import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.RawLocalFileSystem;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

/**
 * Atomic replacement owned by the embedding stream, never delete-then-rename. Local files use raw I/O and a NIO atomic
 * move: Hadoop's local overwrite rename may delete the destination first, and checksum/data renames are separate. HDFS
 * uses its native overwrite rename. S3A/GCS require transactional object close plus an explicitly abortable output stream;
 * older connectors without that capability are refused before opening the destination. A lost close acknowledgement can
 * report failure after the complete new object has committed; it does not permit rollback of that committed object.
 */
class EmbeddingColumnInfoAtomicSwap {
  private static final String ABORTABLE = "fs.capability.outputstream.abortable";

  /** Raw local metadata has one visibility unit, without a separately visible checksum file. */
  static HoodieStorage metadataStorage(HoodieStorage storage) {
    HoodieStorage raw = storage.getRawStorage();
    if (raw.getFileSystem() instanceof LocalFileSystem) {
      return new HoodieHadoopStorage(((LocalFileSystem) raw.getFileSystem()).getRawFileSystem());
    }
    return raw;
  }

  /** Existing checksum sidecars are refused; deleting somebody else's sidecar would introduce a reader race. */
  static void checkLocalChecksum(HoodieStorage storage, StoragePath target) throws IOException {
    if ("file".equalsIgnoreCase(storage.getScheme())) {
      StoragePath checksum = new StoragePath(target.getParent(), "." + target.getName() + ".crc");
      if (storage.exists(checksum)) {
        throw new IOException("Embedding column information requires raw local files without checksum sidecars: " + checksum);
      }
    }
  }

  /** Scratch must already be closed and complete. Unsupported backends never fall back to ordinary overwrite or rename. */
  void replace(HoodieStorage storage, StoragePath scratch, StoragePath target) throws IOException {
    String scheme = storage.getScheme().toLowerCase(Locale.ROOT);
    switch (scheme) {
      case "file":
        checkLocalChecksum(storage, target);
        if (!(storage.getFileSystem() instanceof RawLocalFileSystem)) {
          throw new IOException("Atomic embedding column replacement requires raw local storage");
        }
        Files.move(Paths.get(scratch.toUri().getPath()), Paths.get(target.toUri().getPath()),
            StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        return;
      case "hdfs":
        Configuration conf = storage.getConf().unwrapCopyAs(Configuration.class);
        FileContext.getFileContext(storage.getUri(), conf).rename(new org.apache.hadoop.fs.Path(scratch.toUri()),
            new org.apache.hadoop.fs.Path(target.toUri()), Options.Rename.OVERWRITE);
        return;
      case "s3a":
      case "gs":
        replaceObject(storage, scratch, target);
        return;
      default:
        throw new IOException("Storage scheme " + scheme + " does not provide a supported atomic embedding column replacement");
    }
  }

  private static void replaceObject(HoodieStorage storage, StoragePath scratch, StoragePath target) throws IOException {
    probeObjectOutput(storage, scratch.getParent());
    byte[] complete;
    try (InputStream input = storage.open(scratch)) {
      complete = input.readAllBytes();
    }
    OutputStream output = storage.create(target, true);
    AbortHandle handle = abortHandle(output);
    if (handle == null) {
      // Closing would publish an empty object. The scratch probe proved support; an inconsistent connector is unsafe.
      throw new IOException("Destination output lost its advertised abort capability; refusing to close/publish it");
    }
    writeAndClose(output, handle, complete);
  }

  /** Checks publication support using only an owned temporary object, before prep or data writes. */
  void checkSupported(HoodieStorage storage, StoragePath target) throws IOException {
    String scheme = storage.getScheme().toLowerCase(Locale.ROOT);
    if ("file".equals(scheme)) {
      checkLocalChecksum(storage, target);
      if (!(storage.getFileSystem() instanceof RawLocalFileSystem)) {
        throw new IOException("Atomic embedding column replacement requires raw local storage");
      }
    } else if ("s3a".equals(scheme) || "gs".equals(scheme)) {
      storage.createDirectory(target.getParent());
      probeObjectOutput(storage, target.getParent());
    } else if (!"hdfs".equals(scheme)) {
      throw new IOException("Storage scheme " + scheme + " does not provide a supported atomic embedding column replacement");
    }
  }

  private static void probeObjectOutput(HoodieStorage storage, StoragePath parent) throws IOException {
    // Probe only an owned temporary object, before opening or changing the durable destination.
    StoragePath probe = new StoragePath(parent, UUID.randomUUID() + ".abort-probe");
    try {
      OutputStream output = storage.create(probe, false);
      AbortHandle handle = abortHandle(output);
      if (handle == null) {
        output.close();
        throw new IOException("Atomic embedding column replacement on " + storage.getScheme()
            + " requires a connector advertising " + ABORTABLE);
      }
      handle.abort();
    } finally {
      storage.deleteFile(probe);
    }
  }

  /** Never close a failed upload: close can commit its partial bytes. Abort discards them instead. */
  static void writeAndClose(OutputStream output, AbortHandle handle, byte[] complete) throws IOException {
    try {
      output.write(complete);
      output.close();
    } catch (IOException | RuntimeException failure) {
      try {
        handle.abort();
      } catch (IOException abortFailure) {
        if (abortFailure != failure) {
          failure.addSuppressed(abortFailure);
        }
      }
      throw failure;
    }
  }

  /** Reflection keeps Hadoop 2 compatibility while requiring the newer explicit abort contract on object stores. */
  static AbortHandle abortHandle(OutputStream stream) throws IOException {
    Object current = stream;
    Set<Object> seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    while (current != null && seen.add(current)) {
      AbortHandle handle = abortHandleOf(current);
      if (handle != null) {
        return handle;
      }
      current = wrappedOf(current);
    }
    return null;
  }

  /** The abort handle of {@code stream} itself when it declares the abortable capability, else null. */
  private static AbortHandle abortHandleOf(Object stream) throws IOException {
    try {
      Method capability = stream.getClass().getMethod("hasCapability", String.class);
      if (Boolean.TRUE.equals(capability.invoke(stream, ABORTABLE))) {
        return new AbortHandle(stream, stream.getClass().getMethod("abort"));
      }
    } catch (NoSuchMethodException ignored) {
      // A wrapper may predate the capability while its wrapped stream implements it.
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new IOException("Cannot inspect object output abort capability", e);
    }
    return null;
  }

  /** The stream {@code stream} wraps, through either standard Hadoop output wrapper accessor; null when it wraps none. */
  private static Object wrappedOf(Object stream) throws IOException {
    for (String method : new String[] {"getWrappedStream", "getWrappedOutputStream"}) {
      try {
        return stream.getClass().getMethod(method).invoke(stream);
      } catch (NoSuchMethodException ignored) {
        // Try the other standard Hadoop output wrapper accessor.
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new IOException("Cannot inspect wrapped object output", e);
      }
    }
    return null;
  }

  static final class AbortHandle {
    private final Object stream;
    private final Method method;

    private AbortHandle(Object stream, Method method) {
      this.stream = stream;
      this.method = method;
    }

    private void abort() throws IOException {
      try {
        Object result = method.invoke(stream);
        if (result == null) {
          throw new IOException("Object output abort did not return its cleanup result");
        }
        Class<?> resultType = method.getReturnType();
        IOException cleanup = (IOException) resultType.getMethod("anyCleanupException").invoke(result);
        if (cleanup != null) {
          throw cleanup;
        }
        if (Boolean.TRUE.equals(resultType.getMethod("alreadyClosed").invoke(result))) {
          throw new IOException("Object output was already closed before abort; cleanup is not established");
        }
      } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
        throw new IOException("Cannot abort failed embedding column information upload", e);
      }
    }
  }
}
