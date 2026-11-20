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

import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Objects;

/** The whole-object status checked before a source read, retained through that read's completion. */
final class SourceObjectVersion {
  private final StoragePath path;
  private final FileStatus status;
  private final long length;
  private final long modificationTime;
  private final String versionId;
  private final String etag;
  private final String generation;

  private SourceObjectVersion(StoragePath path, FileStatus status, long length, long modificationTime) throws IOException {
    this.path = path;
    this.status = status;
    this.length = length;
    this.modificationTime = modificationTime;
    this.versionId = attribute(status, "getVersionId");
    this.etag = attribute(status, "getETag", "getEtag");
    this.generation = attribute(status, "getGeneration");
  }

  static SourceObjectVersion read(HoodieStorage storage, StoragePath path) throws IOException {
    Object fs = storage.getFileSystem();
    if (fs instanceof FileSystem) {
      FileStatus status = ((FileSystem) fs).getFileStatus(new Path(path.toUri()));
      return new SourceObjectVersion(path, status, status.getLen(), status.getModificationTime());
    }
    StoragePathInfo info = storage.getPathInfo(path);
    return new SourceObjectVersion(path, null, info.getLength(), info.getModificationTime());
  }

  boolean names(StoragePath candidate) {
    return path.equals(candidate);
  }

  void checkListed(long listedTime, long listedLength) throws BlobSourceChangedException {
    if (length != listedLength || (listedTime > 0 && modificationTime != listedTime)) {
      throw new BlobSourceChangedException("Source object " + path + " changed since it was listed: expected mtime="
          + listedTime + ", size=" + listedLength + "; found mtime=" + modificationTime + ", size=" + length);
    }
  }

  private void recheck(HoodieStorage storage) throws IOException {
    SourceObjectVersion after = read(storage, path);
    // Without an ETag/version, an equal-size overwrite inside one mtime tick remains undetectable on older connectors.
    // A version marker (for example S3's literal "null") must not hide a changed ETag.
    if (length != after.length || modificationTime != after.modificationTime
        || !Objects.equals(versionId, after.versionId) || !Objects.equals(etag, after.etag)
        || !Objects.equals(generation, after.generation)) {
      throw new BlobSourceChangedException("Source object " + path + " changed while it was read");
    }
  }

  SeekableDataInputStream open(HoodieStorage storage, int bufferSize, Long knownLength, long readahead) throws IOException {
    SeekableDataInputStream delegate = SourceObjectOpener.open(storage, path, bufferSize, knownLength, readahead, status);
    CheckedInputStream checked = new CheckedInputStream(delegate, storage);
    return new SeekableDataInputStream(checked) {
      @Override
      public long getPos() throws IOException {
        return delegate.getPos();
      }

      @Override
      public void seek(long position) throws IOException {
        delegate.seek(position);
        checked.verified = false;
      }
    };
  }

  /** Connector-specific status attributes are optional; this module still compiles against Hadoop 2. */
  private static String attribute(FileStatus status, String... aliases) throws IOException {
    if (status == null) {
      return null;
    }
    for (String getter : aliases) {
      try {
        Object value = getter(status.getClass(), getter).invoke(status);
        if (value != null && !value.toString().isEmpty()) {
          return value.toString();
        }
      } catch (NoSuchMethodException absent) {
        // No readable version attribute: retain the connector’s mtime and length checks.
      } catch (InvocationTargetException e) {
        if (e.getCause() instanceof UnsupportedOperationException) {
          continue;
        }
        throw new IOException("Cannot inspect source object version", e);
      } catch (IllegalAccessException e) {
        throw new IOException("Cannot inspect source object version", e);
      }
    }
    return null;
  }

  /** ABFS can return a non-public status implementation; invoke its public interface instead of its concrete getter. */
  private static Method getter(Class<?> type, String name) throws NoSuchMethodException {
    Method method = type.getMethod(name);
    if (Modifier.isPublic(method.getDeclaringClass().getModifiers())) {
      return method;
    }
    for (Class<?> contract : type.getInterfaces()) {
      try {
        return getter(contract, name);
      } catch (NoSuchMethodException absent) {
        // Another public contract or superclass may expose this attribute.
      }
    }
    if (type.getSuperclass() != null) {
      return getter(type.getSuperclass(), name);
    }
    throw new NoSuchMethodException(name);
  }

  private final class CheckedInputStream extends FilterInputStream {
    private final HoodieStorage storage;
    private boolean verified;

    private CheckedInputStream(InputStream input, HoodieStorage storage) {
      super(input);
      this.storage = storage;
    }

    private void verify() throws IOException {
      if (!verified) {
        recheck(storage);
        verified = true;
      }
    }

    @Override
    public int read() throws IOException {
      int value = in.read();
      if (value < 0) {
        verify();
      } else {
        verified = false;
      }
      return value;
    }

    @Override
    public int read(byte[] bytes, int offset, int count) throws IOException {
      int read = in.read(bytes, offset, count);
      if (read < 0) {
        verify();
      } else if (read > 0) {
        verified = false;
      }
      return read;
    }

    @Override
    public void close() throws IOException {
      try (InputStream closing = in) {
        verify();
      }
    }
  }
}
