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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Storage failures must not silently quarantine a present source file. */
public class TestBlobBytesSourceStorageFailures {
  @TempDir
  Path temp;

  public static class FailingFileSystem extends RawLocalFileSystem {
    private String fault;

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
      fault = conf.get("test.source.fault");
      if ("construction".equals(fault)) {
        throw new UnsupportedFileSystemException("injected configured connector failure");
      }
      super.initialize(name, conf);
    }

    @Override
    public URI getUri() {
      return URI.create("r7://bucket");
    }

    @Override
    public FSDataInputStream open(org.apache.hadoop.fs.Path path, int bufferSize) throws IOException {
      if ("recheck".equals(fault)) {
        throw new FileNotFoundException("injected open failure");
      }
      FSDataInputStream delegate = super.open(path, bufferSize);
      return new FSDataInputStream(new FSInputStream() {
        @Override
        public void seek(long position) throws IOException {
          throw new EOFException("injected seek connection failure");
        }

        @Override
        public long getPos() throws IOException {
          return delegate.getPos();
        }

        @Override
        public boolean seekToNewSource(long position) {
          return false;
        }

        @Override
        public int read() throws IOException {
          return delegate.read();
        }

        @Override
        public void close() throws IOException {
          delegate.close();
        }
      });
    }

    @Override
    public FileStatus getFileStatus(org.apache.hadoop.fs.Path path) throws IOException {
      if ("recheck".equals(fault)) {
        throw new IOException("injected status unavailable");
      }
      return super.getFileStatus(path);
    }
  }

  private BlobBytesSource source(String fault) {
    Configuration conf = new Configuration(false);
    conf.set("fs.r7.impl", FailingFileSystem.class.getName());
    conf.setBoolean("fs.r7.impl.disable.cache", true);
    conf.set("test.source.fault", fault);
    return new BlobBytesSource(HadoopFSUtils.getStorageConf(conf));
  }

  private Row blob() throws IOException {
    Path file = Files.write(temp.resolve("source.bin"), new byte[10]);
    StructType schema = (StructType) BlobType.dataType();
    StructType reference = (StructType) schema.apply(HoodieSchema.Blob.EXTERNAL_REFERENCE).dataType();
    return new GenericRowWithSchema(new Object[] {HoodieSchema.Blob.OUT_OF_LINE, null,
        new GenericRowWithSchema(new Object[] {"r7://bucket" + file, 2L, 1L, false}, reference)}, schema);
  }

  @Test
  void failedAbsenceRecheckIsRetryableStorageFailure() throws IOException {
    Row blob = blob();
    IOException failure = assertThrows(IOException.class, () -> source("recheck").open(blob));
    assertFalse(SourceFailures.isContentFailure(failure), failure.toString());
    assertTrue(BlobBytesSource.isTransient(failure));
    assertTrue(failure.getMessage().contains("absence could not be confirmed"));
    assertTrue(failure.getSuppressed().length > 0, "the original open failure is retained");
  }

  @Test
  void eofSeekingInsideALongerObjectIsStorageFailure() throws IOException {
    Row blob = blob();
    IOException failure = assertThrows(IOException.class, () -> source("seek").open(blob));
    assertFalse(SourceFailures.isContentFailure(failure));
    assertTrue(BlobBytesSource.isTransient(failure));
  }

  @Test
  void configuredConnectorConstructionFailureIsPlatformFailure() throws IOException {
    Row blob = blob();
    IOException failure = assertThrows(IOException.class, () -> source("construction").open(blob));
    assertFalse(SourceFailures.isContentFailure(failure));
    assertFalse(BlobBytesSource.isTransient(failure));
    assertTrue(failure.getMessage().contains("Cannot get storage"));
  }
}
