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

package org.apache.hudi.common.table.read.lsm;

import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.serialization.CustomSerializer;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieIOException;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Sequential disk-backed iterator for sorted LSM inputs.
 *
 * <p>The source iterator is drained into a length-prefixed spill file and closed. The resulting
 * iterator reads the records back sequentially, which matches the loser-tree access pattern.
 */
class SpillableLsmRecordIterator<T> implements ClosableIterator<BufferedRecord<T>> {

  private static final int BUFFER_SIZE = 128 * 1024;
  private static final String SPILL_FILE_PREFIX = "hudi-lsm-";
  private static final String SPILL_FILE_SUFFIX = ".spill";

  private final CustomSerializer<BufferedRecord<T>> serializer;
  private final RecordContext<T> recordContext;
  private final File spillFile;
  private final long recordCount;
  private DataInputStream inputStream;
  private long recordsRead;
  private BufferedRecord<T> nextRecord;
  private boolean closed;

  SpillableLsmRecordIterator(ClosableIterator<BufferedRecord<T>> sourceIterator,
                             CustomSerializer<BufferedRecord<T>> serializer,
                             RecordContext<T> recordContext,
                             String spillBasePath) {
    this.serializer = serializer;
    this.recordContext = recordContext;
    Throwable spillFailure = null;
    try {
      Path spillDirectory = Paths.get(spillBasePath);
      Files.createDirectories(spillDirectory);
      this.spillFile = Files.createTempFile(spillDirectory, SPILL_FILE_PREFIX, SPILL_FILE_SUFFIX).toFile();
      this.spillFile.deleteOnExit();
      this.recordCount = spill(sourceIterator);
    } catch (IOException e) {
      spillFailure = e;
      throw new HoodieIOException("Failed to spill LSM input iterator", e);
    } catch (RuntimeException e) {
      spillFailure = e;
      throw e;
    } finally {
      closeSourceIterator(sourceIterator, spillFailure);
    }
  }

  private long spill(ClosableIterator<BufferedRecord<T>> sourceIterator) throws IOException {
    long count = 0;
    try (DataOutputStream outputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(spillFile), BUFFER_SIZE))) {
      while (sourceIterator.hasNext()) {
        byte[] bytes = serializer.serialize(sourceIterator.next().toBinary(recordContext));
        outputStream.writeInt(bytes.length);
        outputStream.write(bytes);
        count++;
      }
    } catch (IOException | RuntimeException e) {
      deleteSpillFile();
      throw e;
    }
    return count;
  }

  @Override
  public boolean hasNext() {
    if (nextRecord != null) {
      return true;
    }
    if (recordsRead >= recordCount) {
      return false;
    }
    try {
      ensureInputStream();
      int length = inputStream.readInt();
      byte[] bytes = new byte[length];
      inputStream.readFully(bytes);
      nextRecord = serializer.deserialize(bytes);
      recordsRead++;
      return true;
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read spilled LSM input iterator", e);
    }
  }

  @Override
  public BufferedRecord<T> next() {
    if (!hasNext()) {
      throw new java.util.NoSuchElementException();
    }
    BufferedRecord<T> record = nextRecord;
    nextRecord = null;
    return record;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    try {
      if (inputStream != null) {
        inputStream.close();
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to close spilled LSM input iterator", e);
    } finally {
      deleteSpillFile();
      closed = true;
    }
  }

  private void ensureInputStream() throws IOException {
    if (inputStream == null) {
      inputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(spillFile), BUFFER_SIZE));
    }
  }

  private void deleteSpillFile() {
    try {
      Files.deleteIfExists(spillFile.toPath());
    } catch (IOException e) {
      throw new HoodieIOException("Failed to delete spilled LSM input file " + spillFile, e);
    }
  }

  private void closeSourceIterator(ClosableIterator<BufferedRecord<T>> sourceIterator,
                                   Throwable spillFailure) {
    try {
      sourceIterator.close();
    } catch (RuntimeException e) {
      if (spillFailure != null) {
        spillFailure.addSuppressed(e);
      } else {
        throw e;
      }
    }
  }
}
