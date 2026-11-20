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

package org.apache.hudi.utilities.unstructured.storage;

import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Properties;

/**
 * Writes bounded streams to fresh managed files under {@code .hoodie/.blob}, named with a new file id, the writing task's
 * token and the round's instant. Completed files are unmarked: failed rounds and recomputed tasks can leave unreferenced
 * copies. Partial failed writes are removed best-effort. Nothing removes unreferenced files.
 */
@Slf4j
public final class OutOfLineWriter {

  /** Suffix of every out-of-line file name. */
  private static final String FILE_EXTENSION = ".bin";

  private final HoodieStorage storage;
  private final String basePath;
  private final String instantTime;
  private final TaskContextSupplier taskContextSupplier;

  /** A writer using the current Spark task's token, or Hudi's default token outside a task. */
  public OutOfLineWriter(HoodieStorage storage, String basePath, String instantTime) {
    this(storage, basePath, instantTime, new SparkTaskContextSupplier());
  }

  @VisibleForTesting
  OutOfLineWriter(HoodieStorage storage, String basePath, String instantTime, TaskContextSupplier taskContextSupplier) {
    this.storage = storage;
    this.basePath = basePath;
    this.instantTime = instantTime;
    this.taskContextSupplier = taskContextSupplier;
  }

  /**
   * Creates a serializable factory for the table at {@code basePath} on the driver. It reads neither {@code storageConf} nor
   * {@code props}: each writer needs only the table path and the storage supplied by its task.
   */
  public static Factory forTable(StorageConfiguration<?> storageConf, String basePath, Properties props) {
    return new Factory(basePath);
  }

  /** A serializable table destination for out-of-line writers. */
  public static class Factory implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String basePath;

    Factory(String basePath) {
      this.basePath = basePath;
    }

    /** Creates the writer of an instant without loading a table or configuration in the task. */
    public OutOfLineWriter forInstant(HoodieStorage storage, String instant) {
      return new OutOfLineWriter(storage, basePath, instant);
    }
  }

  /** A bounded streaming operation; callers classify source and target failures using their existing contracts. */
  @FunctionalInterface
  public interface StreamingWrite {
    void write(OutputStream out) throws IOException;
  }

  /**
   * Generates a fresh relative path and creates and streams it once, without overwrite. Returns after close; a failed stream
   * or close removes its file best-effort. A completed write can remain unreferenced if its round or task later fails.
   */
  public String write(String partition, StreamingWrite stream) throws IOException {
    validatePartition(partition);
    String directory = ManagedBlobPaths.FOLDER + (StringUtils.isNullOrEmpty(partition) ? "" : "/" + partition);
    String name = FSUtils.makeBaseFileName(instantTime, FSUtils.makeWriteToken(taskContextSupplier),
        FSUtils.createNewFileId(FSUtils.createNewFileIdPfx(), 0), FILE_EXTENSION);
    String relative = directory + "/" + name;
    StoragePath target = new StoragePath(basePath, relative);
    boolean created = false;
    try (OutputStream out = storage.create(target, false)) {
      created = true;
      stream.write(out);
    } catch (IOException | RuntimeException failure) {
      if (created) {
        try {
          storage.deleteFile(target);
        } catch (IOException | RuntimeException cleanup) {
          failure.addSuppressed(cleanup);
          log.warn("Could not remove failed out-of-line file {}; it stays unreferenced", target, cleanup);
        }
      }
      throw failure;
    }
    return relative;
  }

  /** Keeps generated paths relative and contained; numeric partition segments carry no special meaning. */
  public static void validatePartition(String partition) {
    if (!StringUtils.isNullOrEmpty(partition)) {
      for (String segment : partition.split("/", -1)) {
        if (segment.isEmpty() || segment.equals(".") || segment.equals("..")) {
          throw new IllegalArgumentException("Out-of-line partition must have relative segments without empty, '.' or '..': " + partition);
        }
      }
    }
  }
}
