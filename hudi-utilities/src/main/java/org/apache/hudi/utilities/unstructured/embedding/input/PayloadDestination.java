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

package org.apache.hudi.utilities.unstructured.embedding.input;

import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.utilities.unstructured.storage.OutOfLineWriter;

import java.io.Serializable;
import java.util.Properties;

/**
 * Where the embedding round writes payloads as it prepares them (the round's instant exists before the transform): the
 * embedding table's base path, under whose {@code .hoodie/.blob} folder each payload file is written and against which its
 * relative reference resolves.
 */
public final class PayloadDestination implements Serializable {

  private static final long serialVersionUID = 1L;

  private final StorageConfiguration<?> storageConf;
  private final String tableBasePath;
  private final OutOfLineWriter.Factory writers;
  private transient HoodieStorage tableStorage;
  private transient OutOfLineWriter writer;
  private transient String writerInstant;

  /**
   * Builds, on the driver, the embedding table that carries the running write client's effective configuration (its
   * timeline-server address, for the payload markers) to each executor.
   *
   * @param storageConf   the storage configuration the embedding table is written with
   * @param tableBasePath the embedding table's base path
   * @param props         the round's properties, the write client's effective configuration
   * @throws org.apache.hudi.exception.HoodieValidationException when the table is below version 9, or when out-of-line markers
   *     are on and the job's markers would fall back to direct markers ({@link OutOfLineWriter#forTable})
   */
  public PayloadDestination(StorageConfiguration<?> storageConf, String tableBasePath, Properties props) {
    this.storageConf = storageConf;
    this.tableBasePath = tableBasePath;
    this.writers = OutOfLineWriter.forTable(storageConf, tableBasePath, props);
  }

  /** The storage of the embedding table, created once per deserialized copy. */
  public HoodieStorage tableStorage() {
    if (tableStorage == null) {
      tableStorage = HoodieStorageUtils.getStorage(tableBasePath, storageConf);
    }
    return tableStorage;
  }

  /** The shared writer, created once per deserialized destination and instant, only for an out-of-line payload. */
  public OutOfLineWriter writer(String instant) {
    if (writer == null || !instant.equals(writerInstant)) {
      writer = writers.forInstant(tableStorage(), instant);
      writerInstant = instant;
    }
    return writer;
  }
}
