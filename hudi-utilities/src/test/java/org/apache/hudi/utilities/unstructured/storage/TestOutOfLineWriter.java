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

import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertThrows;

/** Out-of-line writer: fresh paths, task tokens, bounded streams and a serializable factory. */
class TestOutOfLineWriter {
  private static final String INSTANT = "20261027120000000";

  @TempDir
  Path temp;

  @Test
  void invalidPartitionsAreRejectedBeforeCreate() throws Exception {
    for (String partition : new String[] {"../p", "/p", "p//q", "p/./q", "p/"}) {
      assertThrows(IllegalArgumentException.class, () -> OutOfLineWriter.validatePartition(partition));
    }
  }

  /** Java serialization, as Spark's closure serializer ships a task's captured values. */
  private static OutOfLineWriter.Factory javaRoundTrip(OutOfLineWriter.Factory factory) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(factory);
    }
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (OutOfLineWriter.Factory) in.readObject();
    }
  }

  private String initTable() throws IOException {
    String base = temp.resolve("table").toUri().toString();
    HoodieTableMetaClient.newTableBuilder()
        .setTableName("blob_table").setTableType(HoodieTableType.COPY_ON_WRITE)
        .initTable(HadoopFSUtils.getStorageConf(new Configuration()), base);
    return base;
  }

  /** A task context with fixed ids, as a Spark task attempt supplies them. */
  private static final class LocalTaskContextSupplier extends TaskContextSupplier {
    private final int partitionId;
    private final int stageId;
    private final long attemptId;

    LocalTaskContextSupplier(int partitionId, int stageId, long attemptId) {
      this.partitionId = partitionId;
      this.stageId = stageId;
      this.attemptId = attemptId;
    }

    @Override
    public Supplier<Integer> getPartitionIdSupplier() {
      return () -> partitionId;
    }

    @Override
    public Supplier<Integer> getStageIdSupplier() {
      return () -> stageId;
    }

    @Override
    public Supplier<Long> getAttemptIdSupplier() {
      return () -> attemptId;
    }

    @Override
    public Option<String> getProperty(EngineProperty prop) {
      return Option.empty();
    }

    @Override
    public Supplier<Integer> getTaskAttemptNumberSupplier() {
      return () -> 0;
    }

    @Override
    public Supplier<Integer> getStageAttemptNumberSupplier() {
      return () -> 0;
    }
  }

}
