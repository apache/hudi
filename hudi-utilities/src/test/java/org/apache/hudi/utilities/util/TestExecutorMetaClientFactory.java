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

package org.apache.hudi.utilities.util;

import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FileSystemRetryConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineLayout;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What a phase-2 executor needs in order to reach storage and the archive, and nothing that drags a
 * timeline along with it.
 */
class TestExecutorMetaClientFactory {

  /** Enough that a loaded timeline dominates a byte count; small enough to write in about a second. */
  private static final int INSTANT_COUNT = 1000;

  @Test
  void carriesTheOperatorsRetryAndConsistencyConfigs(@TempDir Path dir) throws IOException {
    HoodieTableMetaClient driverClient = tableAt(dir);

    ExecutorMetaClientFactory factory = ExecutorMetaClientFactory.from(driverClient);
    HoodieTableMetaClient executorClient = factory.newStorageOnlyClient();

    assertEquals(driverClient.getFileSystemRetryConfig().getMaxRetryNumbers(),
        executorClient.getFileSystemRetryConfig().getMaxRetryNumbers());
    assertEquals(driverClient.getFileSystemRetryConfig().isFileSystemActionRetryEnable(),
        executorClient.getFileSystemRetryConfig().isFileSystemActionRetryEnable());
    assertEquals(driverClient.getConsistencyGuardConfig().isConsistencyCheckEnabled(),
        executorClient.getConsistencyGuardConfig().isConsistencyCheckEnabled());
    assertEquals(driverClient.getBasePath().toString(), executorClient.getBasePath().toString());
  }

  /**
   * A non-default retry setting must survive the trip. Guards the actual regression: the builder
   * carries only conf/basePath/loadActiveTimelineOnLoad, so an executor client built by hand gets
   * defaults and every storage call it makes runs unretried.
   */
  @Test
  void nonDefaultRetryConfigIsNotSilentlyLost(@TempDir Path dir) throws IOException {
    StorageConfiguration<?> conf = HadoopFSUtils.getStorageConf(new Configuration(false));
    HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName("executor_client")
        .initTable(conf, dir.toAbsolutePath().toString());
    FileSystemRetryConfig retryConfig = FileSystemRetryConfig.newBuilder()
        .withFileSystemActionRetryEnabled(true)
        .withMaxRetryNumbers(7)
        .build();
    HoodieTableMetaClient driverClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConf(new Configuration(false)))
        .setBasePath(dir.toAbsolutePath().toString())
        .setFileSystemRetryConfig(retryConfig)
        .setConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .build();

    HoodieTableMetaClient executorClient =
        ExecutorMetaClientFactory.from(driverClient).newStorageOnlyClient();

    assertTrue(executorClient.getFileSystemRetryConfig().isFileSystemActionRetryEnable());
    assertEquals(7, executorClient.getFileSystemRetryConfig().getMaxRetryNumbers());
    assertTrue(executorClient.getConsistencyGuardConfig().isConsistencyCheckEnabled());
  }

  /**
   * It rides in a broadcast, so it has to serialize -- and the whole point is that it does not drag a
   * timeline when it does.
   *
   * <p>The table needs real instants for this assertion to discriminate: against an empty timeline a
   * meta client that loads one and a meta client that does not are the same size, and the test would
   * pass without testing anything.
   */
  @Test
  void serializesWithoutDraggingATimeline(@TempDir Path dir) throws Exception {
    HoodieTableMetaClient driverClient = tableAt(dir);
    driverClient.reloadActiveTimeline();

    ExecutorMetaClientFactory factory = ExecutorMetaClientFactory.from(driverClient);

    byte[] encoded = javaSerialize(factory);
    ExecutorMetaClientFactory read;
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(encoded))) {
      read = (ExecutorMetaClientFactory) in.readObject();
    }

    assertEquals(driverClient.getBasePath().toString(), read.getBasePath());
    assertEquals(driverClient.getBasePath().toString(),
        read.newStorageOnlyClient().getBasePath().toString());
    assertTrue(encoded.length * 4 < javaSerialize(driverClient).length,
        "the factory must be far smaller than the meta client it came from, which carries a timeline");
  }

  /**
   * The invariant every adjudication path rests on, asserted against the client the factory builds
   * rather than against anything shipped: an executor must not build an active timeline.
   *
   * <p>Asserted by size rather than by reading a field, because there is no API that reports whether
   * a meta client has loaded its timeline, and tests here do not use reflection. A client carrying
   * {@link #INSTANT_COUNT} instants is an order of magnitude larger than one that is not.
   */
  @Test
  void theClientItBuildsHasNoTimelineAttached(@TempDir Path dir) throws Exception {
    HoodieTableMetaClient driverClient = tableAt(dir);
    driverClient.reloadActiveTimeline();

    HoodieTableMetaClient executorClient =
        ExecutorMetaClientFactory.from(driverClient).newStorageOnlyClient();

    assertTrue(javaSerialize(executorClient).length * 4 < javaSerialize(driverClient).length,
        "the executor's client must not have an active timeline attached");
  }

  private static byte[] javaSerialize(Object payload) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(payload);
    }
    return bytes.toByteArray();
  }

  /** A table carrying enough instants that a loaded timeline is unmistakable in a byte count. */
  private static HoodieTableMetaClient tableAt(Path dir) throws IOException {
    StorageConfiguration<?> conf = HadoopFSUtils.getStorageConf(new Configuration(false));
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName("executor_client")
        .initTable(conf, dir.toAbsolutePath().toString());
    TimelineLayout layout = metaClient.getTimelineLayout();
    String timelinePath = metaClient.getTimelinePath().toString();
    for (int i = 0; i < INSTANT_COUNT; i++) {
      HoodieInstant instant = layout.getInstantGenerator().createNewInstant(
          HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION,
          String.format("2025%013d", i), String.format("2025%013d", i + 1));
      Files.write(Paths.get(timelinePath, layout.getInstantFileNameGenerator().getFileName(instant)),
          "{\"partitionToWriteStats\":{}}".getBytes());
    }
    return metaClient;
  }
}
