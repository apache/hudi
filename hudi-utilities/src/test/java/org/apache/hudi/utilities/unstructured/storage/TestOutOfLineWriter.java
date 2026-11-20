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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.table.marker.TimelineServerBasedWriteMarkers;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredSourceConfig;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredSourceContract;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.UnstructuredTableTestProperties;

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
import java.util.Properties;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Out-of-line writer naming, task write tokens, bounded streams, partition validation, marker settings and the driver-built,
 * serializable factory.
 */
class TestOutOfLineWriter {
  private static final String INSTANT = "20261027120000000";

  @TempDir
  Path temp;

  @Test
  void singleCreateWritesFreshFiles() throws Exception {
    String base = temp.toUri().toString();
    HoodieStorage storage = spy(new HoodieHadoopStorage(new StoragePath(base), HadoopFSUtils.getStorageConf(new Configuration())));
    OutOfLineWriter writer = new OutOfLineWriter(storage, base, INSTANT, Option.empty(), new LocalTaskContextSupplier(2, 5, 9L));
    byte[] bytes = {1, 2, 3, 4};
    String first = writer.write("20261018120000000", out -> out.write(bytes));
    String second = writer.write("20261018120000000", out -> out.write(bytes));
    assertNotEquals(first, second);
    for (String relative : new String[] {first, second}) {
      assertTrue(relative.startsWith(".hoodie/.blob/20261018120000000/"), relative);
      String name = relative.substring(relative.lastIndexOf('/') + 1);
      assertTrue(name.matches("[0-9a-f-]{36}-0_2-5-9_" + INSTANT + "\\.bin"), name);
      assertEquals(INSTANT, FSUtils.getCommitTime(name));
      assertArrayEquals(bytes, Files.readAllBytes(temp.resolve(relative)));
      verify(storage).create(new StoragePath(base, relative), false);
    }
    verify(storage, never()).exists(any(StoragePath.class));
    verify(storage, never()).rename(any(), any());
  }

  @Test
  void managedWriterRefusesAnAbsoluteUriBeforeCreatingAFile() {
    String base = temp.resolve("table").toUri().toString();
    String absolute = temp.resolve("user-owned.bin").toUri().toString();
    HoodieStorage storage = new HoodieHadoopStorage(new StoragePath(base), HadoopFSUtils.getStorageConf(new Configuration()));
    OutOfLineWriter writer = new OutOfLineWriter(storage, base, INSTANT, Option.empty());
    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
        () -> writer.writeAt(absolute, out -> out.write(1)));
    assertTrue(failure.getMessage().contains("managed=false"), failure.getMessage());
    assertTrue(failure.getMessage().contains(absolute), failure.getMessage());
    assertFalse(Files.exists(temp.resolve("user-owned.bin")));
    assertFalse(Files.exists(temp.resolve("table/.hoodie/.blob")));
  }

  @Test
  void failedStreamingRemovesPartial() throws Exception {
    String base = temp.toUri().toString();
    HoodieStorage storage = new HoodieHadoopStorage(new StoragePath(base), HadoopFSUtils.getStorageConf(new Configuration()));
    assertThrows(IOException.class, () -> new OutOfLineWriter(storage, base, INSTANT, Option.empty()).write("p", out -> {
      out.write(1);
      throw new IOException("source reset");
    }));
    try (java.util.stream.Stream<Path> paths = Files.list(temp.resolve(".hoodie/.blob/p"))) {
      assertFalse(paths.anyMatch(path -> path.getFileName().toString().endsWith(".bin")));
    }
  }

  @Test
  void writeTokenComesFromTheWritingTaskAttempt() throws Exception {
    String base = temp.toUri().toString();
    HoodieStorage storage = new HoodieHadoopStorage(new StoragePath(base), HadoopFSUtils.getStorageConf(new Configuration()));
    String first = new OutOfLineWriter(storage, base, INSTANT, Option.empty(), new LocalTaskContextSupplier(3, 7, 41L))
        .write("p", out -> out.write(1));
    String retried = new OutOfLineWriter(storage, base, INSTANT, Option.empty(), new LocalTaskContextSupplier(3, 7, 42L))
        .write("p", out -> out.write(1));
    String firstName = first.substring(first.lastIndexOf('/') + 1);
    String retriedName = retried.substring(retried.lastIndexOf('/') + 1);
    assertEquals("3-7-41", FSUtils.getWriteTokenFromBaseFile(firstName));
    assertEquals("3-7-42", FSUtils.getWriteTokenFromBaseFile(retriedName));
    assertNotEquals(FSUtils.getFileId(firstName), FSUtils.getFileId(retriedName));
    assertEquals(INSTANT, FSUtils.getCommitTime(retriedName));
  }

  @Test
  void serializedFactoryNeedsNoTableInTask() throws Exception {
    String base = initTable();
    StorageConfiguration<?> conf = HadoopFSUtils.getStorageConf(new Configuration());
    Properties props = new Properties();
    props.setProperty(HoodieWriteConfig.BLOB_MARKERS_ENABLE.key(), "false");
    OutOfLineWriter.Factory factory = javaRoundTrip(OutOfLineWriter.forTable(conf, base, props));
    Files.delete(temp.resolve("table/.hoodie/hoodie.properties"));
    HoodieStorage storage = new HoodieHadoopStorage(new StoragePath(base), conf);
    String relative = factory.forInstant(storage, INSTANT).write("p", out -> out.write(7));
    assertArrayEquals(new byte[] {7}, Files.readAllBytes(temp.resolve("table").resolve(relative)));
    assertFalse(Files.exists(temp.resolve("table/.hoodie/.temp/" + INSTANT)));
  }

  @Test
  void invalidPartitionsAreRejectedBeforeCreate() {
    for (String partition : new String[] {"../p", "/p", "p//q", "p/./q", "p/"}) {
      assertThrows(IllegalArgumentException.class, () -> OutOfLineWriter.validatePartition(partition));
    }
  }

  /** {@code props} merged into the job of the embedding stream, the one writer of out-of-line files (its default preprocessors store video frames). */
  private static TypedProperties markers(TypedProperties props) {
    TypedProperties job = TypedProperties.copy(props);
    job.setProperty(UnstructuredSourceConfig.MODE.key(), "embedding");
    job.setProperty(EmbeddingTableConfig.MODE.key(), EmbeddingTableConfig.MODE_PREP_ONLY);
    job.setProperty(UnstructuredSourceConfig.CLOUD.key(), "s3");
    return new UnstructuredSourceContract(job).prepareJob(job);
  }

  /**
   * Unset marker settings are set to timeline-server markers and marker rollback, the same values are accepted, a conflicting one
   * is refused by name; with out-of-line markers off, the job's own values are kept.
   */
  @Test
  void markerSettingsAreSetAndConflictsRejectedUnlessDisabled() {
    TypedProperties merged = markers(new TypedProperties());
    assertEquals("TIMELINE_SERVER_BASED", merged.getString(HoodieWriteConfig.MARKERS_TYPE.key()));
    assertEquals("true", merged.getString(HoodieWriteConfig.ROLLBACK_USING_MARKERS_ENABLE.key()));
    TypedProperties required = new TypedProperties();
    required.setProperty(HoodieWriteConfig.MARKERS_TYPE.key(), "timeline_server_based");
    required.setProperty(HoodieWriteConfig.ROLLBACK_USING_MARKERS_ENABLE.key(), "TRUE");
    markers(required);
    for (String key : new String[] {HoodieWriteConfig.MARKERS_TYPE.key(), HoodieWriteConfig.ROLLBACK_USING_MARKERS_ENABLE.key()}) {
      TypedProperties conflicting = new TypedProperties();
      String value = key.equals(HoodieWriteConfig.MARKERS_TYPE.key()) ? "DIRECT" : "false";
      conflicting.setProperty(key, value);
      IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, () -> markers(conflicting));
      assertTrue(refused.getMessage().startsWith("UnstructuredDataSource (mode embedding) requires " + key + "="), refused.getMessage());
      conflicting.setProperty(HoodieWriteConfig.BLOB_MARKERS_ENABLE.key(), "false");
      assertEquals(value, markers(conflicting).getString(key), "disabled markers keep the job's writer settings");
    }
  }

  /**
   * With the embedded timeline server off, Spark's default marker type is DIRECT: an unset marker type is set to
   * TIMELINE_SERVER_BASED (a remote timeline server serves it), and disabled out-of-line markers set neither.
   */
  @Test
  void markerTypeIsSetWithTheEmbeddedTimelineServerOff() {
    TypedProperties unset = new TypedProperties();
    unset.setProperty(HoodieWriteConfig.EMBEDDED_TIMELINE_SERVER_ENABLE.key(), "false");
    assertEquals("TIMELINE_SERVER_BASED", markers(unset).getString(HoodieWriteConfig.MARKERS_TYPE.key()));

    TypedProperties disabled = TypedProperties.copy(unset);
    disabled.setProperty(HoodieWriteConfig.BLOB_MARKERS_ENABLE.key(), "false");
    assertFalse(markers(disabled).containsKey(HoodieWriteConfig.MARKERS_TYPE.key()));
  }

  @Test
  void existingVersionSixTargetIsRefusedWithAutoUpgradeOff() throws Exception {
    String base = temp.resolve("table").toUri().toString();
    StorageConfiguration<?> conf = HadoopFSUtils.getStorageConf(new Configuration());
    HoodieTableMetaClient.newTableBuilder().setAdditionalProperties(layoutProperties()).setTableName("blob_table").setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableVersion(6).initTable(conf, base);
    Properties props = new Properties();
    props.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "9");
    props.setProperty(HoodieWriteConfig.AUTO_UPGRADE_VERSION.key(), "false");
    HoodieValidationException failure = assertThrows(HoodieValidationException.class,
        () -> OutOfLineWriter.forTable(conf, base, props));
    assertTrue(failure.getMessage().contains("9"), failure.getMessage());
    assertTrue(failure.getMessage().contains("6"), failure.getMessage());
    assertFalse(Files.exists(temp.resolve("table/.hoodie/.blob")), "refusal precedes any copy");
  }

  @Test
  void directMarkerFallbackIsRefusedOnTheDriver() throws Exception {
    String base = initTable();
    StorageConfiguration<?> conf = HadoopFSUtils.getStorageConf(new Configuration());
    Properties props = new Properties();
    props.setProperty(HoodieWriteConfig.EMBEDDED_TIMELINE_SERVER_ENABLE.key(), "false");
    HoodieValidationException failure = assertThrows(HoodieValidationException.class,
        () -> OutOfLineWriter.forTable(conf, base, props));
    assertTrue(failure.getMessage().contains(HoodieWriteConfig.BLOB_MARKERS_ENABLE.key() + "=false"), failure.getMessage());
    assertFalse(Files.exists(temp.resolve("table/.hoodie/.blob")), "refusal precedes any copy");

    props.setProperty(HoodieWriteConfig.BLOB_MARKERS_ENABLE.key(), "false");
    OutOfLineWriter.forTable(conf, base, props);
  }

  @Test
  void timelineServerMarkersAreAcceptedOnTheDriver() throws Exception {
    String base = initTable();
    StorageConfiguration<?> conf = HadoopFSUtils.getStorageConf(new Configuration());
    OutOfLineWriter.Factory factory = OutOfLineWriter.forTable(conf, base, new Properties());
    assertTrue(factory.markers(INSTANT).get() instanceof TimelineServerBasedWriteMarkers);
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
    HoodieTableMetaClient.newTableBuilder().setAdditionalProperties(UnstructuredTableTestProperties.layoutOne())
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

  private static Properties layoutProperties() {
    Properties props = new Properties();
    props.setProperty("hoodie.table.blob.layout.version", "1");
    return props;
  }

}
