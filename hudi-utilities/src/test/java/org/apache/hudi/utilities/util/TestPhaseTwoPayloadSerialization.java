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

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineLayout;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.SerializerInstance;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import scala.reflect.ClassTag$;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 2's driver-read state travels to executors by broadcast, and a broadcast is encoded with
 * {@code spark.serializer} -- Kryo, which {@code UtilHelpers.buildSparkConf} sets -- rather than with
 * the Java serializer every task closure uses.
 *
 * <p>None of these classes is registered with Kryo explicitly, and the Hudi registrar already has to
 * wrap {@code HadoopStorageConfiguration} in a {@code JavaSerializer}, so "it happens to work today"
 * is exactly the kind of fact that needs pinning.
 *
 * <p>Everything here runs with {@code HoodieSparkKryoRegistrar} configured, which is NOT what the
 * launchers without it run -- without the registrar the raw graph breaks, which is why production
 * broadcasts {@code JavaSerializedPayload} and why these green tests are not an argument for
 * unwrapping it. The registrar-less side is pinned by
 * {@code TestShippedTimelineSurvivesSerialization}'s Kryo tests and its raw-context canary.
 *
 * <p>The timeline assertion goes further than a round trip: it reads an instant's content through the
 * deserialized timeline. {@code BaseHoodieTimeline.instantReader} is transient and
 * {@code ActiveTimelineV2.getInstantReader()} returns {@code this}, so a timeline that survives
 * serialization structurally can still be unable to read anything.
 */
class TestPhaseTwoPayloadSerialization {

  private static final int INSTANT_COUNT = 8;

  @Test
  void wholeTimelineSurvivesKryoAndCanStillReadCommitContent(@TempDir Path dir) throws IOException {
    HoodieTableMetaClient metaClient = tableWith(dir, INSTANT_COUNT);
    HoodieTimeline timeline = metaClient.reloadActiveTimeline();

    HoodieTimeline read = (HoodieTimeline) roundTrip(timeline);

    HoodieTimeline completed = read.getCommitsTimeline().filterCompletedInstants();
    assertEquals(INSTANT_COUNT, completed.countInstants());
    HoodieInstant first = completed.firstInstant().get();
    try (InputStream in = completed.getInstantReader().getContentStream(first)) {
      assertTrue(in.read() >= 0, "the round-tripped timeline must still be able to read an instant");
    }
  }

  @Test
  void instantStateIndexSurvivesKryo(@TempDir Path dir) throws IOException {
    HoodieTableMetaClient metaClient = tableWith(dir, INSTANT_COUNT);
    InstantStateIndex index = InstantStateIndex.from(metaClient.reloadActiveTimeline());

    InstantStateIndex read = (InstantStateIndex) roundTrip(index);

    assertEquals(index.getOrderedInstants(), read.getOrderedInstants());
  }

  /**
   * Verifies that service state survives Kryo serialization by calling its public query methods on the
   * round-tripped object. This catches collection fields lost in serialization (null or throwing), but the
   * fixture is an idle table with no ongoing service activity, so non-empty freed-files maps are not tested.
   */
  @Test
  void serviceStateSurvivesKryo(@TempDir Path dir) throws IOException {
    HoodieTableMetaClient metaClient = tableWith(dir, INSTANT_COUNT);
    HoodieTimeline timeline = metaClient.reloadActiveTimeline();
    InstantStateIndex prevIndex = InstantStateIndex.from(timeline);

    ServiceActivityDelta delta = ServiceActivityDelta.between(prevIndex, timeline, metaClient);
    PendingServiceState pending = PendingServiceState.across(timeline, timeline, metaClient);

    ServiceActivityDelta readDelta = (ServiceActivityDelta) roundTrip(delta);
    assertTrue(readDelta.noServiceActivity());
    assertEquals(delta.rolledBackInstants(), readDelta.rolledBackInstants());
    assertEquals(Collections.emptySet(), readDelta.fileNamesFreedByClean("any_partition"));
    assertEquals(Collections.emptySet(), readDelta.fileNamesFreedByRollback("any_partition"));

    PendingServiceState readPending = (PendingServiceState) roundTrip(pending);
    assertEquals(pending.plannedRollbackInstants(), readPending.plannedRollbackInstants());
    assertEquals(Collections.emptySet(), readPending.cleanPlannedFiles("any_partition"));
    assertEquals(Collections.emptySet(), readPending.rollbackPlannedFiles("any_partition"));
  }

  /**
   * The duplicate dimension's context has no other round-trip coverage, and the payload wrap makes
   * its Java-serializability newly load-bearing: the payload constructor eagerly Java-serializes
   * on the driver, so a non-Java-serializable field added to that context would fail every
   * duplicate-dimension run at broadcast creation. Round-tripped WITHOUT the registrar, since the
   * payload's whole point is to not need one, and read through get() so the byte[] path (which
   * local mode never exercises) is what is proven.
   */
  @Test
  void duplicateDimensionContextSurvivesRegistrarLessKryoInsideThePayload(@TempDir Path dir) throws IOException {
    HoodieTableMetaClient metaClient = tableWith(dir, INSTANT_COUNT);
    HoodieTimeline tPrevD = metaClient.reloadActiveTimeline();
    DuplicateBaseFileDimension.AdjudicationContext context =
        new DuplicateBaseFileDimension.AdjudicationContext(
            ExecutorMetaClientFactory.from(metaClient),
            tPrevD.filterCompletedInstants().firstInstant().get().requestedTime(), tPrevD);

    SerializerInstance bareKryo = new KryoSerializer(new SparkConf(false)).newInstance();
    ByteBuffer encoded = bareKryo.serialize(
        new JavaSerializedPayload<>(context), ClassTag$.MODULE$.apply(Object.class));
    @SuppressWarnings("unchecked")
    JavaSerializedPayload<DuplicateBaseFileDimension.AdjudicationContext> shipped =
        (JavaSerializedPayload<DuplicateBaseFileDimension.AdjudicationContext>)
            bareKryo.deserialize(encoded, ClassTag$.MODULE$.apply(Object.class));

    DuplicateBaseFileDimension.AdjudicationContext read = shipped.get();
    assertTrue(read.getMetaClients().newStorageOnlyClient().getTableConfig() != null,
        "the payload must hand back a context whose factory still builds a storage client");
    assertEquals(INSTANT_COUNT, read.getTPrevD().filterCompletedInstants().countInstants());
  }

  /** A table with {@code instantCount} completed commits, each holding readable content. */
  private static HoodieTableMetaClient tableWith(Path dir, int instantCount) throws IOException {
    StorageConfiguration<?> conf = HadoopFSUtils.getStorageConf(new Configuration(false));
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName("phase_two_payloads")
        .initTable(conf, dir.toAbsolutePath().toString());
    TimelineLayout layout = metaClient.getTimelineLayout();
    String timelinePath = metaClient.getTimelinePath().toString();
    for (int i = 0; i < instantCount; i++) {
      HoodieInstant instant = layout.getInstantGenerator().createNewInstant(
          HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION,
          String.format("2025%013d", i), String.format("2025%013d", i + 1));
      Files.write(Paths.get(timelinePath, layout.getInstantFileNameGenerator().getFileName(instant)),
          "{\"partitionToWriteStats\":{}}".getBytes());
    }
    return metaClient;
  }

  /**
   * Encodes and decodes through {@code spark.serializer} as a broadcast variable does -- with
   * {@code HoodieSparkKryoRegistrar} configured, which a launcher may or may not set (some
   * launchers do not); see the class javadoc.
   */
  private static Object roundTrip(Object payload) {
    SparkConf conf = new SparkConf(false).setAppName("phase2-payloads").setMaster("local");
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    conf.set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar");
    SerializerInstance serializer = new KryoSerializer(conf).newInstance();
    ByteBuffer encoded = serializer.serialize(payload, ClassTag$.MODULE$.apply(Object.class));
    return serializer.deserialize(encoded, ClassTag$.MODULE$.apply(Object.class));
  }
}
