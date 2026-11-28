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

package org.apache.hudi.utilities.functional;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.utilities.util.FsMdtDiffDimension;
import org.apache.hudi.utilities.util.InstantStateIndex;
import org.apache.hudi.utilities.util.JavaSerializedPayload;
import org.apache.hudi.utilities.util.PendingServiceState;
import org.apache.hudi.utilities.util.ServiceActivityDelta;

import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.SerializerInstance;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import scala.reflect.ClassTag$;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The active timeline the driver ships to phase 2 must still be able to read commit content once an
 * executor has deserialized it.
 *
 * <p>This guards a failure that reproduces ONLY on a cluster. {@code
 * BaseHoodieTimeline.instantReader} is {@code transient}, so a timeline filtered on the DRIVER --
 * anything returned by {@code filterCompletedInstants()} -- arrives on the executor with a null
 * reader and throws on {@code readCommitMetadata}. Shipping the full active timeline and filtering
 * on the far side is what avoids that, and this test is the only thing that fails when someone
 * "simplifies" it back.
 *
 * <p>The first test below round-trips a bare {@link HoodieTimeline}, which pins the Hudi hazard but
 * would not fail if a future change moved {@code filterCompletedInstants()} to the driver side of
 * {@code new AdjudicationContext(...)} -- the timeline handed to that constructor would then already
 * be pre-filtered and would round-trip fine on its own, while the production path would still break.
 * The second test closes that gap by round-tripping a real
 * {@link FsMdtDiffDimension.AdjudicationContext} and reading commit content off its {@code tAft}
 * after deserialization, the same way {@code adjudicateBatch} does.
 */
@Tag("functional")
class TestShippedTimelineSurvivesSerialization extends ValidatorV2FunctionalTestBase {

  @Test
  void fullActiveTimelineStillReadsCommitContentAfterARoundTrip() throws Exception {
    writeTable(HoodieTableType.COPY_ON_WRITE.name());

    HoodieTimeline shipped = roundTrip(metaClient().reloadActiveTimeline());

    // Filtering happens HERE, on the far side of the round trip, exactly as adjudicateBatch does it.
    HoodieTimeline completed = shipped.filterCompletedInstants();
    assertFalse(completed.getInstants().isEmpty(), "the fixture must have written a commit");

    HoodieInstant instant = completed.getInstants().get(0);
    assertNotNull(completed.readCommitMetadata(instant).getPartitionToWriteStats(),
        "a deserialized active timeline must still be able to read commit content");
  }

  /**
   * The design's Testing item 2, taken literally: a serialization round trip of a real
   * {@code AdjudicationContext}, not just of the timeline inside it. Would fail if
   * {@code filterCompletedInstants()} were ever moved to the driver side of the constructor, since
   * the unfiltered timeline this test ships is what makes the earlier test's hazard reachable at all.
   */
  @Test
  void realAdjudicationContextStillReadsCommitContentAfterARoundTrip() throws Exception {
    writeTable(HoodieTableType.COPY_ON_WRITE.name());

    FsMdtDiffDimension.AdjudicationContext shipped = roundTripContext(realContext());

    HoodieTimeline completed = shipped.getTAft().filterCompletedInstants();
    assertFalse(completed.getInstants().isEmpty(), "the fixture must have written a commit");

    HoodieInstant instant = completed.getInstants().get(0);
    assertNotNull(completed.readCommitMetadata(instant).getPartitionToWriteStats(),
        "a deserialized AdjudicationContext's tAft must still be able to read commit content");
  }

  /**
   * The same round trips under BARE Kryo -- Kryo without {@code HoodieSparkKryoRegistrar}, which
   * is what some launchers run (their conf comes from {@code registerKryoClasses}, which sets
   * the serializer and no registrator). Bare Kryo never calls {@code writeObject}/{@code
   * readObject} and skips transient fields, so a context that survives the Java round trips above
   * still arrives broken on a real executor: {@code HadoopStorageConfiguration} deserializes with
   * a null inner conf, in the factory's storage conf AND inside the shipped timeline's meta
   * client, and NPEs on first use. Local mode hands the driver's object back without a round
   * trip, so only these tests (and a real cluster) reach it. With the registrar configured the
   * raw graph survives -- that is {@code TestPhaseTwoPayloadSerialization}'s setup; the two are
   * different serializer configurations, not contradicting claims.
   *
   * <p>Production therefore broadcasts {@code JavaSerializedPayload<AdjudicationContext>}, never
   * the raw context. These tests prove that shape round-trips; the canary below proves the raw
   * shape still does not, so the wrap is not dead weight. Neither pins the call sites themselves
   * -- that is held by the {@code Broadcast<JavaSerializedPayload<...>>} signatures, which a
   * partial revert cannot compile against.
   */
  @Test
  void shippedPayloadBuildsAStorageClientAfterAKryoRoundTrip() throws Exception {
    writeTable(HoodieTableType.COPY_ON_WRITE.name());

    FsMdtDiffDimension.AdjudicationContext shipped =
        kryoRoundTrip(new JavaSerializedPayload<>(realContext())).get();

    assertNotNull(shipped.getMetaClients().newStorageOnlyClient().getTableConfig(),
        "a Kryo-shipped context must still build a storage-only meta client, exactly as "
            + "adjudicateBatch does on the executor");
  }

  @Test
  void shippedPayloadStillReadsCommitContentAfterAKryoRoundTrip() throws Exception {
    writeTable(HoodieTableType.COPY_ON_WRITE.name());

    FsMdtDiffDimension.AdjudicationContext shipped =
        kryoRoundTrip(new JavaSerializedPayload<>(realContext())).get();

    HoodieTimeline completed = shipped.getTAft().filterCompletedInstants();
    assertFalse(completed.getInstants().isEmpty(), "the fixture must have written a commit");
    HoodieInstant instant = completed.getInstants().get(0);
    assertNotNull(completed.readCommitMetadata(instant).getPartitionToWriteStats(),
        "a Kryo-shipped AdjudicationContext's tAft must still be able to read commit content");
  }

  /**
   * The canary for the wrap: the RAW context does not survive registrar-less Kryo. The day this
   * fails, the underlying graph has become Kryo-safe on its own and {@code JavaSerializedPayload}
   * may be removable; until then, green sibling tests that round-trip the raw graph WITH the
   * registrar are not an argument for unwrapping the broadcast.
   */
  @Test
  void rawContextIsStillBrokenUnderRegistrarLessKryo() throws Exception {
    writeTable(HoodieTableType.COPY_ON_WRITE.name());

    FsMdtDiffDimension.AdjudicationContext raw = kryoRoundTrip(realContext());

    assertThrows(NullPointerException.class, () -> raw.getMetaClients().newStorageOnlyClient(),
        "registrar-less Kryo is expected to null HadoopStorageConfiguration's conf; if it no "
            + "longer does, revisit whether the JavaSerializedPayload wrap is still needed");
  }

  private FsMdtDiffDimension.AdjudicationContext realContext() {
    HoodieTimeline tAft = metaClient().reloadActiveTimeline();
    InstantStateIndex index = InstantStateIndex.from(tAft);
    return FsMdtDiffDimension.AdjudicationContext.forTesting(
        index, index, ServiceActivityDelta.between(index, tAft, metaClient()), PendingServiceState.empty(),
        metaClient(), metaClient().getBasePath().toString(), tAft, false);
  }

  /** Bare {@code spark.serializer} Kryo, deliberately WITHOUT {@code HoodieSparkKryoRegistrar}. */
  @SuppressWarnings("unchecked")
  private static <T> T kryoRoundTrip(T value) {
    SerializerInstance kryo = new KryoSerializer(new SparkConf(false)).newInstance();
    return (T) kryo.deserialize(
        kryo.serialize(value, ClassTag$.MODULE$.apply(Object.class)),
        ClassTag$.MODULE$.apply(Object.class));
  }

  private static HoodieTimeline roundTrip(HoodieTimeline timeline) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(timeline);
    }
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (HoodieTimeline) in.readObject();
    }
  }

  private static FsMdtDiffDimension.AdjudicationContext roundTripContext(
      FsMdtDiffDimension.AdjudicationContext context) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(context);
    }
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (FsMdtDiffDimension.AdjudicationContext) in.readObject();
    }
  }
}
