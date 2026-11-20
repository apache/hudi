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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.client.transaction.ConcurrentOperation;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.exception.HoodiePreCommitRefusalException;
import org.apache.hudi.utilities.exception.HoodieTransformException;
import org.apache.hudi.utilities.streamer.HoodieStreamer;
import org.apache.hudi.utilities.transform.InstantAwareTransformer;
import org.apache.hudi.utilities.transform.Transformer;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * How StreamSync orders the commit and the transformer, selected by the transformer chain alone: a chain with no
 * {@link InstantAwareTransformer} runs as stock (every transformer through the plain apply, before any instant exists); a chain
 * with one makes StreamSync start the commit (REQUESTED) once the source returned data, move it to INFLIGHT right before the first
 * {@link InstantAwareTransformer}, and pass that instant, which is the instant the round then commits. There is no option.
 */
@Tag("functional")
public class TestStreamSyncCommitOrderFlag extends HoodieDeltaStreamerTestBase {

  /** Property naming the target base path, so the transformer can read the table's timeline. */
  private static final String TARGET_BASE_PATH = "test.commit.order.flag.target.base.path";
  /** One entry per call: the instant passed, or an empty string for a call through the plain apply. */
  private static final List<String> SEEN_INSTANTS = new CopyOnWriteArrayList<>();
  /** One entry per call: the requested or inflight instants of the target table at the time of the call. */
  private static final List<List<HoodieInstant>> PENDING_AT_APPLY = new CopyOnWriteArrayList<>();
  /** One entry per data commit: whether the commit's write config admits a second write to the same instant. */
  private static final List<Boolean> MULTI_WRITE_AT_COMMIT = new CopyOnWriteArrayList<>();
  /** One entry per post-commit callback: the instant it was told about. */
  private static final List<String> POST_COMMIT_INSTANTS = new CopyOnWriteArrayList<>();
  /** One entry per pending instant seen at apply: whether its inflight file carries the pending-workload-profile marker. */
  private static final List<Boolean> PENDING_PROFILE_MARKED = new CopyOnWriteArrayList<>();

  @BeforeEach
  void clearCalls() {
    SEEN_INSTANTS.clear();
    PENDING_AT_APPLY.clear();
    MULTI_WRITE_AT_COMMIT.clear();
    POST_COMMIT_INSTANTS.clear();
    PENDING_PROFILE_MARKED.clear();
  }

  /**
   * A plain transformer that records each call and the pending instants at that moment, at pre-commit the commit's
   * same-instant setting, and each post-commit callback; returns its input unchanged.
   */
  public static class PlainRecordingTransformer implements Transformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
      record(properties, "");
      return rowDataset;
    }

    @Override
    public void onPreCommit(JavaSparkContext jsc, SparkSession spark, TypedProperties properties, String instantTime,
                            HoodieTableMetaClient metaClient) {
      // the properties are the commit's write config, so this records the same-instant setting the round wrote with
      MULTI_WRITE_AT_COMMIT.add(properties.getBoolean(HoodieWriteConfig.ALLOW_MULTI_WRITE_ON_SAME_INSTANT_ENABLE.key(), false));
    }

    @Override
    public void onCommit(JavaSparkContext jsc, SparkSession spark, TypedProperties properties, String instantTime) {
      POST_COMMIT_INSTANTS.add(instantTime);
    }
  }

  /** An {@link InstantAwareTransformer} that records each call and the pending instants at that moment; returns its input. */
  public static class RecordingTransformer extends PlainRecordingTransformer implements InstantAwareTransformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties,
                              String instantTime) {
      record(properties, instantTime);
      return rowDataset;
    }
  }

  /** A plain transformer whose pre-commit refuses every round. */
  public static class PlainRefusingTransformer extends PlainRecordingTransformer {
    @Override
    public void onPreCommit(JavaSparkContext jsc, SparkSession spark, TypedProperties properties, String instantTime,
                            HoodieTableMetaClient metaClient) {
      throw new HoodiePreCommitRefusalException("refused " + instantTime);
    }
  }

  /** An {@link InstantAwareTransformer} whose pre-commit refuses every round. */
  public static class RefusingTransformer extends RecordingTransformer {
    @Override
    public void onPreCommit(JavaSparkContext jsc, SparkSession spark, TypedProperties properties, String instantTime,
                            HoodieTableMetaClient metaClient) {
      throw new HoodiePreCommitRefusalException("refused " + instantTime);
    }
  }

  private static void record(TypedProperties properties, String instantTime) {
    SEEN_INSTANTS.add(instantTime);
    String basePath = properties.getString(TARGET_BASE_PATH);
    try {
      if (!fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME))) {
        PENDING_AT_APPLY.add(Collections.emptyList());
        return;
      }
      HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(storage, basePath);
      List<HoodieInstant> pending = metaClient.getActiveTimeline().filterInflightsAndRequested().getInstants();
      PENDING_AT_APPLY.add(pending);
      for (HoodieInstant instant : pending) {
        PENDING_PROFILE_MARKED.add(isWorkloadProfilePending(metaClient, instant));
      }
    } catch (IOException e) {
      throw new HoodieTransformException("Cannot read the timeline of " + basePath, e);
    }
  }

  /** Whether {@code instant} is INFLIGHT and its inflight commit metadata carries the pending-workload-profile marker. */
  private static boolean isWorkloadProfilePending(HoodieTableMetaClient metaClient, HoodieInstant instant) throws IOException {
    if (instant.getState() != HoodieInstant.State.INFLIGHT) {
      return false;
    }
    return Boolean.parseBoolean(metaClient.getActiveTimeline().readCommitMetadata(instant).getExtraMetadata()
        .get(ConcurrentOperation.WORKLOAD_PROFILE_PENDING_KEY));
  }

  private static HoodieDeltaStreamer.Config config(String tableBasePath, Class<?> recordingTransformer) {
    return config(tableBasePath, recordingTransformer, WriteOperationType.INSERT);
  }

  private static HoodieDeltaStreamer.Config config(String tableBasePath, Class<?> recordingTransformer, WriteOperationType operation) {
    List<String> transformers = Arrays.asList(recordingTransformer.getName(),
        TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class.getName());
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, operation, transformers);
    cfg.configs.add(TARGET_BASE_PATH + "=" + tableBasePath);
    return cfg;
  }

  private static List<String> completedCommits(String tableBasePath) {
    return HoodieTestUtils.createMetaClient(storage, tableBasePath).getActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants().getInstantsAsStream().map(HoodieInstant::requestedTime).collect(Collectors.toList());
  }

  /** A job without transformers builds its streamer: asking whether the round starts its instant early needs no transformer. */
  @Test
  void productionConstructorAcceptsAJobWithoutTransformers() throws Exception {
    String tableBasePath = basePath + "/commit_order_no_transformers";
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT, Collections.emptyList());
    HoodieStreamer streamer = new HoodieDeltaStreamer(cfg, jsc);
    try {
      assertEquals(Collections.emptyList(), SEEN_INSTANTS);
      assertEquals(Collections.emptyList(), MULTI_WRITE_AT_COMMIT);
    } finally {
      streamer.shutdownGracefully();
    }
  }

  @Test
  void chainWithoutInstantAwareTransformerCallsThePlainApplyBeforeAnyInstant() throws Exception {
    String tableBasePath = basePath + "/commit_order_plain_chain";
    syncOnce(config(tableBasePath, PlainRecordingTransformer.class));

    assertEquals(Collections.singletonList(""), SEEN_INSTANTS, "called through the plain apply, without an instant");
    assertEquals(Collections.singletonList(Collections.emptyList()), PENDING_AT_APPLY, "no instant was pending at apply");
    assertEquals(Collections.singletonList(false), MULTI_WRITE_AT_COMMIT, "a stock round keeps the same-instant rejection");
    assertEquals(Collections.emptyList(), POST_COMMIT_INSTANTS, "a stock round's transformer is not called back after its commit");
    assertEquals(1, completedCommits(tableBasePath).size());
    assertEquals(SQL_SOURCE_NUM_RECORDS, sqlContext.read().format("hudi").load(tableBasePath).count());
  }

  @Test
  void chainWithInstantAwareTransformerPassesTheInflightInstantThatTheRoundCommits() throws Exception {
    String tableBasePath = basePath + "/commit_order_instant_aware_chain";
    syncOnce(config(tableBasePath, RecordingTransformer.class));

    assertEquals(1, SEEN_INSTANTS.size(), SEEN_INSTANTS.toString());
    String instant = SEEN_INSTANTS.get(0);
    assertNotEquals("", instant, "called with an instant, with no option set");
    List<HoodieInstant> pending = PENDING_AT_APPLY.get(0);
    assertEquals(1, pending.size(), pending.toString());
    assertEquals(instant, pending.get(0).requestedTime(), "the pending instant is the one passed to the transformer");
    assertEquals(HoodieInstant.State.INFLIGHT, pending.get(0).getState(), "the instant is INFLIGHT at apply");
    assertEquals(Collections.singletonList(true), PENDING_PROFILE_MARKED,
        "the round's inflight is marked as pending its workload profile, so a blocking clustering waits for it");
    assertEquals(Collections.singletonList(instant), completedCommits(tableBasePath), "the round committed that instant");
    assertEquals(Collections.singletonList(true), MULTI_WRITE_AT_COMMIT, "only the instant-aware handoff admits the same instant");
    assertEquals(Collections.singletonList(instant), POST_COMMIT_INSTANTS, "the instant-aware round is told about its durable commit");
    assertEquals(SQL_SOURCE_NUM_RECORDS, sqlContext.read().format("hudi").load(tableBasePath).count());
  }

  /**
   * A refusal from the transformer's pre-commit, raised inside the commit transaction, rolls the refused instant back before it
   * propagates, as the round's other pre-commit refusals do: nothing of the round stays pending.
   */
  @ParameterizedTest
  @ValueSource(classes = {PlainRefusingTransformer.class, RefusingTransformer.class})
  void preCommitRefusalRollsTheRefusedInstantBack(Class<?> refusingTransformer) throws Exception {
    String tableBasePath = basePath + "/commit_order_refused_" + refusingTransformer.getSimpleName();
    HoodieDeltaStreamer streamer = new HoodieDeltaStreamer(config(tableBasePath, refusingTransformer), jsc);
    Exception refused = assertThrows(Exception.class, () -> syncOnce(streamer));
    assertTrue(causes(refused).stream().anyMatch(HoodiePreCommitRefusalException.class::isInstance), causes(refused).toString());

    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(storage, tableBasePath);
    assertEquals(Collections.emptyList(), completedCommits(tableBasePath), "the refused round committed nothing");
    assertEquals(Collections.emptyList(), metaClient.getActiveTimeline().filterInflightsAndRequested().getInstants(),
        "the refused instant is not left pending");
    assertEquals(1, metaClient.getActiveTimeline().getRollbackTimeline().filterCompletedInstants().countInstants(),
        "the refused instant was rolled back");
  }

  private static List<Throwable> causes(Throwable failure) {
    List<Throwable> causes = new ArrayList<>();
    for (Throwable cause = failure; cause != null && !causes.contains(cause); cause = cause.getCause()) {
      causes.add(cause);
    }
    return causes;
  }

  /**
   * A bulk insert writes no workload profile over the inflight the round starts, so an instant-aware bulk insert round leaves its
   * inflight unmarked (a marker would stay for the whole write); it writes only new file groups, so clustering need not wait.
   */
  @Test
  void instantAwareBulkInsertRoundLeavesItsInflightUnmarked() throws Exception {
    String tableBasePath = basePath + "/commit_order_instant_aware_bulk_insert";
    syncOnce(config(tableBasePath, RecordingTransformer.class, WriteOperationType.BULK_INSERT));

    assertEquals(1, SEEN_INSTANTS.size(), SEEN_INSTANTS.toString());
    assertEquals(HoodieInstant.State.INFLIGHT, PENDING_AT_APPLY.get(0).get(0).getState(), "the instant is INFLIGHT at apply");
    assertEquals(Collections.singletonList(false), PENDING_PROFILE_MARKED, "a bulk insert round's inflight is not marked");
    assertEquals(Collections.singletonList(SEEN_INSTANTS.get(0)), completedCommits(tableBasePath), "the round committed that instant");
    assertEquals(SQL_SOURCE_NUM_RECORDS, sqlContext.read().format("hudi").load(tableBasePath).count());
  }
}
