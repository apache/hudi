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
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.utilities.exception.HoodiePreCommitRefusalException;
import org.apache.hudi.utilities.exception.HoodieTransformException;
import org.apache.hudi.utilities.transform.InstantAwareTransformer;
import org.apache.hudi.utilities.transform.Transformer;

import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The transformer's commit hooks in a streamer round: {@link Transformer#onPreCommit} runs inside the commit of any round, before
 * its instant completes; a {@link HoodiePreCommitRefusalException} from it fails the round and rolls its instant back; and
 * {@link Transformer#onCommit} runs after the durable commit only in a round whose transformer needs the instant
 * ({@link InstantAwareTransformer}). A stock transformer chain commits as before.
 */
public class TestStreamSyncTransformerCommitHooks extends HoodieDeltaStreamerTestBase {

  /** Property naming the target base path, so a test transformer can look at the table. */
  static final String TARGET_BASE_PATH = "test.commit.hooks.target.base.path";

  /** Every hook call of a recording transformer, in order: {@code apply[@instant]}, {@code preCommit@instant}, {@code commit@instant}. */
  private static final List<String> EVENTS = new CopyOnWriteArrayList<>();
  /** One entry per apply: the pending (requested or inflight) instants of the target table at the time of the call. */
  private static final List<List<String>> PENDING_AT_APPLY = new CopyOnWriteArrayList<>();
  /** One entry per pre-commit call: the state of its instant in the target table's timeline at the time of the call. */
  private static final List<String> STATE_AT_PRE_COMMIT = new CopyOnWriteArrayList<>();
  /** One entry per pre-commit call: the base path of the meta client it was given. */
  private static final List<String> META_CLIENT_AT_PRE_COMMIT = new CopyOnWriteArrayList<>();
  /** One entry per post-commit call: the completed commits of the target table at the time of the call. */
  private static final List<List<String>> COMPLETED_AT_COMMIT = new CopyOnWriteArrayList<>();

  @BeforeEach
  void clearCalls() {
    EVENTS.clear();
    PENDING_AT_APPLY.clear();
    STATE_AT_PRE_COMMIT.clear();
    META_CLIENT_AT_PRE_COMMIT.clear();
    COMPLETED_AT_COMMIT.clear();
  }

  /** A plain transformer that records its apply and both commit hooks. */
  public static class RecordingTransformer implements Transformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
      recordApply(properties, null);
      return rowDataset;
    }

    @Override
    public void onPreCommit(JavaSparkContext jsc, SparkSession sparkSession, TypedProperties properties, String instantTime,
                            HoodieTableMetaClient metaClient) {
      EVENTS.add("preCommit@" + instantTime);
      META_CLIENT_AT_PRE_COMMIT.add(metaClient == null ? "" : metaClient.getBasePath().toString());
      HoodieTimeline commits = metaClient(properties.getString(TARGET_BASE_PATH)).getActiveTimeline().getCommitsTimeline();
      STATE_AT_PRE_COMMIT.add(commits.getInstantsAsStream().filter(instant -> instant.requestedTime().equals(instantTime))
          .map(instant -> instant.getState().name()).findFirst().orElse(""));
    }

    @Override
    public void onCommit(JavaSparkContext jsc, SparkSession sparkSession, TypedProperties properties, String instantTime) {
      EVENTS.add("commit@" + instantTime);
      COMPLETED_AT_COMMIT.add(completedCommits(properties.getString(TARGET_BASE_PATH)));
    }

    static void recordApply(TypedProperties properties, String instantTime) {
      EVENTS.add(instantTime == null ? "apply" : "apply@" + instantTime);
      String basePath = properties.getString(TARGET_BASE_PATH);
      try {
        PENDING_AT_APPLY.add(fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME))
            ? metaClient(basePath).getActiveTimeline().filterInflightsAndRequested().getInstantsAsStream()
                .map(HoodieInstant::requestedTime).collect(Collectors.toList())
            : Collections.emptyList());
      } catch (IOException e) {
        throw new HoodieTransformException("Cannot read the timeline of " + basePath, e);
      }
    }
  }

  /** {@link RecordingTransformer} that needs the round's instant: its round starts the instant before it. */
  public static class RecordingInstantAwareTransformer extends RecordingTransformer implements InstantAwareTransformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties,
                              String instantTime) {
      recordApply(properties, instantTime);
      return rowDataset;
    }
  }

  /** A plain {@link RecordingTransformer} that refuses the commit. */
  public static class RefusingTransformer extends RecordingTransformer {
    @Override
    public void onPreCommit(JavaSparkContext jsc, SparkSession sparkSession, TypedProperties properties, String instantTime,
                            HoodieTableMetaClient metaClient) {
      super.onPreCommit(jsc, sparkSession, properties, instantTime, metaClient);
      throw new HoodiePreCommitRefusalException("injected refusal of " + instantTime);
    }
  }

  /** A {@link RecordingInstantAwareTransformer} that refuses the commit. */
  public static class RefusingInstantAwareTransformer extends RecordingInstantAwareTransformer {
    @Override
    public void onPreCommit(JavaSparkContext jsc, SparkSession sparkSession, TypedProperties properties, String instantTime,
                            HoodieTableMetaClient metaClient) {
      super.onPreCommit(jsc, sparkSession, properties, instantTime, metaClient);
      throw new HoodiePreCommitRefusalException("injected refusal of " + instantTime);
    }
  }

  private static HoodieDeltaStreamer.Config config(String tableBasePath, Class<?>... transformers) {
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT,
        Arrays.stream(transformers).map(Class::getName).collect(Collectors.toList()));
    cfg.configs.add(TARGET_BASE_PATH + "=" + tableBasePath);
    return cfg;
  }

  private static HoodieTableMetaClient metaClient(String tableBasePath) {
    return HoodieTestUtils.createMetaClient(storage, tableBasePath);
  }

  private static List<String> completedCommits(String tableBasePath) {
    return metaClient(tableBasePath).getActiveTimeline().getCommitsTimeline().filterCompletedInstants().getInstantsAsStream()
        .map(HoodieInstant::requestedTime).collect(Collectors.toList());
  }

  private static HoodieCommitMetadata lastCommitMetadata(String tableBasePath) throws IOException {
    HoodieTimeline timeline = metaClient(tableBasePath).getActiveTimeline();
    return timeline.readCommitMetadata(timeline.getCommitsTimeline().filterCompletedInstants().lastInstant().get());
  }

  /** The data files of the table: every file outside its meta folder but the partition metadata. */
  private static List<String> dataFiles(String tableBasePath) throws IOException {
    List<String> files = new ArrayList<>();
    RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path(tableBasePath), true);
    while (it.hasNext()) {
      Path path = it.next().getPath();
      if (!path.toString().contains("/" + HoodieTableMetaClient.METAFOLDER_NAME + "/") && !path.getName().startsWith(".hoodie_partition_metadata")) {
        files.add(path.toString());
      }
    }
    return files;
  }

  private static HoodiePreCommitRefusalException refusalIn(Throwable failure) {
    for (Throwable t = failure; t != null; t = t.getCause() == t ? null : t.getCause()) {
      if (t instanceof HoodiePreCommitRefusalException) {
        return (HoodiePreCommitRefusalException) t;
      }
    }
    return null;
  }

  /**
   * The transformer's pre-commit hook runs inside the commit of any round, while its instant is still INFLIGHT, with the table's
   * meta client. The post-commit hook runs after the durable commit only in an instant-aware round: a plain transformer is not
   * called back.
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void preCommitRunsInsideEveryCommitAndOnCommitOnlyInAnInstantAwareRound(boolean instantAware) throws Exception {
    String tableBasePath = basePath + "/commit_hooks_" + instantAware;
    syncOnce(config(tableBasePath, instantAware ? RecordingInstantAwareTransformer.class : RecordingTransformer.class,
        TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));

    List<String> commits = completedCommits(tableBasePath);
    assertEquals(1, commits.size(), "the round committed");
    String instant = commits.get(0);
    assertEquals(Collections.singletonList(HoodieInstant.State.INFLIGHT.name()), STATE_AT_PRE_COMMIT,
        "the pre-commit hook runs once, before the instant completes");
    assertEquals(1, META_CLIENT_AT_PRE_COMMIT.size());
    assertEquals(new Path(tableBasePath).toUri().getPath(), new Path(META_CLIENT_AT_PRE_COMMIT.get(0)).toUri().getPath(),
        "the pre-commit hook is given the meta client of the committed table");
    if (instantAware) {
      assertEquals(Arrays.asList("apply@" + instant, "preCommit@" + instant, "commit@" + instant), EVENTS);
      assertEquals(Collections.singletonList(commits), COMPLETED_AT_COMMIT, "the instant was completed at callback time");
    } else {
      assertEquals(Arrays.asList("apply", "preCommit@" + instant), EVENTS, "a plain transformer's round does not call onCommit");
      assertTrue(COMPLETED_AT_COMMIT.isEmpty());
    }
    assertEquals(SQL_SOURCE_NUM_RECORDS, sqlContext.read().format("hudi").load(tableBasePath).count());
  }

  /**
   * A {@link HoodiePreCommitRefusalException} from the pre-commit hook fails the round with that refusal, and the round's instant
   * is rolled back: no completed or pending commit is left, nor any data file the write wrote, and the transformer is not called
   * back.
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void preCommitRefusalFailsTheRoundAndRollsItsInstantBack(boolean instantAware) throws Exception {
    String tableBasePath = basePath + "/commit_hooks_refused_" + instantAware;
    HoodieDeltaStreamer.Config cfg = config(tableBasePath,
        instantAware ? RefusingInstantAwareTransformer.class : RefusingTransformer.class,
        TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class);
    Exception failure = assertThrows(Exception.class, () -> syncOnce(new HoodieDeltaStreamer(cfg, jsc)));

    HoodiePreCommitRefusalException refusal = refusalIn(failure);
    assertNotNull(refusal, "the round fails with the refusal");
    assertEquals(Collections.singletonList(HoodieInstant.State.INFLIGHT.name()), STATE_AT_PRE_COMMIT);
    String instant = EVENTS.stream().filter(event -> event.startsWith("preCommit@")).findFirst().get().substring("preCommit@".length());
    assertTrue(refusal.getMessage().contains(instant), refusal.getMessage());
    assertFalse(EVENTS.contains("commit@" + instant), "a refused round's transformer is not called back");

    HoodieTimeline timeline = metaClient(tableBasePath).getActiveTimeline();
    assertTrue(timeline.getCommitsTimeline().empty(), "no completed or pending commit is left: " + timeline.getInstants());
    assertEquals(1, timeline.getRollbackTimeline().filterCompletedInstants().countInstants(), "the refused instant was rolled back");
    assertEquals(Collections.singletonList(instant), timeline.readRollbackMetadata(
        timeline.getRollbackTimeline().filterCompletedInstants().lastInstant().get()).getCommitsRollback());
    assertEquals(Collections.emptyList(), dataFiles(tableBasePath), "the rollback removed the files of the refused write");
  }

  /**
   * A stock transformer chain (no {@link InstantAwareTransformer}) commits exactly as before: no instant exists while it
   * transforms, the default no-op pre-commit hook of a stock transformer leaves the commit as it was, the commit completes
   * without the pending-workload-profile marker, and nothing is called back after it.
   */
  @Test
  void stockTransformerChainCommitsAsBefore() throws Exception {
    String tableBasePath = basePath + "/commit_hooks_stock_chain";
    syncOnce(config(tableBasePath, RecordingTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));

    assertEquals(Collections.singletonList(Collections.emptyList()), PENDING_AT_APPLY, "no instant exists while the chain transforms");
    List<String> commits = completedCommits(tableBasePath);
    assertEquals(1, commits.size(), "the round committed");
    assertEquals(Arrays.asList("apply", "preCommit@" + commits.get(0)), EVENTS, "no onCommit for a stock chain");
    assertTrue(metaClient(tableBasePath).getActiveTimeline().filterInflightsAndRequested().empty(), "nothing is left pending");
    HoodieCommitMetadata metadata = lastCommitMetadata(tableBasePath);
    assertFalse(metadata.getExtraMetadata().containsKey(ConcurrentOperation.WORKLOAD_PROFILE_PENDING_KEY));
    assertEquals(WriteOperationType.INSERT, metadata.getOperationType());
    assertEquals(SQL_SOURCE_NUM_RECORDS, metadata.fetchTotalRecordsWritten());
    assertEquals(SQL_SOURCE_NUM_RECORDS, sqlContext.read().format("hudi").load(tableBasePath).count());
  }
}
