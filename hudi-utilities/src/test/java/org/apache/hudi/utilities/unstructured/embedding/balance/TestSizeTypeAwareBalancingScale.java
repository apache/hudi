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

package org.apache.hudi.utilities.unstructured.embedding.balance;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobColumns;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link SizeTypeAwareBalancingStrategy} over a batch of {@value #FILES} metadata rows in four groups (pdf, image, video and
 * unmapped), generated in Spark: every group gets the task count its stats imply, the groups' partition blocks are disjoint, no
 * partition holds more than its group's budget except by its largest file, and no task reads more than a fraction of the largest
 * group's rows, so ranking a group is spread over the sort's tasks rather than done in one. Shuffle partition coalescing is off,
 * so the sort keeps the {@value #SHUFFLE_PARTITIONS} partitions it would size from a larger batch.
 */
@Tag("functional")
class TestSizeTypeAwareBalancingScale extends SparkClientFunctionalTestHarness {

  private static final long FILES = 100_000L;
  private static final int SHUFFLE_PARTITIONS = 8;
  private static final int P = 8;
  private static final long KIB = 1024L;
  private static final long MIB = 1024L * KIB;
  private static final String GROUP = "group";

  /** The batch: 60% pdf (300 MiB solo files among them), 25% png, 10% mp4, 5% docx (unmapped), sizes spread by id. */
  private Dataset<Row> batch() {
    return spark().range(0, FILES, 1, SHUFFLE_PARTITIONS).selectExpr(
        "sha2(cast(id as string), 256) as " + BlobColumns.SOURCE_PATH_SHA,
        "1000L as " + BlobColumns.SOURCE_MODIFICATION_TIME,
        "case when id % 20 < 12 then 'pdf' when id % 20 < 17 then 'png' when id % 20 < 19 then 'mp4' else 'docx' end as "
            + BlobColumns.SOURCE_FILE_EXTENSION,
        "case when id % 20 < 12 then (case when id % 997 = 0 then 300L * " + MIB + " else ((id * 7919) % 4096) * " + KIB + " end)"
            + " when id % 20 < 17 then ((id * 31) % 8192) * " + KIB
            + " when id % 20 < 19 then ((id * 13) % 200 + 1) * " + MIB
            + " else " + KIB + " end as " + BlobColumns.SOURCE_FILE_SIZE);
  }

  private static TypedProperties props() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "png" + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
        "org.apache.hudi.utilities.unstructured.embedding.prep.ImageInputPreprocessor");
    props.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "mp4" + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
        "org.apache.hudi.utilities.unstructured.embedding.prep.VideoFramePreprocessor");
    props.setProperty(EmbeddingTableConfig.BALANCE_MIN_PARTITIONS.key(), String.valueOf(P));
    return props;
  }

  /** The most shuffle records any one task read while it was registered. */
  private static final class LargestTaskRead extends SparkListener {
    private final AtomicLong records = new AtomicLong();

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
      if (taskEnd.taskMetrics() != null) {
        records.accumulateAndGet(taskEnd.taskMetrics().shuffleReadMetrics().recordsRead(), Math::max);
      }
    }
  }

  @Test
  void groupsGetTheirTasksWithinTheirBudgetsAndNoTaskRanksAWholeGroup() throws Exception {
    Map<String, String> previous = new HashMap<>();
    Map<String, String> conf = new HashMap<>();
    conf.put("spark.sql.shuffle.partitions", String.valueOf(SHUFFLE_PARTITIONS));
    conf.put("spark.sql.adaptive.coalescePartitions.enabled", "false");
    conf.forEach((key, value) -> {
      previous.put(key, spark().conf().get(key, null));
      spark().conf().set(key, value);
    });
    LargestTaskRead largest = new LargestTaskRead();
    try {
      TypedProperties props = props();
      BalanceGroups groups = BalanceGroups.from(props);
      Dataset<Row> batch = batch().withColumn(GROUP, groups.groupColumn(functions.col(BlobColumns.SOURCE_FILE_EXTENSION))).cache();
      Map<String, Long> files = new HashMap<>();
      for (Row row : batch.groupBy(GROUP).agg(functions.count(functions.lit(1))).collectAsList()) {
        files.put(row.getString(0), row.getLong(1));
      }
      assertEquals(4, files.size(), "four groups: " + files.keySet());
      long largestGroup = files.values().stream().mapToLong(Long::longValue).max().getAsLong();

      spark().sparkContext().listenerBus().waitUntilEmpty();
      spark().sparkContext().addSparkListener(largest);
      BalancingAssignment assignment = new SizeTypeAwareBalancingStrategy().assign(batch.drop(GROUP), props);
      Dataset<Row> assigned = assignment.getAssignment().cache();
      assertEquals(FILES, assigned.count());
      spark().sparkContext().listenerBus().waitUntilEmpty();
      spark().sparkContext().removeSparkListener(largest);
      assertTrue(largest.records.get() > 0, "the listener saw the sort's shuffle reads");
      assertTrue(largest.records.get() <= largestGroup / 2,
          "a task read " + largest.records.get() + " shuffle records, the largest group has " + largestGroup);

      // per partition: its groups, files, bytes and largest file
      Column sameVersion = functions.lit(true);
      for (String key : BalancingAssignment.KEY_COLUMNS) {
        sameVersion = sameVersion.and(batch.col(key).equalTo(assigned.col(key)));
      }
      List<Row> partitions = batch.join(assigned, sameVersion).select(batch.col(GROUP), batch.col(BlobColumns.SOURCE_FILE_SIZE),
              assigned.col(BalancingAssignment.PARTITION))
          .groupBy(BalancingAssignment.PARTITION)
          .agg(functions.collect_set(GROUP), functions.count(functions.lit(1)), functions.sum(BlobColumns.SOURCE_FILE_SIZE),
              functions.max(BlobColumns.SOURCE_FILE_SIZE))
          .collectAsList();
      assertEquals(assignment.getNumPartitions(), partitions.size(), "no empty partition");
      Map<String, Set<Integer>> blocks = new HashMap<>();
      long placed = 0;
      for (Row partition : partitions) {
        List<String> partitionGroups = partition.getList(1);
        assertEquals(1, partitionGroups.size(), "partition " + partition.getInt(0) + " holds " + partitionGroups);
        String group = partitionGroups.get(0);
        blocks.computeIfAbsent(group, g -> new HashSet<>()).add(partition.getInt(0));
        placed += partition.getLong(2);
        if (!BalanceGroups.UNMAPPED.equals(group)) {
          long budget = SizeTypeAwareBalancingStrategy.maxBytesPerTask(props, group);
          assertTrue(partition.getLong(3) <= budget + partition.getLong(4),
              "partition " + partition.getInt(0) + " of " + group + " holds " + partition.getLong(3) + " bytes, budget " + budget);
        }
      }
      assertEquals(FILES, placed);

      // the task count each group's stats imply: solo files alone, ceil(rest / budget) dealt tasks, a floor of min(files, P)
      Map<String, Long> expectedTasks = new HashMap<>();
      for (Row row : batch.groupBy(GROUP).agg(functions.count(functions.lit(1)), functions.sum(BlobColumns.SOURCE_FILE_SIZE),
          functions.sum(functions.when(soloColumn(props, groups), 1L).otherwise(0L)),
          functions.sum(functions.when(soloColumn(props, groups), functions.col(BlobColumns.SOURCE_FILE_SIZE)).otherwise(0L)))
          .collectAsList()) {
        String group = row.getString(0);
        long groupFiles = row.getLong(1);
        if (BalanceGroups.UNMAPPED.equals(group)) {
          expectedTasks.put(group, 1L);
          continue;
        }
        long budget = SizeTypeAwareBalancingStrategy.maxBytesPerTask(props, group);
        long rest = row.getLong(2) - row.getLong(4);
        long byBytes = row.getLong(3) + Math.max(groupFiles > row.getLong(3) ? 1 : 0, (rest + budget - 1) / budget);
        expectedTasks.put(group, Math.min(groupFiles, Math.max(byBytes, Math.min(groupFiles, P))));
      }
      Map<String, Long> actualTasks = new HashMap<>();
      blocks.forEach((group, block) -> actualTasks.put(group, (long) block.size()));
      assertEquals(expectedTasks, actualTasks);
      assertEquals(expectedTasks.values().stream().mapToLong(Long::longValue).sum(), assignment.getNumPartitions());
      assertTrue(actualTasks.get(BalanceGroups.PDF) > P, "the pdf solo files take tasks of their own: " + actualTasks);
      assigned.unpersist(true);
      batch.unpersist(true);
    } finally {
      spark().sparkContext().removeSparkListener(largest);
      previous.forEach((key, value) -> {
        if (value == null) {
          spark().conf().unset(key);
        } else {
          spark().conf().set(key, value);
        }
      });
    }
  }

  /** A file at or over its group's budget; unmapped files never are. */
  private static Column soloColumn(TypedProperties props, BalanceGroups groups) {
    Column solo = functions.lit(false);
    for (String group : groups.configuredGroups()) {
      solo = functions.when(functions.col(GROUP).equalTo(group),
          functions.col(BlobColumns.SOURCE_FILE_SIZE).geq(SizeTypeAwareBalancingStrategy.maxBytesPerTask(props, group))).otherwise(solo);
    }
    return solo;
  }
}
