/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.client.clustering.run.strategy;

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.clustering.plan.strategy.SparkSingleFileSortPlanStrategy;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.execution.SQLConfInjectingRDD;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Measures what the write stage of {@link MultipleSparkJobExecutionStrategy#performClustering} ships to every task.
 *
 * <p>Clustering builds one RDD per group and unions them into a single write stage. Spark serializes the stage
 * lineage into one task binary that every task deserializes, so an object a group's lineage captures is shipped
 * to, and deserialized by, every task of the stage, for every group. Objects that do not depend on the group
 * (table schema, write config, reader properties, reader context factory, SQL configs) should therefore be shared
 * instances: Java serialization writes a shared instance once per stream.
 *
 * <p>The test runs the real strategy on a small and a large subset of one plan, serializes the resulting lineage
 * the way Spark's closure serializer does (one {@link ObjectOutputStream}), and counts the distinct instances of
 * each such payload. The counts must not grow with the number of groups.
 */
@Tag("functional")
public class TestClusteringTaskBinaryPayloads extends HoodieClientTestBase {

  private static final int SMALL_PLAN_GROUPS = 2;
  private static final int LARGE_PLAN_GROUPS = 8;
  // dataGen spreads inserts over 3 partitions; with small-file handling off, each batch adds one file group
  // per partition, and the single-file plan strategy emits one clustering group per file group.
  private static final int PARTITIONS = 3;
  private static final int INSERT_BATCHES = (LARGE_PLAN_GROUPS + PARTITIONS - 1) / PARTITIONS;
  private static final int RECORDS_PER_BATCH = 30;
  private static final String SQL_CONF_MAPS = "SQLConfInjectingRDD.sqlConfigs maps";

  /** Payloads that do not depend on the group, so their count must not depend on the number of groups. */
  private static final Map<String, Predicate<Object>> SHARED_PAYLOADS = new LinkedHashMap<>();

  static {
    SHARED_PAYLOADS.put("HoodieSchema", HoodieSchema.class::isInstance);
    SHARED_PAYLOADS.put("HoodieWriteConfig", HoodieWriteConfig.class::isInstance);
    SHARED_PAYLOADS.put("TypedProperties", TypedProperties.class::isInstance);
    SHARED_PAYLOADS.put("ReaderContextFactory", ReaderContextFactory.class::isInstance);
    SHARED_PAYLOADS.put("StructType", StructType.class::isInstance);
  }

  @Test
  public void taskBinaryPayloadsDoNotGrowWithClusteringGroups() throws Exception {
    HoodieWriteConfig config = getConfigBuilder()
        // Direct markers: what is measured here does not involve the timeline server.
        .withEmbeddedTimelineServerEnabled(false)
        .withMarkersType(MarkerType.DIRECT.name())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(0).build())
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringPlanStrategyClass(SparkSingleFileSortPlanStrategy.class.getName())
            .withClusteringMaxNumGroups(INSERT_BATCHES * PARTITIONS)
            .build())
        .build();

    String clusteringInstant;
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      for (int batch = 0; batch < INSERT_BATCHES; batch++) {
        String instant = WriteClientTestUtils.createNewInstantTime();
        WriteClientTestUtils.startCommitWithTime(client, instant);
        JavaRDD<WriteStatus> statuses = client.insert(jsc.parallelize(dataGen.generateInserts(instant, RECORDS_PER_BATCH), 1), instant);
        List<WriteStatus> collected = statuses.collect();
        assertNoWriteErrors(collected);
        client.commit(instant, jsc.parallelize(collected));
      }
      clusteringInstant = (String) client.scheduleClustering(Option.empty()).get();
    }

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieClusteringPlan plan = ClusteringUtils.getClusteringPlan(
        metaClient, INSTANT_GENERATOR.getClusteringCommitRequestedInstant(clusteringInstant)).map(Pair::getRight).get();
    assertTrue(plan.getInputGroups().size() >= LARGE_PLAN_GROUPS,
        "need at least " + LARGE_PLAN_GROUPS + " groups, plan has " + plan.getInputGroups().size());
    HoodieSchema schema = new TableSchemaResolver(metaClient).getTableSchema(false);

    TaskBinary small = taskBinary(config, firstGroups(plan, SMALL_PLAN_GROUPS), schema, clusteringInstant);
    TaskBinary large = taskBinary(config, firstGroups(plan, LARGE_PLAN_GROUPS), schema, clusteringInstant);
    String report = report(small, large);
    LOG.info("Clustering task binary payloads:\n{}", report);

    for (String payload : small.counts.keySet()) {
      assertEquals(small.counts.get(payload), large.counts.get(payload),
          payload + " is shipped per clustering group; every task deserializes all of them.\n" + report);
    }
  }

  private static HoodieClusteringPlan firstGroups(HoodieClusteringPlan plan, int groups) {
    return HoodieClusteringPlan.newBuilder(plan).setInputGroups(plan.getInputGroups().subList(0, groups)).build();
  }

  private TaskBinary taskBinary(HoodieWriteConfig config, HoodieClusteringPlan plan, HoodieSchema schema, String instant)
      throws IOException {
    HoodieSparkTable table = HoodieSparkTable.create(config, context, metaClient);
    RDD<WriteStatus> writeStage = HoodieJavaRDD.getJavaRDD(new SparkSortAndSizeExecutionStrategy<>(table, context, config)
        .performClustering(plan, schema, instant).getWriteStatuses()).rdd();
    // The DAGScheduler resolves partitions and dependencies before serializing the task binary, which fills
    // the lineage fields that get shipped.
    resolveLineage(writeStage, Collections.newSetFromMap(new IdentityHashMap<>()));
    CountingStream out = new CountingStream();
    try (PayloadCountingObjectOutputStream oos = new PayloadCountingObjectOutputStream(out)) {
      oos.writeObject(writeStage);
      oos.flush();
      return new TaskBinary(plan.getInputGroups().size(), out.bytes, oos.counts());
    }
  }

  private static void resolveLineage(RDD<?> rdd, Set<RDD<?>> visited) {
    if (!visited.add(rdd)) {
      return;
    }
    rdd.partitions();
    scala.collection.Iterator<?> deps = rdd.dependencies().iterator();
    while (deps.hasNext()) {
      resolveLineage(((org.apache.spark.Dependency<?>) deps.next()).rdd(), visited);
    }
  }

  private static String report(TaskBinary small, TaskBinary large) {
    int extraGroups = large.groups - small.groups;
    String rows = small.counts.keySet().stream()
        .map(payload -> String.format("  %-38s %6d %6d   %+.1f per group", payload, small.counts.get(payload),
            large.counts.get(payload), (large.counts.get(payload) - small.counts.get(payload)) / (double) extraGroups))
        .collect(Collectors.joining("\n"));
    return String.format("  %-38s %6s %6s%n", "groups", small.groups, large.groups)
        + rows + "\n"
        + String.format("  %-38s %6d %6d   %+.0f bytes per group", "task binary bytes", small.bytes, large.bytes,
            (large.bytes - small.bytes) / (double) extraGroups);
  }

  private static final class TaskBinary {
    final int groups;
    final long bytes;
    final Map<String, Integer> counts;

    TaskBinary(int groups, long bytes, Map<String, Integer> counts) {
      this.groups = groups;
      this.bytes = bytes;
      this.counts = counts;
    }
  }

  /** Counts each distinct object once, as Java serialization writes it once per stream. */
  private static final class PayloadCountingObjectOutputStream extends ObjectOutputStream {
    private final Map<String, Integer> counts = new LinkedHashMap<>();
    private final Set<Object> sqlConfMaps = Collections.newSetFromMap(new IdentityHashMap<>());

    PayloadCountingObjectOutputStream(OutputStream out) throws IOException {
      super(out);
      SHARED_PAYLOADS.keySet().forEach(payload -> counts.put(payload, 0));
      enableReplaceObject(true);
    }

    @Override
    protected Object replaceObject(Object obj) {
      SHARED_PAYLOADS.forEach((payload, matches) -> {
        if (matches.test(obj)) {
          counts.merge(payload, 1, Integer::sum);
        }
      });
      // One SQLConfInjectingRDD per group is inherent; the configs map it carries need not be.
      if (obj instanceof SQLConfInjectingRDD) {
        sqlConfMaps.add(sqlConfigsOf(obj));
      }
      return obj;
    }

    Map<String, Integer> counts() {
      Map<String, Integer> all = new LinkedHashMap<>(counts);
      all.put(SQL_CONF_MAPS, sqlConfMaps.size());
      return all;
    }

    private static Object sqlConfigsOf(Object rdd) {
      Field field = Arrays.stream(SQLConfInjectingRDD.class.getDeclaredFields())
          .filter(f -> f.getName().endsWith("sqlConfigs")).findFirst()
          .orElseThrow(() -> new IllegalStateException("SQLConfInjectingRDD has no sqlConfigs field"));
      try {
        field.setAccessible(true);
        return field.get(rdd);
      } catch (IllegalAccessException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  private static final class CountingStream extends OutputStream {
    long bytes;

    @Override
    public void write(int b) {
      bytes++;
    }

    @Override
    public void write(byte[] b, int off, int len) {
      bytes += len;
    }
  }
}
