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

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.utilities.unstructured.config.NumericConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobColumns;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Gives every preprocessor group ({@link BalanceGroups}: pdf, image, video, a custom class, unmapped) its own block of
 * partitions, sized by the group's byte budget per task ({@link #maxBytesPerTask}) with a floor of
 * {@code min(files, P)} tasks ({@link BalanceParallelism}), so video never shares a task with PDFs or images, no task holds more
 * than its budget except by at most one file, and a small batch still spreads over the cluster ({@link TaskPlan}).
 *
 * <p>A row weighs its {@code source_file_size}; a null, negative or non-integral size weighs 0. All three source version key
 * columns must be present. The batch's metadata (version key, group, weight) is projected and cached once, so no blob byte is
 * read and the batch is scanned once. One distributed aggregate brings the per-group counts to the driver ({@link GroupStats});
 * no per-file row is collected. One distributed sort then ranks the files ({@link SortedRanks}): by group, in the plan's block
 * order, then by weight descending and the three source version key columns, so a file's rank within its group is its global
 * rank minus the files of the groups before it. The files at or over the budget take one partition each, and the rest are dealt
 * in snake order over the remaining partitions ({@link TaskPlan#partitionOf}). The sort is range partitioned, so a group larger
 * than the shuffle's advisory partition size is ranked by several tasks, and the assignment depends only on the batch's
 * versions, never on its partition order, so a recomputed assignment is the same. The assignment keeps the batch's own key
 * column types.
 */
public class SizeTypeAwareBalancingStrategy extends AssignmentBalancingStrategy {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(SizeTypeAwareBalancingStrategy.class);

  private static final String GROUP = "_balance_group";
  private static final String WEIGHT = "_balance_weight";
  private static final String BLOCK = "_balance_block";
  private static final List<DataType> INTEGRAL_TYPES =
      Arrays.asList(DataTypes.ByteType, DataTypes.ShortType, DataTypes.IntegerType, DataTypes.LongType);

  // Inherited optional byte budgets; operators can tune them per preprocessor without changing prepared rows.
  /** Byte budget per task of the {@code pdf} group, 256 MiB. */
  public static final long DEFAULT_BALANCE_PDF_MAX_BYTES_PER_TASK = 256L * 1024 * 1024;
  /** Byte budget per task of the {@code image} group, 2 GiB. */
  public static final long DEFAULT_BALANCE_IMAGE_MAX_BYTES_PER_TASK = 2048L * 1024 * 1024;
  /**
   * Source bytes per video task, 1 GiB: lower to spread source bytes over more tasks; frame memory is per slot (tune slots/heap),
   * while decode time follows duration and resolution.
   */
  public static final long DEFAULT_BALANCE_VIDEO_MAX_BYTES_PER_TASK = 1024L * 1024 * 1024;
  /** Byte budget per task of a custom group, the PDF value. */
  public static final long DEFAULT_BALANCE_DEFAULT_MAX_BYTES_PER_TASK = DEFAULT_BALANCE_PDF_MAX_BYTES_PER_TASK;

  /**
   * The byte budget of one prep task of {@code group}: its {@code <group>.max.bytes.per.task} key
   * ({@link EmbeddingTableConfig#BALANCE_MAX_BYTES_PER_TASK_SUFFIX}) when set (a blank value counts as unset), else the built-in
   * default of {@code pdf}, {@code image} or {@code video}, else {@link EmbeddingTableConfig#BALANCE_DEFAULT_MAX_BYTES_PER_TASK}.
   *
   * @throws IllegalArgumentException naming the key, if the value read is not a positive integer
   */
  public static long maxBytesPerTask(TypedProperties props, String group) {
    String key = EmbeddingTableConfig.BALANCE_PREFIX + group + EmbeddingTableConfig.BALANCE_MAX_BYTES_PER_TASK_SUFFIX;
    if (!StringUtils.isNullOrEmpty(ConfigUtils.getStringWithAltKeys(props, ConfigProperty.key(key).noDefaultValue(), "").trim())) {
      return positiveLong(props, key, DEFAULT_BALANCE_DEFAULT_MAX_BYTES_PER_TASK);
    }
    switch (group) {
      case BalanceGroups.PDF:
        return DEFAULT_BALANCE_PDF_MAX_BYTES_PER_TASK;
      case BalanceGroups.IMAGE:
        return DEFAULT_BALANCE_IMAGE_MAX_BYTES_PER_TASK;
      case BalanceGroups.VIDEO:
        return DEFAULT_BALANCE_VIDEO_MAX_BYTES_PER_TASK;
      default:
        return positiveLong(props, EmbeddingTableConfig.BALANCE_DEFAULT_MAX_BYTES_PER_TASK.key(), DEFAULT_BALANCE_DEFAULT_MAX_BYTES_PER_TASK);
    }
  }

  /**
   * @return the groups (sorted) of the set {@code <group>.max.bytes.per.task} keys whose group is neither {@code default} nor one
   *     of {@code groups}' configured groups (so a key for {@code unmapped}, which has no budget, is listed too): most likely a
   *     typo, which would otherwise leave the group on the default budget without a word
   */
  public static List<String> unknownBudgetGroups(TypedProperties props, BalanceGroups groups) {
    String prefix = EmbeddingTableConfig.BALANCE_PREFIX;
    String suffix = EmbeddingTableConfig.BALANCE_MAX_BYTES_PER_TASK_SUFFIX;
    return props.stringPropertyNames().stream()
        .filter(key -> key.startsWith(prefix) && key.endsWith(suffix) && key.length() > prefix.length() + suffix.length())
        .map(key -> key.substring(prefix.length(), key.length() - suffix.length()))
        .filter(group -> !group.equals("default") && !groups.configuredGroups().contains(group))
        .sorted().collect(Collectors.toList());
  }

  private static long positiveLong(TypedProperties props, String key, long defaultValue) {
    return NumericConfig.positiveLong(props, key, defaultValue, Long.MAX_VALUE);
  }

  @Override
  public BalancingAssignment assign(Dataset<Row> rowsWithoutBlob, TypedProperties writerProps) {
    SparkSession spark = rowsWithoutBlob.sparkSession();
    BalanceGroups groups = BalanceGroups.from(writerProps);
    List<String> unknown = unknownBudgetGroups(writerProps, groups);
    if (!unknown.isEmpty()) {
      LOG.warn("Balance budget keys name groups no configured preprocessor resolves to, so they are not used: {} (groups: {}, "
          + "or default)", unknown, groups.configuredGroups());
    }
    Map<String, Long> budgets = new HashMap<>();
    for (String group : groups.configuredGroups()) {
      budgets.put(group, maxBytesPerTask(writerProps, group));
    }
    BalanceParallelism.Resolved p = parallelism(spark.sparkContext(), writerProps);

    Dataset<Row> weighted = rowsWithoutBlob.select(functions.col(BlobColumns.SOURCE_PATH_SHA),
        functions.col(BlobColumns.SOURCE_MODIFICATION_TIME), functions.col(BlobColumns.SOURCE_FILE_SIZE),
        groups.groupColumn(extensionColumn(rowsWithoutBlob)).as(GROUP), weightColumn(rowsWithoutBlob).as(WEIGHT))
        // metadata only, cached while the aggregate and the sort read it, so the batch is read once
        .persist(EmbeddingTableConfig.persistLevel(writerProps));
    try {
      return assignWeighted(spark, weighted, budgets, p);
    } finally {
      weighted.unpersist(false);
    }
  }

  /** The assignment of the cached metadata rows {@code weighted}: version key, {@code GROUP} and {@code WEIGHT}. */
  private static BalancingAssignment assignWeighted(SparkSession spark, Dataset<Row> weighted, Map<String, Long> budgets,
                                                    BalanceParallelism.Resolved p) {
    Column solo = functions.col(WEIGHT).geq(budgetColumn(budgets));
    List<GroupStats> stats = weighted.groupBy(GROUP).agg(functions.count(functions.lit(1)), functions.sum(WEIGHT),
            functions.sum(functions.when(solo, 1L).otherwise(0L)), functions.sum(functions.when(solo, functions.col(WEIGHT))
                .otherwise(0L)))
        .collectAsList().stream()
        .map(row -> new GroupStats(row.getString(0), row.getLong(1), row.isNullAt(2) ? 0 : row.getLong(2),
            row.isNullAt(3) ? 0 : row.getLong(3), row.isNullAt(4) ? 0 : row.getLong(4)))
        .collect(Collectors.toList());
    TaskPlan plan = TaskPlan.plan(stats, group -> budgets.getOrDefault(group, Long.MAX_VALUE), p.parallelism());
    if (LOG.isInfoEnabled()) {
      LOG.info("Balancing prep by preprocessor group into {} partitions, P = {}: {}", plan.numPartitions(), p,
          stats.stream().sorted((a, b) -> a.group().compareTo(b.group()))
              .map(s -> s.group() + " files=" + s.files() + " bytes=" + s.bytes() + " budget="
                  + (BalanceGroups.UNMAPPED.equals(s.group()) ? "n/a" : String.valueOf(budgets.get(s.group())))
                  + " tasks=" + plan.tasks(s.group()) + " (" + plan.soloTasks(s.group()) + " solo)")
              .collect(Collectors.joining("; ")));
    }
    if (stats.isEmpty()) {
      return new BalancingAssignment(1, spark.createDataFrame(Collections.emptyList(),
          SortedRanks.assignmentSchema(weighted.schema())));
    }

    // the global sort orders the groups by their block order, then each group's files by weight descending and version key,
    // so a file's rank within its group is its global rank minus the files of the groups before it
    Map<String, Long> filesOf = stats.stream().collect(Collectors.toMap(GroupStats::group, GroupStats::files));
    Map<String, Long> firstRank = new HashMap<>();
    Column blockOrder = null;
    long ranked = 0;
    List<String> blocks = plan.groups();
    for (int i = 0; i < blocks.size(); i++) {
      String group = blocks.get(i);
      firstRank.put(group, ranked);
      ranked += filesOf.get(group);
      Column match = functions.col(GROUP).equalTo(functions.lit(group));
      blockOrder = blockOrder == null ? functions.when(match, i) : blockOrder.when(match, i);
    }
    JavaRDD<Row> assignment = SortedRanks.of(weighted.withColumn(BLOCK, blockOrder),
            functions.col(BLOCK).asc(), functions.col(WEIGHT).desc(), functions.col(BlobColumns.SOURCE_PATH_SHA).asc(),
            functions.col(BlobColumns.SOURCE_MODIFICATION_TIME).asc(), functions.col(BlobColumns.SOURCE_FILE_SIZE).asc())
        .map(rowAndRank -> {
          String group = rowAndRank._1.getAs(GROUP);
          return RowFactory.create(rowAndRank._1.get(0), rowAndRank._1.get(1), rowAndRank._1.get(2),
              plan.partitionOf(group, rowAndRank._2 - firstRank.get(group)));
        });
    return new BalancingAssignment(plan.numPartitions(), spark.createDataFrame(assignment,
        SortedRanks.assignmentSchema(weighted.schema())));
  }

  /** P from the job's properties and Spark conf; registered cores exclude the driver's own entry. */
  static BalanceParallelism.Resolved parallelism(SparkContext sc, TypedProperties props) {
    int executors = Math.max(0, sc.getExecutorMemoryStatus().size() - 1);
    int registeredCores = executors * Math.max(1, sc.getConf().getInt(BalanceParallelism.EXECUTOR_CORES, 1));
    return BalanceParallelism.resolve(props, sc.getConf(), sc.master(), registeredCores, sc.defaultParallelism());
  }

  private static Column extensionColumn(Dataset<Row> rows) {
    return Arrays.asList(rows.columns()).contains(BlobColumns.SOURCE_FILE_EXTENSION)
        ? functions.col(BlobColumns.SOURCE_FILE_EXTENSION).cast(DataTypes.StringType)
        : functions.lit(null).cast(DataTypes.StringType);
  }

  /** {@code source_file_size} when the batch has an integral one and it is not negative, else 0. */
  private static Column weightColumn(Dataset<Row> rows) {
    boolean sized = Arrays.stream(rows.schema().fields())
        .anyMatch(f -> f.name().equals(BlobColumns.SOURCE_FILE_SIZE) && INTEGRAL_TYPES.contains(f.dataType()));
    if (!sized) {
      LOG.warn("The prep batch has no integral {} column; every file weighs 0, so only each group's file count and P decide its "
          + "tasks", BlobColumns.SOURCE_FILE_SIZE);
      return functions.lit(0L);
    }
    Column size = functions.col(BlobColumns.SOURCE_FILE_SIZE).cast(DataTypes.LongType);
    return functions.when(size.geq(0), size).otherwise(functions.lit(0L));
  }

  /** The budget of the row's group; {@link Long#MAX_VALUE} for unmapped, which has no budget and no solo file. */
  private static Column budgetColumn(Map<String, Long> budgets) {
    Column budget = null;
    for (Map.Entry<String, Long> entry : budgets.entrySet()) {
      Column match = functions.col(GROUP).equalTo(functions.lit(entry.getKey()));
      budget = budget == null ? functions.when(match, entry.getValue()) : budget.when(match, entry.getValue());
    }
    return budget == null ? functions.lit(Long.MAX_VALUE) : budget.otherwise(functions.lit(Long.MAX_VALUE));
  }
}
