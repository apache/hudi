/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.index.columnstats;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.util.BaseFileUtils;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.io.api.Binary;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Row$;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructType$;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.util.SerializableConfiguration;
import scala.collection.JavaConversions;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.hudi.util.DataTypeUtils.areCompatible;

public class ColumnStatsIndexHelper {

  private static final Logger LOG = LogManager.getLogger(ColumnStatsIndexHelper.class);

  private static final String SPARK_JOB_DESCRIPTION = "spark.job.description";

  private static final String Z_INDEX_FILE_COLUMN_NAME = "file";

  private static final String Z_INDEX_MIN_VALUE_STAT_NAME = "minValue";
  private static final String Z_INDEX_MAX_VALUE_STAT_NAME = "maxValue";
  private static final String Z_INDEX_NUM_NULLS_STAT_NAME = "num_nulls";

  public static String getMinColumnNameFor(String colName) {
    return composeZIndexColName(colName, Z_INDEX_MIN_VALUE_STAT_NAME);
  }

  public static String getMaxColumnNameFor(String colName) {
    return composeZIndexColName(colName, Z_INDEX_MAX_VALUE_STAT_NAME);
  }

  public static String getNumNullsColumnNameFor(String colName) {
    return composeZIndexColName(colName, Z_INDEX_NUM_NULLS_STAT_NAME);
  }

  /**
   * Parse min/max statistics from Parquet footers for provided columns and composes column-stats
   * index table in the following format with 3 statistics denominated for each
   * linear/Z-curve/Hilbert-curve-ordered column. For ex, if original table contained
   * column {@code A}:
   *
   * <pre>
   * +---------------------------+------------+------------+-------------+
   * |          file             | A_minValue | A_maxValue | A_num_nulls |
   * +---------------------------+------------+------------+-------------+
   * | one_base_file.parquet     |          1 |         10 |           0 |
   * | another_base_file.parquet |        -10 |          0 |           5 |
   * +---------------------------+------------+------------+-------------+
   * </pre>
   *
   * NOTE: Currently {@link TimestampType} is not supported, since Parquet writer
   * does not support statistics for it.
   *
   * TODO leverage metadata table after RFC-27 lands
   * @VisibleForTesting
   *
   * @param sparkSession encompassing Spark session
   * @param baseFilesPaths list of base-files paths to be sourced for column-stats index
   * @param orderedColumnSchemas target ordered columns
   * @return Spark's {@link Dataset} holding an index table
   */
  @Nonnull
  public static Dataset<Row> buildColumnStatsTableFor(
      @Nonnull SparkSession sparkSession,
      @Nonnull List<String> baseFilesPaths,
      @Nonnull List<StructField> orderedColumnSchemas
  ) {
    SparkContext sc = sparkSession.sparkContext();
    JavaSparkContext jsc = new JavaSparkContext(sc);

    SerializableConfiguration serializableConfiguration = new SerializableConfiguration(sc.hadoopConfiguration());
    int numParallelism = (baseFilesPaths.size() / 3 + 1);
    List<HoodieColumnRangeMetadata<Comparable>> colMinMaxInfos;
    String previousJobDescription = sc.getLocalProperty(SPARK_JOB_DESCRIPTION);
    try {
      jsc.setJobDescription("Listing parquet column statistics");
      colMinMaxInfos =
          jsc.parallelize(baseFilesPaths, numParallelism)
              .mapPartitions(paths -> {
                ParquetUtils utils = (ParquetUtils) BaseFileUtils.getInstance(HoodieFileFormat.PARQUET);
                Iterable<String> iterable = () -> paths;
                return StreamSupport.stream(iterable.spliterator(), false)
                    .flatMap(path ->
                        utils.readRangeFromParquetMetadata(
                                serializableConfiguration.value(),
                                new Path(path),
                                orderedColumnSchemas.stream()
                                    .map(StructField::name)
                                    .collect(Collectors.toList())
                            )
                            .stream()
                    )
                    .iterator();
              })
              .collect();
    } finally {
      jsc.setJobDescription(previousJobDescription);
    }

    // Group column's metadata by file-paths of the files it belongs to
    Map<String, List<HoodieColumnRangeMetadata<Comparable>>> filePathToColumnMetadataMap =
        colMinMaxInfos.stream()
            .collect(Collectors.groupingBy(HoodieColumnRangeMetadata::getFilePath));

    JavaRDD<Row> allMetaDataRDD =
        jsc.parallelize(new ArrayList<>(filePathToColumnMetadataMap.values()), 1)
            .map(fileColumnsMetadata -> {
              int colSize = fileColumnsMetadata.size();
              if (colSize == 0) {
                return null;
              }

              String filePath = fileColumnsMetadata.get(0).getFilePath();

              List<Object> indexRow = new ArrayList<>();

              // First columns of the Z-index's row is target file-path
              indexRow.add(filePath);

              // For each column
              orderedColumnSchemas.forEach(colSchema -> {
                String colName = colSchema.name();

                HoodieColumnRangeMetadata<Comparable> colMetadata =
                    fileColumnsMetadata.stream()
                        .filter(s -> s.getColumnName().trim().equalsIgnoreCase(colName))
                        .findFirst()
                        .orElse(null);

                DataType colType = colSchema.dataType();
                if (colMetadata == null || colType == null) {
                  throw new HoodieException(String.format("Cannot collect min/max statistics for column (%s)", colSchema));
                }

                Pair<Object, Object> minMaxValue = fetchMinMaxValues(colType, colMetadata);

                indexRow.add(minMaxValue.getLeft());      // min
                indexRow.add(minMaxValue.getRight());     // max
                indexRow.add(colMetadata.getNumNulls());
              });

              return Row$.MODULE$.apply(JavaConversions.asScalaBuffer(indexRow));
            })
            .filter(Objects::nonNull);

    StructType indexSchema = composeIndexSchema(orderedColumnSchemas);

    return sparkSession.createDataFrame(allMetaDataRDD, indexSchema);
  }

  /**
   * <p/>
   * Updates state of the column-stats index by:
   * <ol>
   *   <li>Updating column-stats index with statistics for {@code sourceBaseFiles},
   *   collecting corresponding column statistics from Parquet footers</li>
   *   <li>Merging newly built column-stats index table with the most recent one (if present
   *   and not preempted)</li>
   *   <li>Cleans up any residual index tables, that weren't cleaned up before</li>
   * </ol>
   *
   * @param sparkSession encompassing Spark session
   * @param sourceTableSchema instance of {@link StructType} bearing source table's writer's schema
   * @param sourceBaseFiles list of base-files to be indexed
   * @param orderedCols target ordered columns
   * @param indexFolderPath col-stats index folder path
   * @param commitTime current operation commit instant
   * @param completedCommits all previously completed commit instants
   */
  public static void updateColumnStatsIndexFor(
      @Nonnull SparkSession sparkSession,
      @Nonnull StructType sourceTableSchema,
      @Nonnull List<String> sourceBaseFiles,
      @Nonnull List<String> orderedCols,
      @Nonnull String indexFolderPath,
      @Nonnull String commitTime,
      @Nonnull List<String> completedCommits
  ) {
    FileSystem fs = FSUtils.getFs(indexFolderPath, sparkSession.sparkContext().hadoopConfiguration());

    // Compose new col-stats index table for the given source base files
    Dataset<Row> newColStatsIndexDf =
        buildColumnStatsTableFor(
            sparkSession,
            sourceBaseFiles,
            orderedCols.stream()
                .map(col -> sourceTableSchema.fields()[sourceTableSchema.fieldIndex(col)])
                .collect(Collectors.toList())
        );

    try {
      //
      // Column Stats Index has the following folder structure:
      //
      // .hoodie/
      // ├── .colstatsindex/
      // │   ├── <instant>/
      // │   │   ├── <part-...>.parquet
      // │   │   └── ...
      //
      // If index is currently empty (no persisted tables), we simply create one
      // using clustering operation's commit instance as it's name
      Path newIndexTablePath = new Path(indexFolderPath, commitTime);

      if (!fs.exists(new Path(indexFolderPath))) {
        newColStatsIndexDf.repartition(1)
            .write()
            .format("parquet")
            .mode("overwrite")
            .save(newIndexTablePath.toString());
        return;
      }

      // Filter in all index tables (w/in {@code .zindex} folder)
      List<String> allIndexTables =
          Arrays.stream(
                  fs.listStatus(new Path(indexFolderPath))
              )
              .filter(FileStatus::isDirectory)
              .map(f -> f.getPath().getName())
              .collect(Collectors.toList());

      // Compile list of valid index tables that were produced as part
      // of previously successfully committed iterations
      List<String> validIndexTables =
          allIndexTables.stream()
              .filter(completedCommits::contains)
              .sorted()
              .collect(Collectors.toList());

      List<String> tablesToCleanup =
          allIndexTables.stream()
              .filter(f -> !completedCommits.contains(f))
              .collect(Collectors.toList());

      Dataset<Row> finalColStatsIndexDf;

      // Before writing out new version of the col-stats-index table we need to merge it
      // with the most recent one that were successfully persisted previously
      if (validIndexTables.isEmpty()) {
        finalColStatsIndexDf = newColStatsIndexDf;
      } else {
        // NOTE: That Parquet schema might deviate from the original table schema (for ex,
        //       by upcasting "short" to "integer" types, etc), and hence we need to re-adjust it
        //       prior to merging, since merging might fail otherwise due to schemas incompatibility
        finalColStatsIndexDf =
            tryMergeMostRecentIndexTableInto(
                sparkSession,
                newColStatsIndexDf,
                // Load current most recent col-stats-index table
                sparkSession.read().load(
                    new Path(indexFolderPath, validIndexTables.get(validIndexTables.size() - 1)).toString()
                )
            );

        // Clean up all index tables (after creation of the new index)
        tablesToCleanup.addAll(validIndexTables);
      }

      // Persist new col-stats-index table
      finalColStatsIndexDf
          .repartition(1)
          .write()
          .format("parquet")
          .save(newIndexTablePath.toString());

      // Clean up residual col-stats-index tables that have might have been dangling since
      // previous iterations (due to intermittent failures during previous clean up)
      tablesToCleanup.forEach(f -> {
        try {
          fs.delete(new Path(indexFolderPath, f), true);
        } catch (IOException ie) {
          // NOTE: Exception is deliberately swallowed to not affect overall clustering operation,
          //       since failing col-stats-index table will be attempted to be cleaned up upon subsequent
          //       clustering iteration
          LOG.warn(String.format("Failed to cleanup residual col-stats-index table: %s", f), ie);
        }
      });
    } catch (IOException e) {
      LOG.error("Failed to build new col-stats-index table", e);
      throw new HoodieException("Failed to build new col-stats-index table", e);
    }
  }

  @Nonnull
  private static Dataset<Row> tryMergeMostRecentIndexTableInto(
      @Nonnull SparkSession sparkSession,
      @Nonnull Dataset<Row> newIndexTableDf,
      @Nonnull Dataset<Row> existingIndexTableDf
  ) {
    // NOTE: If new col-stats index table schema is incompatible with that one of existing table
    //       that is most likely due to changing settings of list of Z-ordered columns, that
    //       occurred since last index table have been persisted.
    //
    //       In that case, we simply drop existing index table and just persist the new one;
    //
    //       Also note that we're checking compatibility of _old_ index-table with new one and that
    //       COMPATIBILITY OPERATION DOES NOT COMMUTE (ie if A is compatible w/ B,
    //       B might not necessarily be compatible w/ A)
    if (!areCompatible(existingIndexTableDf.schema(), newIndexTableDf.schema())) {
      return newIndexTableDf;
    }

    String randomSuffix = UUID.randomUUID().toString().replace("-", "");

    String existingIndexTempTableName = "existingIndexTable_" + randomSuffix;
    String newIndexTempTableName = "newIndexTable_" + randomSuffix;

    existingIndexTableDf.registerTempTable(existingIndexTempTableName);
    newIndexTableDf.registerTempTable(newIndexTempTableName);

    List<String> newTableColumns = Arrays.asList(newIndexTableDf.schema().fieldNames());

    // Create merged table by doing full-out join
    return sparkSession.sql(createIndexMergeSql(existingIndexTempTableName, newIndexTempTableName, newTableColumns));
  }

  /**
   * @VisibleForTesting
   */
  @Nonnull
  public static StructType composeIndexSchema(@Nonnull List<StructField> zorderedColumnsSchemas) {
    List<StructField> schema = new ArrayList<>();
    schema.add(new StructField(Z_INDEX_FILE_COLUMN_NAME, StringType$.MODULE$, true, Metadata.empty()));
    zorderedColumnsSchemas.forEach(colSchema -> {
      schema.add(composeColumnStatStructType(colSchema.name(), Z_INDEX_MIN_VALUE_STAT_NAME, colSchema.dataType()));
      schema.add(composeColumnStatStructType(colSchema.name(), Z_INDEX_MAX_VALUE_STAT_NAME, colSchema.dataType()));
      schema.add(composeColumnStatStructType(colSchema.name(), Z_INDEX_NUM_NULLS_STAT_NAME, LongType$.MODULE$));
    });
    return StructType$.MODULE$.apply(schema);
  }

  private static StructField composeColumnStatStructType(String col, String statName, DataType dataType) {
    return new StructField(composeZIndexColName(col, statName), dataType, true, Metadata.empty());
  }

  private static String composeZIndexColName(String col, String statName) {
    // TODO add escaping for
    return String.format("%s_%s", col, statName);
  }

  private static Pair<Object, Object>
      fetchMinMaxValues(
          @Nonnull DataType colType,
          @Nonnull HoodieColumnRangeMetadata<Comparable> colMetadata) {
    if (colType instanceof IntegerType) {
      return Pair.of(
          new Integer(colMetadata.getMinValue().toString()),
          new Integer(colMetadata.getMaxValue().toString())
      );
    } else if (colType instanceof DoubleType) {
      return Pair.of(
          new Double(colMetadata.getMinValue().toString()),
          new Double(colMetadata.getMaxValue().toString())
      );
    } else if (colType instanceof StringType) {
      return Pair.of(
          new String(((Binary) colMetadata.getMinValue()).getBytes()),
          new String(((Binary) colMetadata.getMaxValue()).getBytes())
      );
    } else if (colType instanceof DecimalType) {
      return Pair.of(
          new BigDecimal(colMetadata.getMinValue().toString()),
          new BigDecimal(colMetadata.getMaxValue().toString()));
    } else if (colType instanceof DateType) {
      return Pair.of(
          java.sql.Date.valueOf(colMetadata.getMinValue().toString()),
          java.sql.Date.valueOf(colMetadata.getMaxValue().toString()));
    } else if (colType instanceof LongType) {
      return Pair.of(
          new Long(colMetadata.getMinValue().toString()),
          new Long(colMetadata.getMaxValue().toString()));
    } else if (colType instanceof ShortType) {
      return Pair.of(
          new Short(colMetadata.getMinValue().toString()),
          new Short(colMetadata.getMaxValue().toString()));
    } else if (colType instanceof FloatType) {
      return Pair.of(
          new Float(colMetadata.getMinValue().toString()),
          new Float(colMetadata.getMaxValue().toString()));
    } else if (colType instanceof BinaryType) {
      return Pair.of(
          ((Binary) colMetadata.getMinValue()).getBytes(),
          ((Binary) colMetadata.getMaxValue()).getBytes());
    } else if (colType instanceof BooleanType) {
      return Pair.of(
          Boolean.valueOf(colMetadata.getMinValue().toString()),
          Boolean.valueOf(colMetadata.getMaxValue().toString()));
    } else if (colType instanceof ByteType) {
      return Pair.of(
          Byte.valueOf(colMetadata.getMinValue().toString()),
          Byte.valueOf(colMetadata.getMaxValue().toString()));
    }  else {
      throw new HoodieException(String.format("Not support type:  %s", colType));
    }
  }

  /**
   * @VisibleForTesting
   */
  @Nonnull
  static String createIndexMergeSql(
      @Nonnull String originalIndexTable,
      @Nonnull String newIndexTable,
      @Nonnull List<String> columns
  ) {
    StringBuilder selectBody = new StringBuilder();

    for (int i = 0; i < columns.size(); ++i) {
      String col = columns.get(i);
      String originalTableColumn = String.format("%s.%s", originalIndexTable, col);
      String newTableColumn = String.format("%s.%s", newIndexTable, col);

      selectBody.append(
          // NOTE: We prefer values from the new index table, and fallback to the original one only
          //       in case it does not contain statistics for the given file path
          String.format("if (%s is null, %s, %s) AS %s", newTableColumn, originalTableColumn, newTableColumn, col)
      );

      if (i < columns.size() - 1) {
        selectBody.append(", ");
      }
    }

    return String.format(
        "SELECT %s FROM %s FULL JOIN %s ON %s = %s",
        selectBody,
        originalIndexTable,
        newIndexTable,
        String.format("%s.%s", originalIndexTable, columns.get(0)),
        String.format("%s.%s", newIndexTable, columns.get(0))
    );
  }
}
