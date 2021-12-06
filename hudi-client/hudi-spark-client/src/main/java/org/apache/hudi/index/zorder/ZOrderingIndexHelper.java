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

package org.apache.hudi.index.zorder;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.util.BaseFileUtils;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.optimize.ZOrderingUtil;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.spark.sql.hudi.execution.RangeSampleSort$;
import org.apache.spark.sql.hudi.execution.ZorderingBinarySort;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BinaryType$;
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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

import scala.collection.JavaConversions;

import static org.apache.hudi.util.DataTypeUtils.areCompatible;

public class ZOrderingIndexHelper {

  private static final Logger LOG = LogManager.getLogger(ZOrderingIndexHelper.class);

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
   * Create z-order DataFrame directly
   * first, map all base type data to byte[8], then create z-order DataFrame
   * only support base type data. long,int,short,double,float,string,timestamp,decimal,date,byte
   * this method is more effective than createZIndexDataFrameBySample
   *
   * @param df a spark DataFrame holds parquet files to be read.
   * @param zCols z-sort cols
   * @param fileNum spark partition num
   * @return a dataFrame sorted by z-order.
   */
  public static Dataset<Row> createZIndexedDataFrameByMapValue(Dataset<Row> df, List<String> zCols, int fileNum) {
    Map<String, StructField> columnsMap = Arrays.stream(df.schema().fields()).collect(Collectors.toMap(e -> e.name(), e -> e));
    int fieldNum = df.schema().fields().length;
    List<String> checkCols = zCols.stream().filter(f -> columnsMap.containsKey(f)).collect(Collectors.toList());
    if (zCols.size() != checkCols.size()) {
      return df;
    }
    // only one col to sort, no need to use z-order
    if (zCols.size() == 1) {
      return df.repartitionByRange(fieldNum, org.apache.spark.sql.functions.col(zCols.get(0)));
    }
    Map<Integer, StructField> fieldMap = zCols
        .stream().collect(Collectors.toMap(e -> Arrays.asList(df.schema().fields()).indexOf(columnsMap.get(e)), e -> columnsMap.get(e)));
    // z-sort
    JavaRDD<Row> sortedRdd = df.toJavaRDD().map(row -> {
      List<byte[]> zBytesList = fieldMap.entrySet().stream().map(entry -> {
        int index = entry.getKey();
        StructField field = entry.getValue();
        DataType dataType = field.dataType();
        if (dataType instanceof LongType) {
          return ZOrderingUtil.longTo8Byte(row.isNullAt(index) ? Long.MAX_VALUE : row.getLong(index));
        } else if (dataType instanceof DoubleType) {
          return ZOrderingUtil.doubleTo8Byte(row.isNullAt(index) ? Double.MAX_VALUE : row.getDouble(index));
        } else if (dataType instanceof IntegerType) {
          return ZOrderingUtil.intTo8Byte(row.isNullAt(index) ? Integer.MAX_VALUE : row.getInt(index));
        } else if (dataType instanceof FloatType) {
          return ZOrderingUtil.doubleTo8Byte(row.isNullAt(index) ? Float.MAX_VALUE : row.getFloat(index));
        } else if (dataType instanceof StringType) {
          return ZOrderingUtil.utf8To8Byte(row.isNullAt(index) ? "" : row.getString(index));
        } else if (dataType instanceof DateType) {
          return ZOrderingUtil.longTo8Byte(row.isNullAt(index) ? Long.MAX_VALUE : row.getDate(index).getTime());
        } else if (dataType instanceof TimestampType) {
          return ZOrderingUtil.longTo8Byte(row.isNullAt(index) ? Long.MAX_VALUE : row.getTimestamp(index).getTime());
        } else if (dataType instanceof ByteType) {
          return ZOrderingUtil.byteTo8Byte(row.isNullAt(index) ? Byte.MAX_VALUE : row.getByte(index));
        } else if (dataType instanceof ShortType) {
          return ZOrderingUtil.intTo8Byte(row.isNullAt(index) ? Short.MAX_VALUE : row.getShort(index));
        } else if (dataType instanceof DecimalType) {
          return ZOrderingUtil.longTo8Byte(row.isNullAt(index) ? Long.MAX_VALUE : row.getDecimal(index).longValue());
        } else if (dataType instanceof BooleanType) {
          boolean value = row.isNullAt(index) ? false : row.getBoolean(index);
          return ZOrderingUtil.intTo8Byte(value ? 1 : 0);
        } else if (dataType instanceof BinaryType) {
          return ZOrderingUtil.paddingTo8Byte(row.isNullAt(index) ? new byte[] {0} : (byte[]) row.get(index));
        }
        return null;
      }).filter(f -> f != null).collect(Collectors.toList());
      byte[][] zBytes = new byte[zBytesList.size()][];
      for (int i = 0; i < zBytesList.size(); i++) {
        zBytes[i] = zBytesList.get(i);
      }
      List<Object> zVaules = new ArrayList<>();
      zVaules.addAll(scala.collection.JavaConverters.bufferAsJavaListConverter(row.toSeq().toBuffer()).asJava());
      zVaules.add(ZOrderingUtil.interleaving(zBytes, 8));
      return Row$.MODULE$.apply(JavaConversions.asScalaBuffer(zVaules));
    }).sortBy(f -> new ZorderingBinarySort((byte[]) f.get(fieldNum)), true, fileNum);

    // create new StructType
    List<StructField> newFields = new ArrayList<>();
    newFields.addAll(Arrays.asList(df.schema().fields()));
    newFields.add(new StructField("zIndex", BinaryType$.MODULE$, true, Metadata.empty()));

    // create new DataFrame
    return df.sparkSession().createDataFrame(sortedRdd, StructType$.MODULE$.apply(newFields)).drop("zIndex");
  }

  public static Dataset<Row> createZIndexedDataFrameByMapValue(Dataset<Row> df, String zCols, int fileNum) {
    if (zCols == null || zCols.isEmpty() || fileNum <= 0) {
      return df;
    }
    return createZIndexedDataFrameByMapValue(df,
        Arrays.stream(zCols.split(",")).map(f -> f.trim()).collect(Collectors.toList()), fileNum);
  }

  public static Dataset<Row> createZIndexedDataFrameBySample(Dataset<Row> df, List<String> zCols, int fileNum) {
    return RangeSampleSort$.MODULE$.sortDataFrameBySample(df, JavaConversions.asScalaBuffer(zCols), fileNum,
        HoodieClusteringConfig.BuildLayoutOptimizationStrategy.ZORDER.toCustomString());
  }

  public static Dataset<Row> createZIndexedDataFrameBySample(Dataset<Row> df, String zCols, int fileNum) {
    if (zCols == null || zCols.isEmpty() || fileNum <= 0) {
      return df;
    }
    return createZIndexedDataFrameBySample(df, Arrays.stream(zCols.split(",")).map(f -> f.trim()).collect(Collectors.toList()), fileNum);
  }

  /**
   * Parse min/max statistics from Parquet footers for provided columns and composes Z-index
   * table in the following format with 3 statistics denominated for each Z-ordered column.
   * For ex, if original table contained Z-ordered column {@code A}:
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
   * @param baseFilesPaths list of base-files paths to be sourced for Z-index
   * @param zorderedColumnSchemas target Z-ordered columns
   * @return Spark's {@link Dataset} holding an index table
   */
  @Nonnull
  public static Dataset<Row> buildZIndexTableFor(
      @Nonnull SparkSession sparkSession,
      @Nonnull List<String> baseFilesPaths,
      @Nonnull List<StructField> zorderedColumnSchemas
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
                            zorderedColumnSchemas.stream()
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
              zorderedColumnSchemas.forEach(colSchema -> {
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

    StructType indexSchema = composeIndexSchema(zorderedColumnSchemas);

    return sparkSession.createDataFrame(allMetaDataRDD, indexSchema);
  }

  /**
   * <p/>
   * Updates state of the Z-index by:
   * <ol>
   *   <li>Updating Z-index with statistics for {@code sourceBaseFiles}, collecting corresponding
   *   column statistics from Parquet footers</li>
   *   <li>Merging newly built Z-index table with the most recent one (if present and not preempted)</li>
   *   <li>Cleans up any residual index tables, that weren't cleaned up before</li>
   * </ol>
   *
   * @param sparkSession encompassing Spark session
   * @param sourceTableSchema instance of {@link StructType} bearing source table's writer's schema
   * @param sourceBaseFiles list of base-files to be indexed
   * @param zorderedCols target Z-ordered columns
   * @param zindexFolderPath Z-index folder path
   * @param commitTime current operation commit instant
   * @param completedCommits all previously completed commit instants
   */
  public static void updateZIndexFor(
      @Nonnull SparkSession sparkSession,
      @Nonnull StructType sourceTableSchema,
      @Nonnull List<String> sourceBaseFiles,
      @Nonnull List<String> zorderedCols,
      @Nonnull String zindexFolderPath,
      @Nonnull String commitTime,
      @Nonnull List<String> completedCommits
  ) {
    FileSystem fs = FSUtils.getFs(zindexFolderPath, sparkSession.sparkContext().hadoopConfiguration());

    // Compose new Z-index table for the given source base files
    Dataset<Row> newZIndexDf =
        buildZIndexTableFor(
            sparkSession,
            sourceBaseFiles,
            zorderedCols.stream()
                .map(col -> sourceTableSchema.fields()[sourceTableSchema.fieldIndex(col)])
                .collect(Collectors.toList())
        );

    try {
      //
      // Z-Index has the following folder structure:
      //
      // .hoodie/
      // ├── .zindex/
      // │   ├── <instant>/
      // │   │   ├── <part-...>.parquet
      // │   │   └── ...
      //
      // If index is currently empty (no persisted tables), we simply create one
      // using clustering operation's commit instance as it's name
      Path newIndexTablePath = new Path(zindexFolderPath, commitTime);

      if (!fs.exists(new Path(zindexFolderPath))) {
        newZIndexDf.repartition(1)
            .write()
            .format("parquet")
            .mode("overwrite")
            .save(newIndexTablePath.toString());
        return;
      }

      // Filter in all index tables (w/in {@code .zindex} folder)
      List<String> allIndexTables =
          Arrays.stream(
              fs.listStatus(new Path(zindexFolderPath))
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

      Dataset<Row> finalZIndexDf;
      
      // Before writing out new version of the Z-index table we need to merge it
      // with the most recent one that were successfully persisted previously
      if (validIndexTables.isEmpty()) {
        finalZIndexDf = newZIndexDf;
      } else {
        // NOTE: That Parquet schema might deviate from the original table schema (for ex,
        //       by upcasting "short" to "integer" types, etc), and hence we need to re-adjust it
        //       prior to merging, since merging might fail otherwise due to schemas incompatibility
        finalZIndexDf =
            tryMergeMostRecentIndexTableInto(
                sparkSession,
                newZIndexDf,
                // Load current most recent Z-index table
                sparkSession.read().load(
                    new Path(zindexFolderPath, validIndexTables.get(validIndexTables.size() - 1)).toString()
                )
            );

        // Clean up all index tables (after creation of the new index)
        tablesToCleanup.addAll(validIndexTables);
      }

      // Persist new Z-index table
      finalZIndexDf
        .repartition(1)
        .write()
        .format("parquet")
        .save(newIndexTablePath.toString());

      // Clean up residual Z-index tables that have might have been dangling since
      // previous iterations (due to intermittent failures during previous clean up)
      tablesToCleanup.forEach(f -> {
        try {
          fs.delete(new Path(zindexFolderPath, f), true);
        } catch (IOException ie) {
          // NOTE: Exception is deliberately swallowed to not affect overall clustering operation,
          //       since failing Z-index table will be attempted to be cleaned up upon subsequent
          //       clustering iteration
          LOG.warn(String.format("Failed to cleanup residual Z-index table: %s", f), ie);
        }
      });
    } catch (IOException e) {
      LOG.error("Failed to build new Z-index table", e);
      throw new HoodieException("Failed to build new Z-index table", e);
    }
  }

  @Nonnull
  private static Dataset<Row> tryMergeMostRecentIndexTableInto(
      @Nonnull SparkSession sparkSession,
      @Nonnull Dataset<Row> newIndexTableDf,
      @Nonnull Dataset<Row> existingIndexTableDf
  ) {
    // NOTE: If new Z-index table schema is incompatible with that one of existing table
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

  @Nullable
  private static String mapToSourceTableColumnName(StructField fieldStruct) {
    String name = fieldStruct.name();
    int maxStatSuffixIdx = name.lastIndexOf(String.format("_%s", Z_INDEX_MAX_VALUE_STAT_NAME));
    if (maxStatSuffixIdx != -1) {
      return name.substring(0, maxStatSuffixIdx);
    }

    int minStatSuffixIdx = name.lastIndexOf(String.format("_%s", Z_INDEX_MIN_VALUE_STAT_NAME));
    if (minStatSuffixIdx != -1) {
      return name.substring(0, minStatSuffixIdx);
    }

    int numNullsSuffixIdx = name.lastIndexOf(String.format("_%s", Z_INDEX_NUM_NULLS_STAT_NAME));
    if (numNullsSuffixIdx != -1) {
      return name.substring(0, numNullsSuffixIdx);
    }

    return null;
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
  public static String createIndexMergeSql(
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
