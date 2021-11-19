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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.util.BaseFileUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.optimize.ZOrderingUtil;
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
import org.apache.spark.sql.types.StructType$;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.util.SerializableConfiguration;
import scala.collection.JavaConversions;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ZOrderingIndexHelper {

  private static final String SPARK_JOB_DESCRIPTION = "spark.job.description";

  private static final String Z_INDEX_FILE_COLUMN_NAME = "file";

  private static final String Z_INDEX_MIN_VALUE_STAT_NAME = "minValue";
  private static final String Z_INDEX_MAX_VALUE_STAT_NAME = "maxValue";
  private static final String Z_INDEX_NUM_NULLS_STAT_NAME = "num_nulls";

  public static String getMinColumnNameFor(@Nonnull String colName) {
    return composeZIndexColName(colName, Z_INDEX_MIN_VALUE_STAT_NAME);
  }

  public static String getMaxColumnNameFor(@Nonnull String colName) {
    return composeZIndexColName(colName, Z_INDEX_MAX_VALUE_STAT_NAME);
  }

  public static String getNumNullsColumnNameFor(@Nonnull String colName) {
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
    return RangeSampleSort$.MODULE$.sortDataFrameBySample(df, JavaConversions.asScalaBuffer(zCols), fileNum);
  }

  public static Dataset<Row> createZIndexedDataFrameBySample(Dataset<Row> df, String zCols, int fileNum) {
    if (zCols == null || zCols.isEmpty() || fileNum <= 0) {
      return df;
    }
    return createZIndexedDataFrameBySample(df, Arrays.stream(zCols.split(",")).map(f -> f.trim()).collect(Collectors.toList()), fileNum);
  }

  /**
   * @VisibleForTesting
   * Parse min/max statistics stored in parquet footers for z-sort cols.
   * no support collect statistics from timeStampType, since parquet file has not collect the statistics for timeStampType.
   * to do adapt for rfc-27
   *
   * @param df a spark DataFrame holds parquet files to be read.
   * @param cols z-sort cols
   * @return a dataFrame holds all statistics info.
   */
  public static Dataset<Row> getMinMaxValue(Dataset<Row> df, List<String> cols) {
    Map<String, DataType> colsDataTypesMap =
        Arrays.stream(df.schema().fields())
            .collect(Collectors.toMap(StructField::name, StructField::dataType));

    List<String> scanFiles = Arrays.asList(df.inputFiles());
    SparkContext sc = df.sparkSession().sparkContext();
    JavaSparkContext jsc = new JavaSparkContext(sc);

    SerializableConfiguration serializableConfiguration = new SerializableConfiguration(sc.hadoopConfiguration());
    int numParallelism = (scanFiles.size() / 3 + 1);
    List<HoodieColumnRangeMetadata<Comparable>> colMinMaxInfos;
    String previousJobDescription = sc.getLocalProperty(SPARK_JOB_DESCRIPTION);
    try {
      jsc.setJobDescription("Listing parquet column statistics");
      colMinMaxInfos =
          jsc.parallelize(scanFiles, numParallelism)
              .mapPartitions(paths -> {
                ParquetUtils utils = (ParquetUtils) BaseFileUtils.getInstance(HoodieFileFormat.PARQUET);
                Iterable<String> iterable = () -> paths;
                return StreamSupport.stream(iterable.spliterator(), false)
                    .flatMap(path ->
                        utils.readRangeFromParquetMetadata(serializableConfiguration.value(), new Path(path), cols).stream()
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
              cols.forEach(col -> {
                HoodieColumnRangeMetadata<Comparable> colMetadata =
                    fileColumnsMetadata.stream()
                        .filter(s -> s.getColumnName().trim().equalsIgnoreCase(col))
                        .findFirst()
                        .orElse(null);

                DataType colType = colsDataTypesMap.get(col);
                if (colMetadata == null || colType == null) {
                  throw new HoodieException(String.format("Cannot collect min/max statistics for column (%s)", col));
                }

                Pair<Object, Object> minMaxValue = fetchColumnMinMaxValues(colType, colMetadata);

                indexRow.add(minMaxValue.getLeft());      // min
                indexRow.add(minMaxValue.getRight());     // max
                indexRow.add(colMetadata.getNumNulls());
              });

              return Row$.MODULE$.apply(JavaConversions.asScalaBuffer(indexRow));
            })
            .filter(Objects::nonNull);

    List<StructField> indexSchema = composeIndexSchema(cols, colsDataTypesMap);

    return df.sparkSession().createDataFrame(allMetaDataRDD, StructType$.MODULE$.apply(indexSchema));
  }

  /**
   * Update statistics info.
   * this method will update old index table by full out join,
   * and save the updated table into a new index table based on commitTime.
   * old index table will be cleaned also.
   *
   * @param df a spark DataFrame holds parquet files to be read.
   * @param cols z-sort cols.
   * @param indexPath index store path.
   * @param commitTime current operation commitTime.
   * @param validateCommits all validate commits for current table.
   * @return
   */
  public static void saveStatisticsInfo(Dataset<Row> df, List<String> cols, String indexPath, String commitTime, List<String> validateCommits) {
    Path savePath = new Path(indexPath, commitTime);
    SparkSession spark = df.sparkSession();
    FileSystem fs = FSUtils.getFs(indexPath, spark.sparkContext().hadoopConfiguration());
    Dataset<Row> statisticsDF = ZOrderingIndexHelper.getMinMaxValue(df, cols);
    // try to find last validate index table from index path
    try {
      // If there's currently no index, create one
      if (!fs.exists(new Path(indexPath))) {
        statisticsDF.repartition(1).write().mode("overwrite").save(savePath.toString());
        return;
      }

      // Otherwise, clean up all indexes but the most recent one

      List<String> allIndexTables = Arrays
          .stream(fs.listStatus(new Path(indexPath))).filter(f -> f.isDirectory()).map(f -> f.getPath().getName()).collect(Collectors.toList());
      List<String> candidateIndexTables = allIndexTables.stream().filter(f -> validateCommits.contains(f)).sorted().collect(Collectors.toList());
      List<String> residualTables = allIndexTables.stream().filter(f -> !validateCommits.contains(f)).collect(Collectors.toList());
      Option<Dataset> latestIndexData = Option.empty();
      if (!candidateIndexTables.isEmpty()) {
        latestIndexData = Option.of(spark.read().load(new Path(indexPath, candidateIndexTables.get(candidateIndexTables.size() - 1)).toString()));
        // clean old index table, keep at most 1 index table.
        candidateIndexTables.remove(candidateIndexTables.size() - 1);
        candidateIndexTables.forEach(f -> {
          try {
            fs.delete(new Path(indexPath, f));
          } catch (IOException ie) {
            throw new HoodieException(ie);
          }
        });
      }

      // clean residualTables
      // retried cluster operations at the same instant time is also considered,
      // the residual files produced by retried are cleaned up before save statistics
      // save statistics info to index table which named commitTime
      residualTables.forEach(f -> {
        try {
          fs.delete(new Path(indexPath, f));
        } catch (IOException ie) {
          throw new HoodieException(ie);
        }
      });

      if (latestIndexData.isPresent() && latestIndexData.get().schema().equals(statisticsDF.schema())) {
        // update the statistics info
        String originalTable = "indexTable_" + java.util.UUID.randomUUID().toString().replace("-", "");
        String updateTable = "updateTable_" + java.util.UUID.randomUUID().toString().replace("-", "");
        latestIndexData.get().registerTempTable(originalTable);
        statisticsDF.registerTempTable(updateTable);
        // update table by full out join
        List<String> columns = Arrays.asList(statisticsDF.schema().fieldNames());
        spark.sql(createIndexMergeSql(originalTable, updateTable, columns)).repartition(1).write().save(savePath.toString());
      }
    } catch (IOException e) {
      throw new HoodieException(e);
    }
  }

  @Nonnull
  private static List<StructField> composeIndexSchema(List<String> cols, Map<String, DataType> colsDataTypesMap) {
    List<StructField> schema = new ArrayList<>();
    schema.add(new StructField(Z_INDEX_FILE_COLUMN_NAME, StringType$.MODULE$, true, Metadata.empty()));
    cols.forEach(col -> {
      schema.add(composeColumnStatStructType(col, Z_INDEX_MIN_VALUE_STAT_NAME, colsDataTypesMap.get(col)));
      schema.add(composeColumnStatStructType(col, Z_INDEX_MAX_VALUE_STAT_NAME, colsDataTypesMap.get(col)));
      schema.add(composeColumnStatStructType(col, Z_INDEX_NUM_NULLS_STAT_NAME, LongType$.MODULE$));
    });
    return schema;
  }

  private static StructField composeColumnStatStructType(String col, String statName, DataType dataType) {
    return new StructField(composeZIndexColName(col, statName), dataType, true, Metadata.empty());
  }

  private static String composeZIndexColName(String col, String statName) {
    // TODO add escaping for
    return String.format("%s_%s", col, statName);
  }

  private static Pair<Object, Object>
      fetchColumnMinMaxValues(
          @Nonnull DataType colType,
          @Nonnull HoodieColumnRangeMetadata<Comparable> colMetadata) {
    if (colType instanceof IntegerType) {
      return Pair.of(colMetadata.getMinValue(), colMetadata.getMaxValue());
    } else if (colType instanceof DoubleType) {
      return Pair.of(colMetadata.getMinValue(), colMetadata.getMaxValue());
    } else if (colType instanceof StringType) {
      return Pair.of(
          new String(((Binary) colMetadata.getMinValue()).getBytes()),
          new String(((Binary) colMetadata.getMaxValue()).getBytes())
      );
    } else if (colType instanceof DecimalType) {
      // TODO this will be losing precision
      return Pair.of(
          Double.parseDouble(colMetadata.getStringifier().stringify(Long.valueOf(colMetadata.getMinValue().toString()))),
          Double.parseDouble(colMetadata.getStringifier().stringify(Long.valueOf(colMetadata.getMaxValue().toString()))));
    } else if (colType instanceof DateType) {
      return Pair.of(
          java.sql.Date.valueOf(colMetadata.getStringifier().stringify((int) colMetadata.getMinValue())),
          java.sql.Date.valueOf(colMetadata.getStringifier().stringify((int) colMetadata.getMaxValue())));
    } else if (colType instanceof LongType) {
      return Pair.of(colMetadata.getMinValue(), colMetadata.getMaxValue());
    } else if (colType instanceof ShortType) {
      return Pair.of(
          Short.parseShort(colMetadata.getMinValue().toString()),
          Short.parseShort(colMetadata.getMaxValue().toString()));
    } else if (colType instanceof FloatType) {
      return Pair.of(colMetadata.getMinValue(), colMetadata.getMaxValue());
    } else if (colType instanceof BinaryType) {
      return Pair.of(
          ((Binary) colMetadata.getMinValue()).getBytes(),
          ((Binary) colMetadata.getMaxValue()).getBytes());
    } else if (colType instanceof BooleanType) {
      return Pair.of(colMetadata.getMinValue(), colMetadata.getMaxValue());
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
