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

package org.apache.hudi;

import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.util.JavaScalaConverters;

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
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructType$;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.util.SerializableConfiguration;

import javax.annotation.Nonnull;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

// TODO merge w/ ColumnStatsIndexSupport
public class ColumnStatsIndexHelper {

  public static Pair<Object, Object>
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
          colMetadata.getMinValue().toString(),
          colMetadata.getMaxValue().toString());
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
          ((ByteBuffer) colMetadata.getMinValue()).array(),
          ((ByteBuffer) colMetadata.getMaxValue()).array());
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
   * NOTE: THIS IS ONLY USED IN TESTING CURRENTLY, SINCE DATA SKIPPING IS NOW RELYING ON
   *       METADATA TABLE INDEX
   *
   * Parse min/max statistics from Parquet footers for provided columns and composes column-stats
   * index table in the following format with 3 statistics denominated for each
   * linear/Z-curve/Hilbert-curve-ordered column. For ex, if original table contained
   * column {@code A}:
   *
   * <pre>
   * +---------------------------+------------+------------+-------------+
   * |          file             | A_minValue | A_maxValue | A_nullCount |
   * +---------------------------+------------+------------+-------------+
   * | one_base_file.parquet     |          1 |         10 |           0 |
   * | another_base_file.parquet |        -10 |          0 |           5 |
   * +---------------------------+------------+------------+-------------+
   * </pre>
   * <p>
   * NOTE: Currently {@link TimestampType} is not supported, since Parquet writer
   * does not support statistics for it.
   *
   * @VisibleForTestingOnly
   *
   * @param sparkSession         encompassing Spark session
   * @param baseFilesPaths       list of base-files paths to be sourced for column-stats index
   * @param orderedColumnSchemas target ordered columns
   * @return Spark's {@link Dataset} holding an index table
   * @VisibleForTesting
   */
  @Nonnull
  public static Dataset<Row> buildColumnStatsTableFor(
      @Nonnull SparkSession sparkSession,
      @Nonnull List<String> baseFilesPaths,
      @Nonnull List<StructField> orderedColumnSchemas
  ) {
    SparkContext sc = sparkSession.sparkContext();
    JavaSparkContext jsc = new JavaSparkContext(sc);

    List<String> columnNames = orderedColumnSchemas.stream()
        .map(StructField::name)
        .collect(Collectors.toList());

    SerializableConfiguration serializableConfiguration = new SerializableConfiguration(sc.hadoopConfiguration());
    int numParallelism = (baseFilesPaths.size() / 3 + 1);

    String previousJobDescription = sc.getLocalProperty("spark.job.description");

    List<HoodieColumnRangeMetadata<Comparable>> colMinMaxInfos;
    try {
      jsc.setJobDescription("Listing parquet column statistics");
      colMinMaxInfos =
          jsc.parallelize(baseFilesPaths, numParallelism)
              .mapPartitions(paths -> {
                ParquetUtils utils = new ParquetUtils();
                Iterable<String> iterable = () -> paths;
                return StreamSupport.stream(iterable.spliterator(), false)
                    .flatMap(path -> {
                      HoodieStorage storage = HoodieStorageUtils.getStorage(path, HadoopFSUtils.getStorageConf(serializableConfiguration.value()));
                          return utils.readColumnStatsFromMetadata(
                                  storage,
                                  new StoragePath(path),
                                  columnNames,
                                  HoodieIndexVersion.getCurrentVersion(HoodieTableVersion.current(), MetadataPartitionType.COLUMN_STATS.getPartitionPath())
                              )
                              .stream();
                        }
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
                indexRow.add(colMetadata.getNullCount());
              });

              return Row$.MODULE$.apply(JavaScalaConverters.convertJavaListToScalaSeq(indexRow));
            })
            .filter(Objects::nonNull);

    StructType indexSchema = ColumnStatsIndexSupport$.MODULE$.composeIndexSchema(
        JavaScalaConverters.convertJavaListToScalaSeq(columnNames),
        JavaScalaConverters.convertJavaListToScalaList(columnNames),
          StructType$.MODULE$.apply(orderedColumnSchemas)
    )._1;

    return sparkSession.createDataFrame(allMetaDataRDD, indexSchema);
  }
}
