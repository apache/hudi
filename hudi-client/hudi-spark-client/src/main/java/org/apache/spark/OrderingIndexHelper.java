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

package org.apache.spark;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.util.BaseFileUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.zorder.ZOrderingIndexHelper;
import org.apache.hudi.optimize.HilbertCurveUtils;
import org.apache.hudi.optimize.ZOrderingUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.io.api.Binary;
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
import org.davidmoten.hilbert.HilbertCurve;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.collection.JavaConversions;

public class OrderingIndexHelper {

  private static final String SPARK_JOB_DESCRIPTION = "spark.job.description";

  /**
   * Create optimized DataFrame directly
   * only support base type data. long,int,short,double,float,string,timestamp,decimal,date,byte
   * this method is more effective than createOptimizeDataFrameBySample
   *
   * @param df       a spark DataFrame holds parquet files to be read.
   * @param sortCols ordering columns for the curve
   * @param fileNum  spark partition num
   * @param sortMode layout optimization strategy
   * @return a dataFrame ordered by the curve.
   */
  public static Dataset<Row> createOptimizedDataFrameByMapValue(Dataset<Row> df, List<String> sortCols, int fileNum, String sortMode) {
    Map<String, StructField> columnsMap = Arrays.stream(df.schema().fields()).collect(Collectors.toMap(e -> e.name(), e -> e));
    int fieldNum = df.schema().fields().length;
    List<String> checkCols = sortCols.stream().filter(f -> columnsMap.containsKey(f)).collect(Collectors.toList());
    if (sortCols.size() != checkCols.size()) {
      return df;
    }
    // only one col to sort, no need to use z-order
    if (sortCols.size() == 1) {
      return df.repartitionByRange(fileNum, org.apache.spark.sql.functions.col(sortCols.get(0)));
    }
    Map<Integer, StructField> fieldMap = sortCols
        .stream().collect(Collectors.toMap(e -> Arrays.asList(df.schema().fields()).indexOf(columnsMap.get(e)), e -> columnsMap.get(e)));
    // do optimize
    JavaRDD<Row> sortedRDD = null;
    switch (HoodieClusteringConfig.BuildLayoutOptimizationStrategy.fromValue(sortMode)) {
      case ZORDER:
        sortedRDD = createZCurveSortedRDD(df.toJavaRDD(), fieldMap, fieldNum, fileNum);
        break;
      case HILBERT:
        sortedRDD = createHilbertSortedRDD(df.toJavaRDD(), fieldMap, fieldNum, fileNum);
        break;
      default:
        throw new IllegalArgumentException(String.format("new only support z-order/hilbert optimize but find: %s", sortMode));
    }
    // create new StructType
    List<StructField> newFields = new ArrayList<>();
    newFields.addAll(Arrays.asList(df.schema().fields()));
    newFields.add(new StructField("Index", BinaryType$.MODULE$, true, Metadata.empty()));

    // create new DataFrame
    return df.sparkSession().createDataFrame(sortedRDD, StructType$.MODULE$.apply(newFields)).drop("Index");
  }

  private static JavaRDD<Row> createZCurveSortedRDD(JavaRDD<Row> originRDD, Map<Integer, StructField> fieldMap, int fieldNum, int fileNum) {
    return originRDD.map(row -> {
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
  }

  private static JavaRDD<Row> createHilbertSortedRDD(JavaRDD<Row> originRDD, Map<Integer, StructField> fieldMap, int fieldNum, int fileNum) {
    return originRDD.mapPartitions(rows -> {
      HilbertCurve hilbertCurve = HilbertCurve.bits(63).dimensions(fieldMap.size());
      return new Iterator<Row>() {

        @Override
        public boolean hasNext() {
          return rows.hasNext();
        }

        @Override
        public Row next() {
          Row row = rows.next();
          List<Long> longList = fieldMap.entrySet().stream().map(entry -> {
            int index = entry.getKey();
            StructField field = entry.getValue();
            DataType dataType = field.dataType();
            if (dataType instanceof LongType) {
              return row.isNullAt(index) ? Long.MAX_VALUE : row.getLong(index);
            } else if (dataType instanceof DoubleType) {
              return row.isNullAt(index) ? Long.MAX_VALUE : Double.doubleToLongBits(row.getDouble(index));
            } else if (dataType instanceof IntegerType) {
              return row.isNullAt(index) ? Long.MAX_VALUE : (long)row.getInt(index);
            } else if (dataType instanceof FloatType) {
              return row.isNullAt(index) ? Long.MAX_VALUE : Double.doubleToLongBits((double) row.getFloat(index));
            } else if (dataType instanceof StringType) {
              return row.isNullAt(index) ? Long.MAX_VALUE : ZOrderingUtil.convertStringToLong(row.getString(index));
            } else if (dataType instanceof DateType) {
              return row.isNullAt(index) ? Long.MAX_VALUE : row.getDate(index).getTime();
            } else if (dataType instanceof TimestampType) {
              return row.isNullAt(index) ? Long.MAX_VALUE : row.getTimestamp(index).getTime();
            } else if (dataType instanceof ByteType) {
              return row.isNullAt(index) ? Long.MAX_VALUE : ZOrderingUtil.convertBytesToLong(new byte[] {row.getByte(index)});
            } else if (dataType instanceof ShortType) {
              return row.isNullAt(index) ? Long.MAX_VALUE : (long)row.getShort(index);
            } else if (dataType instanceof DecimalType) {
              return row.isNullAt(index) ? Long.MAX_VALUE : row.getDecimal(index).longValue();
            } else if (dataType instanceof BooleanType) {
              boolean value = row.isNullAt(index) ? false : row.getBoolean(index);
              return value ? Long.MAX_VALUE : 0;
            } else if (dataType instanceof BinaryType) {
              return row.isNullAt(index) ? Long.MAX_VALUE : ZOrderingUtil.convertBytesToLong((byte[]) row.get(index));
            }
            return null;
          }).filter(f -> f != null).collect(Collectors.toList());

          byte[] hilbertValue = HilbertCurveUtils.indexBytes(
              hilbertCurve, longList.stream().mapToLong(l -> l).toArray(), 63);
          List<Object> values = new ArrayList<>();
          values.addAll(scala.collection.JavaConverters.bufferAsJavaListConverter(row.toSeq().toBuffer()).asJava());
          values.add(hilbertValue);
          return Row$.MODULE$.apply(JavaConversions.asScalaBuffer(values));
        }
      };
    }).sortBy(f -> new ZorderingBinarySort((byte[]) f.get(fieldNum)), true, fileNum);
  }

  public static Dataset<Row> createOptimizedDataFrameByMapValue(Dataset<Row> df, String sortCols, int fileNum, String sortMode) {
    if (sortCols == null || sortCols.isEmpty() || fileNum <= 0) {
      return df;
    }
    return createOptimizedDataFrameByMapValue(df,
        Arrays.stream(sortCols.split(",")).map(f -> f.trim()).collect(Collectors.toList()), fileNum, sortMode);
  }

  public static Dataset<Row> createOptimizeDataFrameBySample(Dataset<Row> df, List<String> zCols, int fileNum, String sortMode) {
    return RangeSampleSort$.MODULE$.sortDataFrameBySample(df, JavaConversions.asScalaBuffer(zCols), fileNum, sortMode);
  }

  public static Dataset<Row> createOptimizeDataFrameBySample(Dataset<Row> df, String zCols, int fileNum, String sortMode) {
    if (zCols == null || zCols.isEmpty() || fileNum <= 0) {
      return df;
    }
    return createOptimizeDataFrameBySample(df, Arrays.stream(zCols.split(",")).map(f -> f.trim()).collect(Collectors.toList()), fileNum, sortMode);
  }

  /**
   * Parse min/max statistics stored in parquet footers for z-sort cols.
   * no support collect statistics from timeStampType, since parquet file has not collect the statistics for timeStampType.
   * to do adapt for rfc-27
   *
   * @param df a spark DataFrame holds parquet files to be read.
   * @param cols z-sort cols
   * @return a dataFrame holds all statistics info.
   */
  public static Dataset<Row> getMinMaxValue(Dataset<Row> df, List<String> cols) {
    Map<String, DataType> columnsMap = Arrays.stream(df.schema().fields()).collect(Collectors.toMap(e -> e.name(), e -> e.dataType()));

    List<String> scanFiles = Arrays.asList(df.inputFiles());
    SparkContext sc = df.sparkSession().sparkContext();
    JavaSparkContext jsc = new JavaSparkContext(sc);

    SerializableConfiguration serializableConfiguration = new SerializableConfiguration(sc.hadoopConfiguration());
    int numParallelism = (scanFiles.size() / 3 + 1);
    List<HoodieColumnRangeMetadata<Comparable>> colMinMaxInfos;
    String previousJobDescription = sc.getLocalProperty(SPARK_JOB_DESCRIPTION);
    try {
      jsc.setJobDescription("Listing parquet column statistics");
      colMinMaxInfos = jsc.parallelize(scanFiles, numParallelism).mapPartitions(paths -> {
        Configuration conf = serializableConfiguration.value();
        ParquetUtils parquetUtils = (ParquetUtils) BaseFileUtils.getInstance(HoodieFileFormat.PARQUET);
        List<Collection<HoodieColumnRangeMetadata<Comparable>>> results = new ArrayList<>();
        while (paths.hasNext()) {
          String path = paths.next();
          results.add(parquetUtils.readRangeFromParquetMetadata(conf, new Path(path), cols));
        }
        return results.stream().flatMap(f -> f.stream()).iterator();
      }).collect();
    } finally {
      jsc.setJobDescription(previousJobDescription);
    }

    Map<String, List<HoodieColumnRangeMetadata<Comparable>>> fileToStatsListMap = colMinMaxInfos.stream().collect(Collectors.groupingBy(e -> e.getFilePath()));
    JavaRDD<Row> allMetaDataRDD = jsc.parallelize(new ArrayList<>(fileToStatsListMap.values()), 1).map(f -> {
      int colSize = f.size();
      if (colSize == 0) {
        return null;
      } else {
        List<Object> rows = new ArrayList<>();
        rows.add(f.get(0).getFilePath());
        cols.stream().forEach(col -> {
          HoodieColumnRangeMetadata<Comparable> currentColRangeMetaData =
              f.stream().filter(s -> s.getColumnName().trim().equalsIgnoreCase(col)).findFirst().orElse(null);
          DataType colType = columnsMap.get(col);
          if (currentColRangeMetaData == null || colType == null) {
            throw new HoodieException(String.format("cannot collect min/max statistics for col: %s", col));
          }
          if (colType instanceof IntegerType) {
            rows.add(currentColRangeMetaData.getMinValue());
            rows.add(currentColRangeMetaData.getMaxValue());
          } else if (colType instanceof DoubleType) {
            rows.add(currentColRangeMetaData.getMinValue());
            rows.add(currentColRangeMetaData.getMaxValue());
          } else if (colType instanceof StringType) {
            rows.add(currentColRangeMetaData.getMinValue().toString());
            rows.add(currentColRangeMetaData.getMaxValue().toString());
          } else if (colType instanceof DecimalType) {
            rows.add(new BigDecimal(currentColRangeMetaData.getMinValue().toString()));
            rows.add(new BigDecimal(currentColRangeMetaData.getMaxValue().toString()));
          } else if (colType instanceof DateType) {
            rows.add(java.sql.Date.valueOf(currentColRangeMetaData.getMinValue().toString()));
            rows.add(java.sql.Date.valueOf(currentColRangeMetaData.getMaxValue().toString()));
          } else if (colType instanceof LongType) {
            rows.add(currentColRangeMetaData.getMinValue());
            rows.add(currentColRangeMetaData.getMaxValue());
          } else if (colType instanceof ShortType) {
            rows.add(Short.parseShort(currentColRangeMetaData.getMinValue().toString()));
            rows.add(Short.parseShort(currentColRangeMetaData.getMaxValue().toString()));
          } else if (colType instanceof FloatType) {
            rows.add(currentColRangeMetaData.getMinValue());
            rows.add(currentColRangeMetaData.getMaxValue());
          } else if (colType instanceof BinaryType) {
            rows.add(((Binary)currentColRangeMetaData.getMinValue()).getBytes());
            rows.add(((Binary)currentColRangeMetaData.getMaxValue()).getBytes());
          } else if (colType instanceof BooleanType) {
            rows.add(currentColRangeMetaData.getMinValue());
            rows.add(currentColRangeMetaData.getMaxValue());
          } else if (colType instanceof ByteType) {
            rows.add(Byte.valueOf(currentColRangeMetaData.getMinValue().toString()));
            rows.add(Byte.valueOf(currentColRangeMetaData.getMaxValue().toString()));
          }  else {
            throw new HoodieException(String.format("Not support type:  %s", colType));
          }
          rows.add(currentColRangeMetaData.getNumNulls());
        });
        return Row$.MODULE$.apply(JavaConversions.asScalaBuffer(rows));
      }
    }).filter(f -> f != null);
    List<StructField> allMetaDataSchema = new ArrayList<>();
    allMetaDataSchema.add(new StructField("file", StringType$.MODULE$, true, Metadata.empty()));
    cols.forEach(col -> {
      allMetaDataSchema.add(new StructField(col + "_minValue", columnsMap.get(col), true, Metadata.empty()));
      allMetaDataSchema.add(new StructField(col + "_maxValue", columnsMap.get(col), true, Metadata.empty()));
      allMetaDataSchema.add(new StructField(col + "_num_nulls", LongType$.MODULE$, true, Metadata.empty()));
    });
    return df.sparkSession().createDataFrame(allMetaDataRDD, StructType$.MODULE$.apply(allMetaDataSchema));
  }

  public static Dataset<Row> getMinMaxValue(Dataset<Row> df, String cols) {
    List<String> rawCols = Arrays.asList(cols.split(",")).stream().map(f -> f.trim()).collect(Collectors.toList());
    return getMinMaxValue(df, rawCols);
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
  public static void saveStatisticsInfo(Dataset<Row> df, String cols, String indexPath, String commitTime, List<String> validateCommits) {
    Path savePath = new Path(indexPath, commitTime);
    SparkSession spark = df.sparkSession();
    FileSystem fs = FSUtils.getFs(indexPath, spark.sparkContext().hadoopConfiguration());
    Dataset<Row> statisticsDF = OrderingIndexHelper.getMinMaxValue(df, cols);
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
        List columns = Arrays.asList(statisticsDF.schema().fieldNames());
        spark.sql(ZOrderingIndexHelper.createIndexMergeSql(originalTable, updateTable, columns)).repartition(1).write().save(savePath.toString());
      } else {
        statisticsDF.repartition(1).write().mode("overwrite").save(savePath.toString());
      }
    } catch (IOException e) {
      throw new HoodieException(e);
    }
  }
}
