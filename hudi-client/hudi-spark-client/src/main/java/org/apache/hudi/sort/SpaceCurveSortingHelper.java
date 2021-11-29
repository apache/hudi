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

package org.apache.hudi.sort;

import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.optimize.HilbertCurveUtils;
import org.apache.hudi.optimize.ZOrderingUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Row$;
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
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType$;
import org.apache.spark.sql.types.TimestampType;
import org.davidmoten.hilbert.HilbertCurve;
import scala.collection.JavaConversions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SpaceCurveSortingHelper {

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
}
