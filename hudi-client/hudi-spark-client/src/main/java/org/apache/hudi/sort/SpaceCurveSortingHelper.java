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

import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.optimize.HilbertCurveUtils;
import org.apache.hudi.optimize.ZOrderingUtil;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
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
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructType$;
import org.apache.spark.sql.types.TimestampType;
import org.davidmoten.hilbert.HilbertCurve;
import scala.collection.JavaConversions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SpaceCurveSortingHelper {

  private static final Logger LOG = LogManager.getLogger(SpaceCurveSortingHelper.class);

  /**
   * Orders provided {@link Dataset} by mapping values of the provided list of columns
   * {@code orderByCols} onto a specified space curve (Z-curve, Hilbert, etc)
   *
   * <p/>
   * NOTE: Only support base data-types: long,int,short,double,float,string,timestamp,decimal,date,byte.
   *       This method is more effective than {@link #createOptimizeDataFrameBySample} leveraging
   *       data sampling instead of direct mapping
   *
   * @param df Spark {@link Dataset} holding data to be ordered
   * @param orderByCols list of columns to be ordered by
   * @param targetPartitionCount target number of output partitions
   * @param sortMode target space-curve to map onto
   * @return a {@link Dataset} holding data ordered by mapping tuple of values from provided columns
   *         onto a specified space-curve
   */
  public static Dataset<Row> orderDataFrameByMappingValues(Dataset<Row> df, List<String> orderByCols, int targetPartitionCount, String sortMode) {
    Map<String, StructField> columnsMap =
        Arrays.stream(df.schema().fields())
            .collect(Collectors.toMap(StructField::name, Function.identity()));

    List<String> checkCols =
        orderByCols.stream()
            .filter(columnsMap::containsKey)
            .collect(Collectors.toList());

    if (orderByCols.size() != checkCols.size()) {
      LOG.error(String.format("Trying to ordering over a column(s) not present in the schema (%s); skipping", CollectionUtils.diff(orderByCols, checkCols)));
      return df;
    }

    // In case when there's just one column to be ordered by, we can skip space-curve
    // ordering altogether (since it will match linear ordering anyway)
    if (orderByCols.size() == 1) {
      String orderByColName = orderByCols.get(0);
      LOG.debug(String.format("Single column to order by (%s), skipping space-curve ordering", orderByColName));

      // TODO validate if we need Spark to re-partition
      return df.repartitionByRange(targetPartitionCount, new Column(orderByColName));
    }

    int fieldNum = df.schema().fields().length;

    Map<Integer, StructField> fieldMap =
        orderByCols.stream()
            .collect(
                Collectors.toMap(e -> Arrays.asList(df.schema().fields()).indexOf(columnsMap.get(e)), columnsMap::get));

    JavaRDD<Row> sortedRDD;
    switch (HoodieClusteringConfig.BuildLayoutOptimizationStrategy.fromValue(sortMode)) {
      case ZORDER:
        sortedRDD = createZCurveSortedRDD(df.toJavaRDD(), fieldMap, fieldNum, targetPartitionCount);
        break;
      case HILBERT:
        sortedRDD = createHilbertSortedRDD(df.toJavaRDD(), fieldMap, fieldNum, targetPartitionCount);
        break;
      default:
        throw new IllegalArgumentException(String.format("new only support z-order/hilbert optimize but find: %s", sortMode));
    }

    // Compose new {@code StructType} for ordered RDDs
    StructType newStructType = composeOrderedRDDStructType(df.schema());

    return df.sparkSession()
        .createDataFrame(sortedRDD, newStructType)
        .drop("Index");
  }

  private static StructType composeOrderedRDDStructType(StructType schema) {
    return StructType$.MODULE$.apply(
        CollectionUtils.combine(
            Arrays.asList(schema.fields()),
            Arrays.asList(new StructField("Index", BinaryType$.MODULE$, true, Metadata.empty()))
        )
    );
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

  public static Dataset<Row> orderDataFrameByMappingValues(Dataset<Row> df, String sortCols, int fileNum, String sortMode) {
    if (sortCols == null || sortCols.isEmpty() || fileNum <= 0) {
      return df;
    }
    return orderDataFrameByMappingValues(df,
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
