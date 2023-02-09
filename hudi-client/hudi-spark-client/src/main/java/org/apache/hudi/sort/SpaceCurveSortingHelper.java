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

import org.apache.hudi.common.util.BinaryUtil;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.optimize.HilbertCurveUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hudi.execution.ByteArraySorting;
import org.apache.spark.sql.hudi.execution.RangeSampleSort$;
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
import scala.collection.mutable.WrappedArray;

import javax.annotation.Nonnull;
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
   *       This method is more effective than {@link #orderDataFrameBySamplingValues} leveraging
   *       data sampling instead of direct mapping
   *
   * @param df Spark {@link Dataset} holding data to be ordered
   * @param orderByCols list of columns to be ordered by
   * @param targetPartitionCount target number of output partitions
   * @param layoutOptStrategy target layout optimization strategy
   * @return a {@link Dataset} holding data ordered by mapping tuple of values from provided columns
   *         onto a specified space-curve
   */
  public static Dataset<Row> orderDataFrameByMappingValues(
      Dataset<Row> df,
      HoodieClusteringConfig.LayoutOptimizationStrategy layoutOptStrategy,
      List<String> orderByCols,
      int targetPartitionCount
  ) {
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
    switch (layoutOptStrategy) {
      case ZORDER:
        sortedRDD = createZCurveSortedRDD(df.toJavaRDD(), fieldMap, fieldNum, targetPartitionCount);
        break;
      case HILBERT:
        sortedRDD = createHilbertSortedRDD(df.toJavaRDD(), fieldMap, fieldNum, targetPartitionCount);
        break;
      default:
        throw new UnsupportedOperationException(String.format("Not supported layout-optimization strategy (%s)", layoutOptStrategy));
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
      byte[][] zBytes = fieldMap.entrySet().stream()
        .map(entry -> {
          int index = entry.getKey();
          StructField field = entry.getValue();
          return mapColumnValueTo8Bytes(row, index, field.dataType());
        })
        .toArray(byte[][]::new);

      // Interleave received bytes to produce Z-curve ordinal
      byte[] zOrdinalBytes = BinaryUtil.interleaving(zBytes, 8);
      return appendToRow(row, zOrdinalBytes);
    })
      .sortBy(f -> new ByteArraySorting((byte[]) f.get(fieldNum)), true, fileNum);
  }

  private static JavaRDD<Row> createHilbertSortedRDD(JavaRDD<Row> originRDD, Map<Integer, StructField> fieldMap, int fieldNum, int fileNum) {
    // NOTE: Here {@code mapPartitions} is used to make sure Hilbert curve instance is initialized
    //       only once per partition
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
          long[] longs = fieldMap.entrySet().stream()
              .mapToLong(entry -> {
                int index = entry.getKey();
                StructField field = entry.getValue();
                return mapColumnValueToLong(row, index, field.dataType());
              })
              .toArray();

          // Map N-dimensional coordinates into position on the Hilbert curve
          byte[] hilbertCurvePosBytes = HilbertCurveUtils.indexBytes(hilbertCurve, longs, 63);
          return appendToRow(row, hilbertCurvePosBytes);
        }
      };
    })
        .sortBy(f -> new ByteArraySorting((byte[]) f.get(fieldNum)), true, fileNum);
  }

  private static Row appendToRow(Row row, Object value) {
    // NOTE: This is an ugly hack to avoid array re-allocation --
    //       Spark's {@code Row#toSeq} returns array of Objects
    Object[] currentValues = (Object[]) ((WrappedArray<Object>) row.toSeq()).array();
    return RowFactory.create(CollectionUtils.append(currentValues, value));
  }

  @Nonnull
  private static byte[] mapColumnValueTo8Bytes(Row row, int index, DataType dataType) {
    if (dataType instanceof LongType) {
      return BinaryUtil.longTo8Byte(row.isNullAt(index) ? Long.MAX_VALUE : row.getLong(index));
    } else if (dataType instanceof DoubleType) {
      return BinaryUtil.doubleTo8Byte(row.isNullAt(index) ? Double.MAX_VALUE : row.getDouble(index));
    } else if (dataType instanceof IntegerType) {
      return BinaryUtil.intTo8Byte(row.isNullAt(index) ? Integer.MAX_VALUE : row.getInt(index));
    } else if (dataType instanceof FloatType) {
      return BinaryUtil.doubleTo8Byte(row.isNullAt(index) ? Float.MAX_VALUE : row.getFloat(index));
    } else if (dataType instanceof StringType) {
      return BinaryUtil.utf8To8Byte(row.isNullAt(index) ? "" : row.getString(index));
    } else if (dataType instanceof DateType) {
      return BinaryUtil.longTo8Byte(row.isNullAt(index) ? Long.MAX_VALUE : row.getDate(index).getTime());
    } else if (dataType instanceof TimestampType) {
      return BinaryUtil.longTo8Byte(row.isNullAt(index) ? Long.MAX_VALUE : row.getTimestamp(index).getTime());
    } else if (dataType instanceof ByteType) {
      return BinaryUtil.byteTo8Byte(row.isNullAt(index) ? Byte.MAX_VALUE : row.getByte(index));
    } else if (dataType instanceof ShortType) {
      return BinaryUtil.intTo8Byte(row.isNullAt(index) ? Short.MAX_VALUE : row.getShort(index));
    } else if (dataType instanceof DecimalType) {
      return BinaryUtil.longTo8Byte(row.isNullAt(index) ? Long.MAX_VALUE : row.getDecimal(index).longValue());
    } else if (dataType instanceof BooleanType) {
      boolean value = row.isNullAt(index) ? false : row.getBoolean(index);
      return BinaryUtil.intTo8Byte(value ? 1 : 0);
    } else if (dataType instanceof BinaryType) {
      return BinaryUtil.paddingTo8Byte(row.isNullAt(index) ? new byte[] {0} : (byte[]) row.get(index));
    }

    throw new UnsupportedOperationException(String.format("Unsupported data-type (%s)", dataType.typeName()));
  }

  private static long mapColumnValueToLong(Row row, int index, DataType dataType) {
    if (dataType instanceof LongType) {
      return row.isNullAt(index) ? Long.MAX_VALUE : row.getLong(index);
    } else if (dataType instanceof DoubleType) {
      return row.isNullAt(index) ? Long.MAX_VALUE : Double.doubleToLongBits(row.getDouble(index));
    } else if (dataType instanceof IntegerType) {
      return row.isNullAt(index) ? Long.MAX_VALUE : (long) row.getInt(index);
    } else if (dataType instanceof FloatType) {
      return row.isNullAt(index) ? Long.MAX_VALUE : Double.doubleToLongBits((double) row.getFloat(index));
    } else if (dataType instanceof StringType) {
      return row.isNullAt(index) ? Long.MAX_VALUE : BinaryUtil.convertStringToLong(row.getString(index));
    } else if (dataType instanceof DateType) {
      return row.isNullAt(index) ? Long.MAX_VALUE : row.getDate(index).getTime();
    } else if (dataType instanceof TimestampType) {
      return row.isNullAt(index) ? Long.MAX_VALUE : row.getTimestamp(index).getTime();
    } else if (dataType instanceof ByteType) {
      return row.isNullAt(index) ? Long.MAX_VALUE : BinaryUtil.convertBytesToLong(new byte[] {row.getByte(index)});
    } else if (dataType instanceof ShortType) {
      return row.isNullAt(index) ? Long.MAX_VALUE : (long) row.getShort(index);
    } else if (dataType instanceof DecimalType) {
      return row.isNullAt(index) ? Long.MAX_VALUE : row.getDecimal(index).longValue();
    } else if (dataType instanceof BooleanType) {
      boolean value = row.isNullAt(index) ? false : row.getBoolean(index);
      return value ? Long.MAX_VALUE : 0;
    } else if (dataType instanceof BinaryType) {
      return row.isNullAt(index) ? Long.MAX_VALUE : BinaryUtil.convertBytesToLong((byte[]) row.get(index));
    }

    throw new UnsupportedOperationException(String.format("Unsupported data-type (%s)", dataType.typeName()));
  }

  public static Dataset<Row> orderDataFrameBySamplingValues(
      Dataset<Row> df,
      HoodieClusteringConfig.LayoutOptimizationStrategy layoutOptStrategy,
      List<String> orderByCols,
      int targetPartitionCount
  ) {
    return RangeSampleSort$.MODULE$.sortDataFrameBySample(df, layoutOptStrategy, JavaConversions.asScalaBuffer(orderByCols), targetPartitionCount);
  }
}
