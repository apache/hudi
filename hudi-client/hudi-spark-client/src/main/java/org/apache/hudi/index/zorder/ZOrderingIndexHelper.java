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
}
