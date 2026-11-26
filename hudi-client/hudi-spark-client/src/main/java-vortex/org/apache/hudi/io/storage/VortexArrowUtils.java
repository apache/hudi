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

package org.apache.hudi.io.storage;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.stream.Collectors;

/**
 * Utility class for converting Arrow types to Spark SQL data types.
 *
 * <p>This class provides static methods to convert Arrow field definitions and type definitions into their
 * corresponding Spark SQL DataType representations. The mapping matches Spark 4.1's own {@code ArrowUtils}, extended
 * where Vortex produces types Spark's mapping does not cover:
 *
 * <ul>
 *   <li>the Arrow view types map to their logical Spark types: {@code Utf8View} to {@code StringType},
 *       {@code BinaryView} to {@code BinaryType}, and {@code ListView} to {@code ArrayType}
 *   <li>timestamps of every unit map to {@code TimestampType}/{@code TimestampNTZType};
 *       {@link VortexArrowColumnVector} normalizes non-microsecond values on read
 * </ul>
 *
 * <p>The one intentional divergence from Spark 4.1 is Arrow's Time type: Spark's {@code TimeType} only exists in Spark
 * 4.1+, and this module compiles a single source set against both Spark 3.5 and 4.1, so Time is rejected.
 *
 * <p>Ported from {@code dev.vortex.spark.ArrowUtils} in the Vortex Spark connector 0.93.0
 * (Apache License 2.0, Copyright the Vortex contributors). The connector relocates Arrow, so
 * Hudi cannot use it with the plain Arrow vectors that vortex-jni returns.
 */
public final class VortexArrowUtils {
  private VortexArrowUtils() {
  }

  /**
   * Converts the Arrow schema of a Vortex data source or scan to a Spark {@link StructType}.
   *
   * @param arrowSchema Arrow schema from a Vortex data source or scan
   * @return the Spark schema with the same fields
   */
  public static StructType toStructType(Schema arrowSchema) {
    Field root = new Field("root", FieldType.notNullable(ArrowType.Struct.INSTANCE), arrowSchema.getFields());
    return (StructType) fromArrowField(root);
  }

  /**
   * Converts an Arrow Field to a Spark SQL DataType.
   *
   * <p>This method handles nested types like structs, lists and maps by recursively converting their child fields.
   * For non-nested types, it delegates to {@link #fromArrowType(ArrowType)}.
   *
   * @param field the Arrow field to convert
   * @return the corresponding Spark SQL DataType
   * @throws UnsupportedOperationException if the Arrow type is not supported
   */
  public static DataType fromArrowField(Field field) {
    switch (field.getType().getTypeID()) {
      case Struct:
        return DataTypes.createStructType(field.getChildren().stream()
            .map(child -> {
              DataType dt = fromArrowField(child);
              return new StructField(child.getName(), dt, child.isNullable(), Metadata.empty());
            })
            .collect(Collectors.toList()));
      case List:
      case ListView: {
        Field elementField = field.getChildren().get(0);
        DataType elementType = fromArrowField(elementField);
        return DataTypes.createArrayType(elementType, elementField.isNullable());
      }
      case Map: {
        Field entries = field.getChildren().get(0);
        Field keyField = entries.getChildren().get(0);
        Field valueField = entries.getChildren().get(1);
        return DataTypes.createMapType(
            fromArrowField(keyField), fromArrowField(valueField), valueField.isNullable());
      }
      default:
        return fromArrowType(field.getType());
    }
  }

  /**
   * Converts an Arrow type to a Spark SQL DataType.
   *
   * <p>This method maps non-nested Arrow types to their corresponding Spark SQL types, following Spark 4.1's own
   * Arrow type mapping plus the view types (see the class documentation).
   *
   * @param dt the Arrow type to convert
   * @return the corresponding Spark SQL DataType
   * @throws UnsupportedOperationException if the Arrow type has no Spark representation
   */
  public static DataType fromArrowType(ArrowType dt) {
    switch (dt.getTypeID()) {
      case Bool:
        return DataTypes.BooleanType;
      case Int: {
        ArrowType.Int intType = (ArrowType.Int) dt;
        if (!intType.getIsSigned()) {
          throw new UnsupportedOperationException("Unsupported Arrow unsigned integer type: " + dt);
        }
        switch (intType.getBitWidth()) {
          case 8:
            return DataTypes.ByteType;
          case 16:
            return DataTypes.ShortType;
          case 32:
            return DataTypes.IntegerType;
          case 64:
            return DataTypes.LongType;
          default:
            throw new UnsupportedOperationException("Unsupported Arrow integer bit width: " + dt);
        }
      }
      case FloatingPoint: {
        ArrowType.FloatingPoint floatType = (ArrowType.FloatingPoint) dt;
        switch (floatType.getPrecision()) {
          case SINGLE:
            return DataTypes.FloatType;
          case DOUBLE:
            return DataTypes.DoubleType;
          default:
            throw new UnsupportedOperationException("Unsupported Arrow float precision: " + dt);
        }
      }
      case Decimal: {
        ArrowType.Decimal decimalType = (ArrowType.Decimal) dt;
        return DataTypes.createDecimalType(decimalType.getPrecision(), decimalType.getScale());
      }
      case Utf8:
      case LargeUtf8:
      case Utf8View:
        return DataTypes.StringType;
      case Binary:
      case LargeBinary:
      case BinaryView:
        return DataTypes.BinaryType;
      case Date: {
        ArrowType.Date dateType = (ArrowType.Date) dt;
        if (dateType.getUnit() == DateUnit.DAY) {
          return DataTypes.DateType;
        }
        throw new UnsupportedOperationException("Unsupported Arrow date unit: " + dt);
      }
      case Timestamp: {
        // Spark timestamps are physically microseconds; VortexArrowColumnVector's accessor
        // normalizes second/millisecond/nanosecond values on read.
        ArrowType.Timestamp ts = (ArrowType.Timestamp) dt;
        return ts.getTimezone() != null ? DataTypes.TimestampType : DataTypes.TimestampNTZType;
      }
      case Null:
        return DataTypes.NullType;
      case Interval: {
        ArrowType.Interval interval = (ArrowType.Interval) dt;
        switch (interval.getUnit()) {
          case YEAR_MONTH:
            return DataTypes.createYearMonthIntervalType();
          case MONTH_DAY_NANO:
            return DataTypes.CalendarIntervalType;
          default:
            throw new UnsupportedOperationException("Unsupported Arrow interval unit: " + dt);
        }
      }
      case Duration: {
        ArrowType.Duration duration = (ArrowType.Duration) dt;
        if (duration.getUnit() != TimeUnit.MICROSECOND) {
          throw new UnsupportedOperationException("Unsupported Arrow duration unit: " + dt);
        }
        return DataTypes.createDayTimeIntervalType();
      }
      case Time:
        // Spark 3.5 has no TIME type (TimeType only exists in Spark 4.1+), and this
        // module compiles a single source set against both Spark versions.
        throw new UnsupportedOperationException("Arrow Time type has no Spark 3.5 representation: " + dt);
      default:
        throw new UnsupportedOperationException(
            "Unsupported Arrow type: " + dt + " (type id: " + dt.getTypeID() + ")");
    }
  }
}
