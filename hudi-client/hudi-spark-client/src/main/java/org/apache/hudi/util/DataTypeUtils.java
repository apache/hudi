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

package org.apache.hudi.util;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.ByteType$;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType$;
import org.apache.spark.sql.types.FloatType$;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.ShortType$;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.VarcharType$;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class DataTypeUtils {

  private static Map<Class<?>, Set<Class<?>>> sparkPrimitiveTypesCompatibilityMap =
      new HashMap<Class<?>, Set<Class<?>>>() {{

        // Integral types
        put(ShortType$.class,
            newHashSet(ByteType$.class, ShortType$.class));
        put(IntegerType$.class,
            newHashSet(ByteType$.class, ShortType$.class, IntegerType$.class));
        put(LongType$.class,
            newHashSet(ByteType$.class, ShortType$.class, IntegerType$.class, LongType$.class));

        // Float types
        put(DoubleType$.class,
            newHashSet(FloatType$.class, DoubleType$.class));

        // String types
        put(StringType$.class,
            newHashSet(VarcharType$.class, StringType$.class));
      }};

  /**
   * Validates whether one {@link StructType} is compatible w/ the other one.
   * Compatibility rules are defined like following: types A and B are considered
   * compatible iff
   *
   * <ol>
   *   <li>A and B are identical</li>
   *   <li>All values comprising A domain are contained w/in B domain (for ex, {@code ShortType}
   *   in this sense is compatible w/ {@code IntegerType})</li>
   * </ol>
   *
   * @param left operand
   * @param right operand
   * @return true if {@code left} instance of {@link StructType} is compatible w/ the {@code right}
   */
  public static boolean areCompatible(@Nonnull DataType left, @Nonnull DataType right) {
    // First, check if types are equal
    if (Objects.equals(left, right)) {
      return true;
    }

    // If not, check whether both are instances of {@code StructType} that
    // should be matched structurally
    if (left instanceof StructType && right instanceof StructType) {
      return areCompatible((StructType) left, (StructType) right);
    }

    // If not, simply check if those data-types constitute compatibility
    // relationship outlined above; otherwise return false
    return sparkPrimitiveTypesCompatibilityMap.getOrDefault(left.getClass(), Collections.emptySet())
        .contains(right.getClass());
  }

  private static boolean areCompatible(@Nonnull StructType left, @Nonnull StructType right) {
    StructField[] oneSchemaFields = left.fields();
    StructField[] anotherSchemaFields = right.fields();

    if (oneSchemaFields.length != anotherSchemaFields.length) {
      return false;
    }

    for (int i = 0; i < oneSchemaFields.length; ++i) {
      StructField oneField = oneSchemaFields[i];
      StructField anotherField = anotherSchemaFields[i];
      // NOTE: Metadata is deliberately omitted from comparison
      if (!Objects.equals(oneField.name(), anotherField.name())
          || !areCompatible(oneField.dataType(), anotherField.dataType())
          || oneField.nullable() != anotherField.nullable()) {
        return false;
      }
    }

    return true;
  }

  private static <T> HashSet<T> newHashSet(T... ts) {
    return new HashSet<>(Arrays.asList(ts));
  }

  /**
   * Try to find current sparktype whether contains that DecimalType which's scale < Decimal.MAX_LONG_DIGITS().
   *
   * @param sparkType spark schema.
   * @return found result.
   */
  public static boolean foundSmallPrecisionDecimalType(DataType sparkType) {
    if (sparkType instanceof StructType) {
      StructField[] fields = ((StructType) sparkType).fields();
      return Arrays.stream(fields).anyMatch(f -> foundSmallPrecisionDecimalType(f.dataType()));
    } else if (sparkType instanceof MapType) {
      MapType map = (MapType) sparkType;
      return foundSmallPrecisionDecimalType(map.keyType()) || foundSmallPrecisionDecimalType(map.valueType());
    } else if (sparkType instanceof ArrayType) {
      return foundSmallPrecisionDecimalType(((ArrayType) sparkType).elementType());
    } else if (sparkType instanceof DecimalType) {
      DecimalType decimalType = (DecimalType) sparkType;
      return decimalType.precision() < Decimal.MAX_LONG_DIGITS();
    }
    return false;
  }
}
