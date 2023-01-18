/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.keygen;

import org.apache.hudi.exception.HoodieKeyException;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.apache.hudi.keygen.KeyGenUtils.DEFAULT_RECORD_KEY_PARTS_SEPARATOR;

public class CompositeSparkRecordKeyGenerator implements SparkRecordKeyGeneratorInterface {

  List<String> recordKeyFields;
  BuiltinKeyGenerator.SparkRowAccessor sparkRowAccessor;

  public CompositeSparkRecordKeyGenerator(List<String> recordKeyFields, BuiltinKeyGenerator.SparkRowAccessor rowAccessor) {
    this.recordKeyFields = recordKeyFields;
    this.sparkRowAccessor = rowAccessor;
  }

  @Override
  public String getRecordKey(Row row) {
    return combineCompositeRecordKey(sparkRowAccessor.getRecordKeyParts(row));
  }

  @Override
  public UTF8String getRecordKey(InternalRow internalRow, StructType schema) {
    return combineCompositeRecordKeyUnsafe(sparkRowAccessor.getRecordKeyParts(internalRow));
  }

  /**
   * NOTE: This method has to stay final (so that it's easier for JIT compiler to apply certain
   * optimizations, like inlining)
   */
  protected final String combineCompositeRecordKey(Object... recordKeyParts) {
    return combineCompositeRecordKeyInternal(
        StringPartitionPathFormatter.JavaStringBuilder::new,
        BuiltinKeyGenerator::toString,
        BuiltinKeyGenerator::handleNullOrEmptyCompositeKeyPart,
        BuiltinKeyGenerator::isNullOrEmptyCompositeKeyPart,
        recordKeyParts
    );
  }

  /**
   * NOTE: This method has to stay final (so that it's easier for JIT compiler to apply certain
   * optimizations, like inlining)
   */
  protected final UTF8String combineCompositeRecordKeyUnsafe(Object... recordKeyParts) {
    return combineCompositeRecordKeyInternal(
        UTF8StringPartitionPathFormatter.UTF8StringBuilder::new,
        BuiltinKeyGenerator::toUTF8String,
        BuiltinKeyGenerator::handleNullOrEmptyCompositeKeyPartUTF8,
        BuiltinKeyGenerator::isNullOrEmptyCompositeKeyPartUTF8,
        recordKeyParts
    );
  }

  private <S> S combineCompositeRecordKeyInternal(
      Supplier<PartitionPathFormatterBase.StringBuilder<S>> builderFactory,
      Function<Object, S> converter,
      Function<S, S> emptyKeyPartHandler,
      Predicate<S> isNullOrEmptyKeyPartPredicate,
      Object... recordKeyParts
  ) {
    boolean hasNonNullNonEmptyPart = false;

    PartitionPathFormatterBase.StringBuilder<S> sb = builderFactory.get();
    for (int i = 0; i < recordKeyParts.length; ++i) {
      // NOTE: If record-key part has already been a string [[toString]] will be a no-op
      S convertedKeyPart = emptyKeyPartHandler.apply(converter.apply(recordKeyParts[i]));

      sb.appendJava(recordKeyFields.get(i));
      sb.appendJava(BuiltinKeyGenerator.COMPOSITE_KEY_FIELD_VALUE_INFIX);
      sb.append(convertedKeyPart);
      // This check is to validate that overall composite-key has at least one non-null, non-empty
      // segment
      hasNonNullNonEmptyPart |= !isNullOrEmptyKeyPartPredicate.test(convertedKeyPart);

      if (i < recordKeyParts.length - 1) {
        sb.appendJava(DEFAULT_RECORD_KEY_PARTS_SEPARATOR);
      }
    }

    if (hasNonNullNonEmptyPart) {
      return sb.build();
    } else {
      throw new HoodieKeyException(String.format("All of the values for (%s) were either null or empty", recordKeyFields));
    }
  }
}
