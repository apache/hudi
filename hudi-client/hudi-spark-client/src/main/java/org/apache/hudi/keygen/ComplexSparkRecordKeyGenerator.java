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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.hudi.keygen.KeyGenUtils.DEFAULT_COMPOSITE_KEY_FILED_VALUE;
import static org.apache.hudi.keygen.KeyGenUtils.DEFAULT_RECORD_KEY_PARTS_SEPARATOR;

public class ComplexSparkRecordKeyGenerator implements SparkRecordKeyGenerator {

  List<String> recordKeyFields;
  SparkRowAccessor rowAccessor;

  public ComplexSparkRecordKeyGenerator(List<String> recordKeyFields, SparkRowAccessor rowAccessor) {
    this.recordKeyFields = recordKeyFields;
    this.rowAccessor = rowAccessor;
  }

  @Override
  public String getRecordKey(Row row) {
    return combineRecordKey(recordKeyFields, Arrays.asList(rowAccessor.getRecordKeyParts(row)));
  }

  @Override
  public UTF8String getRecordKey(InternalRow internalRow, StructType schema) {
    return combineRecordKeyUnsafe(recordKeyFields, Arrays.asList(rowAccessor.getRecordKeyParts(internalRow)));
  }

  /**
   * NOTE: This method has to stay final (so that it's easier for JIT compiler to apply certain
   * optimizations, like inlining)
   */
  protected final String combineRecordKey(List<String> fieldNames, List<Object> recordKeyParts) {
    return combineRecordKeyInternal(
        StringPartitionPathFormatter.JavaStringBuilder::new,
        SparkRowUtils::toString,
        SparkRowUtils::handleNullRecordKey,
        fieldNames,
        recordKeyParts
    );
  }

  /**
   * NOTE: This method has to stay final (so that it's easier for JIT compiler to apply certain
   * optimizations, like inlining)
   */
  protected final UTF8String combineRecordKeyUnsafe(List<String> fieldNames, List<Object> recordKeyParts) {
    return combineRecordKeyInternal(
        UTF8StringPartitionPathFormatter.UTF8StringBuilder::new,
        SparkRowUtils::toUTF8String,
        SparkRowUtils::handleNullRecordKey,
        fieldNames,
        recordKeyParts
    );
  }

  private <S> S combineRecordKeyInternal(
      Supplier<PartitionPathFormatterBase.StringBuilder<S>> builderFactory,
      Function<Object, S> converter,
      Function<S, S> emptyKeyPartHandler,
      List<String> fieldNames,
      List<Object> recordKeyParts
  ) {
    if (recordKeyParts.size() == 1) {
      return emptyKeyPartHandler.apply(converter.apply(recordKeyParts.get(0)));
    }

    PartitionPathFormatterBase.StringBuilder<S> sb = builderFactory.get();
    for (int i = 0; i < recordKeyParts.size(); ++i) {
      sb.appendJava(fieldNames.get(i)).appendJava(DEFAULT_COMPOSITE_KEY_FILED_VALUE);
      // NOTE: If record-key part has already been a string [[toString]] will be a no-op
      sb.append(emptyKeyPartHandler.apply(converter.apply(recordKeyParts.get(i))));

      if (i < recordKeyParts.size() - 1) {
        sb.appendJava(DEFAULT_RECORD_KEY_PARTS_SEPARATOR);
      }
    }

    return sb.build();
  }
}