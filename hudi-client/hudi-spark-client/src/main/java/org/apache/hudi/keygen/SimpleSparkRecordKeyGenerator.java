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

public class SimpleSparkRecordKeyGenerator implements SparkRecordKeyGenerator {

  private final SparkRowAccessor sparkRowAccessor;

  public SimpleSparkRecordKeyGenerator(SparkRowAccessor rowAccessor) {
    this.sparkRowAccessor = rowAccessor;
  }

  @Override
  public String getRecordKey(Row row) {
    Object[] recordKeys = sparkRowAccessor.getRecordKeyParts(row);
    // NOTE: [[SimpleKeyGenerator]] is restricted to allow only primitive (non-composite)
    //       record-key field
    if (recordKeys[0] == null) {
      return SparkRowUtils.handleNullRecordKey(null);
    } else {
      return SparkRowUtils.requireNonNullNonEmptyKey(recordKeys[0].toString());
    }
  }

  @Override
  public UTF8String getRecordKey(InternalRow internalRow, StructType schema) {
    Object[] recordKeyValues = sparkRowAccessor.getRecordKeyParts(internalRow);
    // NOTE: [[SimpleKeyGenerator]] is restricted to allow only primitive (non-composite)
    //       record-key field
    if (recordKeyValues[0] == null) {
      return SparkRowUtils.handleNullRecordKey(null);
    } else if (recordKeyValues[0] instanceof UTF8String) {
      return SparkRowUtils.requireNonNullNonEmptyKey((UTF8String) recordKeyValues[0]);
    } else {
      return SparkRowUtils.requireNonNullNonEmptyKey(UTF8String.fromString(recordKeyValues[0].toString()));
    }
  }
}
