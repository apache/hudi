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

package org.apache.hudi.keygen;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Spark key generator interface.
 */
public interface SparkKeyGeneratorInterface extends KeyGeneratorInterface {

  String getRecordKey(Row row);

  String getPartitionPath(Row row);

  String getPartitionPath(InternalRow internalRow, StructType structType);

  /**
   * validates record key
   * @param schema schema of the internalRow
   * @throws IllegalArgumentException
   */
  default void validateKeyGenProps(StructType schema) throws IllegalArgumentException {
    Set<String> columnSet = new HashSet<>(Arrays.asList(schema.fieldNames()));
    for (String recordKeyFieldName : getRecordKeyFieldNames()) {
      if (!columnSet.contains(recordKeyFieldName)) {
        throw new IllegalArgumentException("record key '" + recordKeyFieldName + "' does not exist in incoming dataframe schema: " + schema);
      }
    }
  }
}
