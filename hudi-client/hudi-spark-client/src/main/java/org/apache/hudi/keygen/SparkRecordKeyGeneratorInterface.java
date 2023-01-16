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

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIMethod;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Spark's record key generator interface to assist in generating record key for a given spark row.
 */
public interface SparkRecordKeyGeneratorInterface {

  /**
   * Extracts record key from Spark's {@link Row}
   *
   * @param row instance of {@link Row} from which record-key is extracted
   * @return record's (primary) key
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  String getRecordKey(Row row);

  /**
   * Extracts record key from Spark's {@link InternalRow}
   *
   * NOTE: Difference b/w {@link Row} and {@link InternalRow} is that {@link InternalRow} could
   *       internally hold just a binary representation of the data, while {@link Row} has it
   *       deserialized into JVM-native representation (like {@code Integer}, {@code Long},
   *       {@code String}, etc)
   *
   * @param row instance of {@link InternalRow} from which record-key is extracted
   * @param schema schema {@link InternalRow} is adhering to
   * @return record-key as instance of {@link UTF8String}
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  UTF8String getRecordKey(InternalRow row, StructType schema);
}
