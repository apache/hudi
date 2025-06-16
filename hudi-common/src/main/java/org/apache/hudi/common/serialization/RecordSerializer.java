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

package org.apache.hudi.common.serialization;

import java.io.Serializable;

/**
 * Serializer for engine-specific record, e.g., {@code InternalRow} in Spark,
 * {@code RowData} in Flink and {@code IndexedRecord} for Avro.
 */
public interface RecordSerializer<T> extends Serializable {
  /**
   * Serialize the given record into bytes.
   *
   * @param record Engine-specific record.
   *
   * @return Serialized bytes for the record.
   */
  byte[] serialize(T record);

  /**
   * Deserialize the given bytes into engine-specific record with given schema.
   *
   * @param bytes Bytes for record.
   * @param schemaId Encoded id for the record schema.
   *
   * @return Engine-specific record.
   */
  T deserialize(byte[] bytes, int schemaId);
}
