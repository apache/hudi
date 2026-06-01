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

package org.apache.hudi.common.engine;

import org.apache.avro.generic.GenericRecord;

import javax.annotation.Nullable;

/**
 * Result of {@link RecordContext#extractDataFromRecord} carrying the engine-native row plus,
 * when extraction went through an Avro payload (e.g. {@code ExpressionPayload} in SQL MERGE INTO),
 * the original Avro record produced by {@code HoodieRecord#toIndexedRecord}.
 *
 * <p>The original Avro record is propagated to {@link org.apache.hudi.common.table.read.BufferedRecord}
 * so downstream code (e.g. {@code UpdateProcessor}) can decode the payload bytes with the correct
 * source schema instead of re-serializing the engine-native row through a possibly-mismatched schema.
 */
public final class ExtractedData<T> {

  private final T data;
  @Nullable
  private final GenericRecord originalAvroRecord;

  private ExtractedData(T data, @Nullable GenericRecord originalAvroRecord) {
    this.data = data;
    this.originalAvroRecord = originalAvroRecord;
  }

  public static <T> ExtractedData<T> of(T data) {
    return new ExtractedData<>(data, null);
  }

  public static <T> ExtractedData<T> of(T data, @Nullable GenericRecord originalAvroRecord) {
    return new ExtractedData<>(data, originalAvroRecord);
  }

  public T getData() {
    return data;
  }

  @Nullable
  public GenericRecord getOriginalAvroRecord() {
    return originalAvroRecord;
  }
}
