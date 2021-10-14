/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.utils;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.BaseAvroPayload;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.configuration.Configuration;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.lang.reflect.Constructor;

/**
 * Util to create hoodie pay load instance.
 */
public class PayloadCreation implements Serializable {
  private static final long serialVersionUID = 1L;

  private final boolean shouldCombine;
  private final Constructor<?> constructor;
  private final String preCombineField;

  private PayloadCreation(
      boolean shouldCombine,
      Constructor<?> constructor,
      @Nullable String preCombineField) {
    this.shouldCombine = shouldCombine;
    this.constructor = constructor;
    this.preCombineField = preCombineField;
  }

  public static PayloadCreation instance(Configuration conf) throws Exception {
    boolean shouldCombine = conf.getBoolean(FlinkOptions.PRE_COMBINE)
        || WriteOperationType.fromValue(conf.getString(FlinkOptions.OPERATION)) == WriteOperationType.UPSERT;
    String preCombineField = null;
    final Class<?>[] argTypes;
    final Constructor<?> constructor;
    if (shouldCombine) {
      preCombineField = conf.getString(FlinkOptions.PRECOMBINE_FIELD);
      argTypes = new Class<?>[] {GenericRecord.class, Comparable.class};
    } else {
      argTypes = new Class<?>[] {Option.class};
    }
    final String clazz = conf.getString(FlinkOptions.PAYLOAD_CLASS_NAME);
    constructor = ReflectionUtils.getClass(clazz).getConstructor(argTypes);
    return new PayloadCreation(shouldCombine, constructor, preCombineField);
  }

  public HoodieRecordPayload<?> createPayload(GenericRecord record) throws Exception {
    if (shouldCombine) {
      ValidationUtils.checkState(preCombineField != null);
      Comparable<?> orderingVal = (Comparable<?>) HoodieAvroUtils.getNestedFieldVal(record,
          preCombineField, false);
      return (HoodieRecordPayload<?>) constructor.newInstance(record, orderingVal);
    } else {
      return (HoodieRecordPayload<?>) this.constructor.newInstance(Option.of(record));
    }
  }

  public HoodieRecordPayload<?> createDeletePayload(BaseAvroPayload payload) throws Exception {
    if (shouldCombine) {
      return (HoodieRecordPayload<?>) constructor.newInstance(null, payload.orderingVal);
    } else {
      return (HoodieRecordPayload<?>) this.constructor.newInstance(Option.empty());
    }
  }
}
