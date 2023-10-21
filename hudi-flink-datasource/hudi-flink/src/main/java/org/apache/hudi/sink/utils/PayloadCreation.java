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
import org.apache.hudi.configuration.OptionsResolver;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.configuration.Configuration;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.Properties;

/**
 * Util to create hoodie pay load instance.
 */
public class PayloadCreation implements Serializable {
  private static final long serialVersionUID = 1L;

  private final boolean shouldCombine;
  private final boolean shouldUsePropsForPayload;
  private final Constructor<?> constructor;
  private final String preCombineField;

  private PayloadCreation(
      boolean shouldCombine,
      boolean shouldUsePropsForPayload,
      Constructor<?> constructor,
      @Nullable String preCombineField) {
    this.shouldCombine = shouldCombine;
    this.shouldUsePropsForPayload = shouldUsePropsForPayload;
    this.constructor = constructor;
    this.preCombineField = preCombineField;
  }

  public static PayloadCreation instance(Configuration conf) throws Exception {
    String preCombineField = OptionsResolver.getPreCombineField(conf);
    boolean needCombine = conf.getBoolean(FlinkOptions.PRE_COMBINE)
        || WriteOperationType.fromValue(conf.getString(FlinkOptions.OPERATION)) == WriteOperationType.UPSERT;
    boolean shouldCombine = needCombine && preCombineField != null;
    boolean shouldUsePropsForPayload = true;

    Class<?>[] argTypes;
    Constructor<?> constructor;
    if (shouldCombine) {
      argTypes = new Class<?>[] {GenericRecord.class, Comparable.class, Properties.class};
    } else {
      argTypes = new Class<?>[] {Option.class, Properties.class};
    }
    final String clazz = conf.getString(FlinkOptions.PAYLOAD_CLASS_NAME);
    try {
      constructor = ReflectionUtils.getClass(clazz).getConstructor(argTypes);
    } catch (NoSuchMethodException e) {
      shouldUsePropsForPayload = false;
      if (shouldCombine) {
        argTypes = new Class<?>[] {GenericRecord.class, Comparable.class};
      } else {
        argTypes = new Class<?>[] {Option.class};
      }
      constructor = ReflectionUtils.getClass(clazz).getConstructor(argTypes);
    }
    return new PayloadCreation(shouldCombine, shouldUsePropsForPayload, constructor, preCombineField);
  }

  public static Properties extractPropsFromConfiguration(Configuration config) {
    Properties props = new Properties();
    config.addAllToProperties(props);
    return props;
  }

  public HoodieRecordPayload<?> createPayload(GenericRecord record, Properties props) throws Exception {
    if (shouldCombine) {
      ValidationUtils.checkState(preCombineField != null);
      Comparable<?> orderingVal = (Comparable<?>) HoodieAvroUtils.getNestedFieldVal(record,
          preCombineField, false, false);
      if (shouldUsePropsForPayload) {
        return (HoodieRecordPayload<?>) constructor.newInstance(record, orderingVal, props);
      }
      return (HoodieRecordPayload<?>) constructor.newInstance(record, orderingVal);
    } else {
      if (shouldUsePropsForPayload) {
        return (HoodieRecordPayload<?>) this.constructor.newInstance(Option.of(record), props);
      }
      return (HoodieRecordPayload<?>) this.constructor.newInstance(Option.of(record));
    }
  }

  public HoodieRecordPayload<?> createDeletePayload(BaseAvroPayload payload, Properties props) throws Exception {
    if (shouldCombine) {
      if (shouldUsePropsForPayload) {
        return (HoodieRecordPayload<?>) constructor.newInstance(null, payload.getOrderingVal(), props);
      }
      return (HoodieRecordPayload<?>) constructor.newInstance(null, payload.getOrderingVal());
    } else {
      if (shouldUsePropsForPayload) {
        return (HoodieRecordPayload<?>) this.constructor.newInstance(Option.empty(), props);
      }
      return (HoodieRecordPayload<?>) this.constructor.newInstance(Option.empty());
    }
  }
}
