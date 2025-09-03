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

package org.apache.hudi.expression;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class Comparators {

  private static final Map<Type, Comparator<?>> COMPARATORS = Collections.unmodifiableMap(
      new HashMap<Type, Comparator<?>>() {
        {
          put(Types.BooleanType.get(), Comparator.naturalOrder());
          put(Types.IntType.get(), Comparator.naturalOrder());
          put(Types.LongType.get(), Comparator.naturalOrder());
          put(Types.FloatType.get(), Comparator.naturalOrder());
          put(Types.DoubleType.get(), Comparator.naturalOrder());
          put(Types.DateType.get(), Comparator.naturalOrder());
          put(Types.TimeType.get(), Comparator.naturalOrder());
          put(Types.TimestampType.get(), Comparator.naturalOrder());
          put(Types.TimestampMillisType.get(), Comparator.naturalOrder());
          put(Types.LocalTimestampMillisType.get(), Comparator.naturalOrder());
          put(Types.LocalTimestampMicrosType.get(), Comparator.naturalOrder());
          put(Types.StringType.get(), Comparator.naturalOrder());
          put(Types.UUIDType.get(), Comparator.naturalOrder());
        }
      });

  public static <T> Comparator<T> forType(Type.PrimitiveType type) {
    return (Comparator<T>) Option.ofNullable(COMPARATORS.get(type))
        .orElseThrow(() -> new UnsupportedOperationException("The desired type " + type + " doesn't support comparator yet"));
  }
}
