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

package org.apache.hudi.common.table.log;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TypeCaster {
  private TypeCaster() {
  }

  public static Comparable<?> castValue(Comparable<?> value, Schema.Type newType) {
    if (value instanceof Integer) {
      Integer v = (Integer) value;
      switch (newType) {
        case INT:
          return v;
        case LONG:
          return v.longValue();
        case FLOAT:
          return v.floatValue();
        case DOUBLE:
          return v.doubleValue();
        case STRING:
          return v.toString();
        default:
          throw new UnsupportedOperationException(
              String.format("Cannot cast integer value (%d) to %s", v, newType));
      }
    } else if (value instanceof Long) {
      Long v = (Long) value;
      switch (newType) {
        case LONG:
          return v;
        case FLOAT:
          return v.floatValue();
        case DOUBLE:
          return v.doubleValue();
        case STRING:
          return v.toString();
        default:
          throw new UnsupportedOperationException(
              String.format("Cannot cast long value (%d) to %s", v, newType));
      }
    } else if (value instanceof Float) {
      Float v = (Float) value;
      switch (newType) {
        case FLOAT:
          return v;
        case DOUBLE:
          return v.doubleValue();
        case STRING:
          return v.toString();
        default:
          throw new UnsupportedOperationException(
              String.format("Cannot cast float value (%f) to %s", v, newType));
      }
    } else if (value instanceof Double) {
      Double v = (Double) value;
      switch (newType) {
        case DOUBLE:
          return v;
        case STRING:
          return v.toString();
        default:
          throw new UnsupportedOperationException(
              String.format("Cannot cast double value (%f) to %s", v, newType));
      }
    } else if (value instanceof String) {
      if (newType == Type.STRING) {
        return value;
      } else {
        throw new UnsupportedOperationException(
            String.format("Cannot cast string value (%s) to %s", value, newType));
      }
    } else {
      throw new UnsupportedOperationException(
          String.format("Unsupported value type: %s", value.getClass()));
    }
  }
}
