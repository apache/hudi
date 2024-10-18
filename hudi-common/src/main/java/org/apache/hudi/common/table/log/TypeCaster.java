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
              String.format("Cast from Integer to %s is not supported", newType)
          );
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
              String.format("Cast from Long to %s is not supported", newType)
          );
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
              String.format("Cast from Float to %s is not supported", newType)
          );
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
              String.format("Cast from Double to %s is not supported", newType)
          );
      }
    } else if (value instanceof String) {
      String v = (String) value;
      if (newType == Type.STRING) {
        return v;
      } else {
        throw new UnsupportedOperationException(
            String.format("Cast from String to %s is not supported", newType)
        );
      }
    } else {
      throw new UnsupportedOperationException(
          String.format("Unsupported value type: %s", value.getClass())
      );
    }
  }
}
