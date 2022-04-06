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

package org.apache.hudi.internal.schema;

import java.io.Serializable;
import java.util.List;
import java.util.Locale;

/**
 * The type of a schema, reference avro schema.
 * now avro version used by hoodie, not support localTime.
 * to do add support for localTime if avro version is updated
 */
public interface Type extends Serializable {
  enum TypeID {
    RECORD, ARRAY, MAP, FIXED, STRING, BINARY,
    INT, LONG, FLOAT, DOUBLE, DATE, BOOLEAN, TIME, TIMESTAMP, DECIMAL, UUID;
    private String name;
    TypeID() {
      this.name = this.name().toLowerCase(Locale.ROOT);
    }

    public String getName() {
      return name;
    }
  }

  static TypeID fromValue(String value) {
    try {
      return TypeID.valueOf(value.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(String.format("Invalid value of Type: %s", value));
    }
  }

  TypeID typeId();

  default boolean isNestedType() {
    return false;
  }

  abstract class PrimitiveType implements Type {
    @Override
    public boolean isNestedType() {
      return false;
    }
  }

  abstract class NestedType implements Type {

    @Override
    public boolean isNestedType() {
      return true;
    }

    public abstract List<Types.Field> fields();

    public abstract Type fieldType(String name);

    public abstract Types.Field field(int id);
  }
}
