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

package org.apache.hudi.internal.schema.utils;

import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;

import java.util.Arrays;
import java.util.HashSet;

public class Conversions {

  private static final HashSet<Type.TypeID> SUPPORTED_PARTITION_TYPES = new HashSet<>(
      Arrays.asList(Type.TypeID.INT,
          Type.TypeID.LONG,
          Type.TypeID.BOOLEAN,
          Type.TypeID.FLOAT,
          Type.TypeID.DECIMAL,
          Type.TypeID.DECIMAL_FIXED,
          Type.TypeID.DECIMAL_BYTES,
          Type.TypeID.DOUBLE,
          Type.TypeID.UUID,
          Type.TypeID.DATE,
          Type.TypeID.STRING));

  public static boolean isPartitionSchemaSupportedConversion(Types.RecordType schema) {
    for (Types.Field field: schema.fields()) {
      if (!SUPPORTED_PARTITION_TYPES.contains(field.type().typeId())) {
        return false;
      }
    }

    return true;
  }
}
