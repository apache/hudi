/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.avro.processors;

import org.apache.hudi.common.schema.HoodieSchema;

import java.util.List;

public abstract class FixedTypeProcessor extends JsonFieldProcessor {
  protected byte[] convertToJavaObject(Object value, String name, HoodieSchema schema) {
    // The ObjectMapper use List to represent FixedType
    // eg: "decimal_val": [0, 0, 14, -63, -52] will convert to ArrayList<Integer>
    List<Integer> converval = (List<Integer>) value;
    byte[] src = new byte[converval.size()];
    for (int i = 0; i < converval.size(); i++) {
      src[i] = converval.get(i).byteValue();
    }
    byte[] dst = new byte[schema.getFixedSize()];
    System.arraycopy(src, 0, dst, 0, Math.min(schema.getFixedSize(), src.length));
    return dst;
  }
}
