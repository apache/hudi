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

package org.apache.hudi.avro;

import org.apache.avro.Schema;

import java.util.List;

import static org.apache.hudi.avro.AvroSchemaUtils.getNonNullTypeFromUnion;

public class AvroSchemaComparatorForRecordProjection extends AvroSchemaComparatorForSchemaEvolution {

  private static final AvroSchemaComparatorForRecordProjection INSTANCE = new AvroSchemaComparatorForRecordProjection();

  public static boolean areSchemasProjectionEquivalent(Schema s1, Schema s2) {
    return INSTANCE.schemaEqualsInternal(s1, s2);
  }

  @Override
  protected boolean schemaEqualsInternal(Schema s1, Schema s2) {
    if (s1 == s2) {
      return true;
    }
    if (s1 == null || s2 == null) {
      return false;
    }
    return super.schemaEqualsInternal(getNonNullTypeFromUnion(s1), getNonNullTypeFromUnion(s2));
  }

  @Override
  protected boolean validateRecord(Schema s1, Schema s2) {
    return true;
  }

  @Override
  protected boolean validateField(Schema.Field f1, Schema.Field f2) {
    return f1.name().equalsIgnoreCase(f2.name());
  }

  @Override
  protected boolean enumSchemaEquals(Schema s1, Schema s2) {
    List<String> symbols1 = s1.getEnumSymbols();
    List<String> symbols2 = s2.getEnumSymbols();
    if (symbols1.size() > symbols2.size()) {
      return false;
    }

    for (int i = 0; i < symbols1.size(); i++) {
      if (!symbols1.get(i).equalsIgnoreCase(symbols2.get(i))) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected boolean unionSchemaEquals(Schema s1, Schema s2) {
    throw new UnsupportedOperationException("union not supported for projection equivalence");
  }

  @Override
  protected boolean validateFixed(Schema s1, Schema s2) {
    return s1.getFixedSize() == s2.getFixedSize();
  }
}
