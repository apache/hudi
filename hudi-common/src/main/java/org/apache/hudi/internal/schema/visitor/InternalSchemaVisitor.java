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

package org.apache.hudi.internal.schema.visitor;

import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;

import java.util.List;

/**
 * Base class of schema visitor.
 */
public abstract class InternalSchemaVisitor<T> {

  public void beforeField(Types.Field field) {
  }

  public void afterField(Types.Field field) {
  }

  public void beforeArrayElement(Types.Field elementField) {
    beforeField(elementField);
  }

  public void afterArrayElement(Types.Field elementField) {
    afterField(elementField);
  }

  public void beforeMapKey(Types.Field keyField) {
    beforeField(keyField);
  }

  public void afterMapKey(Types.Field keyField) {
    afterField(keyField);
  }

  public void beforeMapValue(Types.Field valueField) {
    beforeField(valueField);
  }

  public void afterMapValue(Types.Field valueField) {
    afterField(valueField);
  }

  public T schema(InternalSchema schema, T recordResult) {
    return null;
  }

  public T record(Types.RecordType record, List<T> fieldResults) {
    return null;
  }

  public T field(Types.Field field, T fieldResult) {
    return null;
  }

  public T array(Types.ArrayType array, T elementResult) {
    return null;
  }

  public T map(Types.MapType map, T keyResult, T valueResult) {
    return null;
  }

  public T primitive(Type.PrimitiveType primitive) {
    return null;
  }
}

