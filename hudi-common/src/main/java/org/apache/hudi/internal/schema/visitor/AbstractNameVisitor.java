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

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

public abstract class AbstractNameVisitor<T> extends InternalSchemaVisitor<T> {
  protected final Deque<String> fieldNames = new LinkedList<>();
  protected final T nameToId;

  protected AbstractNameVisitor(T nameToId) {
    this.nameToId = nameToId;
  }

  @Override
  public void afterField(Types.Field field) {
    fieldNames.pop();
  }

  @Override
  public void afterArrayElement(Types.Field elementField) {
    fieldNames.pop();
  }

  @Override
  public void afterMapKey(Types.Field keyField) {
    fieldNames.pop();
  }

  @Override
  public void afterMapValue(Types.Field valueField) {
    fieldNames.pop();
  }

  @Override
  public T record(Types.RecordType record, List<T> fieldResults) {
    return nameToId;
  }

  @Override
  public T primitive(Type.PrimitiveType primitive) {
    return nameToId;
  }

  @Override
  public T schema(InternalSchema schema, T recordResult) {
    return nameToId;
  }
}
