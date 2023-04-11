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

import org.apache.hudi.internal.schema.Type;

public class AttributeReference extends LeafExpression {

  private final String name;
  private final Type type;
  private final boolean nullable;

  public AttributeReference(String name, Type type) {
    this.name = name;
    this.type = type;
    this.nullable = true;
  }

  public AttributeReference(String name, Type type, boolean nullable) {
    this.name = name;
    this.type = type;
    this.nullable = nullable;
  }

  public String getName() {
    return name;
  }

  @Override
  public Type getDataType() {
    return type;
  }

  public boolean isNullable() {
    return nullable;
  }

  @Override
  public <T> T accept(ExpressionVisitor<T> exprVisitor) {
    return exprVisitor.visitAttribute(this);
  }

  @Override
  public String toString() {
    return name + "[type: " + type.typeId().getName() + ", nullable: " + nullable + "]";
  }
}
