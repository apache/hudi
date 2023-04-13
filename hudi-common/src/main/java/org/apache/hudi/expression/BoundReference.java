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

public class BoundReference extends LeafExpression {

  private final int ordinal;
  private final Type type;

  public BoundReference(int ordinal, Type type) {
    this.ordinal = ordinal;
    this.type = type;
  }

  @Override
  public Type getDataType() {
    return type;
  }

  @Override
  public Object eval(StructLike data) {
    return data.get(ordinal, this.type.typeId().getClassTag());
  }

  @Override
  public <T> T accept(ExpressionVisitor<T> exprVisitor) {
    return exprVisitor.visitBoundReference(this);
  }

  @Override
  public String toString() {
    return "boundReference[ordinal: " + ordinal + ", type: "
        + type.typeId().getName() + "]";
  }
}
