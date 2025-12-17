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
import org.apache.hudi.internal.schema.Types;

import lombok.AllArgsConstructor;
import lombok.Getter;

import javax.xml.bind.DatatypeConverter;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.UUID;

@AllArgsConstructor
@Getter
public class Literal<T> extends LeafExpression {

  public static <V> Literal from(V value) {
    if (value instanceof Integer || value instanceof Short) {
      return new Literal<>(value, Types.IntType.get());
    }

    if (value instanceof Byte) {
      return new Literal<>(((Byte)value).intValue(), Types.IntType.get());
    }

    if (value instanceof Long) {
      return new Literal<>(value, Types.LongType.get());
    }

    if (value instanceof Boolean) {
      return new Literal<>(value, Types.BooleanType.get());
    }

    if (value instanceof Double) {
      return new Literal<>(value, Types.DoubleType.get());
    }

    if (value instanceof Float) {
      return new Literal<>(value, Types.FloatType.get());
    }

    if (value instanceof BigDecimal) {
      BigDecimal decimal = (BigDecimal) value;
      return new Literal<>(value, Types.DecimalType.get(decimal.precision(), decimal.scale()));
    }

    if (value instanceof CharSequence) {
      return new Literal<>(value, Types.StringType.get());
    }

    if (value instanceof byte[]) {
      byte[] bytes = (byte[]) value;
      return new Literal<>(ByteBuffer.wrap(bytes), Types.FixedType.getFixed(bytes.length));
    }

    if (value instanceof ByteBuffer) {
      return new Literal<>(value, Types.BinaryType.get());
    }

    if (value instanceof UUID) {
      return new Literal<>(value, Types.UUIDType.get());
    }

    throw new IllegalArgumentException("Cannot convert value from class "
        + value.getClass().getName() + " to Literal");
  }

  @Getter
  private final T value;
  private final Type dataType;

  @Override
  public Object eval(StructLike data) {
    return value;
  }

  @Override
  public <T> T accept(ExpressionVisitor<T> exprVisitor) {
    return exprVisitor.visitLiteral(this);
  }

  @Override
  public String toString() {
    if (value == null) {
      return "null";
    }

    if (value instanceof ByteBuffer) {
      return DatatypeConverter.printHexBinary(((ByteBuffer)value).array());
    }

    return value.toString();
  }
}
