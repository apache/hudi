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

package org.apache.hudi.io.storage;

import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.execution.arrow.ArrowFieldWriter;
import org.apache.spark.sql.execution.arrow.ArrowWriter;
import org.apache.spark.sql.execution.arrow.ArrayWriter;
import org.apache.spark.sql.execution.arrow.ArrowWriter$;
import org.apache.spark.sql.execution.arrow.DecimalWriter;
import org.apache.spark.sql.execution.arrow.MapWriter;
import org.apache.spark.sql.execution.arrow.StringWriter;
import org.apache.spark.sql.execution.arrow.StructWriter;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.ArrowUtils$;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

/**
 * Makes the Spark {@link ArrowWriter} that the Vortex writer uses. It is the same as
 * {@link ArrowWriter#create(VectorSchemaRoot)}, but it writes string and decimal values, also in
 * structs, arrays and maps, with less work:
 * <ul>
 *   <li>Spark writes a string through a heap {@code ByteBuffer}. Arrow then makes a duplicate of the
 *   buffer and sets its position and limit for each value. This writer copies the bytes from the
 *   array of the string.</li>
 *   <li>Spark writes each decimal as a {@link BigDecimal}, and Arrow converts its unscaled value to
 *   a new byte array. This writer writes an unscaled value that fits in a long directly.</li>
 * </ul>
 */
final class VortexArrowFieldWriters {

  private VortexArrowFieldWriters() {
  }

  static ArrowWriter create(VectorSchemaRoot root) {
    List<FieldVector> vectors = root.getFieldVectors();
    ArrowFieldWriter[] writers = new ArrowFieldWriter[vectors.size()];
    for (int i = 0; i < writers.length; i++) {
      FieldVector vector = vectors.get(i);
      vector.allocateNew();
      writers[i] = fieldWriter(vector);
    }

    return new ArrowWriter(root, writers);
  }

  private static ArrowFieldWriter fieldWriter(ValueVector vector) {
    if (vector instanceof VarCharVector) {
      return new ArrayStringWriter((VarCharVector) vector);
    }

    if (vector instanceof DecimalVector) {
      DecimalVector decimals = (DecimalVector) vector;
      return new LongDecimalWriter(decimals, decimals.getPrecision(), decimals.getScale());
    }

    // Make the writers of nested types here, so that their values also use the writers above. Use
    // the Spark type of the field, as Spark does: a vector of a special type, for example a variant
    // in a struct vector, goes to Spark.
    DataType type = ArrowUtils$.MODULE$.fromArrowField(vector.getField());
    if (type instanceof MapType && vector instanceof MapVector) {
      StructVector entries = (StructVector) ((MapVector) vector).getDataVector();
      return new MapWriter((MapVector) vector, entries,
          fieldWriter(entries.getChild(MapVector.KEY_NAME)),
          fieldWriter(entries.getChild(MapVector.VALUE_NAME)));
    }

    if (type instanceof ArrayType && vector instanceof ListVector) {
      return new ArrayWriter((ListVector) vector, fieldWriter(((ListVector) vector).getDataVector()));
    }

    if (type instanceof StructType && vector instanceof StructVector) {
      StructVector struct = (StructVector) vector;
      ArrowFieldWriter[] children = new ArrowFieldWriter[struct.size()];
      for (int i = 0; i < children.length; i++) {
        children[i] = fieldWriter(struct.getChildByOrdinal(i));
      }
      return new StructWriter(struct, children);
    }

    return ArrowWriter$.MODULE$.createFieldWriter(vector);
  }

  /** Writes a string from the byte array that holds it, without a {@code ByteBuffer}. */
  private static final class ArrayStringWriter extends StringWriter {

    private final VarCharVector vector;

    private ArrayStringWriter(VarCharVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public void setValue(SpecializedGetters input, int ordinal) {
      UTF8String value = input.getUTF8String(ordinal);
      Object base = value.getBaseObject();
      if (base instanceof byte[]) {
        int start = (int) (value.getBaseOffset() - Platform.BYTE_ARRAY_OFFSET);
        vector.setSafe(count(), (byte[]) base, start, value.numBytes());
      } else {
        vector.setSafe(count(), value.getBytes());
      }
    }
  }

  /**
   * Writes a decimal with an unscaled value that fits in a long as that long. Other values use the
   * {@link BigDecimal} path of Spark. A value that does not fit the precision of the column is null,
   * as in Spark.
   */
  private static final class LongDecimalWriter extends DecimalWriter {

    private final DecimalVector vector;
    private final int precision;
    private final int scale;

    private LongDecimalWriter(DecimalVector vector, int precision, int scale) {
      super(vector, precision, scale);
      this.vector = vector;
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public void setValue(SpecializedGetters input, int ordinal) {
      Decimal decimal = input.getDecimal(ordinal, precision, scale);
      if (!decimal.changePrecision(precision, scale)) {
        setNull();
        return;
      }

      if (precision <= Decimal.MAX_LONG_DIGITS()) {
        vector.setSafe(count(), decimal.toUnscaledLong());
        return;
      }

      BigDecimal value = decimal.toJavaBigDecimal();
      BigInteger unscaled = value.unscaledValue();
      if (unscaled.bitLength() < Long.SIZE) {
        vector.setSafe(count(), unscaled.longValue());
      } else {
        vector.setSafe(count(), value);
      }
    }
  }
}
