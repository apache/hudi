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

package org.apache.parquet.avro;

import org.apache.hudi.common.avro.HoodieAvroUtils;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.SchemaRepair;
import org.apache.parquet.schema.Type;

import java.math.BigDecimal;

/**
 * Wraps a parquet-avro {@link AvroRecordConverter} so that decimal columns stored as INT32 or INT64
 * read as the Avro decimal of the read schema.
 *
 * <p>parquet-avro has no Avro counterpart for an integer-backed decimal and hands the unscaled
 * integer to the Avro field as is. This wrapper keeps the parent's converters for every other field
 * and substitutes {@link IntegerDecimalConverter} for such columns, at the top level and inside nested
 * records. Such columns inside arrays, maps or multi-type unions are rejected rather than read wrong.
 *
 * <p>It wraps rather than subclasses because the parent's constructors differ across the parquet
 * versions Hudi builds against; the record itself is still created and filled by the parent.
 */
class IntegerDecimalRecordConverter extends GroupConverter {

  private final AvroRecordConverter<?> delegate;
  private final Converter[] converters;

  IntegerDecimalRecordConverter(AvroRecordConverter<?> delegate, GroupType parquetSchema, Schema avroSchema, GenericData model) {
    this.delegate = delegate;
    this.converters = new Converter[parquetSchema.getFieldCount()];
    for (int i = 0; i < parquetSchema.getFieldCount(); i++) {
      Type parquetField = parquetSchema.getType(i);
      Converter inherited = delegate.getConverter(i);
      Schema.Field avroField = SchemaRepair.containsIntegerBackedDecimal(parquetField) ? findField(avroSchema, parquetField.getName()) : null;
      if (avroField == null) {
        converters[i] = inherited;
        continue;
      }
      Schema fieldSchema = AvroSchemaConverter.getNonNull(avroField.schema());
      ParentValueContainer container = new ParentValueContainer() {
        @Override
        public void add(Object value) {
          delegate.set(avroField.name(), avroField.pos(), value);
        }
      };
      if (parquetField.isPrimitive()) {
        converters[i] = new IntegerDecimalConverter(container, parquetField.asPrimitiveType(), fieldSchema, model);
      } else if (inherited instanceof AvroRecordConverter && fieldSchema.getType() == Schema.Type.RECORD) {
        converters[i] = new IntegerDecimalRecordConverter((AvroRecordConverter<?>) inherited, parquetField.asGroupType(), fieldSchema, model);
      } else {
        throw new UnsupportedOperationException("Reading a decimal stored as INT32 or INT64 under field '" + parquetField.getName()
            + "' of Avro type " + fieldSchema.getType() + " is not yet supported by the Avro parquet reader; "
            + "only top-level columns and record members are");
      }
    }
  }

  /** Resolves a parquet field name against the Avro schema the way parquet-avro does, honoring aliases. */
  private static Schema.Field findField(Schema avroSchema, String name) {
    Schema.Field field = avroSchema.getField(name);
    if (field != null) {
      return field;
    }
    return avroSchema.getFields().stream().filter(f -> f.aliases().contains(name)).findFirst().orElse(null);
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  public void start() {
    delegate.start();
  }

  @Override
  public void end() {
    delegate.end();
  }

  /**
   * Materializer that reads through a parent parquet-avro materializer with its root converter wrapped.
   */
  static final class Materializer<T> extends RecordMaterializer<T> {
    private final RecordMaterializer<T> delegate;
    private final GroupConverter root;

    Materializer(RecordMaterializer<T> delegate, MessageType requestedSchema, Schema avroSchema, GenericData model) {
      GroupConverter parentRoot = delegate.getRootConverter();
      if (!(parentRoot instanceof AvroRecordConverter)) {
        throw new IllegalStateException("Expected parquet-avro's record converter, found " + parentRoot.getClass().getName());
      }
      this.delegate = delegate;
      this.root = new IntegerDecimalRecordConverter((AvroRecordConverter<?>) parentRoot, requestedSchema, avroSchema, model);
    }

    @Override
    public T getCurrentRecord() {
      return delegate.getCurrentRecord();
    }

    @Override
    public void skipCurrentRecord() {
      delegate.skipCurrentRecord();
    }

    @Override
    public GroupConverter getRootConverter() {
      return root;
    }
  }

  /**
   * Converts an INT32 or INT64 decimal column to the Avro decimal of the read schema. The unscaled
   * integer carries the file's scale; the Avro schema's scale wins, and a change that would drop
   * digits fails rather than round.
   */
  static final class IntegerDecimalConverter extends PrimitiveConverter {
    private final ParentValueContainer parent;
    private final String columnName;
    private final Schema avroSchema;
    private final LogicalTypes.Decimal avroDecimal;
    private final int fileScale;
    private final boolean int32;
    private Object[] dictionary;

    IntegerDecimalConverter(ParentValueContainer parent, PrimitiveType parquetType, Schema avroSchema, GenericData model) {
      if (!(avroSchema.getLogicalType() instanceof LogicalTypes.Decimal)) {
        throw new IllegalArgumentException("Parquet decimal column " + parquetType.getName()
            + " requires an Avro decimal in the read schema, found " + avroSchema);
      }
      this.parent = ParentValueContainer.getConversionContainer(parent, model.getConversionFor(avroSchema.getLogicalType()), avroSchema);
      this.columnName = parquetType.getName();
      this.avroSchema = avroSchema;
      this.avroDecimal = (LogicalTypes.Decimal) avroSchema.getLogicalType();
      this.fileScale = ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) parquetType.getLogicalTypeAnnotation()).getScale();
      this.int32 = parquetType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT32;
    }

    @Override
    public void addInt(int value) {
      parent.add(convert(value));
    }

    @Override
    public void addLong(long value) {
      parent.add(convert(value));
    }

    @Override
    public boolean hasDictionarySupport() {
      return true;
    }

    @Override
    public void setDictionary(Dictionary dict) {
      dictionary = new Object[dict.getMaxId() + 1];
      for (int i = 0; i <= dict.getMaxId(); i++) {
        dictionary[i] = convert(int32 ? dict.decodeToInt(i) : dict.decodeToLong(i));
      }
    }

    @Override
    public void addValueFromDictionary(int dictionaryId) {
      parent.add(dictionary[dictionaryId]);
    }

    private Object convert(long unscaled) {
      BigDecimal value = BigDecimal.valueOf(unscaled, fileScale);
      if (fileScale != avroDecimal.getScale()) {
        try {
          value = value.setScale(avroDecimal.getScale());
        } catch (ArithmeticException e) {
          throw new IllegalArgumentException("Decimal column " + columnName + " value " + value
              + " cannot be read at scale " + avroDecimal.getScale() + " without losing digits", e);
        }
      }
      return avroSchema.getType() == Schema.Type.FIXED
          ? HoodieAvroUtils.DECIMAL_CONVERSION.toFixed(value, avroSchema, avroDecimal)
          : HoodieAvroUtils.DECIMAL_CONVERSION.toBytes(value, avroSchema, avroDecimal);
    }
  }
}
