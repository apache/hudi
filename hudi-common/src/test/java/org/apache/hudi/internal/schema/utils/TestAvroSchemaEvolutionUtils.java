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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.exception.HoodieNullSchemaTypeException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.InternalSchemaBuilder;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.action.TableChanges;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;

import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link AvroSchemaEvolutionUtils}.
 */
public class TestAvroSchemaEvolutionUtils {

  String schemaStr = "{\"type\":\"record\",\"name\":\"newTableName\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"data\","
      + "\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"preferences\",\"type\":[\"null\","
      + "{\"type\":\"record\",\"name\":\"preferences\",\"namespace\":\"newTableName\",\"fields\":[{\"name\":\"feature1\","
      + "\"type\":\"boolean\"},{\"name\":\"feature2\",\"type\":[\"null\",\"boolean\"],\"default\":null}]}],"
      + "\"default\":null},{\"name\":\"locations\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"record\","
      + "\"name\":\"locations\",\"namespace\":\"newTableName\",\"fields\":[{\"name\":\"lat\",\"type\":\"float\"},{\"name\":\"long\","
      + "\"type\":\"float\"}]}}},{\"name\":\"points\",\"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\","
      + "{\"type\":\"record\",\"name\":\"points\",\"namespace\":\"newTableName\",\"fields\":[{\"name\":\"x\",\"type\":\"long\"},"
      + "{\"name\":\"y\",\"type\":\"long\"}]}]}],\"default\":null},{\"name\":\"doubles\",\"type\":{\"type\":\"array\",\"items\":\"double\"}},"
      + "{\"name\":\"properties\",\"type\":[\"null\",{\"type\":\"map\",\"values\":[\"null\",\"string\"]}],\"default\":null}]}";

  @Test
  public void testPrimitiveTypes() {
    Schema[] avroPrimitives = new Schema[] {
        Schema.create(Schema.Type.BOOLEAN),
        Schema.create(Schema.Type.INT),
        Schema.create(Schema.Type.LONG),
        Schema.create(Schema.Type.FLOAT),
        Schema.create(Schema.Type.DOUBLE),
        LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)),
        LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG)),
        LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)),
        Schema.create(Schema.Type.STRING),
        LogicalTypes.uuid().addToSchema(Schema.createFixed("t1.fixed", null, null, 16)),
        Schema.createFixed("t1.fixed", null, null, 12),
        Schema.create(Schema.Type.BYTES),
        LogicalTypes.decimal(9, 4).addToSchema(Schema.createFixed("t1.fixed", null, null, 4))};

    Type[] primitiveTypes = new Type[] {
        Types.BooleanType.get(),
        Types.IntType.get(),
        Types.LongType.get(),
        Types.FloatType.get(),
        Types.DoubleType.get(),
        Types.DateType.get(),
        Types.TimeType.get(),
        Types.TimestampType.get(),
        Types.StringType.get(),
        Types.UUIDType.get(),
        Types.FixedType.getFixed(12),
        Types.BinaryType.get(),
        Types.DecimalTypeFixed.get(9, 4, 4)
    };

    for (int i = 0; i < primitiveTypes.length; i++) {
      Type convertPrimitiveResult = AvroInternalSchemaConverter.convertToField(avroPrimitives[i]);
      Assertions.assertEquals(convertPrimitiveResult, primitiveTypes[i]);
      Schema convertResult = AvroInternalSchemaConverter.convert(primitiveTypes[i], "t1");
      Assertions.assertEquals(convertResult, avroPrimitives[i]);
    }
  }

  @Test
  public void testRecordAndPrimitiveTypes() {
    Types.RecordType record = Types.RecordType.get(Arrays.asList(new Types.Field[] {
        Types.Field.get(0, "bool", Types.BooleanType.get()),
        Types.Field.get(1, "int", Types.IntType.get()),
        Types.Field.get(2, "long", Types.LongType.get()),
        Types.Field.get(3, "float", Types.FloatType.get()),
        Types.Field.get(4, "double", Types.DoubleType.get()),
        Types.Field.get(5, "date", Types.DateType.get()),
        Types.Field.get(6, "time", Types.TimeType.get()),
        Types.Field.get(7, "timestamp", Types.TimestampType.get()),
        Types.Field.get(8, "string", Types.StringType.get()),
        Types.Field.get(9, "uuid", Types.UUIDType.get()),
        Types.Field.get(10, "fixed", Types.FixedType.getFixed(10)),
        Types.Field.get(11, "binary", Types.BinaryType.get()),
        Types.Field.get(12, "decimal", Types.DecimalTypeFixed.get(10, 2, 5))
    }));

    Schema schema = create("t1",
        new Schema.Field("bool", AvroInternalSchemaConverter.nullableSchema(Schema.create(Schema.Type.BOOLEAN)), null, JsonProperties.NULL_VALUE),
        new Schema.Field("int", AvroInternalSchemaConverter.nullableSchema(Schema.create(Schema.Type.INT)), null, JsonProperties.NULL_VALUE),
        new Schema.Field("long", AvroInternalSchemaConverter.nullableSchema(Schema.create(Schema.Type.LONG)), null, JsonProperties.NULL_VALUE),
        new Schema.Field("float", AvroInternalSchemaConverter.nullableSchema(Schema.create(Schema.Type.FLOAT)), null, JsonProperties.NULL_VALUE),
        new Schema.Field("double", AvroInternalSchemaConverter.nullableSchema(Schema.create(Schema.Type.DOUBLE)), null, JsonProperties.NULL_VALUE),
        new Schema.Field("date", AvroInternalSchemaConverter.nullableSchema(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))), null, JsonProperties.NULL_VALUE),
        new Schema.Field("time", AvroInternalSchemaConverter.nullableSchema(LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG))), null, JsonProperties.NULL_VALUE),
        new Schema.Field("timestamp", AvroInternalSchemaConverter.nullableSchema(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))), null, JsonProperties.NULL_VALUE),
        new Schema.Field("string", AvroInternalSchemaConverter.nullableSchema(Schema.create(Schema.Type.STRING)), null, JsonProperties.NULL_VALUE),
        new Schema.Field("uuid", AvroInternalSchemaConverter.nullableSchema(LogicalTypes.uuid().addToSchema(Schema.createFixed("t1.uuid.fixed", null, null, 16))), null, JsonProperties.NULL_VALUE),
        new Schema.Field("fixed", AvroInternalSchemaConverter.nullableSchema(Schema.createFixed("t1.fixed.fixed", null, null, 10)), null, JsonProperties.NULL_VALUE),
        new Schema.Field("binary", AvroInternalSchemaConverter.nullableSchema(Schema.create(Schema.Type.BYTES)), null, JsonProperties.NULL_VALUE),
        new Schema.Field("decimal", AvroInternalSchemaConverter.nullableSchema(LogicalTypes.decimal(10, 2)
            .addToSchema(Schema.createFixed("t1.decimal.fixed", null, null, 5))), null, JsonProperties.NULL_VALUE));
    Schema convertedSchema = AvroInternalSchemaConverter.convert(record, "t1");
    Assertions.assertEquals(convertedSchema, schema);
    Types.RecordType convertedRecord = AvroInternalSchemaConverter.convert(schema).getRecord();
    Assertions.assertEquals(convertedRecord, record);
  }

  private Schema create(String name, Schema.Field... fields) {
    return Schema.createRecord(name, null, null, false, Arrays.asList(fields));
  }

  @Test
  public void testArrayType() {
    Type arrayNestRecordType = Types.ArrayType.get(0, false,
        Types.RecordType.get(Arrays.asList(Types.Field.get(1, false, "a", Types.FloatType.get()),
            Types.Field.get(2, false, "b", Types.FloatType.get()))));

    Schema schema = SchemaBuilder.array().items(create("t1",
        new Schema.Field("a", Schema.create(Schema.Type.FLOAT), null, null),
        new Schema.Field("b", Schema.create(Schema.Type.FLOAT), null, null)));
    Schema convertedSchema = AvroInternalSchemaConverter.convert(arrayNestRecordType, "t1");
    Assertions.assertEquals(convertedSchema, schema);
    Types.ArrayType convertedRecord = (Types.ArrayType) AvroInternalSchemaConverter.convertToField(schema);
    Assertions.assertEquals(convertedRecord, arrayNestRecordType);
  }

  @Test
  public void testComplexConvert() {
    Schema schema = new Schema.Parser().parse(schemaStr);

    Types.RecordType recordType = Types.RecordType.get(Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "data", Types.StringType.get()),
        Types.Field.get(2, true, "preferences",
            Types.RecordType.get(Types.Field.get(7, false, "feature1",
                Types.BooleanType.get()), Types.Field.get(8, true, "feature2", Types.BooleanType.get()))),
        Types.Field.get(3, false, "locations", Types.MapType.get(9, 10, Types.StringType.get(),
            Types.RecordType.get(Types.Field.get(11, false, "lat", Types.FloatType.get()), Types.Field.get(12, false, "long", Types.FloatType.get())), false)),
        Types.Field.get(4, true, "points", Types.ArrayType.get(13, true,
            Types.RecordType.get(Types.Field.get(14, false, "x", Types.LongType.get()), Types.Field.get(15, false, "y", Types.LongType.get())))),
        Types.Field.get(5, false, "doubles", Types.ArrayType.get(16, false, Types.DoubleType.get())),
        Types.Field.get(6, true, "properties", Types.MapType.get(17, 18, Types.StringType.get(), Types.StringType.get()))
    );
    InternalSchema internalSchema = new InternalSchema(recordType);

    Type convertRecord = AvroInternalSchemaConverter.convert(schema).getRecord();
    Assertions.assertEquals(convertRecord, internalSchema.getRecord());
    Assertions.assertEquals(schema, AvroInternalSchemaConverter.convert(internalSchema, "newTableName"));
  }

  @Test
  public void testNullFieldType() {
    Schema schema = create("t1",
        new Schema.Field("nullField", Schema.create(Schema.Type.NULL), null, JsonProperties.NULL_VALUE));
    Throwable t = assertThrows(HoodieNullSchemaTypeException.class,
        () -> AvroInternalSchemaConverter.convert(schema));
    assertTrue(t.getMessage().contains("'t1.nullField'"));

    Schema schemaArray = create("t2",
        new Schema.Field("nullArray", Schema.createArray(Schema.create(Schema.Type.NULL)), null, null));
    t = assertThrows(HoodieNullSchemaTypeException.class,
        () -> AvroInternalSchemaConverter.convert(schemaArray));
    assertTrue(t.getMessage().contains("'t2.nullArray.element'"));

    Schema schemaMap = create("t3",
        new Schema.Field("nullMap", Schema.createMap(Schema.create(Schema.Type.NULL)), null, null));
    t = assertThrows(HoodieNullSchemaTypeException.class,
        () -> AvroInternalSchemaConverter.convert(schemaMap));
    assertTrue(t.getMessage().contains("'t3.nullMap.value'"));


    Schema schemaComplex = create("t4",
        new Schema.Field("complexField", Schema.createMap(
            create("nestedStruct",
                new Schema.Field("nestedArray", Schema.createArray(Schema.createMap(Schema.create(Schema.Type.NULL))),
                    null, null))), null, null));
    t = assertThrows(HoodieNullSchemaTypeException.class,
        () -> AvroInternalSchemaConverter.convert(schemaComplex));
    assertTrue(t.getMessage().contains("'t4.nestedStruct.nestedArray.element.value'"));
  }

  @Test
  public void testRefreshNewId() {
    Types.RecordType record = Types.RecordType.get(Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "data", Types.StringType.get()),
        Types.Field.get(2, true, "preferences",
            Types.RecordType.get(Types.Field.get(4, false, "feature1",
                Types.BooleanType.get()), Types.Field.get(5, true, "feature2", Types.BooleanType.get()))),
        Types.Field.get(3, false, "locations", Types.MapType.get(6, 7, Types.StringType.get(),
            Types.RecordType.get(Types.Field.get(8, false, "lat", Types.FloatType.get()), Types.Field.get(9, false, "long", Types.FloatType.get())), false))
    );
    AtomicInteger newId = new AtomicInteger(100);
    Types.RecordType recordWithNewId = (Types.RecordType) InternalSchemaBuilder.getBuilder().refreshNewId(record, newId);

    Types.RecordType newRecord = Types.RecordType.get(Types.Field.get(100, false, "id", Types.IntType.get()),
        Types.Field.get(101, true, "data", Types.StringType.get()),
        Types.Field.get(102, true, "preferences",
            Types.RecordType.get(Types.Field.get(104, false, "feature1",
                Types.BooleanType.get()), Types.Field.get(105, true, "feature2", Types.BooleanType.get()))),
        Types.Field.get(103, false, "locations", Types.MapType.get(106, 107, Types.StringType.get(),
            Types.RecordType.get(Types.Field.get(108, false, "lat", Types.FloatType.get()), Types.Field.get(109, false, "long", Types.FloatType.get())), false))
    );
    Assertions.assertEquals(newRecord, recordWithNewId);
  }

  @Test
  public void testFixNullOrdering() {
    Schema schema = SchemaTestUtil.getSchemaFromResource(TestAvroSchemaEvolutionUtils.class, "/nullWrong.avsc");
    Schema expectedSchema = SchemaTestUtil.getSchemaFromResource(TestAvroSchemaEvolutionUtils.class, "/nullRight.avsc");
    Assertions.assertEquals(expectedSchema, AvroInternalSchemaConverter.fixNullOrdering(schema));
    Assertions.assertEquals(expectedSchema, AvroInternalSchemaConverter.fixNullOrdering(expectedSchema));
  }

  @Test
  public void testFixNullOrderingSameSchemaCheck() {
    Schema schema = SchemaTestUtil.getSchemaFromResource(TestAvroSchemaEvolutionUtils.class, "/source_evolved.avsc");
    Assertions.assertEquals(schema, AvroInternalSchemaConverter.fixNullOrdering(schema));
  }

  public enum Enum {
    ENUM1, ENUM2
  }

  /**
   * test record data type changes.
   * int => long/float/double/string
   * long => float/double/string
   * float => double/String
   * double => String/Decimal
   * Decimal => Decimal/String
   * String => date/decimal
   * date => String
   * enum => String
   */
  @Test
  @Disabled
  public void testReWriteRecordWithTypeChanged() {
    String enumSchema = "{\"type\":\"enum\",\"name\":\"Enum\",\"namespace\":\"org.apache.hudi.internal.schema.utils.TestAvroSchemaEvolutionUtils$\",\"symbols\":[\"ENUM1\",\"ENUM2\"]}";
    Schema avroSchema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"h0_record\",\"namespace\":\"hoodie.h0\",\"fields\""
        + ":[{\"name\":\"id\",\"type\":[\"null\",\"int\"],\"default\":null},"
        + "{\"name\":\"comb\",\"type\":[\"null\",\"int\"],\"default\":null},"
        + "{\"name\":\"com1\",\"type\":[\"null\",\"int\"],\"default\":null},"
        + "{\"name\":\"col0\",\"type\":[\"null\",\"int\"],\"default\":null},"
        + "{\"name\":\"col1\",\"type\":[\"null\",\"long\"],\"default\":null},"
        + "{\"name\":\"col11\",\"type\":[\"null\",\"long\"],\"default\":null},"
        + "{\"name\":\"col12\",\"type\":[\"null\",\"long\"],\"default\":null},"
        + "{\"name\":\"col2\",\"type\":[\"null\",\"float\"],\"default\":null},"
        + "{\"name\":\"col21\",\"type\":[\"null\",\"float\"],\"default\":null},"
        + "{\"name\":\"col3\",\"type\":[\"null\",\"double\"],\"default\":null},"
        + "{\"name\":\"col31\",\"type\":[\"null\",\"double\"],\"default\":null},"
        + "{\"name\":\"col4\",\"type\":[\"null\",{\"type\":\"fixed\",\"name\":\"fixed\",\"namespace\":\"hoodie.h0.h0_record.col4\","
        + "\"size\":5,\"logicalType\":\"decimal\",\"precision\":10,\"scale\":4}],\"default\":null},"
        + "{\"name\":\"col41\",\"type\":[\"null\",{\"type\":\"fixed\",\"name\":\"fixed\",\"namespace\":\"hoodie.h0.h0_record.col41\","
        + "\"size\":5,\"logicalType\":\"decimal\",\"precision\":10,\"scale\":4}],\"default\":null},"
        + "{\"name\":\"col5\",\"type\":[\"null\",\"string\"],\"default\":null},"
        + "{\"name\":\"col51\",\"type\":[\"null\",\"string\"],\"default\":null},"
        + "{\"name\":\"col6\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}],\"default\":null},"
        + "{\"name\":\"col7\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}],\"default\":null},"
        + "{\"name\":\"col8\",\"type\":[\"null\",\"boolean\"],\"default\":null},"
        + "{\"name\":\"col9\",\"type\":[\"null\",\"bytes\"],\"default\":null},{\"name\":\"par\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}],\"default\":null},"
        + "{\"name\":\"enum\",\"type\":[\"null\"," + enumSchema + "],\"default\":null}"
        + "]}");
    // create a test record with avroSchema
    GenericData.Record avroRecord = new GenericData.Record(avroSchema);
    avroRecord.put("id", 1);
    avroRecord.put("comb", 100);
    avroRecord.put("com1", -100);
    avroRecord.put("col0", 256);
    avroRecord.put("col1", 1000L);
    avroRecord.put("col11", -100L);
    avroRecord.put("col12", 2000L);
    avroRecord.put("col2", -5.001f);
    avroRecord.put("col21", 5.001f);
    avroRecord.put("col3", 12.999d);
    avroRecord.put("col31", 9999.999d);
    Schema currentDecimalType = avroSchema.getField("col4").schema().getTypes().get(1);
    BigDecimal bd = new BigDecimal("123.456").setScale(((LogicalTypes.Decimal) currentDecimalType.getLogicalType()).getScale());
    avroRecord.put("col4", HoodieAvroUtils.DECIMAL_CONVERSION.toFixed(bd, currentDecimalType, currentDecimalType.getLogicalType()));
    Schema currentDecimalType1 = avroSchema.getField("col41").schema().getTypes().get(1);
    BigDecimal bd1 = new BigDecimal("7890.456").setScale(((LogicalTypes.Decimal) currentDecimalType1.getLogicalType()).getScale());
    avroRecord.put("col41", HoodieAvroUtils.DECIMAL_CONVERSION.toFixed(bd1, currentDecimalType1, currentDecimalType1.getLogicalType()));

    avroRecord.put("col5", "2011-01-01");
    avroRecord.put("col51", "199.342");
    avroRecord.put("col6", 18987);
    avroRecord.put("col7", 1640491505000000L);
    avroRecord.put("col8", false);
    ByteBuffer bb = ByteBuffer.wrap(new byte[] {97, 48, 53});
    avroRecord.put("col9", bb);
    avroRecord.put("enum", new GenericData.EnumSymbol(new Schema.Parser().parse(enumSchema), Enum.ENUM1));
    Assertions.assertEquals(GenericData.get().validate(avroSchema, avroRecord), true);
    InternalSchema internalSchema = AvroInternalSchemaConverter.convert(avroSchema);
    // do change type operation
    TableChanges.ColumnUpdateChange updateChange = TableChanges.ColumnUpdateChange.get(internalSchema);
    updateChange
        .updateColumnType("id", Types.LongType.get())
        .updateColumnType("comb", Types.FloatType.get())
        .updateColumnType("com1", Types.DoubleType.get())
        .updateColumnType("col0", Types.StringType.get())
        .updateColumnType("col1", Types.FloatType.get())
        .updateColumnType("col11", Types.DoubleType.get())
        .updateColumnType("col12", Types.StringType.get())
        .updateColumnType("col2", Types.DoubleType.get())
        .updateColumnType("col21", Types.StringType.get())
        .updateColumnType("col3", Types.StringType.get())
        .updateColumnType("col31", Types.DecimalTypeBytes.get(18, 9))
        .updateColumnType("col4", Types.DecimalTypeFixed.get(18, 9, 5))
        .updateColumnType("col41", Types.StringType.get())
        .updateColumnType("col5", Types.DateType.get())
        .updateColumnType("col51", Types.DecimalTypeBytes.get(18, 9))
        .updateColumnType("col6", Types.StringType.get())
        .updateColumnType("enum", Types.StringType.get());
    InternalSchema newSchema = SchemaChangeUtils.applyTableChanges2Schema(internalSchema, updateChange);
    Schema newAvroSchema = AvroInternalSchemaConverter.convert(newSchema, avroSchema.getFullName());
    GenericRecord newRecord = HoodieAvroUtils.rewriteRecordWithNewSchema(avroRecord, newAvroSchema, Collections.emptyMap());

    Assertions.assertEquals("ENUM1", newRecord.get("enum"));
    Assertions.assertEquals(GenericData.get().validate(newAvroSchema, newRecord), true);
  }

  @Test
  public void testReWriteNestRecord() {
    Types.RecordType record = Types.RecordType.get(Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "data", Types.StringType.get()),
        Types.Field.get(2, true, "preferences",
            Types.RecordType.get(Types.Field.get(5, false, "feature1",
                Types.BooleanType.get()), Types.Field.get(6, true, "feature2", Types.BooleanType.get()))),
        Types.Field.get(3, false,"doubles", Types.ArrayType.get(7, false, Types.DoubleType.get())),
        Types.Field.get(4, false, "locations", Types.MapType.get(8, 9, Types.StringType.get(),
            Types.RecordType.get(Types.Field.get(10, false, "lat", Types.FloatType.get()), Types.Field.get(11, false, "long", Types.FloatType.get())), false))
    );
    Schema schema = AvroInternalSchemaConverter.convert(record, "test1");
    GenericData.Record avroRecord = new GenericData.Record(schema);
    GenericData.get().validate(schema, avroRecord);
    avroRecord.put("id", 2);
    avroRecord.put("data", "xs");
    // fill record type
    GenericData.Record preferencesRecord = new GenericData.Record(AvroInternalSchemaConverter.convert(record.fieldType("preferences"), "test1.preferences"));
    preferencesRecord.put("feature1", false);
    preferencesRecord.put("feature2", true);
    Assertions.assertEquals(GenericData.get().validate(AvroInternalSchemaConverter.convert(record.fieldType("preferences"), "test1.preferences"), preferencesRecord), true);
    avroRecord.put("preferences", preferencesRecord);
    // fill mapType
    Map<String, GenericData.Record> locations = new HashMap<>();
    Schema mapSchema = AvroInternalSchemaConverter.convert(((Types.MapType)record.fieldByNameCaseInsensitive("locations").type()).valueType(), "test1.locations");
    GenericData.Record locationsValue = new GenericData.Record(mapSchema);
    locationsValue.put("lat", 1.2f);
    locationsValue.put("long", 1.4f);
    GenericData.Record locationsValue1 = new GenericData.Record(mapSchema);
    locationsValue1.put("lat", 2.2f);
    locationsValue1.put("long", 2.4f);
    locations.put("key1", locationsValue);
    locations.put("key2", locationsValue1);
    avroRecord.put("locations", locations);

    List<Double> doubles = new ArrayList<>();
    doubles.add(2.0d);
    doubles.add(3.0d);
    avroRecord.put("doubles", doubles);

    // do check
    Assertions.assertTrue(GenericData.get().validate(schema, avroRecord));
    // create newSchema
    Types.RecordType newRecord = Types.RecordType.get(
        Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "data", Types.StringType.get()),
        Types.Field.get(2, true, "preferences",
            Types.RecordType.get(
                Types.Field.get(5, false, "feature1", Types.BooleanType.get()),
                Types.Field.get(5, true, "featurex", Types.BooleanType.get()),
                Types.Field.get(6, true, "feature2", Types.BooleanType.get()))),
        Types.Field.get(3, false,"doubles", Types.ArrayType.get(7, false, Types.DoubleType.get())),
        Types.Field.get(4, false, "locations", Types.MapType.get(8, 9, Types.StringType.get(),
            Types.RecordType.get(
                Types.Field.get(10, true, "laty", Types.FloatType.get()),
                Types.Field.get(11, false, "long", Types.FloatType.get())), false)
        )
    );

    Schema newAvroSchema = AvroInternalSchemaConverter.convert(newRecord, schema.getName());
    GenericRecord newAvroRecord = HoodieAvroUtils.rewriteRecordWithNewSchema(avroRecord, newAvroSchema, Collections.emptyMap());
    // test the correctly of rewrite
    Assertions.assertEquals(GenericData.get().validate(newAvroSchema, newAvroRecord), true);

    // test rewrite with rename
    InternalSchema internalSchema = AvroInternalSchemaConverter.convert(schema);
    // do change rename operation
    TableChanges.ColumnUpdateChange updateChange = TableChanges.ColumnUpdateChange.get(internalSchema);
    updateChange
        .renameColumn("id", "idx")
        .renameColumn("data", "datax")
        .renameColumn("preferences.feature1", "f1")
        .renameColumn("preferences.feature2", "f2")
        .renameColumn("locations.value.lat", "lt");
    InternalSchema internalSchemaRename = SchemaChangeUtils.applyTableChanges2Schema(internalSchema, updateChange);
    Schema avroSchemaRename = AvroInternalSchemaConverter.convert(internalSchemaRename, schema.getFullName());
    Map<String, String> renameCols = InternalSchemaUtils.collectRenameCols(internalSchema, internalSchemaRename);
    GenericRecord avroRecordRename = HoodieAvroUtils.rewriteRecordWithNewSchema(avroRecord, avroSchemaRename, renameCols);
    // test the correctly of rewrite
    Assertions.assertEquals(GenericData.get().validate(avroSchemaRename, avroRecordRename), true);
  }

  @Test
  public void testEvolutionSchemaFromNewAvroSchema() {
    Types.RecordType oldRecord = Types.RecordType.get(
        Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "data", Types.StringType.get()),
        Types.Field.get(2, true, "preferences",
            Types.RecordType.get(
                Types.Field.get(5, false, "feature1", Types.BooleanType.get()),
                Types.Field.get(6, true, "featurex", Types.BooleanType.get()),
                Types.Field.get(7, true, "feature2", Types.BooleanType.get()))),
        Types.Field.get(3, false,"doubles", Types.ArrayType.get(8, false, Types.DoubleType.get())),
        Types.Field.get(4, false, "locations", Types.MapType.get(9, 10, Types.StringType.get(),
            Types.RecordType.get(
                Types.Field.get(11, false, "laty", Types.FloatType.get()),
                Types.Field.get(12, false, "long", Types.FloatType.get())), false)
        )
    );
    InternalSchema oldSchema = new InternalSchema(oldRecord);
    Types.RecordType evolvedRecord = Types.RecordType.get(
        Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "data", Types.StringType.get()),
        Types.Field.get(2, true, "preferences",
            Types.RecordType.get(
                Types.Field.get(5, false, "feature1", Types.BooleanType.get()),
                Types.Field.get(5, true, "featurex", Types.BooleanType.get()),
                Types.Field.get(6, true, "feature2", Types.BooleanType.get()),
                Types.Field.get(5, true, "feature3", Types.BooleanType.get()))),
        Types.Field.get(3, false,"doubles", Types.ArrayType.get(7, false, Types.DoubleType.get())),
        Types.Field.get(4, false, "locations", Types.MapType.get(8, 9, Types.StringType.get(),
            Types.RecordType.get(
                Types.Field.get(10, false, "laty", Types.FloatType.get()),
                Types.Field.get(11, false, "long", Types.FloatType.get())), false)
        ),
        Types.Field.get(0, false, "add1", Types.IntType.get()),
        Types.Field.get(2, true, "addStruct",
            Types.RecordType.get(
                Types.Field.get(5, false, "nest1", Types.BooleanType.get()),
                Types.Field.get(5, true, "nest2", Types.BooleanType.get())))
    );
    evolvedRecord = (Types.RecordType)InternalSchemaBuilder.getBuilder().refreshNewId(evolvedRecord, new AtomicInteger(0));
    Schema evolvedAvroSchema = AvroInternalSchemaConverter.convert(evolvedRecord, "test1");
    InternalSchema result = AvroSchemaEvolutionUtils.reconcileSchema(evolvedAvroSchema, oldSchema, false);
    Types.RecordType checkedRecord = Types.RecordType.get(
        Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "data", Types.StringType.get()),
        Types.Field.get(2, true, "preferences",
            Types.RecordType.get(
                Types.Field.get(5, false, "feature1", Types.BooleanType.get()),
                Types.Field.get(6, true, "featurex", Types.BooleanType.get()),
                Types.Field.get(7, true, "feature2", Types.BooleanType.get()),
                Types.Field.get(17, true, "feature3", Types.BooleanType.get()))),
        Types.Field.get(3, false,"doubles", Types.ArrayType.get(8, false, Types.DoubleType.get())),
        Types.Field.get(4, false, "locations", Types.MapType.get(9, 10, Types.StringType.get(),
            Types.RecordType.get(
                Types.Field.get(11, false, "laty", Types.FloatType.get()),
                Types.Field.get(12, false, "long", Types.FloatType.get())), false)
        ),
        Types.Field.get(13, true, "add1", Types.IntType.get()),
        Types.Field.get(14, true, "addStruct",
            Types.RecordType.get(
                Types.Field.get(15, false, "nest1", Types.BooleanType.get()),
                Types.Field.get(16, true, "nest2", Types.BooleanType.get())))
    );
    Assertions.assertEquals(result.getRecord(), checkedRecord);
  }

  @Test
  public void testReconcileSchema() {
    // simple schema test
    // a: boolean, b: int, c: long, d: date
    Schema schema = create("simple",
        new Schema.Field("a", AvroInternalSchemaConverter.nullableSchema(Schema.create(Schema.Type.BOOLEAN)), null, JsonProperties.NULL_VALUE),
        new Schema.Field("b", AvroInternalSchemaConverter.nullableSchema(Schema.create(Schema.Type.INT)), null, JsonProperties.NULL_VALUE),
        new Schema.Field("c", AvroInternalSchemaConverter.nullableSchema(Schema.create(Schema.Type.LONG)), null, JsonProperties.NULL_VALUE),
        new Schema.Field("d", AvroInternalSchemaConverter.nullableSchema(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))), null, JsonProperties.NULL_VALUE));
    // a: boolean, c: long, c_1: long, d: date
    Schema incomingSchema = create("simpleIncoming",
        new Schema.Field("a", AvroInternalSchemaConverter.nullableSchema(Schema.create(Schema.Type.BOOLEAN)), null, JsonProperties.NULL_VALUE),
        new Schema.Field("a1", AvroInternalSchemaConverter.nullableSchema(Schema.create(Schema.Type.LONG)), null, JsonProperties.NULL_VALUE),
        new Schema.Field("c", AvroInternalSchemaConverter.nullableSchema(Schema.create(Schema.Type.LONG)), null, JsonProperties.NULL_VALUE),
        new Schema.Field("c1", AvroInternalSchemaConverter.nullableSchema(Schema.create(Schema.Type.LONG)), null, JsonProperties.NULL_VALUE),
        new Schema.Field("c2", AvroInternalSchemaConverter.nullableSchema(Schema.create(Schema.Type.LONG)), null, JsonProperties.NULL_VALUE),
        new Schema.Field("d", AvroInternalSchemaConverter.nullableSchema(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))), null, JsonProperties.NULL_VALUE),
        new Schema.Field("d1", AvroInternalSchemaConverter.nullableSchema(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))), null, JsonProperties.NULL_VALUE),
        new Schema.Field("d2", AvroInternalSchemaConverter.nullableSchema(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))), null, JsonProperties.NULL_VALUE));

    Schema simpleCheckSchema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"simple\",\"fields\":[{\"name\":\"a\",\"type\":[\"null\",\"boolean\"],\"default\":null},"
        + "{\"name\":\"b\",\"type\":[\"null\",\"int\"],\"default\":null},"
        + "{\"name\":\"c\",\"type\":[\"null\",\"long\"],\"default\":null},"
        + "{\"name\":\"d\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}],\"default\":null},"
        + "{\"name\":\"a1\",\"type\":[\"null\",\"long\"],\"default\":null},"
        + "{\"name\":\"c1\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"c2\",\"type\":[\"null\",\"long\"],\"default\":null},"
        + "{\"name\":\"d1\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}],\"default\":null},"
        + "{\"name\":\"d2\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}],\"default\":null}]}");

    Schema simpleReconcileSchema = AvroInternalSchemaConverter.convert(AvroSchemaEvolutionUtils
        .reconcileSchema(incomingSchema, AvroInternalSchemaConverter.convert(schema), false), "schemaNameFallback");
    Assertions.assertEquals(simpleCheckSchema, simpleReconcileSchema);
  }

  @Test
  public void testNotEvolveSchemaIfReconciledSchemaUnchanged() {
    // a: boolean, c: long, c_1: long, d: date
    Schema oldSchema = create("simple",
        new Schema.Field("a", AvroInternalSchemaConverter.nullableSchema(Schema.create(Schema.Type.BOOLEAN)), null, JsonProperties.NULL_VALUE),
        new Schema.Field("b", AvroInternalSchemaConverter.nullableSchema(Schema.create(Schema.Type.INT)), null, JsonProperties.NULL_VALUE),
        new Schema.Field("c", AvroInternalSchemaConverter.nullableSchema(Schema.create(Schema.Type.LONG)), null, JsonProperties.NULL_VALUE),
        new Schema.Field("d", AvroInternalSchemaConverter.nullableSchema(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))), null, JsonProperties.NULL_VALUE));
    // incoming schema is part of old schema
    // a: boolean, b: int, c: long
    Schema incomingSchema = create("simple",
        new Schema.Field("a", AvroInternalSchemaConverter.nullableSchema(Schema.create(Schema.Type.BOOLEAN)), null, JsonProperties.NULL_VALUE),
        new Schema.Field("b", AvroInternalSchemaConverter.nullableSchema(Schema.create(Schema.Type.INT)), null, JsonProperties.NULL_VALUE),
        new Schema.Field("c", AvroInternalSchemaConverter.nullableSchema(Schema.create(Schema.Type.LONG)), null, JsonProperties.NULL_VALUE));

    InternalSchema oldInternalSchema = AvroInternalSchemaConverter.convert(oldSchema);
    // set a non-default schema id for old table schema, e.g., 2.
    oldInternalSchema.setSchemaId(2);
    InternalSchema evolvedSchema = AvroSchemaEvolutionUtils.reconcileSchema(incomingSchema, oldInternalSchema, false);
    // the evolved schema should be the old table schema, since there is no type change at all.
    Assertions.assertEquals(oldInternalSchema, evolvedSchema);
  }
}
