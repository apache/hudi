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
 * KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import scala.Function1;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class TestAvroConversionUtils {

  @Test
  void testInternalRowToAvroConverterHandlesVectorFields() {
    HoodieSchema hoodieSchema = HoodieSchema.createRecord(
        "test_record",
        null,
        "org.apache.hudi.test",
        false,
        Arrays.asList(
            HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
            HoodieSchemaField.of(
                "embedding",
                HoodieSchema.createVector(4, HoodieSchema.Vector.VectorElementType.FLOAT),
                null,
                null)));

    StructType catalystSchema = new StructType(new StructField[] {
        new StructField("id", DataTypes.StringType, false, Metadata.empty()),
        new StructField("embedding", new ArrayType(DataTypes.FloatType, false), false, Metadata.empty())
    });

    GenericInternalRow row = new GenericInternalRow(2);
    row.update(0, UTF8String.fromString("id-1"));
    row.update(1, new GenericArrayData(new float[] {1.0f, -2.5f, 3.25f, 4.5f}));

    Function1<org.apache.spark.sql.catalyst.InternalRow, GenericRecord> converter =
        AvroConversionUtils$.MODULE$.createInternalRowToAvroConverter(
            catalystSchema,
            hoodieSchema.toAvroSchema(),
            hoodieSchema.isNullable());

    GenericRecord record = converter.apply(row);
    GenericData.Fixed fixed = assertInstanceOf(GenericData.Fixed.class, record.get("embedding"));

    ByteBuffer buffer = ByteBuffer.allocate(16).order(HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER);
    buffer.putFloat(1.0f);
    buffer.putFloat(-2.5f);
    buffer.putFloat(3.25f);
    buffer.putFloat(4.5f);

    assertArrayEquals(buffer.array(), fixed.bytes());
  }
}
