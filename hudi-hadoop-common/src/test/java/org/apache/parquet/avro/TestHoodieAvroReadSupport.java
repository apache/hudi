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

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.ConversionPatterns;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestHoodieAvroReadSupport {
  private final Type legacyListType = ConversionPatterns.listType(Type.Repetition.REQUIRED, "legacyList",
      Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.REPEATED).named("double_field"));
  private final Type legacyListTypeWithObject = ConversionPatterns.listType(Type.Repetition.REQUIRED, "legacyList",
      new MessageType("foo", Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.REPEATED).named("double_field")));
  private final Type listType = ConversionPatterns.listOfElements(Type.Repetition.REQUIRED, "newList",
      Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.REPEATED).named("element"));
  private final Type integerField = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED).named("int_field");
  private final Type legacyMapType = new GroupType(Type.Repetition.OPTIONAL, "my_map", OriginalType.MAP, new GroupType(Type.Repetition.REPEATED, "map",
      Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED).named("key"),
      Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED).named("value")));
  private final Type mapType = ConversionPatterns.stringKeyMapType(Type.Repetition.OPTIONAL, "newMap",
      Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED).named("value"));
  private final Configuration configuration = mock(Configuration.class);

  @Test
  void fileContainsLegacyList() {
    when(configuration.getBoolean(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE_DEFAULT)).thenReturn(false);
    MessageType messageType = new MessageType("LegacyList", integerField, legacyListType, mapType);
    new HoodieAvroReadSupport<>().init(configuration, Collections.emptyMap(), messageType);
    verify(configuration).set(eq(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE), eq("true"), anyString());
  }

  @Test
  void fileContainsLegacyListWithElements() {
    when(configuration.getBoolean(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE_DEFAULT)).thenReturn(false);
    MessageType messageType = new MessageType("LegacyList", integerField, legacyListTypeWithObject, mapType);
    new HoodieAvroReadSupport<>().init(configuration, Collections.emptyMap(), messageType);
    verify(configuration).set(eq(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE), eq("true"), anyString());
  }

  @Test
  void fileContainsLegacyMap() {
    when(configuration.getBoolean(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE_DEFAULT)).thenReturn(false);
    MessageType messageType = new MessageType("LegacyList", integerField, legacyMapType, listType);
    new HoodieAvroReadSupport<>().init(configuration, Collections.emptyMap(), messageType);
    verify(configuration).set(eq(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE), eq("true"), anyString());
  }

  @Test
  void fileContainsNewListAndMap() {
    when(configuration.get(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE)).thenReturn(null);
    MessageType messageType = new MessageType("newFieldTypes", listType, mapType, integerField);
    new HoodieAvroReadSupport<>().init(configuration, Collections.emptyMap(), messageType);
    verify(configuration).set(eq(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE), eq("false"), anyString());
  }

  @Test
  void fileContainsNoListOrMap() {
    MessageType messageType = new MessageType("noListOrMap", integerField);
    new HoodieAvroReadSupport<>().init(configuration, Collections.emptyMap(), messageType);
    verify(configuration, never()).set(anyString(), anyString());
  }

  @Test
  void nestedLegacyList() {
    when(configuration.getBoolean(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE_DEFAULT)).thenReturn(false);
    MessageType nested = new MessageType("Nested", integerField, legacyListType);
    MessageType messageType = new MessageType("NestedList", integerField, nested);
    new HoodieAvroReadSupport<>().init(configuration, Collections.emptyMap(), messageType);
    verify(configuration).set(eq(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE), eq("true"), anyString());
  }

  @Test
  void nestedLegacyMap() {
    when(configuration.getBoolean(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE_DEFAULT)).thenReturn(false);
    MessageType nested = new MessageType("Nested", integerField, legacyMapType);
    MessageType messageType = new MessageType("NestedList", integerField, nested);
    new HoodieAvroReadSupport<>().init(configuration, Collections.emptyMap(), messageType);
    verify(configuration).set(eq(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE), eq("true"), anyString());
  }

  @Test
  void mapWithLegacyList() {
    when(configuration.getBoolean(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE_DEFAULT)).thenReturn(false);
    Type listValue = ConversionPatterns.listType(Type.Repetition.REQUIRED, "value",
        Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.REPEATED).named("double_field"));
    Type mapWithList = ConversionPatterns.stringKeyMapType(Type.Repetition.OPTIONAL, "newMap", listValue);
    MessageType messageType = new MessageType("NestedList", integerField, mapWithList);
    new HoodieAvroReadSupport<>().init(configuration, Collections.emptyMap(), messageType);
    verify(configuration).set(eq(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE), eq("true"), anyString());
  }
}
