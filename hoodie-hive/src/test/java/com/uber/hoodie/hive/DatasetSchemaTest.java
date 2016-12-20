/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.hive;

import com.uber.hoodie.hive.client.SchemaUtil;
import com.uber.hoodie.hive.model.HoodieDatasetReference;
import com.uber.hoodie.hive.model.SchemaDifference;
import com.uber.hoodie.hive.util.TestUtil;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.model.InitializationError;
import parquet.schema.MessageType;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class DatasetSchemaTest {
    @Before
    public void setUp() throws IOException, InterruptedException {
        TestUtil.setUp();
    }

    @Test
    public void testSchemaDiff() throws IOException, InitializationError {
        HoodieDatasetReference metadata = TestUtil
            .createDataset("test1", "/tmp/hdfs/DatasetSchemaTest/testSchema/", 5, "/nation.schema");
        HoodieHiveSchemaSyncTask schema =
            HoodieHiveSchemaSyncTask.newBuilder().withReference(metadata)
                .withConfiguration(TestUtil.hDroneConfiguration).build();
        SchemaDifference diff = schema.getSchemaDifference();
        assertEquals("There should be 4 columns to be added", 4, diff.getAddColumnTypes().size());
        assertEquals("No update columns expected", 0, diff.getUpdateColumnTypes().size());
        assertEquals("No delete columns expected", 0, diff.getDeleteColumns().size());
        schema.sync();

        schema = HoodieHiveSchemaSyncTask.newBuilder().withReference(metadata)
            .withConfiguration(TestUtil.hDroneConfiguration).build();
        diff = schema.getSchemaDifference();
        assertEquals("After sync, there should not be any new columns to add", 0,
            diff.getAddColumnTypes().size());
        assertEquals("After sync, there should not be any new columns to update", 0,
            diff.getUpdateColumnTypes().size());
        assertEquals("After sync, there should not be any new columns to delete", 0,
            diff.getDeleteColumns().size());
    }

    @Test
    public void testSchemaEvolution() throws IOException, InitializationError {
        int initialPartitionsCount = 5;
        HoodieDatasetReference metadata = TestUtil
            .createDataset("test1", "/tmp/hdfs/DatasetSchemaTest/testSchema/",
                initialPartitionsCount, "/nation.schema");
        HoodieHiveSchemaSyncTask schema =
            HoodieHiveSchemaSyncTask.newBuilder().withReference(metadata)
                .withConfiguration(TestUtil.hDroneConfiguration).build();
        schema.sync();

        schema = HoodieHiveSchemaSyncTask.newBuilder().withReference(metadata)
            .withConfiguration(TestUtil.hDroneConfiguration).build();
        SchemaDifference diff = schema.getSchemaDifference();
        assertEquals("After sync, diff should be empty", true, diff.isEmpty());
        int newSchemaversion = 2;
        int newPartitionsCount = 2;
        TestUtil.evolveDataset(metadata, newPartitionsCount, "/nation_evolved.schema",
            DateTime.now().getMillis(), newSchemaversion);
        schema = HoodieHiveSchemaSyncTask.newBuilder().withReference(metadata)
            .withConfiguration(TestUtil.hDroneConfiguration).build();
        diff = schema.getSchemaDifference();
        assertEquals("Schema has evolved, there should be a diff", false, diff.isEmpty());
        assertEquals("Schema has evolved, there should be 1 column to add", 1,
            diff.getAddColumnTypes().size());
        assertEquals("Schema has evolved, there should be 1 column to update", 1,
            diff.getUpdateColumnTypes().size());
        assertEquals(0, diff.getDeleteColumns().size());
    }

    /**
     * Testing converting array types to Hive field declaration strings,
     * according to the Parquet-113 spec:
     * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
     */
    @Test
    public void testSchemaConvertArray() throws IOException {
        // Testing the 3-level annotation structure
        MessageType schema =
            parquet.schema.Types.buildMessage().optionalGroup().as(parquet.schema.OriginalType.LIST)
                .repeatedGroup().optional(PrimitiveType.PrimitiveTypeName.INT32).named("element")
                .named("list").named("int_list").named("ArrayOfInts");

        String schemaString = SchemaUtil.generateSchemaString(schema);
        assertEquals("`int_list` ARRAY< int>", schemaString);

        // A array of arrays
        schema =
            parquet.schema.Types.buildMessage().optionalGroup().as(parquet.schema.OriginalType.LIST)
                .repeatedGroup().requiredGroup().as(OriginalType.LIST).repeatedGroup()
                .required(PrimitiveType.PrimitiveTypeName.INT32).named("element").named("list")
                .named("element").named("list").named("int_list_list").named("ArrayOfArrayOfInts");

        schemaString = SchemaUtil.generateSchemaString(schema);
        assertEquals("`int_list_list` ARRAY< ARRAY< int>>", schemaString);

        // A list of integers
        schema =
            parquet.schema.Types.buildMessage().optionalGroup().as(parquet.schema.OriginalType.LIST)
                .repeated(PrimitiveType.PrimitiveTypeName.INT32).named("element").named("int_list")
                .named("ArrayOfInts");

        schemaString = SchemaUtil.generateSchemaString(schema);
        assertEquals("`int_list` ARRAY< int>", schemaString);

        // A list of structs with two fields
        schema =
            parquet.schema.Types.buildMessage().optionalGroup().as(parquet.schema.OriginalType.LIST)
                .repeatedGroup().required(PrimitiveType.PrimitiveTypeName.BINARY).named("str")
                .required(PrimitiveType.PrimitiveTypeName.INT32).named("num").named("element")
                .named("tuple_list").named("ArrayOfTuples");

        schemaString = SchemaUtil.generateSchemaString(schema);
        assertEquals("`tuple_list` ARRAY< STRUCT< `str` : binary, `num` : int>>", schemaString);

        // A list of structs with a single field
        // For this case, since the inner group name is "array", we treat the
        // element type as a one-element struct.
        schema =
            parquet.schema.Types.buildMessage().optionalGroup().as(parquet.schema.OriginalType.LIST)
                .repeatedGroup().required(PrimitiveType.PrimitiveTypeName.BINARY).named("str")
                .named("array").named("one_tuple_list").named("ArrayOfOneTuples");

        schemaString = SchemaUtil.generateSchemaString(schema);
        assertEquals("`one_tuple_list` ARRAY< STRUCT< `str` : binary>>", schemaString);

        // A list of structs with a single field
        // For this case, since the inner group name ends with "_tuple", we also treat the
        // element type as a one-element struct.
        schema =
            parquet.schema.Types.buildMessage().optionalGroup().as(parquet.schema.OriginalType.LIST)
                .repeatedGroup().required(PrimitiveType.PrimitiveTypeName.BINARY).named("str")
                .named("one_tuple_list_tuple").named("one_tuple_list").named("ArrayOfOneTuples2");

        schemaString = SchemaUtil.generateSchemaString(schema);
        assertEquals("`one_tuple_list` ARRAY< STRUCT< `str` : binary>>", schemaString);

        // A list of structs with a single field
        // Unlike the above two cases, for this the element type is the type of the
        // only field in the struct.
        schema =
            parquet.schema.Types.buildMessage().optionalGroup().as(parquet.schema.OriginalType.LIST)
                .repeatedGroup().required(PrimitiveType.PrimitiveTypeName.BINARY).named("str")
                .named("one_tuple_list").named("one_tuple_list").named("ArrayOfOneTuples3");

        schemaString = SchemaUtil.generateSchemaString(schema);
        assertEquals("`one_tuple_list` ARRAY< binary>", schemaString);

        // A list of maps
        schema =
            parquet.schema.Types.buildMessage().optionalGroup().as(parquet.schema.OriginalType.LIST)
                .repeatedGroup().as(OriginalType.MAP).repeatedGroup().as(OriginalType.MAP_KEY_VALUE)
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8)
                .named("string_key").required(PrimitiveType.PrimitiveTypeName.INT32)
                .named("int_value").named("key_value").named("array").named("map_list")
                .named("ArrayOfMaps");

        schemaString = SchemaUtil.generateSchemaString(schema);
        assertEquals("`map_list` ARRAY< MAP< string, int>>", schemaString);
    }
}
