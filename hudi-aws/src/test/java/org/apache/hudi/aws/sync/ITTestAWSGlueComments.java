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

package org.apache.hudi.aws.sync;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.sync.common.model.FieldSchema;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for AWS Glue comment synchronization functionality.
 * These tests validate that comments are properly set, updated, and synchronized
 * between Hudi table schemas and AWS Glue catalog using moto for AWS service mocking.
 */
public class ITTestAWSGlueComments extends AWSGlueIntegrationTestBase {

  private static final String COMMENT_TABLE_NAME = "test_comments_table";

  @BeforeEach
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // Hudi table is already created in the base class
  }

  /**
   * Tests basic comment setting functionality through the updateTableComments method.
   * Validates that comments are properly synchronized to AWS Glue catalog.
   */
  @Test
  public void testUpdateTableCommentsBasic() throws Exception {
    // Create table in Glue without comments
    createTableInGlue(COMMENT_TABLE_NAME, createColumnsWithoutComments());

    // Prepare field schemas with comments
    List<FieldSchema> fieldsWithComments = createFieldSchemasWithComments();

    // Get current field schemas from metastore (without comments)
    List<FieldSchema> fieldsFromMetastore = glueSync.getMetastoreFieldSchemas(COMMENT_TABLE_NAME);

    // Test the updateTableComments method
    boolean updated = glueSync.updateTableComments(COMMENT_TABLE_NAME, fieldsFromMetastore, fieldsWithComments);

    // Should return true indicating comments were updated
    assertTrue(updated, "updateTableComments should return true when comments are added");

    // Verify comments were actually set in Glue
    verifyCommentsInGlue(COMMENT_TABLE_NAME, createExpectedComments());
  }

  /**
   * Tests that updateTableComments returns false when no changes are needed.
   */
  @Test
  public void testUpdateTableCommentsNoChange() throws Exception {
    // Create table in Glue with comments already set
    createTableInGlue(COMMENT_TABLE_NAME, createColumnsWithComments());

    // Prepare field schemas with same comments
    List<FieldSchema> fieldsWithComments = createFieldSchemasWithComments();

    // Get current field schemas from metastore (with comments)
    List<FieldSchema> fieldsFromMetastore = glueSync.getMetastoreFieldSchemas(COMMENT_TABLE_NAME);

    // Test the updateTableComments method
    boolean updated = glueSync.updateTableComments(COMMENT_TABLE_NAME, fieldsFromMetastore, fieldsWithComments);

    // Should return false indicating no changes were made
    assertFalse(updated, "updateTableComments should return false when no changes are needed");
  }

  /**
   * Tests comment removal functionality.
   */
  @Test
  public void testRemoveTableComments() throws Exception {
    // Create table in Glue with comments
    createTableInGlue(COMMENT_TABLE_NAME, createColumnsWithComments());

    // Prepare field schemas without comments (removing them)
    List<FieldSchema> fieldsWithoutComments = createFieldSchemasWithoutComments();

    // Get current field schemas from metastore (with comments)
    List<FieldSchema> fieldsFromMetastore = glueSync.getMetastoreFieldSchemas(COMMENT_TABLE_NAME);

    // Test the updateTableComments method to remove comments
    boolean updated = glueSync.updateTableComments(COMMENT_TABLE_NAME, fieldsFromMetastore, fieldsWithoutComments);

    // Should return true indicating comments were removed
    assertTrue(updated, "updateTableComments should return true when comments are removed");

    // Verify comments were actually removed in Glue
    verifyNoCommentsInGlue(COMMENT_TABLE_NAME);
  }

  /**
   * Tests partial comment updates (some fields have comments, others don't).
   */
  @Test
  public void testPartialCommentUpdate() throws Exception {
    // Create table in Glue without comments
    createTableInGlue(COMMENT_TABLE_NAME, createColumnsWithoutComments());

    // Prepare field schemas with partial comments (only some fields have comments)
    List<FieldSchema> fieldsWithPartialComments = createFieldSchemasWithPartialComments();

    // Get current field schemas from metastore
    List<FieldSchema> fieldsFromMetastore = glueSync.getMetastoreFieldSchemas(COMMENT_TABLE_NAME);

    // Test the updateTableComments method
    boolean updated = glueSync.updateTableComments(COMMENT_TABLE_NAME, fieldsFromMetastore, fieldsWithPartialComments);

    // Should return true indicating comments were updated
    assertTrue(updated, "updateTableComments should return true when partial comments are added");

    // Verify partial comments were set correctly
    verifyPartialCommentsInGlue(COMMENT_TABLE_NAME);
  }

  // Helper methods for test setup and verification

  private void createTableInGlue(String tableName, List<Column> columns) {
    StorageDescriptor.Builder storageDescriptorBuilder = StorageDescriptor.builder()
        .columns(columns)
        .location(tablePath)
        .inputFormat("org.apache.hadoop.mapred.TextInputFormat")
        .outputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
        .serdeInfo(SerDeInfo.builder()
            .serializationLibrary("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
            .build());

    TableInput tableInput = TableInput.builder()
        .name(tableName)
        .storageDescriptor(storageDescriptorBuilder.build())
        .build();

    CreateTableRequest createTableRequest = CreateTableRequest.builder()
        .catalogId(CATALOG_ID)
        .databaseName(getDatabaseName())
        .tableInput(tableInput)
        .build();

    glueClient.createTable(createTableRequest);
  }

  private List<Column> createColumnsWithoutComments() {
    List<Column> columns = new ArrayList<>();
    columns.add(Column.builder().name("field1").type("string").build());
    columns.add(Column.builder().name("field2").type("string").build());
    columns.add(Column.builder().name("name").type("string").build());
    columns.add(Column.builder().name("favorite_number").type("bigint").build());
    columns.add(Column.builder().name("favorite_color").type("string").build());
    columns.add(Column.builder().name("favorite_movie").type("string").build());
    return columns;
  }

  private List<Column> createColumnsWithComments() {
    List<Column> columns = new ArrayList<>();
    columns.add(Column.builder().name("field1").type("string").comment("Field 1 comment").build());
    columns.add(Column.builder().name("field2").type("string").comment("Field 2 comment").build());
    columns.add(Column.builder().name("name").type("string").comment("Name comment").build());
    columns.add(Column.builder().name("favorite_number").type("bigint").comment("Favorite number comment").build());
    columns.add(Column.builder().name("favorite_color").type("string").comment("Favorite color comment").build());
    columns.add(Column.builder().name("favorite_movie").type("string").comment("Favorite movie comment").build());
    return columns;
  }

  private List<FieldSchema> createFieldSchemasWithComments() {
    List<FieldSchema> fields = new ArrayList<>();
    fields.add(new FieldSchema("field1", "string", Option.of("Field 1 comment")));
    fields.add(new FieldSchema("field2", "string", Option.of("Field 2 comment")));
    fields.add(new FieldSchema("name", "string", Option.of("Name comment")));
    fields.add(new FieldSchema("favorite_number", "bigint", Option.of("Favorite number comment")));
    fields.add(new FieldSchema("favorite_color", "string", Option.of("Favorite color comment")));
    fields.add(new FieldSchema("favorite_movie", "string", Option.of("Favorite movie comment")));
    return fields;
  }

  private List<FieldSchema> createFieldSchemasWithoutComments() {
    List<FieldSchema> fields = new ArrayList<>();
    fields.add(new FieldSchema("field1", "string", Option.empty()));
    fields.add(new FieldSchema("field2", "string", Option.empty()));
    fields.add(new FieldSchema("name", "string", Option.empty()));
    fields.add(new FieldSchema("favorite_number", "bigint", Option.empty()));
    fields.add(new FieldSchema("favorite_color", "string", Option.empty()));
    fields.add(new FieldSchema("favorite_movie", "string", Option.empty()));
    return fields;
  }

  private List<FieldSchema> createFieldSchemasWithPartialComments() {
    List<FieldSchema> fields = new ArrayList<>();
    fields.add(new FieldSchema("field1", "string", Option.of("Field 1 comment")));
    fields.add(new FieldSchema("field2", "string", Option.empty())); // No comment
    fields.add(new FieldSchema("name", "string", Option.of("Name comment")));
    fields.add(new FieldSchema("favorite_number", "bigint", Option.empty())); // No comment
    fields.add(new FieldSchema("favorite_color", "string", Option.of("Favorite color comment")));
    fields.add(new FieldSchema("favorite_movie", "string", Option.empty())); // No comment
    return fields;
  }

  private Map<String, String> createExpectedComments() {
    Map<String, String> expectedComments = new HashMap<>();
    expectedComments.put("field1", "Field 1 comment");
    expectedComments.put("field2", "Field 2 comment");
    expectedComments.put("name", "Name comment");
    expectedComments.put("favorite_number", "Favorite number comment");
    expectedComments.put("favorite_color", "Favorite color comment");
    expectedComments.put("favorite_movie", "Favorite movie comment");
    return expectedComments;
  }

  private void verifyCommentsInGlue(String tableName, Map<String, String> expectedComments) {
    GetTableRequest getTableRequest = GetTableRequest.builder()
        .catalogId(CATALOG_ID)
        .databaseName(getDatabaseName())
        .name(tableName)
        .build();

    GetTableResponse response = glueClient.getTable(getTableRequest);
    Table table = response.table();

    for (Column column : table.storageDescriptor().columns()) {
      String expectedComment = expectedComments.get(column.name());
      assertNotNull(expectedComment, "Expected comment for column: " + column.name());
      assertEquals(expectedComment, column.comment(),
          "Comment mismatch for column: " + column.name());
    }
  }

  private void verifyNoCommentsInGlue(String tableName) {
    GetTableRequest getTableRequest = GetTableRequest.builder()
        .catalogId(CATALOG_ID)
        .databaseName(getDatabaseName())
        .name(tableName)
        .build();

    GetTableResponse response = glueClient.getTable(getTableRequest);
    Table table = response.table();

    for (Column column : table.storageDescriptor().columns()) {
      assertNull(column.comment(), "Expected no comment for column: " + column.name());
    }
  }

  private void verifyPartialCommentsInGlue(String tableName) {
    GetTableRequest getTableRequest = GetTableRequest.builder()
        .catalogId(CATALOG_ID)
        .databaseName(getDatabaseName())
        .name(tableName)
        .build();

    GetTableResponse response = glueClient.getTable(getTableRequest);
    Table table = response.table();

    Map<String, String> expectedPartialComments = new HashMap<>();
    expectedPartialComments.put("field1", "Field 1 comment");
    expectedPartialComments.put("name", "Name comment");
    expectedPartialComments.put("favorite_color", "Favorite color comment");

    for (Column column : table.storageDescriptor().columns()) {
      String expectedComment = expectedPartialComments.get(column.name());
      if (expectedComment != null) {
        assertEquals(expectedComment, column.comment(),
            "Comment mismatch for column: " + column.name());
      } else {
        assertNull(column.comment(), "Expected no comment for column: " + column.name());
      }
    }
  }
}