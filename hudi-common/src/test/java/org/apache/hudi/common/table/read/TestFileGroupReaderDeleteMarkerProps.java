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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_KEY;
import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_MARKER;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A table at version 9 or later persists a custom delete marker only under the
 * {@code hoodie.record.merge.property.} prefix, and {@link ConfigUtils#getMergeProps} is what strips that
 * prefix back to the plain {@code hoodie.payload.delete.field} / {@code .marker} keys that
 * {@link DeleteContext} reads. Read-path callers hand {@link HoodieFileGroupReader} the properties as they
 * come, so the reader is the only thing that can perform that merge before the schema handler is built.
 */
public class TestFileGroupReaderDeleteMarkerProps {

  private static final String DELETE_FIELD = "op";
  private static final String DELETE_VALUE = "D";

  private static final HoodieSchema TABLE_SCHEMA = HoodieSchema.fromAvroSchema(
      SchemaBuilder.record("rec").fields()
          .requiredString(HoodieRecord.RECORD_KEY_METADATA_FIELD)
          .requiredString("key")
          .requiredLong("ts")
          .requiredString(DELETE_FIELD)
          .endRecord());

  private static final HoodieSchema REQUESTED_SCHEMA = HoodieSchema.fromAvroSchema(
      SchemaBuilder.record("rec").fields()
          .requiredString("key")
          .endRecord());

  private static final HoodieSchema REQUESTED_SCHEMA_WITH_DELETE_FIELD = HoodieSchema.fromAvroSchema(
      SchemaBuilder.record("rec").fields()
          .requiredString("key")
          .requiredString(DELETE_FIELD)
          .endRecord());

  private static HoodieTableConfig versionNineTableConfigWithCustomDeleteMarker() {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(HoodieTableConfig.VERSION, String.valueOf(HoodieTableVersion.NINE.versionCode()));
    tableConfig.setValue(HoodieTableConfig.RECORD_MERGE_MODE, RecordMergeMode.COMMIT_TIME_ORDERING.name());
    tableConfig.setValue(RECORD_MERGE_PROPERTY_PREFIX + DELETE_KEY, DELETE_FIELD);
    tableConfig.setValue(RECORD_MERGE_PROPERTY_PREFIX + DELETE_MARKER, DELETE_VALUE);
    return tableConfig;
  }

  /**
   * The properties a query engine hands the reader: the table's own properties, in which the custom delete
   * marker only exists in its prefixed form. Nothing un-prefixes them before the reader is built.
   */
  private static TypedProperties readerProps(HoodieTableConfig tableConfig) {
    return TypedProperties.copy(tableConfig.getProps());
  }

  /**
   * Builds a file group reader over a log-file-bearing split and returns the schema handler it installed on
   * the reader context.
   */
  private static FileGroupReaderSchemaHandler<String> schemaHandlerOfReader(TypedProperties properties,
                                                                           HoodieTableConfig tableConfig) {
    return schemaHandlerOfReader(properties, tableConfig, REQUESTED_SCHEMA);
  }

  private static FileGroupReaderSchemaHandler<String> schemaHandlerOfReader(TypedProperties properties,
                                                                           HoodieTableConfig tableConfig,
                                                                           HoodieSchema requestedSchema) {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class, RETURNS_DEEP_STUBS);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(metaClient.getBasePath()).thenReturn(new StoragePath("file:///tmp/hoodie_test_table"));

    AtomicReference<FileGroupReaderSchemaHandler<String>> installedHandler = new AtomicReference<>();
    HoodieReaderContext<String> readerContext = mock(HoodieReaderContext.class, RETURNS_DEEP_STUBS);
    // The reader context reports log files so the schema handler takes the merge path; the split itself stays
    // empty because the reader is only built, never iterated - no file is ever opened.
    when(readerContext.getHasLogFiles()).thenReturn(true);
    when(readerContext.getHasBootstrapBaseFile()).thenReturn(false);
    when(readerContext.getInstantRange()).thenReturn(Option.empty());
    when(readerContext.getMergeMode()).thenReturn(RecordMergeMode.COMMIT_TIME_ORDERING);
    when(readerContext.getRecordContext().supportsParquetRowIndex()).thenReturn(false);
    doAnswer(invocation -> {
      installedHandler.set(invocation.getArgument(0));
      return null;
    }).when(readerContext).setSchemaHandler(any());
    when(readerContext.getSchemaHandler()).thenAnswer(invocation -> installedHandler.get());

    HoodieFileGroupReader.<String>builder()
        .withReaderContext(readerContext)
        .withHoodieTableMetaClient(metaClient)
        .withLatestCommitTime("001")
        .withDataSchema(TABLE_SCHEMA)
        .withRequestedSchema(requestedSchema)
        .withProps(properties)
        .withLogFiles(Stream.empty())
        .withPartitionPath("")
        .withStart(0L)
        .withLength(Long.MAX_VALUE)
        .build();

    return installedHandler.get();
  }

  private static List<String> requiredFieldNames(FileGroupReaderSchemaHandler<String> handler) {
    return handler.getRequiredSchema().getFields().stream().map(field -> field.name()).collect(Collectors.toList());
  }

  /**
   * Sanity check on the premise: the un-prefixing lives in getMergeProps, so the properties the reader is
   * handed do not carry the plain delete keys at all.
   */
  @Test
  public void mergePropsIsWhatUnprefixesTheDeleteMarker() {
    HoodieTableConfig tableConfig = versionNineTableConfigWithCustomDeleteMarker();
    TypedProperties props = readerProps(tableConfig);

    assertNull(props.getProperty(DELETE_KEY), "reader props must not carry the unprefixed delete key");
    assertEquals(DELETE_FIELD, ConfigUtils.getMergeProps(props, tableConfig).getProperty(DELETE_KEY));

    assertTrue(new DeleteContext(props, TABLE_SCHEMA).getCustomDeleteMarkerKeyValue().isEmpty(),
        "DeleteContext built from the reader props sees no custom delete marker");
    assertTrue(new DeleteContext(ConfigUtils.getMergeProps(props, tableConfig), TABLE_SCHEMA)
            .getCustomDeleteMarkerKeyValue().isPresent(),
        "DeleteContext built from merged props sees the custom delete marker");
  }

  /**
   * The reader must merge the table's record-merge properties in before building the schema handler.
   * Otherwise the handler's DeleteContext carries no marker, and since FileGroupRecordBuffer takes its
   * DeleteContext from that very handler, custom deletes stop being recognised on the whole read path.
   */
  @Test
  public void readerResolvesTheCustomDeleteMarkerFromPrefixedTableProps() {
    HoodieTableConfig tableConfig = versionNineTableConfigWithCustomDeleteMarker();

    FileGroupReaderSchemaHandler<String> handler = schemaHandlerOfReader(readerProps(tableConfig), tableConfig);

    assertTrue(handler.getDeleteContext().getCustomDeleteMarkerKeyValue().isPresent(),
        "the schema handler the reader installs must resolve the custom delete marker");
    assertTrue(requiredFieldNames(handler).contains(DELETE_FIELD),
        "required schema must contain the custom delete column " + DELETE_FIELD);
  }

  /**
   * The delete column becomes a mandatory field, so it must not be appended twice when the query already
   * asked for it.
   */
  @Test
  public void deleteColumnIsNotDuplicatedWhenAlreadyRequested() {
    HoodieTableConfig tableConfig = versionNineTableConfigWithCustomDeleteMarker();

    FileGroupReaderSchemaHandler<String> handler =
        schemaHandlerOfReader(readerProps(tableConfig), tableConfig, REQUESTED_SCHEMA_WITH_DELETE_FIELD);

    assertEquals(1, requiredFieldNames(handler).stream().filter(DELETE_FIELD::equals).count(),
        "the custom delete column must appear exactly once in the required schema");
  }

  /**
   * Control: a reader handed properties that already carry the plain delete keys - which is how the existing
   * engine tests are configured - resolves the marker either way, which is why this defect stayed hidden.
   */
  @Test
  public void readerAlsoResolvesAMarkerSuppliedInPlainForm() {
    HoodieTableConfig tableConfig = versionNineTableConfigWithCustomDeleteMarker();
    TypedProperties props = readerProps(tableConfig);
    props.setProperty(DELETE_KEY, DELETE_FIELD);
    props.setProperty(DELETE_MARKER, DELETE_VALUE);

    FileGroupReaderSchemaHandler<String> handler = schemaHandlerOfReader(props, tableConfig);

    assertTrue(handler.getDeleteContext().getCustomDeleteMarkerKeyValue().isPresent());
    assertTrue(requiredFieldNames(handler).contains(DELETE_FIELD));
  }
}
