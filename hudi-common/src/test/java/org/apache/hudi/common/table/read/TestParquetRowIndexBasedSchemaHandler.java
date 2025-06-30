package org.apache.hudi.common.table.read;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.read.ParquetRowIndexBasedSchemaHandler.getPositionalMergeField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestParquetRowIndexBasedSchemaHandler extends SchemaHandlerTestBase {

  @Test
  public void testCowBootstrapWithPositionMerge() {
    HoodieReaderContext<String> readerContext = mockReaderContext(true, false, true, false, null);
    HoodieTableConfig hoodieTableConfig = mock(HoodieTableConfig.class);
    Schema requestedSchema = generateProjectionSchema("begin_lat", "tip_history", "_hoodie_record_key", "rider");
    when(readerContext.getNeedsBootstrapMerge()).thenReturn(true);
    FileGroupReaderSchemaHandler schemaHandler = createSchemaHandler(readerContext, DATA_SCHEMA, requestedSchema, hoodieTableConfig, true);
    //meta cols must go first in the required schema
    Schema expectedRequiredSchema = generateProjectionSchema("_hoodie_record_key", "begin_lat", "tip_history", "rider");
    assertEquals(expectedRequiredSchema, schemaHandler.getRequiredSchema());
    Pair<List<Schema.Field>, List<Schema.Field>> bootstrapFields = schemaHandler.getBootstrapRequiredFields();
    assertEquals(Arrays.asList(getField("_hoodie_record_key"), getPositionalMergeField()), bootstrapFields.getLeft());
    assertEquals(Arrays.asList(getField("begin_lat"), getField("tip_history"), getField("rider"), getPositionalMergeField()), bootstrapFields.getRight());

    when(readerContext.getNeedsBootstrapMerge()).thenReturn(false);
    schemaHandler = createSchemaHandler(readerContext, DATA_SCHEMA, DATA_COLS_ONLY_SCHEMA, hoodieTableConfig, true);
    assertEquals(DATA_COLS_ONLY_SCHEMA, schemaHandler.getRequiredSchema());
    bootstrapFields = schemaHandler.getBootstrapRequiredFields();
    assertTrue(bootstrapFields.getLeft().isEmpty());
    assertEquals(Arrays.asList(getField("begin_lat"), getField("tip_history"), getField("rider")), bootstrapFields.getRight());

    when(readerContext.getNeedsBootstrapMerge()).thenReturn(false);
    schemaHandler = createSchemaHandler(readerContext, DATA_SCHEMA, META_COLS_ONLY_SCHEMA, hoodieTableConfig, true);
    assertEquals(META_COLS_ONLY_SCHEMA, schemaHandler.getRequiredSchema());
    bootstrapFields = schemaHandler.getBootstrapRequiredFields();
    assertEquals(Arrays.asList(getField("_hoodie_commit_seqno"), getField("_hoodie_record_key")), bootstrapFields.getLeft());
    assertTrue(bootstrapFields.getRight().isEmpty());
  }

  private static Stream<Arguments> testMorParams() {
    return testMorParams(true);
  }

  @ParameterizedTest
  @MethodSource("testMorParams")
  public void testMor(RecordMergeMode mergeMode,
                      boolean hasPrecombine,
                      boolean isProjectionCompatible,
                      boolean mergeUseRecordPosition,
                      boolean supportsParquetRowIndex,
                      boolean hasBuiltInDelete) throws IOException {
    super.testMor(mergeMode, hasPrecombine, isProjectionCompatible, mergeUseRecordPosition, supportsParquetRowIndex, hasBuiltInDelete);
  }

  @ParameterizedTest
  @MethodSource("testMorParams")
  public void testMorBootstrap(RecordMergeMode mergeMode,
                               boolean hasPrecombine,
                               boolean isProjectionCompatible,
                               boolean mergeUseRecordPosition,
                               boolean supportsParquetRowIndex,
                               boolean hasBuiltInDelete) throws IOException {
    super.testMorBootstrap(mergeMode, hasPrecombine, isProjectionCompatible, mergeUseRecordPosition, supportsParquetRowIndex, hasBuiltInDelete);
  }
}
