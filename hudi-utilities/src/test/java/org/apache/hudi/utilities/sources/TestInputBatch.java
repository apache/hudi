package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.schema.RowBasedSchemaProvider;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestInputBatch {

  @Test
  public void getSchemaProviderShouldThrowException() {
    final InputBatch<String> inputBatch = new InputBatch<>(Option.of("foo"), null, null);
    Throwable t = assertThrows(HoodieException.class, inputBatch::getSchemaProvider);
    assertEquals("Please provide a valid schema provider class!", t.getMessage());
  }

  @Test
  public void getSchemaProviderShouldReturnNullSchemaProvider() {
    final InputBatch<String> inputBatch = new InputBatch<>(Option.empty(), null, null);
    SchemaProvider schemaProvider = inputBatch.getSchemaProvider();
    assertTrue(schemaProvider instanceof InputBatch.NullSchemaProvider);
  }

  @Test
  public void getSchemaProviderShouldReturnGivenSchemaProvider() {
    SchemaProvider schemaProvider = new RowBasedSchemaProvider(null);
    final InputBatch<String> inputBatch = new InputBatch<>(Option.of("foo"), null, schemaProvider);
    assertSame(schemaProvider, inputBatch.getSchemaProvider());
  }
}
