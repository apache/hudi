package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.schema.RowBasedSchemaProvider;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertSame;

public class TestInputBatch {

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void getSchemaProviderShouldThrowException() {
    final InputBatch<String> inputBatch = new InputBatch<>(Option.of("foo"), null, null);
    exceptionRule.expect(HoodieException.class);
    exceptionRule.expectMessage("Please provide a valid schema provider class!");
    inputBatch.getSchemaProvider();
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
