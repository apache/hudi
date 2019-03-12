package com.uber.hoodie.bench.generator;

import static junit.framework.TestCase.assertEquals;

import com.uber.hoodie.common.util.collection.Pair;
import org.apache.avro.Schema;
import org.junit.Test;


public class TestGenericRecordPayloadEstimator {

  @Test
  public void testSimpleSchemaSize() throws Exception {
    Schema schema = new Schema.Parser().parse(getClass().getClassLoader()
        .getResourceAsStream("hoodie-bench-config/source.avsc"));
    GenericRecordFullPayloadSizeEstimator estimator =
        new GenericRecordFullPayloadSizeEstimator(schema);
    Pair<Integer, Integer> estimateAndNumComplexFields = estimator.typeEstimateAndNumComplexFields();
    assertEquals(estimateAndNumComplexFields.getRight().intValue(), 0);
    assertEquals(estimateAndNumComplexFields.getLeft().intValue(), 156);
  }

  @Test
  public void testComplexSchemaSize() throws Exception {
    Schema schema = new Schema.Parser().parse(getClass().getClassLoader()
        .getResourceAsStream("hoodie-bench-config/complex-source.avsc"));
    GenericRecordFullPayloadSizeEstimator estimator =
        new GenericRecordFullPayloadSizeEstimator(schema);
    Pair<Integer, Integer> estimateAndNumComplexFields = estimator.typeEstimateAndNumComplexFields();
    assertEquals(estimateAndNumComplexFields.getRight().intValue(), 1);
    assertEquals(estimateAndNumComplexFields.getLeft().intValue(), 1278);
  }

}
