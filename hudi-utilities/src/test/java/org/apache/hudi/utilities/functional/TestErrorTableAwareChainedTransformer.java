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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.functional;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.exception.HoodieTransformException;
import org.apache.hudi.utilities.transform.ErrorTableAwareChainedTransformer;
import org.apache.hudi.utilities.transform.Transformer;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.apache.hudi.config.HoodieErrorTableConfig.ERROR_TABLE_ENABLED;
import static org.apache.hudi.utilities.streamer.BaseErrorTableWriter.ERROR_TABLE_CURRUPT_RECORD_COL_NAME;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

@Tag("functional")
public class TestErrorTableAwareChainedTransformer extends SparkClientFunctionalTestHarness {

  @Test
  public void testForErrorTableConfig() {
    Dataset<Row> original = getTestDataset();

    Transformer t1 = getErrorEventHandlerTransformer();
    Transformer t2 = (jsc, sparkSession, dataset, properties) -> dataset.withColumn("foo", dataset.col("foo").cast(IntegerType));
    ErrorTableAwareChainedTransformer transformer = new ErrorTableAwareChainedTransformer(Arrays.asList(t1, t2));
    TypedProperties properties = new TypedProperties();
    properties.setProperty(ERROR_TABLE_ENABLED.key(), "true");
    Dataset<Row> transformed = transformer.apply(jsc(), spark(), original, properties);
    List<Row> rows = transformed.collectAsList();
    assertArrayEquals(new String[]{"foo", ERROR_TABLE_CURRUPT_RECORD_COL_NAME}, transformed.columns());
    assertEquals(2, transformed.filter(new Column(ERROR_TABLE_CURRUPT_RECORD_COL_NAME)
        .isNotNull()).count());
    assertEquals(100, rows.get(0).getInt(0));
    assertEquals(200, rows.get(1).getInt(0));

  }

  @Test
  public void testForErrorRecordColumn() {
    Dataset<Row> original = getTestDataset();

    Transformer t1 = getErrorEventHandlerTransformer();
    Transformer t2 = getErrorRecordColumnDropTransformer();
    Transformer t3 = (jsc, sparkSession, dataset, properties) -> dataset.withColumn("foo",
        dataset.col("foo").cast(IntegerType));
    TypedProperties properties = new TypedProperties();
    properties.setProperty(ERROR_TABLE_ENABLED.key(), "true");
    ErrorTableAwareChainedTransformer transformer = new ErrorTableAwareChainedTransformer(Arrays.asList(t1, t2, t3));
    assertThrows(HoodieValidationException.class, () -> transformer.apply(jsc(), spark(), original, properties));
  }

  private Dataset<Row> getTestDataset() {
    StructType schema = DataTypes.createStructType(
        new StructField[]{
            createStructField("foo", StringType, false)
        });
    Row r1 = RowFactory.create("100");
    Row r2 = RowFactory.create("200");
    return spark().sqlContext().createDataFrame(Arrays.asList(r1, r2), schema);
  }

  private Transformer getErrorEventHandlerTransformer() {
    return (jsc, sparkSession, dataset, properties) -> {
      boolean isErrorTableEnabledInTransformer = properties.getBoolean(ERROR_TABLE_ENABLED.key(), ERROR_TABLE_ENABLED.defaultValue());
      if (isErrorTableEnabledInTransformer) {
        dataset = dataset.withColumn(ERROR_TABLE_CURRUPT_RECORD_COL_NAME, functions.when(new Column(ERROR_TABLE_CURRUPT_RECORD_COL_NAME)
                .isNull(),
            functions.lit("true")
        ).otherwise(new Column(ERROR_TABLE_CURRUPT_RECORD_COL_NAME)));
      }
      return dataset;
    };
  }

  private Transformer getErrorRecordColumnDropTransformer() {
    return (jsc, sparkSession, dataset, properties) -> dataset.select("foo");
  }

  @ParameterizedTest
  @ValueSource(strings = {
      // empty identifier
      ":org.apache.hudi.utilities.transform.FlatteningTransformer,T2:org.apache.hudi.utilities.transform.FlatteningTransformer",
      // same identifier
      "T1:org.apache.hudi.utilities.transform.FlatteningTransformer,T1:org.apache.hudi.utilities.transform.FlatteningTransformer",
      // Two colons in transformer config
      "T1::org.apache.hudi.utilities.transform.FlatteningTransformer",
      // either all transformers have identifier or none have
      "org.apache.hudi.utilities.transform.FlatteningTransformer,T1:org.apache.hudi.utilities.transform.FlatteningTransformer"
  })
  public void testErrorTableAwareChainedTransformerValidationFails(String transformerName) {
    assertThrows(HoodieTransformException.class,
        () -> new ErrorTableAwareChainedTransformer(Arrays.asList(transformerName.split(",")), Option::empty));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "T1:org.apache.hudi.utilities.transform.FlatteningTransformer,T2:org.apache.hudi.utilities.transform.FlatteningTransformer",
      "T2:org.apache.hudi.utilities.transform.FlatteningTransformer,T1:org.apache.hudi.utilities.transform.FlatteningTransformer",
      "abc:org.apache.hudi.utilities.transform.FlatteningTransformer,def:org.apache.hudi.utilities.transform.FlatteningTransformer",
      "org.apache.hudi.utilities.transform.FlatteningTransformer,org.apache.hudi.utilities.transform.FlatteningTransformer"
  })
  public void testErrorTableAwareChainedTransformerValidationPasses(String transformerName) {
    ErrorTableAwareChainedTransformer transformer = new ErrorTableAwareChainedTransformer(Arrays.asList(transformerName.split(",")),
        Option::empty);
    assertNotNull(transformer);
  }
}
