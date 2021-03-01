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

package org.apache.hudi.utilities.transform;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.sources.helpers.AvroKafkaSourceHelpers;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.anyString;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlQueryBasedTransformerTest {

  @Test
  void applyNoKafkaFieldsAreInjected() {
    Dataset<Row> dataset = mock(Dataset.class);
    dataset.registerTempTable(anyString());

    SparkSession sparkSession = mock(SparkSession.class);
    ArgumentCaptor<String> ac = ArgumentCaptor.forClass(String.class);

    SqlQueryBasedTransformer t = new SqlQueryBasedTransformer();
    TypedProperties props = new TypedProperties();
    props.put("hoodie.deltastreamer.transformer.sql", "sql query <SRC> <KAFKA_FIELDS>");
    props.put(AvroKafkaSourceHelpers.INJECT_KAFKA_META_FIELDS, "false");
    Dataset<Row> res = t.apply(null, sparkSession, dataset, props);
    verify(sparkSession, times(1)).sql(ac.capture());

    assertTrue(ac.getValue().startsWith("sql query HOODIE_SRC_TMP_TABLE_"));
    assertTrue(ac.getValue().endsWith(AvroKafkaSourceHelpers.KAFKA_META_FIELDS_PATTERN));
  }

  @Test
  void applyKafkaFieldsAreInjected() {
    Dataset<Row> dataset = mock(Dataset.class);
    dataset.registerTempTable(anyString());

    SparkSession sparkSession = mock(SparkSession.class);
    ArgumentCaptor<String> ac = ArgumentCaptor.forClass(String.class);

    SqlQueryBasedTransformer t = new SqlQueryBasedTransformer();
    TypedProperties props = new TypedProperties();
    props.put("hoodie.deltastreamer.transformer.sql", "sql query <SRC> <KAFKA_FIELDS>");
    props.put(AvroKafkaSourceHelpers.INJECT_KAFKA_META_FIELDS, "true");

    Dataset<Row> res = t.apply(null, sparkSession, dataset, props);
    verify(sparkSession, times(1)).sql(ac.capture());
    assertTrue(ac.getValue().startsWith("sql query HOODIE_SRC_TMP_TABLE_"));
    assertTrue(ac.getValue().endsWith(AvroKafkaSourceHelpers.ALL_KAFKA_META_FIELDS));
  }
}
