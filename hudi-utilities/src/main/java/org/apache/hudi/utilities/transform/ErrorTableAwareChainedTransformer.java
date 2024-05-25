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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.streamer.ErrorTableUtils;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.function.Supplier;

/**
 * A {@link Transformer} to chain other {@link Transformer}s and apply sequentially.
 * Adds errorTableCorruptRecordColumn at the beginning of transformations and validates
 * if that column is not dropped in any of the transformations.
 */
public class ErrorTableAwareChainedTransformer extends ChainedTransformer {
  public ErrorTableAwareChainedTransformer(List<String> configuredTransformers, Supplier<Option<Schema>> sourceSchemaSupplier) {
    super(configuredTransformers, sourceSchemaSupplier);
  }

  public ErrorTableAwareChainedTransformer(List<Transformer> transformers) {
    super(transformers);
  }

  @Override
  public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset,
                            TypedProperties properties) {
    Dataset<Row> dataset = rowDataset;
    dataset = ErrorTableUtils.addNullValueErrorTableCorruptRecordColumn(dataset);
    for (TransformerInfo transformerInfo : transformers) {
      Transformer transformer = transformerInfo.getTransformer();
      dataset = transformer.apply(jsc, sparkSession, dataset, transformerInfo.getProperties(properties, transformers));
      // validate in every stage to ensure ErrorRecordColumn not dropped by one of the transformer and added by next transformer.
      ErrorTableUtils.validate(dataset);
    }
    return dataset;
  }

  @Override
  public StructType transformedSchema(JavaSparkContext jsc, SparkSession sparkSession, StructType incomingStruct, TypedProperties properties) {
    return super.transformedSchema(jsc, sparkSession, incomingStruct, properties);
  }
}
