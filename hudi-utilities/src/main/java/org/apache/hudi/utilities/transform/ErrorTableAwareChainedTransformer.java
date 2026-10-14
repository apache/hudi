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
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.utilities.streamer.ErrorTableUtils;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.function.Supplier;

import static org.apache.hudi.utilities.streamer.BaseErrorTableWriter.ERROR_TABLE_CURRUPT_RECORD_COL_NAME;

/**
 * A {@link Transformer} to chain other {@link Transformer}s and apply sequentially.
 * Adds errorTableCorruptRecordColumn at the beginning of transformations and re-adds it as null
 * if a transformer drops it (e.g. a custom column-projecting transformer). Dropping the column is
 * rejected only if it held values, since those error records cannot be realigned with the
 * transformer output and would otherwise be written to the target table.
 */
public class ErrorTableAwareChainedTransformer extends ChainedTransformer {
  public ErrorTableAwareChainedTransformer(List<String> configuredTransformers, Supplier<Option<HoodieSchema>> sourceSchemaSupplier) {
    super(configuredTransformers, sourceSchemaSupplier);
  }

  public ErrorTableAwareChainedTransformer(List<Transformer> transformers) {
    super(transformers);
  }

  @Override
  public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset,
                            TypedProperties properties) {
    Dataset<Row> dataset = rowDataset;
    // Until the source supplies the column or a transformer keeps it, it is known to be all null.
    boolean corruptRecordMayHaveValues = ErrorTableUtils.isErrorTableCorruptRecordColumnPresent(dataset);
    dataset = ErrorTableUtils.addNullValueErrorTableCorruptRecordColumn(dataset);
    for (TransformerInfo transformerInfo : transformers) {
      Transformer transformer = transformerInfo.getTransformer();
      Dataset<Row> input = dataset;
      dataset = transformer.apply(jsc, sparkSession, input, transformerInfo.getProperties(properties, transformers));
      if (ErrorTableUtils.isErrorTableCorruptRecordColumnPresent(dataset)) {
        corruptRecordMayHaveValues = true;
        continue;
      }
      if (corruptRecordMayHaveValues && !input.filter(new Column(ERROR_TABLE_CURRUPT_RECORD_COL_NAME).isNotNull()).isEmpty()) {
        throw new HoodieValidationException(String.format("Transformer %s dropped column %s, which holds error records "
            + "marked by the source or an earlier transformer. Keep the column in its output, or apply it before the "
            + "transformers that mark error records.", transformer.getClass().getName(), ERROR_TABLE_CURRUPT_RECORD_COL_NAME));
      }
      // The column held no values, so re-adding it as null loses nothing.
      dataset = ErrorTableUtils.addNullValueErrorTableCorruptRecordColumn(dataset);
      corruptRecordMayHaveValues = false;
    }
    return dataset;
  }

  @Override
  public StructType transformedSchema(JavaSparkContext jsc, SparkSession sparkSession, StructType incomingStruct, TypedProperties properties) {
    return super.transformedSchema(jsc, sparkSession, incomingStruct, properties);
  }
}
