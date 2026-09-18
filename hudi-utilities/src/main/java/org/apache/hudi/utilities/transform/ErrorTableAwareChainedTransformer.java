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
 * Adds errorTableCorruptRecordColumn at the beginning of transformations and preserves
 * its values across transformers that drop it (e.g. custom column-projecting transformers).
 * Values are stashed before each transformer and restored via positional RDD zip if the
 * transformer dropped the column.
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
    dataset = ErrorTableUtils.addNullValueErrorTableCorruptRecordColumn(dataset);
    for (TransformerInfo transformerInfo : transformers) {
      Transformer transformer = transformerInfo.getTransformer();

      // Stash _corrupt_record values before the transformer can drop them
      Dataset<Row> corruptRecordStash = null;
      if (ErrorTableUtils.isErrorTableCorruptRecordColumnPresent(dataset)) {
        corruptRecordStash = dataset.select(new Column(ERROR_TABLE_CURRUPT_RECORD_COL_NAME));
        corruptRecordStash.cache();
        // Force materialization so the stash is computed and stored before the transformer
        // runs. Without this, both stash and transformed dataset recompute the shared
        // upstream lineage independently at zip time — if that lineage has non-deterministic
        // row ordering (e.g. shuffle/repartition), the zip silently misaligns values.
        corruptRecordStash.count();
      }

      dataset = transformer.apply(jsc, sparkSession, dataset, transformerInfo.getProperties(properties, transformers));

      if (!ErrorTableUtils.isErrorTableCorruptRecordColumnPresent(dataset)) {
        if (corruptRecordStash != null) {
          // Restore original values via positional zip — works when the transformer
          // only projects columns (row count and partition layout unchanged).
          dataset = ErrorTableUtils.restoreCorruptRecordColumn(sparkSession, dataset, corruptRecordStash);
        } else {
          dataset = ErrorTableUtils.addNullValueErrorTableCorruptRecordColumn(dataset);
        }
      }

      if (corruptRecordStash != null) {
        corruptRecordStash.unpersist();
      }
    }
    return dataset;
  }

  @Override
  public StructType transformedSchema(JavaSparkContext jsc, SparkSession sparkSession, StructType incomingStruct, TypedProperties properties) {
    return super.transformedSchema(jsc, sparkSession, incomingStruct, properties);
  }
}
