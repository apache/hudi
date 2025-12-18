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

package org.apache.hudi;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.FileGroupReaderSchemaHandler;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.spark.sql.HoodieInternalRowUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import scala.Function1;

import static org.apache.hudi.common.config.HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY;
import static org.apache.spark.sql.HoodieInternalRowUtils.getCachedSchema;

/**
 * An abstract class implementing {@link HoodieReaderContext} to handle {@link InternalRow}s.
 * Subclasses need to implement {@link #getFileRecordIterator} with the reader logic.
 */
public abstract class BaseSparkInternalRowReaderContext extends HoodieReaderContext<InternalRow> {

  protected BaseSparkInternalRowReaderContext(StorageConfiguration<?> storageConfig,
                                              HoodieTableConfig tableConfig,
                                              BaseSparkInternalRecordContext recordContext) {
    super(storageConfig, tableConfig, Option.empty(), Option.empty(), recordContext);
  }

  @Override
  public Option<HoodieRecordMerger> getRecordMerger(RecordMergeMode mergeMode, String mergeStrategyId, String mergeImplClasses) {
    // TODO(HUDI-7843):
    // get rid of event time and commit time ordering. Just return Option.empty
    switch (mergeMode) {
      case EVENT_TIME_ORDERING:
        return Option.of(new DefaultSparkRecordMerger());
      case COMMIT_TIME_ORDERING:
        return Option.of(new OverwriteWithLatestSparkRecordMerger());
      case CUSTOM:
      default:
        Option<HoodieRecordMerger> recordMerger = HoodieRecordUtils.createValidRecordMerger(EngineType.SPARK, mergeImplClasses, mergeStrategyId);
        if (recordMerger.isEmpty()) {
          throw new IllegalArgumentException("No valid spark merger implementation set for `"
              + RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY + "`");
        }
        return recordMerger;
    }
  }

  /**
   * Constructs a transformation that will take a row and convert it to a new row with the given schema and adds in the values for the partition columns if they are missing in the returned row.
   * It is assumed that the `to` schema will contain the partition fields.
   * @param from the original schema
   * @param to the schema the row will be converted to
   * @param partitionFieldAndValues the partition fields and their values, if any are required by the reader
   * @return a function for transforming the row
   */
  protected UnaryOperator<InternalRow> getBootstrapProjection(HoodieSchema from, HoodieSchema to, List<Pair<String, Object>> partitionFieldAndValues) {
    Map<Integer, Object> partitionValuesByIndex = partitionFieldAndValues.stream()
        .collect(Collectors.toMap(pair -> to.getField(pair.getKey()).orElseThrow(() -> new IllegalArgumentException("Missing field: " + pair.getKey())).pos(), Pair::getRight));
    Function1<InternalRow, UnsafeRow> unsafeRowWriter =
        HoodieInternalRowUtils.getCachedUnsafeRowWriter(getCachedSchema(from.toAvroSchema()), getCachedSchema(to.toAvroSchema()), Collections.emptyMap(), partitionValuesByIndex);
    return row -> (InternalRow) unsafeRowWriter.apply(row);
  }

  @Override
  public void setSchemaHandler(FileGroupReaderSchemaHandler<InternalRow> schemaHandler) {
    super.setSchemaHandler(schemaHandler);
    // init ordering value converter: java -> engine type
    List<String> orderingFieldNames = HoodieRecordUtils.getOrderingFieldNames(getMergeMode(), tableConfig);
    HoodieSchema schema = schemaHandler.getRequiredSchema();
    ((BaseSparkInternalRecordContext) recordContext).initOrderingValueConverter(schema, orderingFieldNames);
  }
}
