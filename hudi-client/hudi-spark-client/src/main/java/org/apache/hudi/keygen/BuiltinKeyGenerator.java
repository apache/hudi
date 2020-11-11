/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.keygen;

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.AvroConversionHelper;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import scala.Function1;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base class for the built-in key generators. Contains methods structured for
 * code reuse amongst them.
 */
public abstract class BuiltinKeyGenerator extends BaseKeyGenerator implements SparkKeyGeneratorInterface {

  private static final String STRUCT_NAME = "hoodieRowTopLevelField";
  private static final String NAMESPACE = "hoodieRow";
  private transient Function1<Object, Object> converterFn = null;
  protected StructType structType;

  protected Map<String, List<Integer>> recordKeyPositions = new HashMap<>();
  protected Map<String, List<Integer>> partitionPathPositions = new HashMap<>();

  protected BuiltinKeyGenerator(TypedProperties config) {
    super(config);
  }

  /**
   * Fetch record key from {@link Row}.
   * @param row instance of {@link Row} from which record key is requested.
   * @return the record key of interest from {@link Row}.
   */
  @Override
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public String getRecordKey(Row row) {
    if (null == converterFn) {
      converterFn = AvroConversionHelper.createConverterToAvro(row.schema(), STRUCT_NAME, NAMESPACE);
    }
    GenericRecord genericRecord = (GenericRecord) converterFn.apply(row);
    return getKey(genericRecord).getRecordKey();
  }

  /**
   * Fetch partition path from {@link Row}.
   * @param row instance of {@link Row} from which partition path is requested
   * @return the partition path of interest from {@link Row}.
   */
  @Override
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public String getPartitionPath(Row row) {
    if (null == converterFn) {
      converterFn = AvroConversionHelper.createConverterToAvro(row.schema(), STRUCT_NAME, NAMESPACE);
    }
    GenericRecord genericRecord = (GenericRecord) converterFn.apply(row);
    return getKey(genericRecord).getPartitionPath();
  }

  void buildFieldPositionMapIfNeeded(StructType structType) {
    if (this.structType == null) {
      // parse simple fields
      getRecordKeyFields().stream()
          .filter(f -> !(f.contains(".")))
          .forEach(f -> {
            if (structType.getFieldIndex(f).isDefined()) {
              recordKeyPositions.put(f, Collections.singletonList((Integer) (structType.getFieldIndex(f).get())));
            } else {
              throw new HoodieKeyException("recordKey value not found for field: \"" + f + "\"");
            }
          });
      // parse nested fields
      getRecordKeyFields().stream()
          .filter(f -> f.contains("."))
          .forEach(f -> recordKeyPositions.put(f, RowKeyGeneratorHelper.getNestedFieldIndices(structType, f, true)));
      // parse simple fields
      if (getPartitionPathFields() != null) {
        getPartitionPathFields().stream().filter(f -> !f.isEmpty()).filter(f -> !(f.contains(".")))
            .forEach(f -> {
              if (structType.getFieldIndex(f).isDefined()) {
                partitionPathPositions.put(f,
                    Collections.singletonList((Integer) (structType.getFieldIndex(f).get())));
              } else {
                partitionPathPositions.put(f, Collections.singletonList(-1));
              }
            });
        // parse nested fields
        getPartitionPathFields().stream().filter(f -> !f.isEmpty()).filter(f -> f.contains("."))
            .forEach(f -> partitionPathPositions.put(f,
                RowKeyGeneratorHelper.getNestedFieldIndices(structType, f, false)));
      }
      this.structType = structType;
    }
  }
}

