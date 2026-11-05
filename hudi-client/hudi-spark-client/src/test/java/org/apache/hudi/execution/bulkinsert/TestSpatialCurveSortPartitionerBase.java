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

package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.config.HoodieClusteringConfig.LayoutOptimizationStrategy;
import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.Collections;

public class TestSpatialCurveSortPartitionerBase {

  private static final HoodieSchema SCHEMA = HoodieSchema.createRecord("rec", null, null, Arrays.asList(
      HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
      HoodieSchemaField.of("s", HoodieSchema.createRecord("s", null, null, Collections.singletonList(
          HoodieSchemaField.of("level", HoodieSchema.create(HoodieSchemaType.INT)))))));

  @ParameterizedTest
  @EnumSource(value = LayoutOptimizationStrategy.class, names = {"ZORDER", "HILBERT"})
  void rejectsWhatTheCurveHelpersCannotLookUp(LayoutOptimizationStrategy strategy) {
    // SpaceCurveSortingHelper looks a sort column up among the frame's top-level fields by exact
    // name, so a nested path is not a column it can find, and a differently cased name is a
    // different column - both would be dropped or fail unattributably without this check.
    HoodieException nested = Assertions.assertThrows(HoodieException.class,
        () -> SpatialCurveSortPartitionerBase.validateOrderByColumns(new String[] {"s.level"}, SCHEMA, strategy));
    Assertions.assertTrue(nested.getMessage().contains("'s.level'") && nested.getMessage().contains(strategy.name()),
        "The error must name the column and the strategy, got: " + nested.getMessage());

    HoodieException cased = Assertions.assertThrows(HoodieException.class,
        () -> SpatialCurveSortPartitionerBase.validateOrderByColumns(new String[] {"ID"}, SCHEMA, strategy));
    Assertions.assertTrue(cased.getMessage().contains("'ID'"),
        "The error must name the column, got: " + cased.getMessage());
  }

  @Test
  void acceptsTopLevelColumnsIncludingMetaFields() {
    // The clustering strategy validates against the schema the curve is built over, which carries
    // the metadata fields, so a `_hoodie_*` sort column is a top-level column like any other.
    HoodieSchema withMetaFields = HoodieSchemaUtils.addMetadataFields(SCHEMA);
    Assertions.assertDoesNotThrow(() -> SpatialCurveSortPartitionerBase.validateOrderByColumns(
        new String[] {"_hoodie_commit_time", " id ", "s"}, withMetaFields, LayoutOptimizationStrategy.ZORDER));
  }

  @Test
  void noColumnsIsANoOp() {
    // The strategy only calls this when the sort columns are configured, but a partitioner built
    // straight from a write config may hold neither.
    Assertions.assertDoesNotThrow(() -> SpatialCurveSortPartitionerBase.validateOrderByColumns(
        null, SCHEMA, LayoutOptimizationStrategy.ZORDER));
    Assertions.assertDoesNotThrow(() -> SpatialCurveSortPartitionerBase.validateOrderByColumns(
        new String[0], SCHEMA, LayoutOptimizationStrategy.HILBERT));
  }
}
