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

package org.apache.hudi.common.testutils;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Utils to generate schemas for schema on write evolution tests.
 */
public class SchemaOnWriteEvolutionTestUtils extends SchemaEvolutionTestUtilsBase {

  /**
   * generate the schema for schema on write evolution testing
   *
   * @param configs        test configs to decide which evolution cases to run
   * @param iteration      the current iteration of evolution
   * @param maxIterations  the maximum number of iterations that this test should run.
   *                       it is 0 indexed so if you evolve 3 times, maxIterations should be 2
   *
   * @return the generated schema
   */
  public static Schema generateExtendedSchema(SchemaOnWriteConfigs configs, int iteration, int maxIterations) {
    List<SchemaEvolutionField> baseFields = new ArrayList<>();
    for (int i = 0; i < maxIterations; i++) {
      for (SchemaOnWriteTypePromotionCase evolution : SchemaOnWriteTypePromotionCase.values()) {
        if (evolution.isEnabled.test(configs)) {
          if (i >= iteration) {
            baseFields.add(new SchemaEvolutionField(evolution.getFormattedName(), evolution.before, null));
          } else {
            baseFields.add(new SchemaEvolutionField(evolution.getFormattedName(), evolution.after, null));
          }
        }
      }
    }

    for (int i = 0; i < maxIterations; i++) {
      // Add new field if we are testing adding new fields
      if (configs.addNewFieldSupport && i < iteration) {
        baseFields.add(new SchemaEvolutionField("BoolAddField" + i, Schema.Type.BOOLEAN, null));
      }
    }

    return generateExtendedSchema(configs, baseFields);
  }

  public static class SchemaOnWriteConfigs extends SchemaEvolutionTestUtilsBase.SchemaEvolutionConfigBase {
    public boolean addNewFieldSupport = true;

    // String
    public boolean stringToBytesSupport = true;

    // Bytes
    public boolean bytesToStringSupport = true;
  }

  private enum SchemaOnWriteTypePromotionCase {
    INT_TO_INT(Schema.Type.INT, Schema.Type.INT, config -> true),
    INT_TO_LONG(Schema.Type.INT, Schema.Type.LONG, config -> config.intToLongSupport),
    INT_TO_FLOAT(Schema.Type.INT, Schema.Type.FLOAT, config -> config.intToFloatSupport),
    INT_TO_DOUBLE(Schema.Type.INT, Schema.Type.DOUBLE, config -> config.intToDoubleSupport),
    INT_TO_STRING(Schema.Type.INT, Schema.Type.STRING, config -> config.intToStringSupport),
    LONG_TO_LONG(Schema.Type.LONG, Schema.Type.LONG, config -> true),
    LONG_TO_FLOAT(Schema.Type.LONG, Schema.Type.FLOAT, config -> config.longToFloatSupport),
    LONG_TO_DOUBLE(Schema.Type.LONG, Schema.Type.DOUBLE, config -> config.longToDoubleSupport),
    LONG_TO_STRING(Schema.Type.LONG, Schema.Type.STRING, config -> config.longToStringSupport),
    FLOAT_TO_FLOAT(Schema.Type.FLOAT, Schema.Type.FLOAT, config -> true),
    FLOAT_TO_DOUBLE(Schema.Type.FLOAT, Schema.Type.DOUBLE, config -> config.floatToDoubleSupport),
    FLOAT_TO_STRING(Schema.Type.FLOAT, Schema.Type.STRING, config -> config.floatToStringSupport),
    DOUBLE_TO_DOUBLE(Schema.Type.DOUBLE, Schema.Type.DOUBLE, config -> true),
    DOUBLE_TO_STRING(Schema.Type.DOUBLE, Schema.Type.STRING, config -> config.doubleToStringSupport),
    STRING_TO_STRING(Schema.Type.STRING, Schema.Type.STRING, config -> true),
    STRING_TO_BYTES(Schema.Type.STRING, Schema.Type.BYTES, config -> config.stringToBytesSupport),
    BYTES_TO_BYTES(Schema.Type.BYTES, Schema.Type.BYTES, config -> true),
    BYTES_TO_STRING(Schema.Type.BYTES, Schema.Type.STRING, config -> config.bytesToStringSupport);

    public final Schema.Type before;
    public final Schema.Type after;
    public final Predicate<SchemaOnWriteConfigs> isEnabled;

    public String getFormattedName() {
      return Arrays.stream(this.name().split("_"))
          .map(word -> word.charAt(0) + word.substring(1).toLowerCase())
          .collect(Collectors.joining(""));
    }

    SchemaOnWriteTypePromotionCase(Schema.Type before, Schema.Type after, Predicate<SchemaOnWriteConfigs> isEnabled) {
      this.before = before;
      this.after = after;
      this.isEnabled = isEnabled;
    }
  }
}
