/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.adapter;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.variant.BinaryVariant;
import org.apache.flink.util.CollectionUtil;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Adapter utils.
 */
public class DataTypeAdapterTestUtils {
  public static void assertAsBinaryVariant(Object variantObject) {
    assertInstanceOf(BinaryVariant.class, variantObject, "Variant column should be a BinaryVariant");
  }

  /**
   * Compares a VARIANT column value to the canonical encoding of {@code PARSE_JSON(jsonLiteral)} in the
   * same session (Flink 2.1.1 does not support CAST/JSON_STRING on VARIANT in SQL yet).
   */
  public static void assertVariantMatchesParseJson(
      TableEnvironment tEnv, Object actual, String jsonLiteral) throws Exception {
    assertAsBinaryVariant(actual);
    Object expected = parseJsonLiteral(tEnv, jsonLiteral);
    assertArrayEquals(
        DataTypeAdapter.getVariantMetadata(expected), DataTypeAdapter.getVariantMetadata(actual));
    assertArrayEquals(
        DataTypeAdapter.getVariantValue(expected), DataTypeAdapter.getVariantValue(actual));
  }

  public static Object parseJsonLiteral(TableEnvironment tEnv, String jsonLiteral) throws Exception {
    String escaped = jsonLiteral.replace("'", "''");
    return CollectionUtil.iteratorToList(
            tEnv.executeSql("SELECT PARSE_JSON('" + escaped + "')").collect())
        .get(0)
        .getField(0);
  }
}
