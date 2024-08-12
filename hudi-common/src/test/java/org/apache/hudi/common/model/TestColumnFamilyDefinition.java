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

package org.apache.hudi.common.model;

import org.apache.hudi.exception.HoodieValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.apache.hudi.common.model.ColumnFamilyDefinition.fromConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests hoodie write stat {@link ColumnFamilyDefinition}.
 */
public class TestColumnFamilyDefinition {

  static Stream<Object> configValueBadCases() {
    return Stream.of(
        Arguments.of(null, "id,col1,ts;ts", "Column family name must not be empty"),
        Arguments.of("", "id,col1,ts;ts", "Column family name must not be empty"),
        Arguments.of("cf", null, "Column family must contain more than 1 column"),
        Arguments.of("cf", "", "Column family must contain more than 1 column"),
        Arguments.of("cf", "id", "Column family must contain more than 1 column"),
        Arguments.of("cf", ";ts", "Column family must contain more than 1 column"),
        Arguments.of("cf", "id;ts", "Column family must contain more than 1 column"),
        Arguments.of("cf", "id,col1,,ts;ts", "Column name must not be empty"),
        Arguments.of("cf", "id,col1,ts,ts", "Family's columns must not contain duplicates"),
        Arguments.of("cf", "id,col1,col2;ts", "Family's columns must contain preCombine column: ts"),
        Arguments.of("cf", "id,col1,ts;ts,", "Family's columns must contain preCombine column: ts,"),
        Arguments.of("cf", "id,col1,ts;;ts", "Only one semicolon delimiter allowed to separate preCombine column"),
        Arguments.of("cf", "id,col1,ts;ts; ", "Only one semicolon delimiter allowed to separate preCombine column")
    );
  }

  @ParameterizedTest
  @MethodSource("configValueBadCases")
  public void testFromConfigFailure(String name, String configValue, String expectedExceptionMsg) {
    Exception ex = assertThrows(HoodieValidationException.class,
        () -> fromConfig(name, configValue)
    );
    assertEquals(expectedExceptionMsg, ex.getMessage());
  }

  @Test
  public void testFromConfigSuccess() {
    // without preCombine column
    ColumnFamilyDefinition cfd = fromConfig("cf", "id , col1, col2  ");
    assertEquals("cf", cfd.getName());
    assertEquals(3, cfd.getColumns().size());
    assertEquals("id", cfd.getColumns().get(0));
    assertEquals("col1", cfd.getColumns().get(1));
    assertEquals("col2", cfd.getColumns().get(2));
    assertNull(cfd.getPreCombine());
    // check toConfigValue()
    assertEquals("id,col1,col2", cfd.toConfigValue());
    assertEquals("id,col1,col2", fromConfig("cf", "id,col1,col2;").toConfigValue());
    assertEquals("id,col1,col2", fromConfig("cf", "id,col1,col2; ").toConfigValue());
    assertEquals("id,col1,col2", fromConfig("cf", "id,col1,col2;;").toConfigValue());
    assertEquals("id,col1,col2", fromConfig("cf", "id,col1,col2; ;").toConfigValue());

    // with preCombine column
    cfd = fromConfig("cf", " id, col1 , ts ; ts ");
    assertEquals("cf", cfd.getName());
    assertEquals(3, cfd.getColumns().size());
    assertEquals("id", cfd.getColumns().get(0));
    assertEquals("col1", cfd.getColumns().get(1));
    assertEquals("ts", cfd.getColumns().get(2));
    assertEquals("ts", cfd.getPreCombine());
    // check toConfigValue()
    assertEquals("id,col1,ts;ts", cfd.toConfigValue());
    assertEquals("id,col1,ts;ts", fromConfig("cf", "id,col1,ts;ts;").toConfigValue());
  }

}
