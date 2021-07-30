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

package org.apache.hudi.utilities;

import org.apache.hudi.utilities.HoodieSnapshotExporter.OutputFormatValidator;

import com.beust.jcommander.ParameterException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestHoodieSnapshotExporter {

  @ParameterizedTest
  @ValueSource(strings = {"json", "parquet", "orc", "hudi"})
  public void testValidateOutputFormatWithValidFormat(String format) {
    assertDoesNotThrow(() -> {
      new OutputFormatValidator().validate(null, format);
    });
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "JSON"})
  public void testValidateOutputFormatWithInvalidFormat(String format) {
    assertThrows(ParameterException.class, () -> {
      new OutputFormatValidator().validate(null, format);
    });
  }

  @ParameterizedTest
  @NullSource
  public void testValidateOutputFormatWithNullFormat(String format) {
    assertThrows(ParameterException.class, () -> {
      new OutputFormatValidator().validate(null, format);
    });
  }
}
