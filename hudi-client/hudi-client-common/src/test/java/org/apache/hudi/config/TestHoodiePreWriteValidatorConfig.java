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

package org.apache.hudi.config;

import org.apache.hudi.common.config.TypedProperties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

import static org.apache.hudi.config.HoodiePreWriteValidatorConfig.VALIDATOR_CLASS_NAMES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for {@link HoodiePreWriteValidatorConfig}.
 */
class TestHoodiePreWriteValidatorConfig {

  @Test
  void testValidatorClassNamesConfigProperty() {
    // documentation is not null
    assertNotNull(VALIDATOR_CLASS_NAMES.doc());
    assertNotEquals("", VALIDATOR_CLASS_NAMES.doc());

    // default value is empty string
    assertEquals("", VALIDATOR_CLASS_NAMES.defaultValue());

    // config key is correct
    assertEquals("hoodie.prewrite.validators", VALIDATOR_CLASS_NAMES.key());
  }

  @Test
  void testNewBuilder() {
    HoodiePreWriteValidatorConfig.Builder builder = HoodiePreWriteValidatorConfig.newBuilder();
    assertNotNull(builder);

    // Verify builder creates a valid config
    HoodiePreWriteValidatorConfig config = builder.build();
    assertNotNull(config);
    assertNotNull(config.getProps());
  }

  @Test
  void testBuilderWithValidatorClassNames() {
    String validatorClassName = "org.apache.hudi.TestValidator";
    HoodiePreWriteValidatorConfig config = HoodiePreWriteValidatorConfig.newBuilder()
        .withPreWriteValidator(validatorClassName)
        .build();

    assertEquals(validatorClassName, config.getString(VALIDATOR_CLASS_NAMES));
  }

  @Test
  void testBuilderWithMultipleValidators() {
    String validators = "org.apache.hudi.Validator1,org.apache.hudi.Validator2";
    HoodiePreWriteValidatorConfig config = HoodiePreWriteValidatorConfig.newBuilder()
        .withPreWriteValidator(validators)
        .build();

    assertEquals(validators, config.getString(VALIDATOR_CLASS_NAMES));
  }

  @Test
  void testBuilderWithDefaultValue() {
    HoodiePreWriteValidatorConfig config = HoodiePreWriteValidatorConfig.newBuilder()
        .build();

    assertEquals("", config.getString(VALIDATOR_CLASS_NAMES));
  }

  @Test
  void testBuilderFromProperties() {
    Properties props = new Properties();
    String validatorClassName = "org.apache.hudi.TestValidator";
    props.setProperty(VALIDATOR_CLASS_NAMES.key(), validatorClassName);

    HoodiePreWriteValidatorConfig config = HoodiePreWriteValidatorConfig.newBuilder()
        .fromProperties(props)
        .build();

    assertEquals(validatorClassName, config.getString(VALIDATOR_CLASS_NAMES));
  }

  @Test
  void testBuilderFromFile(@TempDir Path tempDir) throws IOException {
    File configFile = tempDir.resolve("test-config.properties").toFile();
    Properties props = new Properties();
    String validatorClassName = "org.apache.hudi.TestValidator";
    props.setProperty(VALIDATOR_CLASS_NAMES.key(), validatorClassName);

    try (FileOutputStream fos = new FileOutputStream(configFile)) {
      props.store(fos, "Test config");
    }

    HoodiePreWriteValidatorConfig config = HoodiePreWriteValidatorConfig.newBuilder()
        .fromFile(configFile)
        .build();

    assertEquals(validatorClassName, config.getString(VALIDATOR_CLASS_NAMES));
  }

  @Test
  void testBuilderChainingFromPropertiesAndWithPreWriteValidator() {
    Properties props = new Properties();
    props.setProperty(VALIDATOR_CLASS_NAMES.key(), "org.apache.hudi.Validator1");

    String validator2 = "org.apache.hudi.Validator2";
    HoodiePreWriteValidatorConfig config = HoodiePreWriteValidatorConfig.newBuilder()
        .fromProperties(props)
        .withPreWriteValidator(validator2)
        .build();

    // withPreWriteValidator should override the value from properties
    assertEquals(validator2, config.getString(VALIDATOR_CLASS_NAMES));
  }

  @Test
  void testBuilderChainingFromFileAndWithPreWriteValidator(@TempDir Path tempDir) throws IOException {
    File configFile = tempDir.resolve("test-config.properties").toFile();
    Properties props = new Properties();
    props.setProperty(VALIDATOR_CLASS_NAMES.key(), "org.apache.hudi.Validator1");

    try (FileOutputStream fos = new FileOutputStream(configFile)) {
      props.store(fos, "Test config");
    }

    String validator2 = "org.apache.hudi.Validator2";
    HoodiePreWriteValidatorConfig config = HoodiePreWriteValidatorConfig.newBuilder()
        .fromFile(configFile)
        .withPreWriteValidator(validator2)
        .build();

    // withPreWriteValidator should override the value from file
    assertEquals(validator2, config.getString(VALIDATOR_CLASS_NAMES));
  }

  @Test
  void testConfigInTypedProperties() {
    TypedProperties props = new TypedProperties();
    String validatorClassName = "org.apache.hudi.TestValidator";
    props.setProperty(VALIDATOR_CLASS_NAMES.key(), validatorClassName);

    assertEquals(validatorClassName, props.getString(VALIDATOR_CLASS_NAMES.key(), VALIDATOR_CLASS_NAMES.defaultValue()));
  }

  @Test
  void testEmptyStringValidatorClassNames() {
    HoodiePreWriteValidatorConfig config = HoodiePreWriteValidatorConfig.newBuilder()
        .withPreWriteValidator("")
        .build();

    assertEquals("", config.getString(VALIDATOR_CLASS_NAMES));
  }

  @Test
  void testBuilderReturnsThis() {
    HoodiePreWriteValidatorConfig.Builder builder = HoodiePreWriteValidatorConfig.newBuilder();
    Properties props = new Properties();

    // Verify fromProperties returns the same builder instance (for chaining)
    HoodiePreWriteValidatorConfig.Builder result = builder.fromProperties(props);
    assertNotNull(result);

    // Verify withPreWriteValidator returns the builder (for chaining)
    result = builder.withPreWriteValidator("test.Validator");
    assertNotNull(result);
  }

  @Test
  void testConfigPropertiesInitialized() {
    HoodiePreWriteValidatorConfig config = HoodiePreWriteValidatorConfig.newBuilder().build();

    // Verify the config has properties initialized
    assertNotNull(config.getProps());

    // Verify default value is set
    assertEquals("", config.getString(VALIDATOR_CLASS_NAMES));
  }
}
