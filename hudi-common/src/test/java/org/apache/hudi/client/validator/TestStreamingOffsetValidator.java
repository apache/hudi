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

package org.apache.hudi.client.validator;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.CheckpointUtils.CheckpointFormat;
import org.apache.hudi.exception.HoodieValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for StreamingOffsetValidator base class - Phase 1 core functionality.
 * Uses a mock validator implementation to test common validation logic.
 */
public class TestStreamingOffsetValidator {

  /**
   * Mock implementation of StreamingOffsetValidator for testing.
   */
  private static class MockOffsetValidator extends StreamingOffsetValidator {
    public MockOffsetValidator(TypedProperties config) {
      super(config, "test.checkpoint.key", CheckpointFormat.DELTASTREAMER_KAFKA);
    }

    // Expose protected method for testing
    public void testValidateOffsetConsistency(long offsetDiff, long recordsWritten,
                                                String current, String previous) {
      validateOffsetConsistency(offsetDiff, recordsWritten, current, previous);
    }
  }

  @Test
  public void testExactMatchValidation() {
    TypedProperties config = new TypedProperties();
    config.setProperty(StreamingOffsetValidator.TOLERANCE_PERCENTAGE_KEY, "0.0");
    config.setProperty(StreamingOffsetValidator.WARN_ONLY_MODE_KEY, "false");

    MockOffsetValidator validator = new MockOffsetValidator(config);

    // Exact match - should pass
    assertDoesNotThrow(() ->
        validator.testValidateOffsetConsistency(1000, 1000, "cur", "prev"));
  }

  @Test
  public void testStrictModeFailure() {
    TypedProperties config = new TypedProperties();
    config.setProperty(StreamingOffsetValidator.TOLERANCE_PERCENTAGE_KEY, "0.0");
    config.setProperty(StreamingOffsetValidator.WARN_ONLY_MODE_KEY, "false");

    MockOffsetValidator validator = new MockOffsetValidator(config);

    // Mismatch in strict mode - should fail
    assertThrows(HoodieValidationException.class, () ->
        validator.testValidateOffsetConsistency(1000, 900, "cur", "prev"));
  }

  @Test
  public void testValidationWithTolerance() {
    TypedProperties config = new TypedProperties();
    config.setProperty(StreamingOffsetValidator.TOLERANCE_PERCENTAGE_KEY, "10.0");
    config.setProperty(StreamingOffsetValidator.WARN_ONLY_MODE_KEY, "false");

    MockOffsetValidator validator = new MockOffsetValidator(config);

    // 5% deviation - within 10% tolerance - should pass
    assertDoesNotThrow(() ->
        validator.testValidateOffsetConsistency(1000, 950, "cur", "prev"));

    // 10% deviation - exactly at tolerance - should pass
    assertDoesNotThrow(() ->
        validator.testValidateOffsetConsistency(1000, 900, "cur", "prev"));

    // 15% deviation - exceeds 10% tolerance - should fail
    assertThrows(HoodieValidationException.class, () ->
        validator.testValidateOffsetConsistency(1000, 850, "cur", "prev"));
  }

  @Test
  public void testWarnOnlyMode() {
    TypedProperties config = new TypedProperties();
    config.setProperty(StreamingOffsetValidator.TOLERANCE_PERCENTAGE_KEY, "0.0");
    config.setProperty(StreamingOffsetValidator.WARN_ONLY_MODE_KEY, "true");

    MockOffsetValidator validator = new MockOffsetValidator(config);

    // Even with large deviation, warn-only mode should not throw
    assertDoesNotThrow(() ->
        validator.testValidateOffsetConsistency(1000, 500, "cur", "prev"));

    assertDoesNotThrow(() ->
        validator.testValidateOffsetConsistency(1000, 0, "cur", "prev"));
  }

  @Test
  public void testEdgeCaseZeroBoth() {
    TypedProperties config = new TypedProperties();
    config.setProperty(StreamingOffsetValidator.TOLERANCE_PERCENTAGE_KEY, "0.0");
    config.setProperty(StreamingOffsetValidator.WARN_ONLY_MODE_KEY, "false");

    MockOffsetValidator validator = new MockOffsetValidator(config);

    // Both zero - 0% deviation - should pass
    assertDoesNotThrow(() ->
        validator.testValidateOffsetConsistency(0, 0, "cur", "prev"));
  }

  @Test
  public void testEdgeCaseOneZero() {
    TypedProperties config = new TypedProperties();
    config.setProperty(StreamingOffsetValidator.TOLERANCE_PERCENTAGE_KEY, "0.0");
    config.setProperty(StreamingOffsetValidator.WARN_ONLY_MODE_KEY, "false");

    MockOffsetValidator validator = new MockOffsetValidator(config);

    // Offset is zero but records written - 100% deviation - should fail
    assertThrows(HoodieValidationException.class, () ->
        validator.testValidateOffsetConsistency(0, 1000, "cur", "prev"));

    // Offset is non-zero but no records - 100% deviation - should fail
    assertThrows(HoodieValidationException.class, () ->
        validator.testValidateOffsetConsistency(1000, 0, "cur", "prev"));
  }

  @Test
  public void testDefaultConfiguration() {
    TypedProperties config = new TypedProperties();
    // No explicit config - should use defaults

    MockOffsetValidator validator = new MockOffsetValidator(config);

    // Default tolerance is 0.0 (strict mode)
    assertDoesNotThrow(() ->
        validator.testValidateOffsetConsistency(1000, 1000, "cur", "prev"));

    assertThrows(HoodieValidationException.class, () ->
        validator.testValidateOffsetConsistency(1000, 999, "cur", "prev"));
  }

  @Test
  public void testSupportsMetadataValidation() {
    TypedProperties config = new TypedProperties();
    MockOffsetValidator validator = new MockOffsetValidator(config);

    // StreamingOffsetValidator should always support metadata validation
    assert validator.supportsMetadataValidation();
  }
}
